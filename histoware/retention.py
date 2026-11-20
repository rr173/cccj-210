"""Warehouse version retirement: policy, rehearsal plans, approval.

Replay contract
---------------
A *managed* warehouse (see ``warehouse.init_retention``) carries a
retention policy: a non-decreasing waterline ``W`` of admitted warehouse
versions and a set of exceptions ``work order -> exact version``.
Version ``V`` is replayable when ``W <= V <= current``, or when some
exception names ``V`` below the waterline.  Below the waterline only
exception versions survive.  One version may be protected by several
work orders; lifting one does not lift another.  An exception can only
name a version replayable under the previous policy, so retirement is
one-way: a retired version can never be re-adopted.  Exceptions are
exact versions, never ranges.

What is evicted
---------------
For one *endpoint* (region, collector, generation, ts) the revisions
have introduction versions ``v(1), v(2), ...`` in revision order (a
late low revision can have a larger introduction version than a higher
one).  At warehouse version V the visible revision is the greatest
revision with introduction version <= V.  Revision ``i`` is selected by
V exactly when ``v_i <= V < m_i`` where ``m_i`` is the smallest
introduction version among strictly higher revisions (infinity when
there is none).  It is kept when the replay set intersects
``[v_i, m_i - 1]``, i.e. when ``v_i <= max{R in replay set : R < m_i}``.

This is the *original max-visible-revision* rule.  It is deliberately
not "delete rows whose own version is not replayable": late low
revisions and same-batch records that never became the visible maximum
are judged exactly like everything else, and the checkpoint clock plays
no part.  Retained rows keep their batch id and line number.  Eviction
allocates no warehouse version, touches no batch-ledger row and swaps
no database file.  Freed pages return to SQLite's freelist and may be
reused by later inserts; the file is not shrunk and no secure erase is
promised.

The replay set is ``{W..current} U {protected versions}``.  For each row
``max_replayable_below(m_i)`` is found by binary search over the
coalesced replay intervals, so classification is one ordered index walk
of the checkpoint table - O(rows * log(1 + exceptions)) - and never a
per-version endpoint rescan or a revision-by-version/exception matrix.
Only evicted revision keys are staged (in a file-backed TEMP table);
payloads and the key set are never materialised in process memory.

Plans and receipts
------------------
``rehearse`` reads one read-only snapshot and writes nothing to the
warehouse.  The plan is bound to the warehouse identifier, the current
version observed in that snapshot, the policy generation and the
canonical target policy, plus the eviction digest: an order-independent
SHA-256 over every evicted revision identity and canonical payload hash.
The plan id is a deterministic hash of the whole plan content (excluding
the wall-clock ``created_utc``), so the same warehouse state and target
always yield the same id and business figures.  ``approve`` re-reads
the plan, re-derives the eviction set from current state, and refuses a
plan that was tampered with, belongs to another warehouse, or was
overtaken by a load or policy change.  The policy change, row deletions,
acceptance receipts and retirement receipt commit in one transaction;
a crash or write error leaves neither a half-applied policy nor a
phantom success receipt.  Re-submitting a committed plan (even after
later loads or policies) returns the original receipt without work.
"""
from __future__ import annotations

import bisect
import hashlib
import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone

from .common import HistowareError, canonical_json, canonical_payload_bytes
from .warehouse import (META_POLICY_GEN, META_POLICY_GC, META_SCHEMA_VERSION,
                        META_WAREHOUSE_ID, META_WATERMARK,
                        RETENTION_SCHEMA_VERSION, connect_warehouse,
                        get_current_version, get_meta, is_managed,
                        read_policy)

PLAN_SCHEMA_VERSION = "1"
PAYLOAD_ENCODING = "histoware-canonical-json-v1"

# Testing seam: called at named points inside approval.  `None` in
# production.  Tests use it to abort before commit (including os._exit
# in a subprocess) and verify that nothing partial is observable.
_TEST_HOOK = None


class RetentionError(HistowareError):
    """A retention request is invalid or cannot be applied."""


# ---------------------------------------------------------------------------
# Replay sets as coalesced intervals
# ---------------------------------------------------------------------------

def replay_intervals(watermark, exceptions, current):
    """Sorted, disjoint, coalesced intervals of replayable versions."""
    if watermark > current:
        return []
    raw = [(watermark, current)]
    for ex in exceptions:
        v = ex["version"]
        if v < watermark:
            raw.append((v, v))
    raw.sort()
    out = []
    for lo, hi in raw:
        if out and lo <= out[-1][1] + 1:
            out[-1] = (out[-1][0], max(out[-1][1], hi))
        else:
            out.append((lo, hi))
    return out


def max_replayable_below(starts, intervals, m):
    """Largest replayable version strictly below ``m``, or None."""
    idx = bisect.bisect_left(starts, m) - 1
    if idx < 0:
        return None
    lo, hi = intervals[idx]
    return hi if hi < m else m - 1


def is_replayable(intervals, version):
    for lo, hi in intervals:
        if lo <= version <= hi:
            return True
        if lo > version:
            return False
    return False


def subtract_intervals(a, b):
    """Intervals in ``a`` not covered by ``b``."""
    out = []
    for lo, hi in a:
        pieces = [(lo, hi)]
        for blo, bhi in b:
            nxt = []
            for plo, phi in pieces:
                if bhi < plo or blo > phi:
                    nxt.append((plo, phi))
                    continue
                if plo < blo:
                    nxt.append((plo, blo - 1))
                if phi > bhi:
                    nxt.append((bhi + 1, phi))
            pieces = nxt
        out.extend(pieces)
    return out


def newly_retired(previous, target, current):
    """Versions (>= 1) that lose replay eligibility when moving from the
    ``previous`` policy to ``target``.  A version already retired earlier
    is not re-reported, and the pseudo-version 0 (the empty warehouse)
    is never reported."""
    if current < 1:
        return []
    return subtract_intervals(
        [(max(lo, 1), hi) for lo, hi in
         replay_intervals(previous["watermark"], previous["exceptions"],
                          current) if hi >= 1],
        replay_intervals(target["watermark"], target["exceptions"],
                         current))


def intervals_json(intervals):
    return [[lo, hi] for lo, hi in intervals]


# ---------------------------------------------------------------------------
# Target policy validation
# ---------------------------------------------------------------------------

def _normalise_target(target, current, previous):
    """Validate the complete target policy and return its canonical form.

    ``target`` is ``{"watermark": int, "exceptions": [{"work_order": str,
    "version": int}, ...]}``.  Both keys are required: the exceptions list
    is the complete replacement set.  The watermark may not move
    backwards, may not exceed the current version, and exception work
    order names must be unique and non-empty.  An exception may only name
    a version replayable under the previous policy.
    """
    if not isinstance(target, dict):
        raise RetentionError("target policy must be a JSON object")
    extra = set(target) - {"watermark", "exceptions"}
    if extra:
        raise RetentionError(
            "unexpected target fields: " + ", ".join(sorted(extra)))
    if "watermark" not in target:
        raise RetentionError("target policy must carry a 'watermark'")
    if "exceptions" not in target:
        raise RetentionError(
            "target policy must carry the complete 'exceptions' list")
    wm_in = target["watermark"]
    if type(wm_in) is not int or wm_in < 0:
        raise RetentionError("watermark must be a non-negative integer")
    if wm_in < previous["watermark"]:
        raise RetentionError(
            f"watermark {wm_in} would move backwards from"
            f" {previous['watermark']}")
    if wm_in > current:
        raise RetentionError(
            f"watermark {wm_in} is beyond the current warehouse version"
            f" {current}")
    watermark = wm_in

    raw = target["exceptions"]
    if not isinstance(raw, list):
        raise RetentionError("'exceptions' must be a list")
    previous_intervals = replay_intervals(
        previous["watermark"], previous["exceptions"], current)
    seen = set()
    parsed = []
    for item in raw:
        if isinstance(item, dict):
            extra = set(item) - {"work_order", "version"}
            if extra:
                raise RetentionError(
                    "unexpected exception fields: "
                    + ", ".join(sorted(extra)))
            name, version = item.get("work_order"), item.get("version")
        elif isinstance(item, (list, tuple)) and len(item) == 2:
            name, version = item
        else:
            raise RetentionError(
                "each exception must be {work_order, version}")
        if type(name) is not str or name.strip() == "":
            raise RetentionError(
                "exception work order names must be non-empty strings")
        if type(version) is not int or version < 0:
            raise RetentionError(
                f"exception for work order {name!r} must name a"
                f" non-negative integer version")
        if version > current:
            raise RetentionError(
                f"exception for work order {name!r} names version {version}"
                f" beyond the current warehouse version {current}")
        if name in seen:
            raise RetentionError(
                f"duplicate exception for work order {name!r}")
        if not is_replayable(previous_intervals, version):
            raise RetentionError(
                f"exception for work order {name!r} names version {version},"
                f" which is not replayable under the current policy;"
                f" retired versions cannot be re-adopted")
        seen.add(name)
        parsed.append({"work_order": name, "version": version})
    parsed.sort(key=lambda e: (e["version"], e["work_order"]))
    return {"watermark": watermark, "exceptions": parsed}


def _policy_gc(exceptions):
    """Stored policy: work order -> version.  Work order names are unique;
    one version may be protected by several work orders and stays
    protected while any of them remains."""
    return {ex["work_order"]: ex["version"] for ex in exceptions}


def _load_target_file(path):
    try:
        with open(path, "rb") as fh:
            data = json.load(fh)
    except OSError as exc:
        raise RetentionError(f"cannot read target policy: {exc}") from exc
    except ValueError as exc:
        raise RetentionError(f"invalid target policy JSON: {exc}") from exc
    return data


# ---------------------------------------------------------------------------
# One-pass eviction classification
# ---------------------------------------------------------------------------

# One ordered index walk over the checkpoint primary key, in descending
# revision order within each endpoint.  That order is satisfied natively
# by a reverse scan of the primary-key index, so no sort is needed.  The
# scan maintains, per endpoint, the minimum introduction version among
# strictly higher revisions (the cutoff m_i); it is O(rows) independent
# of the number of versions, endpoints or exceptions.
_SCAN_SQL = """
SELECT rowid, region, collector, generation, ts, revision, version,
       cuts, counts, total, batch_id, line_no
FROM checkpoint
ORDER BY region DESC, collector DESC, generation DESC, ts DESC,
         revision DESC
"""

_WORK_DDL = (
    "CREATE TEMP TABLE IF NOT EXISTS retention_evict("
    " rowid INTEGER PRIMARY KEY,"
    " region TEXT NOT NULL, collector TEXT NOT NULL,"
    " generation TEXT NOT NULL, ts INTEGER NOT NULL,"
    " revision INTEGER NOT NULL, version INTEGER NOT NULL,"
    " batch_id TEXT NOT NULL, line_no INTEGER NOT NULL,"
    " content_sha256 TEXT NOT NULL, payload_bytes INTEGER NOT NULL,"
    " entry_hash TEXT NOT NULL)")

_WORK_INSERT = (
    "INSERT INTO retention_evict(rowid, region, collector, generation, ts,"
    " revision, version, batch_id, line_no, content_sha256, payload_bytes,"
    " entry_hash) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)")


class Analysis:
    """Aggregates of one classification pass; payloads never retained."""

    __slots__ = ("rows", "points", "kept_rows", "kept_bytes",
                 "evicted_rows", "evicted_bytes", "digest", "passes")

    def __init__(self):
        self.rows = 0
        self.points = 0
        self.kept_rows = 0
        self.kept_bytes = 0
        self.evicted_rows = 0
        self.evicted_bytes = 0
        self.digest = None
        self.passes = 0

    def stats(self):
        return {
            "classification_passes": self.passes,
            "revisions_examined": self.rows,
            "endpoints_examined": self.points,
        }


def _entry_hash(region, collector, generation, ts, revision, version,
                batch_id, line_no, content_sha256):
    raw = "\x1f".join([
        region, collector, generation, str(ts), str(revision), str(version),
        batch_id, str(line_no), content_sha256])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _classify(conn, watermark, exceptions, current):
    """Classify every checkpoint row once; return an :class:`Analysis`.

    Kept rows contribute their canonical payload length.  Evicted rows
    contribute their canonical payload length and content hash and an
    entry in the file-backed ``retention_evict`` TEMP table.  The
    eviction digest is a SHA-256 over the sorted entry hashes, so it
    identifies the exact removal set independently of scan order; the
    sort lives in the temp store, never in Python memory.
    ``stats()["classification_passes"]`` is 1 no matter how many versions,
    endpoints or exceptions the policy carries.
    """
    intervals = replay_intervals(watermark, exceptions, current)
    starts = [lo for lo, _ in intervals]
    conn.execute(_WORK_DDL)
    conn.execute("DELETE FROM retention_evict")
    analysis = Analysis()
    analysis.passes = 1
    buf = []
    cur_point = None
    higher_min = None            # min intro version among higher revisions
    for (rowid, region, collector, generation, ts, revision, version,
         cuts, counts, total, batch_id, line_no) in conn.execute(_SCAN_SQL):
        point = (region, collector, generation, ts)
        if point != cur_point:
            analysis.points += 1
            cur_point = point
            higher_min = None
        analysis.rows += 1
        # Cutoff for this row: the smallest introduction version among
        # all strictly higher revisions (already seen, descending order).
        cutoff = higher_min
        if higher_min is None or version < higher_min:
            higher_min = version
        if cutoff is None:
            keep = True
        else:
            f = max_replayable_below(starts, intervals, cutoff)
            keep = f is not None and version <= f
        payload = canonical_payload_bytes(
            region, collector, generation, ts, revision,
            cuts, counts, total)
        nbytes = len(payload)
        if keep:
            analysis.kept_rows += 1
            analysis.kept_bytes += nbytes
            continue
        sha = hashlib.sha256(payload).hexdigest()
        eh = _entry_hash(region, collector, generation, ts, revision,
                         version, batch_id, line_no, sha)
        analysis.evicted_rows += 1
        analysis.evicted_bytes += nbytes
        buf.append((rowid, region, collector, generation, ts, revision,
                    version, batch_id, line_no, sha, nbytes, eh))
        if len(buf) >= 2000:
            conn.executemany(_WORK_INSERT, buf)
            buf.clear()
    if buf:
        conn.executemany(_WORK_INSERT, buf)
        buf.clear()
    digest = hashlib.sha256()
    for (eh,) in conn.execute(
            "SELECT entry_hash FROM retention_evict ORDER BY entry_hash"):
        digest.update(eh.encode("ascii"))
        digest.update(b"\n")
    analysis.digest = digest.hexdigest()
    return analysis


# ---------------------------------------------------------------------------
# Plans
# ---------------------------------------------------------------------------

def _plan_body(warehouse_id, current, policy_generation, target, retired,
               analysis, replay):
    return {
        "schema_version": PLAN_SCHEMA_VERSION,
        "warehouse_id": warehouse_id,
        "base_current_version": current,
        "base_policy_generation": policy_generation,
        "target": target,
        "newly_retired_versions": intervals_json(retired),
        "replay_intervals_after": intervals_json(replay),
        "retained_payload_rows": analysis.kept_rows,
        "retained_payload_bytes": analysis.kept_bytes,
        "evicted_payload_rows": analysis.evicted_rows,
        "evicted_payload_bytes": analysis.evicted_bytes,
        "eviction_digest": analysis.digest,
        "payload_encoding": PAYLOAD_ENCODING,
        "stats": analysis.stats(),
    }


def plan_id_for(body):
    return hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()


def _utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _rollback_quiet(conn):
    try:
        conn.execute("ROLLBACK")
    except sqlite3.Error:
        pass


def init_retention(db_path):
    """Explicitly bring a warehouse under retention management.

    Idempotent: an already managed warehouse keeps its identifier,
    watermark, exceptions and policy generation untouched.  Never
    deletes checkpoint data.
    """
    conn = connect_warehouse(db_path, create=True)
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            if get_meta(conn, META_WAREHOUSE_ID) is None:
                conn.executemany(
                    "INSERT INTO meta(key, value) VALUES (?, ?)"
                    " ON CONFLICT(key) DO NOTHING",
                    [(META_WAREHOUSE_ID, str(uuid.uuid4())),
                     (META_WATERMARK, "0"),
                     (META_POLICY_GC, "{}"),
                     (META_POLICY_GEN, "0"),
                     (META_SCHEMA_VERSION, RETENTION_SCHEMA_VERSION)])
                created = True
            else:
                created = False
            conn.execute("COMMIT")
        except BaseException:
            _rollback_quiet(conn)
            raise
        policy = read_policy(conn)
        return {
            "warehouse_id": policy["warehouse_id"],
            "created": created,
            "managed": True,
            "watermark": policy["watermark"],
            "exceptions": policy["exceptions"],
            "policy_generation": policy["policy_generation"],
            "current_version": get_current_version(conn),
        }
    finally:
        conn.close()


def _open_read_snapshot(db_path):
    if not os.path.exists(db_path):
        raise HistowareError(f"warehouse not found: {db_path}")
    uri = "file:" + db_path.replace("?", "%3f") + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, isolation_level=None)
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA temp_store = 1")
    conn.execute("BEGIN")
    return conn


def rehearse(db_path, target):
    """Compute a plan from a read-only snapshot; never writes the warehouse."""
    if isinstance(target, str):
        target = _load_target_file(target)
    conn = _open_read_snapshot(db_path)
    try:
        if not is_managed(conn):
            raise RetentionError(
                "warehouse is not under retention management; run"
                " 'retire init' first")
        current = get_current_version(conn)
        policy = read_policy(conn)
        normalised = _normalise_target(target, current, policy)
        replay = replay_intervals(normalised["watermark"],
                                  normalised["exceptions"], current)
        retired = newly_retired(policy, normalised, current)
        analysis = _classify(conn, normalised["watermark"],
                             normalised["exceptions"], current)
        conn.execute("ROLLBACK")
        body = _plan_body(policy["warehouse_id"], current,
                          policy["policy_generation"], normalised, retired,
                          analysis, replay)
        plan_id = plan_id_for(body)
        plan = dict(body)
        plan["plan_id"] = plan_id
        plan["created_utc"] = _utcnow()
        return plan
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Approval
# ---------------------------------------------------------------------------

_PLAN_META_KEYS = {"plan_id", "created_utc"}


def _receipt_dict(row):
    (receipt_id, plan_id, warehouse_id, base_version, evicted_rows,
     evicted_bytes, policy_generation, created_utc, detail_json) = row
    return {
        "receipt_id": receipt_id,
        "kind": "retirement",
        "plan_id": plan_id,
        "warehouse_id": warehouse_id,
        "base_version": base_version,
        "evicted_rows": evicted_rows,
        "evicted_payload_bytes": evicted_bytes,
        "policy_generation": policy_generation,
        "created_utc": created_utc,
        "detail": json.loads(detail_json) if detail_json else {},
    }


_RECEIPT_SQL = (
    "SELECT id, plan_id, warehouse_id, base_version, evicted_rows,"
    " evicted_bytes, policy_generation, created_utc, detail_json"
    " FROM receipt WHERE kind = 'retirement'")


def _find_retirement_receipt(conn, plan_id):
    return conn.execute(_RECEIPT_SQL + " AND plan_id = ?",
                        (plan_id,)).fetchone()


def _load_plan_arg(plan):
    """Accept a plan dict, a plan-file path, or a JSON string."""
    if isinstance(plan, dict):
        return plan
    if isinstance(plan, str) and os.path.exists(plan):
        try:
            with open(plan, "rb") as fh:
                return json.load(fh)
        except ValueError as exc:
            raise RetentionError(f"invalid plan JSON: {exc}") from exc
        except OSError as exc:
            raise RetentionError(f"cannot read plan: {exc}") from exc
    if isinstance(plan, str):
        try:
            return json.loads(plan)
        except ValueError as exc:
            raise RetentionError(f"invalid plan JSON: {exc}") from exc
    raise RetentionError("plan must be a JSON object or file path")


def approve(db_path, plan):
    """Re-verify a rehearsal plan and apply it atomically."""
    plan = _load_plan_arg(plan)
    if not isinstance(plan, dict):
        raise RetentionError("plan must be a JSON object")
    claimed_id = plan.get("plan_id")
    if not isinstance(claimed_id, str) or len(claimed_id) != 64:
        raise RetentionError("plan is missing its plan_id")
    body = {k: v for k, v in plan.items() if k not in _PLAN_META_KEYS}
    if plan_id_for(body) != claimed_id:
        raise RetentionError(
            "plan content does not match its plan_id; the plan was modified"
            " or is not a histoware plan")
    required = ("schema_version", "warehouse_id", "base_current_version",
                "base_policy_generation", "target",
                "newly_retired_versions", "replay_intervals_after",
                "retained_payload_rows", "retained_payload_bytes",
                "evicted_payload_rows", "evicted_payload_bytes",
                "eviction_digest", "payload_encoding", "stats")
    for key in required:
        if key not in body:
            raise RetentionError(f"plan is missing {key!r}")
    if body["schema_version"] != PLAN_SCHEMA_VERSION:
        raise RetentionError(
            f"unsupported plan schema {body['schema_version']!r}")
    if body["payload_encoding"] != PAYLOAD_ENCODING:
        raise RetentionError(
            f"unsupported payload encoding {body['payload_encoding']!r}")

    conn = connect_warehouse(db_path, create=False)
    try:
        conn.execute("PRAGMA temp_store = 1")
        conn.execute("BEGIN IMMEDIATE")
        try:
            # A committed plan returns its original receipt, even after
            # later loads or policy changes; it never re-executes.
            row = _find_retirement_receipt(conn, claimed_id)
            if row is not None:
                conn.execute("ROLLBACK")
                return {"reused": True, "receipt": _receipt_dict(row),
                        "evicted_rows": 0, "evicted_payload_bytes": 0,
                        "policy_generation": row[6]}

            if not is_managed(conn):
                raise RetentionError(
                    "warehouse is not under retention management; run"
                    " 'retire init' first")
            current = get_current_version(conn)
            policy = read_policy(conn)
            if body["warehouse_id"] != policy["warehouse_id"]:
                raise RetentionError(
                    "plan belongs to a different warehouse")
            if body["base_current_version"] != current:
                raise RetentionError(
                    f"plan is stale: it was rehearsed at warehouse version"
                    f" {body['base_current_version']} but the current"
                    f" version is {current}")
            if body["base_policy_generation"] != policy["policy_generation"]:
                raise RetentionError(
                    f"plan is stale: it was rehearsed under policy"
                    f" generation {body['base_policy_generation']} but the"
                    f" current generation is {policy['policy_generation']}")

            normalised = _normalise_target(body["target"], current, policy)
            if normalised != body["target"]:
                raise RetentionError(
                    "plan target policy is not in canonical form")
            replay = replay_intervals(normalised["watermark"],
                                      normalised["exceptions"], current)
            retired = newly_retired(policy, normalised, current)

            analysis = _classify(conn, normalised["watermark"],
                                 normalised["exceptions"], current)
            expected = {
                "newly_retired_versions": intervals_json(retired),
                "replay_intervals_after": intervals_json(replay),
                "retained_payload_rows": analysis.kept_rows,
                "retained_payload_bytes": analysis.kept_bytes,
                "evicted_payload_rows": analysis.evicted_rows,
                "evicted_payload_bytes": analysis.evicted_bytes,
                "eviction_digest": analysis.digest,
                "stats": analysis.stats(),
            }
            mismatches = [key for key, value in expected.items()
                          if body[key] != value]
            if mismatches:
                raise RetentionError(
                    "plan does not match the current warehouse state or"
                    " revision set (" + ", ".join(mismatches) + ");"
                    " re-run rehearsal")

            if _TEST_HOOK is not None:
                _TEST_HOOK("after_analysis", analysis)

            freelist_before = conn.execute(
                "PRAGMA main.freelist_count").fetchone()[0]
            deleted = conn.execute(
                "DELETE FROM checkpoint WHERE rowid IN ("
                "SELECT rowid FROM retention_evict)").rowcount
            if deleted != analysis.evicted_rows:
                raise RetentionError(
                    f"internal mismatch: classified {analysis.evicted_rows}"
                    f" evicted revisions but deleted {deleted}")
            # Minimal acceptance credentials: key, first version and
            # provenance, canonical content digest.  No buckets, counts or
            # totals are copied out of the evicted rows.
            conn.execute(
                "INSERT INTO receipt(kind, warehouse_id, region, collector,"
                " generation, ts, revision, first_batch, first_line,"
                " first_version, content_sha256, content_bytes, created_utc)"
                " SELECT 'content', ?, region, collector, generation, ts,"
                "        revision, batch_id, line_no, version,"
                "        content_sha256, payload_bytes, ?"
                " FROM retention_evict",
                (policy["warehouse_id"], _utcnow()))

            if _TEST_HOOK is not None:
                _TEST_HOOK("before_policy", analysis)

            policy_changed = (
                normalised["watermark"] != policy["watermark"]
                or _policy_gc(normalised["exceptions"])
                != _policy_gc(policy["exceptions"]))
            generation = policy["policy_generation"] + (1 if policy_changed
                                                         else 0)
            new_meta = [
                (META_WATERMARK, str(normalised["watermark"])),
                (META_POLICY_GC,
                 canonical_json(_policy_gc(normalised["exceptions"]))),
                (META_POLICY_GEN, str(generation)),
                (META_SCHEMA_VERSION, RETENTION_SCHEMA_VERSION)]
            conn.executemany(
                "INSERT INTO meta(key, value) VALUES(?, ?)"
                " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                new_meta)

            freelist_after = conn.execute(
                "PRAGMA main.freelist_count").fetchone()[0]
            base_version = current
            detail = {
                "newly_retired_versions": intervals_json(retired),
                "replay_intervals_after": intervals_json(replay),
                "retained_payload_rows": analysis.kept_rows,
                "retained_payload_bytes": analysis.kept_bytes,
                "eviction_digest": analysis.digest,
                "target": normalised,
                "stats": analysis.stats(),
                "freelist_pages_before": freelist_before,
                "freelist_pages_after": freelist_after,
                "payload_encoding": PAYLOAD_ENCODING,
                "note": "freed pages are reusable by later inserts; the"
                        " SQLite file is not shrunk and no secure erase is"
                        " performed",
            }
            created = _utcnow()
            cur = conn.execute(
                "INSERT INTO receipt(kind, warehouse_id, plan_id,"
                " base_version, evicted_rows, evicted_bytes,"
                " policy_generation, detail_json, created_utc)"
                " VALUES ('retirement', ?, ?, ?, ?, ?, ?, ?, ?)",
                (policy["warehouse_id"], claimed_id, base_version,
                 analysis.evicted_rows, analysis.evicted_bytes, generation,
                 canonical_json(detail), created))
            receipt_id = cur.lastrowid

            if _TEST_HOOK is not None:
                _TEST_HOOK("before_commit", analysis)

            conn.execute("COMMIT")
        except BaseException:
            _rollback_quiet(conn)
            raise
        return {
            "reused": False,
            "receipt": {
                "receipt_id": receipt_id,
                "kind": "retirement",
                "plan_id": claimed_id,
                "warehouse_id": policy["warehouse_id"],
                "base_version": base_version,
                "evicted_rows": analysis.evicted_rows,
                "evicted_payload_bytes": analysis.evicted_bytes,
                "policy_generation": generation,
                "created_utc": created,
                "detail": detail,
            },
            "evicted_rows": analysis.evicted_rows,
            "evicted_payload_bytes": analysis.evicted_bytes,
            "policy_generation": generation,
        }
    finally:
        conn.close()


def get_status(db_path):
    conn = connect_warehouse(db_path, create=False)
    try:
        policy = read_policy(conn)
        current = get_current_version(conn)
        out = {
            "managed": policy["managed"],
            "warehouse_id": policy["warehouse_id"],
            "current_version": current,
            "watermark": policy["watermark"],
            "exceptions": policy["exceptions"],
            "policy_generation": policy["policy_generation"],
        }
        if policy["managed"]:
            intervals = replay_intervals(
                policy["watermark"], policy["exceptions"], current)
            out["replay_intervals"] = intervals_json(intervals)
            out["retired_versions"] = intervals_json(
                subtract_intervals([(1, current)], intervals)
                if current >= 1 else [])
            out["receipts"] = [
                _receipt_dict(row) for row in conn.execute(
                    _RECEIPT_SQL + " ORDER BY id")]
            out["evicted_content_receipts"] = conn.execute(
                "SELECT COUNT(*) FROM receipt WHERE kind = 'content'"
            ).fetchone()[0]
        return out
    finally:
        conn.close()


def get_receipt(db_path, plan_id):
    conn = connect_warehouse(db_path, create=False)
    try:
        row = _find_retirement_receipt(conn, plan_id)
        if row is None:
            raise RetentionError(
                f"no retirement receipt for plan {plan_id!r}")
        return _receipt_dict(row)
    finally:
        conn.close()
