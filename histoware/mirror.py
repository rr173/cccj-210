"""Checkpoint mirroring by file handoff: central export, offline field
receive, and an independent SQLite query interface.

Purpose
-------
Duty machines in the delivery regions can lose contact with the central
latency warehouse for hours, yet field troubleshooting still needs the
most recently synchronised cumulative checkpoint of every probe, with
its first provenance (load version, batch, line).  Copying the whole
managed warehouse would carry other regions, historical revisions and
the retention ledger, and a copied shift report cannot be *advanced*
from a confirmed position.  Mirroring instead ships the warehouse's
*selected rows* — the revision each version would choose for every
endpoint — as self-contained package files: the centre exports, the
field receives offline into an independent SQLite mirror and queries it
without any contact with the centre.  A mirror database is a query
copy only: it cannot act as the warehouse for ``load`` or ``retire``,
and the source warehouse stays the single authoritative writer under
its identifier (no two-master merging).

Subscription
------------
A mirror subscribes either to **all** regions (regions appearing later
are included automatically) or to one explicit non-empty set of region
names; region lists are compared as sets.  The subscription is part of
every package's identity and is fixed per mirror database.

Package format (``histoware-mirror-package``, format version 1)
---------------------------------------------------------------
A package is UTF-8 JSON Lines, one canonical JSON object per line
(sorted keys, compact separators, ASCII-escaped):

* line 1 — ``header``: format, format version, kind (``base`` /
  ``delta``), source warehouse id, subscription, and the snapshot
  version (base) or the [base_version, end_version] interval (delta);
* ``row`` records (base only): the complete selected row of every
  subscribed endpoint at the snapshot version — unaggregated cuts,
  cumulative counts, total, revision and first provenance
  (``first_version`` / ``first_batch`` / ``first_line``) — strictly
  ordered by endpoint key;
* ``unit`` + ``change`` records (delta only): one ``unit`` per commit
  version in (base, end], in order and with no gaps (a version with no
  subscribed change still gets its unit), each naming its warehouse
  batch and change count, followed by exactly that many ``change``
  records, strictly ordered by endpoint key.  A change is an ``appear``
  (``old_revision`` null) or a ``replace`` (expected replaced revision
  in ``old_revision``) and carries the new row's full payload and first
  provenance, which must point at the unit's own version and batch;
* ``trailer`` — the explicit end boundary: the record count, per-kind
  totals and a SHA-256 over every preceding byte (header and data).

Counts and totals stay exact decimal integers end to end (never floats,
never truncated).  A package contains no machine paths or wall-clock
stamps: the same source view, interval and subscription re-export the
same logical package, byte for byte.

What counts as a change
-----------------------
The selected revision of an endpoint at version V is the greatest
revision whose introducing batch version is <= V.  A commit unit has
exactly one change for an endpoint iff that selected revision differs
before and after the unit's commit; several endpoints changing in one
unit produce one change each.  Two revisions of one endpoint arriving
in the same batch publish only the final selected one; a late lower
revision in a later batch is not a change; but two selections in two
successive batches produce both changes.

Export eligibility
------------------
Exports run on one read-only snapshot of a *managed* warehouse (the
identifier comes from ``retire init``; uninitialised warehouses get a
clear error) and never change the retention policy.  A base export
needs its version replayable (version 0, the empty warehouse, only when
replayable).  A delta export needs the whole closed interval [B, E]
replayable; the first non-replayable version is reported as the gap —
continuity is never faked from newer rows or endpoint end states, and
no shadow copy of evicted payloads is kept to make a delta possible.
Commits or evictions landing mid-export are invisible to the snapshot:
the package reflects exactly the original view.  A failed export never
replaces the target file (temporary file + atomic rename).

Receive
-------
The reader rejects truncated, corrupt or illegal packages: bad records,
duplicate endpoint keys inside one commit unit, missing or out-of-order
commit units, count or digest mismatches, content after the trailer.
A delta package only attaches exactly at the mirror's current cursor;
a different source warehouse, a different subscription, an
``expected-absent``/``expected-old-revision`` mismatch, or an
overlapping/gapped range is never silently accepted or partially
spliced.  Mirror rows, the cursor and the package receipt commit in one
transaction, so a multi-version package is never half-visible, and a
receiver killed mid-way leaves neither partial rows nor a success
receipt.  Re-sending a package already received in the current
generation returns its existing receipt, even after later progress;
receipts of past generations stay queryable for audit but cannot
authorise anything in the new generation.  Rebuilding requires an
explicit base package from the same source and subscription with a
version newer than the cursor, and replaces the endpoint set wholesale
(two snapshots are never merged).
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import datetime, timezone

from .common import (HistowareError, LineError, MAX_INT64,
                     RetiredVersionError, canonical_json,
                     checkpoint_from_values, load_json_object)
from .retention import is_replayable, replay_intervals
from .warehouse import get_current_version, get_meta, read_policy

PACKAGE_FORMAT = "histoware-mirror-package"
PACKAGE_FORMAT_VERSION = 1
MIRROR_SCHEMA_VERSION = "1"

MIRROR_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
-- The currently selected row of every mirrored endpoint.  Counts and
-- totals are decimal TEXT (they may exceed 2^63-1).  first_* is the
-- first provenance of the selected revision: the warehouse version,
-- batch id and line number that introduced it.
CREATE TABLE IF NOT EXISTS mirror_row(
  region        TEXT NOT NULL,
  collector     TEXT NOT NULL,
  generation    TEXT NOT NULL,
  ts            INTEGER NOT NULL,
  revision      INTEGER NOT NULL,
  cuts          TEXT NOT NULL,
  counts        TEXT NOT NULL,
  total         TEXT NOT NULL,
  first_version INTEGER NOT NULL,
  first_batch   TEXT NOT NULL,
  first_line    INTEGER NOT NULL,
  PRIMARY KEY(region, collector, generation, ts)
);
-- One row per accepted package.  Receipts of past generations are kept
-- for audit; only the current generation's receipts authorise an
-- idempotent re-send.
CREATE TABLE IF NOT EXISTS receipt(
  id              INTEGER PRIMARY KEY,
  generation      INTEGER NOT NULL,
  kind            TEXT NOT NULL CHECK(kind IN ('base','delta')),
  warehouse_id    TEXT NOT NULL,
  subscription    TEXT NOT NULL,
  base_version    INTEGER NOT NULL,
  end_version     INTEGER NOT NULL,
  package_sha256  TEXT NOT NULL,
  package_records INTEGER NOT NULL,
  changes         INTEGER NOT NULL,
  received_utc    TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_receipt_generation_package
  ON receipt(generation, package_sha256);
"""

META_MIRROR_SCHEMA = "mirror_schema_version"
META_GENERATION = "generation"
META_WAREHOUSE_ID = "warehouse_id"
META_SUBSCRIPTION = "subscription"
META_CURSOR = "cursor"

# Testing seam: called at named points inside receive (None in
# production).  Tests use it to abort before commit (including
# os._exit in a subprocess) and verify that nothing partial is visible.
_TEST_HOOK = None


class MirrorError(HistowareError):
    """An export or receive request is invalid or cannot be applied."""


class PackageError(HistowareError):
    """A mirror package file is truncated, corrupt or illegal."""


# ---------------------------------------------------------------------------
# Subscriptions
# ---------------------------------------------------------------------------

def canonical_subscription(regions):
    """Canonical subscription: ``{"regions": "all"}`` for a full
    subscription (regions appearing later included) or
    ``{"regions": [...]}`` with the sorted unique names of an explicit
    non-empty set.  Region lists are compared as sets."""
    if regions is None:
        return {"regions": "all"}
    names = list(regions)
    if not names:
        raise MirrorError(
            "an explicit subscription must name at least one region")
    for name in names:
        if type(name) is not str or name == "":
            raise MirrorError(
                "subscription regions must be non-empty strings")
    return {"regions": sorted(set(names))}


def parse_regions_arg(text):
    """CLI form: ``all`` for a full subscription, else a comma-separated
    list of region names (compared as a set)."""
    if text == "all":
        return None
    names = [part.strip() for part in text.split(",")]
    if not names or any(not name for name in names):
        raise MirrorError(
            "--regions must be 'all' or a comma-separated list of"
            " non-empty region names")
    return names


def _region_filter(region_list):
    """SQL fragment + parameters restricting to the subscribed regions."""
    if region_list is None:
        return "", []
    marks = ",".join("?" for _ in region_list)
    return f" AND region IN ({marks})", list(region_list)


# ---------------------------------------------------------------------------
# Small shared helpers
# ---------------------------------------------------------------------------

def _utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _rollback_quiet(conn):
    try:
        conn.execute("ROLLBACK")
    except sqlite3.Error:
        pass


def _retry_locked(fn, attempts=200, delay=0.01):
    import time
    for i in range(attempts):
        try:
            return fn()
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc) and "busy" not in str(exc):
                raise
            if i == attempts - 1:
                raise
            time.sleep(delay)


def _set_meta(conn, key, value):
    conn.execute(
        "INSERT INTO meta(key, value) VALUES (?, ?)"
        " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value))


# ---------------------------------------------------------------------------
# Package writing (export side)
# ---------------------------------------------------------------------------

class _PackageWriter:
    """Streams canonical record lines to a file while accumulating the
    running SHA-256 over header and data records.  Memory is O(1)."""

    def __init__(self, path):
        self._fh = open(path, "wb")
        self._hasher = hashlib.sha256()
        self.records = 0
        self._closed = False

    def write_record(self, obj):
        line = canonical_json(obj).encode("utf-8") + b"\n"
        self._fh.write(line)
        self._hasher.update(line)
        self.records += 1

    def finish(self, trailer_extra):
        trailer = {"record": "trailer", "records": self.records,
                   "sha256": self._hasher.hexdigest()}
        trailer.update(trailer_extra)
        self._fh.write(canonical_json(trailer).encode("utf-8") + b"\n")
        self._fh.close()
        self._closed = True
        return trailer["sha256"]

    def abort(self):
        if not self._closed:
            self._closed = True
            try:
                self._fh.close()
            except OSError:
                pass


def _open_read_snapshot(db_path):
    """One consistent read view of the warehouse: every eligibility
    check, batch lookup and row read of an export runs inside it, so
    commits or evictions landing mid-export cannot leak into the
    package."""
    if not os.path.exists(db_path):
        raise MirrorError(f"warehouse not found: {db_path}")
    uri = "file:" + db_path.replace("?", "%3f") + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, isolation_level=None)
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA temp_store = 1")   # file-backed temp tables
    conn.execute("BEGIN")
    return conn


def _managed_policy(conn):
    policy = read_policy(conn)
    if not policy["managed"]:
        raise MirrorError(
            "warehouse is not under retention management; run"
            " 'retire init' first: mirror exports are anchored to the"
            " managed warehouse identifier")
    return policy


def _first_gap(intervals, lo, hi):
    """Smallest version in the closed interval [lo, hi] not covered by
    the coalesced replay intervals, or None when fully replayable."""
    v = lo
    for ilo, ihi in intervals:
        if ihi < v:
            continue
        if ilo > v:
            return v
        v = ihi + 1
        if v > hi:
            return None
    return v if v <= hi else None


def export_base(db_path, regions, version, out_path):
    """Export a base package: the selected row of every subscribed
    endpoint at warehouse version ``version`` (0 = the empty warehouse,
    only when replayable).  Establishes a new mirror generation."""
    canon = canonical_subscription(regions)
    region_list = None if canon["regions"] == "all" else canon["regions"]
    if type(version) is not int or version < 0:
        raise MirrorError(f"invalid base version {version!r}")
    conn = _open_read_snapshot(db_path)
    tmp_out = None
    try:
        policy = _managed_policy(conn)
        current = get_current_version(conn)
        if version > current:
            raise MirrorError(
                f"warehouse version {version} not available (current"
                f" version {current})")
        intervals = replay_intervals(policy["watermark"],
                                     policy["exceptions"], current)
        if not is_replayable(intervals, version):
            raise RetiredVersionError(version, policy["watermark"],
                                      policy["policy_generation"])
        if _TEST_HOOK is not None:
            _TEST_HOOK("export_snapshot")
        where, params = _region_filter(region_list)
        grouped = ("SELECT region, collector, generation, ts,"
                   " MAX(revision) AS mr FROM checkpoint"
                   " WHERE version <= ?" + where +
                   " GROUP BY region, collector, generation, ts")
        n_rows = conn.execute(f"SELECT COUNT(*) FROM ({grouped})",
                              [version] + params).fetchone()[0]
        tmp_out = f"{out_path}.tmp-{os.getpid()}"
        try:
            os.unlink(tmp_out)
        except FileNotFoundError:
            pass
        writer = _PackageWriter(tmp_out)
        try:
            writer.write_record({
                "record": "header", "format": PACKAGE_FORMAT,
                "format_version": PACKAGE_FORMAT_VERSION, "kind": "base",
                "warehouse_id": policy["warehouse_id"],
                "subscription": canon, "version": version})
            sql = ("SELECT c.region, c.collector, c.generation, c.ts,"
                   " c.revision, c.cuts, c.counts, c.total, c.batch_id,"
                   " c.line_no, c.version FROM checkpoint c JOIN ("
                   + grouped + ") m ON c.region = m.region"
                   " AND c.collector = m.collector"
                   " AND c.generation = m.generation AND c.ts = m.ts"
                   " AND c.revision = m.mr"
                   " ORDER BY c.region, c.collector, c.generation, c.ts")
            for (region, collector, generation, ts, revision, cuts,
                 counts, total, batch_id, line_no, first_version) in \
                    conn.execute(sql, [version] + params):
                writer.write_record({
                    "record": "row", "region": region,
                    "collector": collector, "generation": generation,
                    "ts": ts, "revision": revision,
                    "cuts_ms": json.loads(cuts),
                    "counts": json.loads(counts), "total": int(total),
                    "first_version": first_version,
                    "first_batch": batch_id, "first_line": line_no})
            digest = writer.finish({"rows": n_rows})
        except BaseException:
            writer.abort()
            raise
        os.replace(tmp_out, out_path)
        tmp_out = None
        conn.execute("ROLLBACK")
        return {
            "kind": "base",
            "warehouse_id": policy["warehouse_id"],
            "subscription": canon,
            "version": version,
            "rows": n_rows,
            "package_sha256": digest,
            "package_records": writer.records,
            "out": out_path,
            "stats": {"rows_examined": n_rows},
        }
    finally:
        conn.close()
        if tmp_out is not None:
            try:
                os.unlink(tmp_out)
            except OSError:
                pass


# Per-version decided changes spill into this file-backed TEMP table so
# the unit record (which must precede its changes and declare their
# count) can be written without holding the version's changes — let
# alone the whole package — in memory.
_CHANGE_DDL = (
    "CREATE TEMP TABLE IF NOT EXISTS mirror_export_change("
    " region TEXT NOT NULL, collector TEXT NOT NULL,"
    " generation TEXT NOT NULL, ts INTEGER NOT NULL,"
    " old_revision INTEGER, revision INTEGER NOT NULL,"
    " cuts TEXT NOT NULL, counts TEXT NOT NULL, total TEXT NOT NULL,"
    " first_version INTEGER NOT NULL, first_batch TEXT NOT NULL,"
    " first_line INTEGER NOT NULL)")

_VERSION_ROWS_SQL = (
    "SELECT region, collector, generation, ts, revision, cuts, counts,"
    " total, batch_id, line_no FROM checkpoint WHERE version = ?")

_PREV_REVISION_SQL = (
    "SELECT revision FROM checkpoint WHERE region = ? AND collector = ?"
    " AND generation = ? AND ts = ? AND version <= ?"
    " ORDER BY revision DESC LIMIT 1")


def _flush_candidate(conn, row, version, stats):
    """Decide whether the newest revision introduced by ``version`` for
    one endpoint changes the selected row; if so, stage the change."""
    (region, collector, generation, ts, revision, cuts, counts, total,
     batch_id, line_no) = row
    prev = conn.execute(
        _PREV_REVISION_SQL,
        (region, collector, generation, ts, version - 1)).fetchone()
    stats["endpoint_probes"] += 1
    prev_rev = prev[0] if prev is not None else None
    if prev_rev is not None and revision <= prev_rev:
        return 0                # late lower revision: not a change
    conn.execute(
        "INSERT INTO mirror_export_change(region, collector, generation,"
        " ts, old_revision, revision, cuts, counts, total, first_version,"
        " first_batch, first_line) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (region, collector, generation, ts, prev_rev, revision, cuts,
         counts, total, version, batch_id, line_no))
    return 1


def _stage_version_changes(conn, version, where, params, stats):
    """Stage one commit version's changes; returns the change count.

    Rows stream from the version index range ordered by endpoint and
    revision, so only the newest revision per endpoint is a candidate;
    the selected revision before this version is one O(log n) primary
    key probe per affected endpoint.  Work is proportional to the
    batches and changes involved, never to the warehouse size.
    """
    conn.execute("DELETE FROM mirror_export_change")
    cursor = conn.execute(
        _VERSION_ROWS_SQL + where +
        " ORDER BY region, collector, generation, ts, revision",
        [version] + params)
    count = 0
    current_key = None
    best = None
    for row in cursor:
        stats["rows_examined"] += 1
        key = row[:4]
        if current_key is not None and key != current_key:
            count += _flush_candidate(conn, best, version, stats)
        current_key = key
        best = row                # rows ascend by revision per endpoint
    if current_key is not None:
        count += _flush_candidate(conn, best, version, stats)
    return count


def export_delta(db_path, regions, base, end, out_path):
    """Export a catch-up package advancing a mirror that holds version
    ``base`` to version ``end`` (0 <= base < end).  Carries one commit
    unit per version in (base, end], in order, even for versions with
    no subscribed change."""
    canon = canonical_subscription(regions)
    region_list = None if canon["regions"] == "all" else canon["regions"]
    if (type(base) is not int or type(end) is not int
            or base < 0 or end <= base):
        raise MirrorError(
            f"delta export requires integer versions 0 <= B < E, got"
            f" {base!r} -> {end!r}")
    conn = _open_read_snapshot(db_path)
    tmp_out = None
    try:
        policy = _managed_policy(conn)
        current = get_current_version(conn)
        if end > current:
            raise MirrorError(
                f"delta end version {end} is beyond the current"
                f" warehouse version {current}")
        intervals = replay_intervals(policy["watermark"],
                                     policy["exceptions"], current)
        gap = _first_gap(intervals, base, end)
        if gap is not None:
            raise MirrorError(
                f"cannot export delta {base}..{end}: warehouse version"
                f" {gap} is the first version of the closed interval"
                f" that is not replayable under the current retention"
                f" policy (watermark {policy['watermark']}, policy"
                f" generation {policy['policy_generation']}); rebuild"
                f" the field mirror from a base package at a replayable"
                f" version instead")
        if _TEST_HOOK is not None:
            _TEST_HOOK("export_snapshot")
        batches = {}
        for v, b in conn.execute(
                "SELECT version, batch_id FROM batch"
                " WHERE version > ? AND version <= ?", (base, end)):
            batches[v] = b
        where, params = _region_filter(region_list)
        conn.execute(_CHANGE_DDL)
        stats = {"versions": end - base, "rows_examined": 0,
                 "endpoint_probes": 0, "changes": 0}
        tmp_out = f"{out_path}.tmp-{os.getpid()}"
        try:
            os.unlink(tmp_out)
        except FileNotFoundError:
            pass
        writer = _PackageWriter(tmp_out)
        try:
            writer.write_record({
                "record": "header", "format": PACKAGE_FORMAT,
                "format_version": PACKAGE_FORMAT_VERSION, "kind": "delta",
                "warehouse_id": policy["warehouse_id"],
                "subscription": canon, "base_version": base,
                "end_version": end})
            for v in range(base + 1, end + 1):
                batch_id = batches.get(v)
                if batch_id is None:
                    raise MirrorError(
                        f"warehouse batch ledger has no batch for"
                        f" version {v}; the warehouse is inconsistent")
                n = _stage_version_changes(conn, v, where, params, stats)
                writer.write_record({"record": "unit", "version": v,
                                     "batch_id": batch_id, "changes": n})
                for (region, collector, generation, ts, old_rev, revision,
                     cuts, counts, total, first_version, first_batch,
                     first_line) in conn.execute(
                        "SELECT region, collector, generation, ts,"
                        " old_revision, revision, cuts, counts, total,"
                        " first_version, first_batch, first_line"
                        " FROM mirror_export_change ORDER BY rowid"):
                    writer.write_record({
                        "record": "change", "region": region,
                        "collector": collector, "generation": generation,
                        "ts": ts, "old_revision": old_rev,
                        "revision": revision,
                        "cuts_ms": json.loads(cuts),
                        "counts": json.loads(counts), "total": int(total),
                        "first_version": first_version,
                        "first_batch": first_batch,
                        "first_line": first_line})
                stats["changes"] += n
            digest = writer.finish({"units": end - base,
                                    "changes": stats["changes"]})
        except BaseException:
            writer.abort()
            raise
        os.replace(tmp_out, out_path)
        tmp_out = None
        conn.execute("ROLLBACK")
        return {
            "kind": "delta",
            "warehouse_id": policy["warehouse_id"],
            "subscription": canon,
            "base_version": base,
            "end_version": end,
            "units": end - base,
            "changes": stats["changes"],
            "package_sha256": digest,
            "package_records": writer.records,
            "out": out_path,
            "stats": stats,
        }
    finally:
        conn.close()
        if tmp_out is not None:
            try:
                os.unlink(tmp_out)
            except OSError:
                pass


# ---------------------------------------------------------------------------
# Package reading (receive side)
# ---------------------------------------------------------------------------

def _pkg_keys(obj, allowed, line_no):
    keys = set(obj)
    if keys != allowed:
        parts = []
        missing = sorted(allowed - keys)
        extra = sorted(keys - allowed)
        if missing:
            parts.append("missing " + ", ".join(repr(k) for k in missing))
        if extra:
            parts.append("unexpected " + ", ".join(repr(k) for k in extra))
        raise PackageError(f"line {line_no}: bad fields: " + "; ".join(parts))


def _pkg_str(obj, key, line_no):
    value = obj[key]
    if type(value) is not str or value == "":
        raise PackageError(
            f"line {line_no}: {key!r} must be a non-empty string")
    return value


def _pkg_int(obj, key, line_no, lo, hi=MAX_INT64):
    value = obj[key]
    if type(value) is not int or not (lo <= value <= hi):
        raise PackageError(
            f"line {line_no}: {key!r} must be an integer in [{lo}, {hi}]")
    return value


def _validate_subscription(value, line_no):
    if not isinstance(value, dict) or set(value) != {"regions"}:
        raise PackageError(
            f"line {line_no}: 'subscription' must be an object with"
            " exactly one 'regions' field")
    regions = value["regions"]
    if regions == "all":
        return {"regions": "all"}
    if type(regions) is not list or not regions:
        raise PackageError(
            f"line {line_no}: 'regions' must be 'all' or a non-empty"
            " array of region names")
    seen = set()
    for name in regions:
        if type(name) is not str or name == "":
            raise PackageError(
                f"line {line_no}: region names must be non-empty strings")
        if name in seen:
            raise PackageError(
                f"line {line_no}: duplicate region {name!r} in the"
                " subscription")
        seen.add(name)
    return {"regions": sorted(regions)}


_BASE_HEADER_KEYS = {"record", "format", "format_version", "kind",
                     "warehouse_id", "subscription", "version"}
_DELTA_HEADER_KEYS = {"record", "format", "format_version", "kind",
                      "warehouse_id", "subscription", "base_version",
                      "end_version"}


def _validate_header(obj, line_no):
    kind = obj.get("kind")
    if kind == "base":
        _pkg_keys(obj, _BASE_HEADER_KEYS, line_no)
    elif kind == "delta":
        _pkg_keys(obj, _DELTA_HEADER_KEYS, line_no)
    else:
        raise PackageError(
            f"line {line_no}: 'kind' must be 'base' or 'delta'")
    if obj["format"] != PACKAGE_FORMAT:
        raise PackageError(
            f"line {line_no}: unsupported package format"
            f" {obj['format']!r}")
    if (type(obj["format_version"]) is not int
            or obj["format_version"] != PACKAGE_FORMAT_VERSION):
        raise PackageError(
            f"line {line_no}: unsupported package format version"
            f" {obj['format_version']!r}")
    header = {
        "kind": kind,
        "warehouse_id": _pkg_str(obj, "warehouse_id", line_no),
        "subscription": _validate_subscription(obj["subscription"], line_no),
    }
    if kind == "base":
        header["version"] = _pkg_int(obj, "version", line_no, 0)
    else:
        header["base_version"] = _pkg_int(obj, "base_version", line_no, 0)
        header["end_version"] = _pkg_int(obj, "end_version", line_no, 0)
        if header["end_version"] <= header["base_version"]:
            raise PackageError(
                f"line {line_no}: 'end_version' must be greater than"
                " 'base_version'")
    return header


def _validate_unit(obj, line_no):
    _pkg_keys(obj, {"record", "version", "batch_id", "changes"}, line_no)
    return {
        "version": _pkg_int(obj, "version", line_no, 1),
        "batch_id": _pkg_str(obj, "batch_id", line_no),
        "changes": _pkg_int(obj, "changes", line_no, 0),
    }


def _norm_payload_rec(obj, line_no):
    """Validate the checkpoint payload fields with the warehouse's own
    rules and return the record in storage-canonical form (counts and
    total as exact decimal text, never floats)."""
    try:
        cp = checkpoint_from_values({
            "region": obj.get("region"),
            "collector": obj.get("collector"),
            "generation": obj.get("generation"),
            "ts": obj.get("ts"),
            "revision": obj.get("revision"),
            "cuts_ms": obj.get("cuts_ms"),
            "counts": obj.get("counts"),
            "total": obj.get("total"),
        }, line_no)
    except LineError as exc:
        raise PackageError(f"line {line_no}: {exc.message}") from exc
    return {
        "region": cp.region, "collector": cp.collector,
        "generation": cp.generation, "ts": cp.ts, "revision": cp.revision,
        "cuts": cp.cuts_json, "counts": cp.counts_json,
        "total": cp.total_str,
    }


def _validate_row(obj, line_no):
    _pkg_keys(obj, {"record", "region", "collector", "generation", "ts",
                    "revision", "cuts_ms", "counts", "total",
                    "first_version", "first_batch", "first_line"}, line_no)
    rec = _norm_payload_rec(obj, line_no)
    rec["first_version"] = _pkg_int(obj, "first_version", line_no, 1)
    rec["first_batch"] = _pkg_str(obj, "first_batch", line_no)
    rec["first_line"] = _pkg_int(obj, "first_line", line_no, 1)
    return rec


def _validate_change(obj, line_no):
    _pkg_keys(obj, {"record", "region", "collector", "generation", "ts",
                    "old_revision", "revision", "cuts_ms", "counts",
                    "total", "first_version", "first_batch", "first_line"},
              line_no)
    rec = _norm_payload_rec(obj, line_no)
    old = obj["old_revision"]
    if old is not None:
        if type(old) is not int or not (0 <= old <= MAX_INT64):
            raise PackageError(
                f"line {line_no}: 'old_revision' must be null or an"
                " integer in [0, 2^63-1]")
        if old >= rec["revision"]:
            raise PackageError(
                f"line {line_no}: 'old_revision' {old} must be lower"
                f" than 'revision' {rec['revision']}")
    rec["old_revision"] = old
    rec["first_version"] = _pkg_int(obj, "first_version", line_no, 1)
    rec["first_batch"] = _pkg_str(obj, "first_batch", line_no)
    rec["first_line"] = _pkg_int(obj, "first_line", line_no, 1)
    return rec


def _validate_trailer(obj, line_no, kind):
    if kind == "base":
        _pkg_keys(obj, {"record", "records", "rows", "sha256"}, line_no)
    else:
        _pkg_keys(obj, {"record", "records", "units", "changes", "sha256"},
                  line_no)
    trailer = {"records": _pkg_int(obj, "records", line_no, 1)}
    if kind == "base":
        trailer["rows"] = _pkg_int(obj, "rows", line_no, 0)
    else:
        trailer["units"] = _pkg_int(obj, "units", line_no, 1)
        trailer["changes"] = _pkg_int(obj, "changes", line_no, 0)
    sha = obj["sha256"]
    if (type(sha) is not str or len(sha) != 64
            or any(c not in "0123456789abcdef" for c in sha)):
        raise PackageError(
            f"line {line_no}: 'sha256' must be 64 lowercase hex"
            " characters")
    trailer["sha256"] = sha
    return trailer


class _Validator:
    """Streaming structural validation of one package, O(1) memory:
    row and change records must arrive strictly ordered by endpoint
    key, which also makes duplicate keys inside one commit unit (or one
    base snapshot) detectable without keeping any key set."""

    def __init__(self):
        self.header = None
        self.prev_key = None
        self.unit_version = None
        self.unit_batch = None
        self.unit_declared = 0
        self.unit_seen = 0
        self.units_seen = 0
        self.rows_seen = 0
        self.changes_seen = 0
        self.expected_unit = None

    def feed(self, obj, line_no):
        rtype = obj.get("record")
        if self.header is None:
            if rtype != "header":
                raise PackageError(
                    f"line {line_no}: the first record must be the"
                    " package header")
            self.header = _validate_header(obj, line_no)
            if self.header["kind"] == "delta":
                self.expected_unit = self.header["base_version"] + 1
            return ("header", self.header)
        if rtype == "header":
            raise PackageError(f"line {line_no}: duplicate header record")
        if self.header["kind"] == "base":
            if rtype != "row":
                raise PackageError(
                    f"line {line_no}: a base package carries row records"
                    f" only, not {rtype!r}")
            rec = _validate_row(obj, line_no)
            self._order(rec, line_no)
            self.rows_seen += 1
            return ("row", rec)
        if rtype == "unit":
            self._close_unit(line_no)
            rec = _validate_unit(obj, line_no)
            if rec["version"] != self.expected_unit:
                raise PackageError(
                    f"line {line_no}: commit unit version"
                    f" {rec['version']} is missing or out of order"
                    f" (expected {self.expected_unit})")
            self.expected_unit += 1
            self.unit_version = rec["version"]
            self.unit_batch = rec["batch_id"]
            self.unit_declared = rec["changes"]
            self.unit_seen = 0
            self.prev_key = None
            self.units_seen += 1
            return ("unit", rec)
        if rtype == "change":
            if self.unit_version is None:
                raise PackageError(
                    f"line {line_no}: change record outside any commit"
                    " unit")
            rec = _validate_change(obj, line_no)
            if (rec["first_version"] != self.unit_version
                    or rec["first_batch"] != self.unit_batch):
                raise PackageError(
                    f"line {line_no}: change first provenance (version"
                    f" {rec['first_version']}, batch"
                    f" {rec['first_batch']!r}) does not match its commit"
                    f" unit (version {self.unit_version}, batch"
                    f" {self.unit_batch!r})")
            self._order(rec, line_no)
            self.unit_seen += 1
            self.changes_seen += 1
            return ("change", rec)
        raise PackageError(
            f"line {line_no}: unexpected record type {rtype!r} in a"
            " delta package")

    def _order(self, rec, line_no):
        key = (rec["region"], rec["collector"], rec["generation"],
               rec["ts"])
        if self.prev_key is not None and key <= self.prev_key:
            if key == self.prev_key:
                raise PackageError(
                    f"line {line_no}: duplicate record for endpoint"
                    f" {key} within the same commit unit or snapshot")
            raise PackageError(
                f"line {line_no}: records out of endpoint-key order")
        self.prev_key = key

    def _close_unit(self, line_no):
        if (self.unit_version is not None
                and self.unit_seen != self.unit_declared):
            raise PackageError(
                f"line {line_no}: commit unit {self.unit_version}"
                f" declares {self.unit_declared} changes but carries"
                f" {self.unit_seen}")

    def finish(self, obj, line_no, n_records, digest):
        if self.header is None:
            raise PackageError(f"line {line_no}: trailer without a header")
        self._close_unit(line_no)
        trailer = _validate_trailer(obj, line_no, self.header["kind"])
        if trailer["records"] != n_records:
            raise PackageError(
                f"line {line_no}: trailer declares {trailer['records']}"
                f" records but the package carries {n_records}")
        if trailer["sha256"] != digest:
            raise PackageError(
                f"line {line_no}: package digest mismatch (corrupt or"
                " modified package)")
        if self.header["kind"] == "base":
            if trailer["rows"] != self.rows_seen:
                raise PackageError(
                    f"line {line_no}: trailer declares"
                    f" {trailer['rows']} rows but the package carries"
                    f" {self.rows_seen}")
        else:
            if self.expected_unit != self.header["end_version"] + 1:
                raise PackageError(
                    f"line {line_no}: commit units stop at version"
                    f" {self.expected_unit - 1} but the package claims"
                    f" to reach {self.header['end_version']}")
            if trailer["units"] != self.units_seen:
                raise PackageError(
                    f"line {line_no}: trailer declares"
                    f" {trailer['units']} commit units but the package"
                    f" carries {self.units_seen}")
            if trailer["changes"] != self.changes_seen:
                raise PackageError(
                    f"line {line_no}: trailer declares"
                    f" {trailer['changes']} changes but the package"
                    f" carries {self.changes_seen}")
        return trailer


def iter_package(path):
    """Yield ("header"|"row"|"unit"|"change"|"trailer", record) events
    from one package file, fully validating structure, ordering, counts
    and the trailing digest.  Streams line by line; raises PackageError
    on any violation."""
    validator = _Validator()
    hasher = hashlib.sha256()
    n_records = 0
    with open(path, "rb") as fh:
        line_no = 0
        for raw in fh:
            line_no += 1
            if not raw.endswith(b"\n"):
                raise PackageError(
                    f"line {line_no}: record not newline-terminated"
                    " (truncated package?)")
            try:
                obj = load_json_object(raw, line_no)
            except LineError as exc:
                raise PackageError(f"line {line_no}: {exc.message}") from exc
            if obj.get("record") == "trailer":
                trailer = validator.finish(obj, line_no, n_records,
                                           hasher.hexdigest())
                yield ("trailer", trailer)
                for _ in fh:
                    line_no += 1
                    raise PackageError(
                        f"line {line_no}: content after the trailer"
                        " record")
                return
            event = validator.feed(obj, line_no)
            hasher.update(raw)
            n_records += 1
            yield event
    if line_no == 0:
        raise PackageError("empty package file")
    raise PackageError(
        "unexpected end of package: the trailer record is missing"
        " (truncated package?)")


def read_package(path):
    """Validate a package file end to end; return (header, trailer)."""
    header = trailer = None
    for kind, rec in iter_package(path):
        if kind == "header":
            header = rec
        elif kind == "trailer":
            trailer = rec
    return header, trailer


# ---------------------------------------------------------------------------
# Mirror database (field side)
# ---------------------------------------------------------------------------

def connect_mirror(path, create=False):
    if not create and not os.path.exists(path):
        raise MirrorError(f"mirror not found: {path}")
    conn = sqlite3.connect(path, isolation_level=None)
    conn.execute("PRAGMA busy_timeout = 30000")
    if create:
        _retry_locked(lambda: conn.execute("PRAGMA journal_mode = WAL"))
        conn.execute("PRAGMA temp_store = 1")
        has_meta = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
            " AND name = 'meta'").fetchone()
        if has_meta is not None:
            if (get_meta(conn, META_MIRROR_SCHEMA) is None
                    and get_meta(conn, "current_version") is not None):
                conn.close()
                raise MirrorError(
                    f"{path} is a histoware warehouse, not a mirror"
                    " database")
        _retry_locked(lambda: conn.executescript(MIRROR_SCHEMA))
        conn.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES (?, ?)",
            (META_MIRROR_SCHEMA, MIRROR_SCHEMA_VERSION))
        conn.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES (?, ?)",
            (META_GENERATION, "0"))
    return conn


_RECEIPT_SQL = (
    "SELECT id, generation, kind, warehouse_id, subscription,"
    " base_version, end_version, package_sha256, package_records,"
    " changes, received_utc FROM receipt")


def _receipt_dict(row):
    (rid, generation, kind, warehouse_id, subscription, base_version,
     end_version, package_sha256, package_records, changes,
     received_utc) = row
    return {
        "receipt_id": rid,
        "generation": generation,
        "kind": kind,
        "warehouse_id": warehouse_id,
        "subscription": json.loads(subscription),
        "base_version": base_version,
        "end_version": end_version,
        "package_sha256": package_sha256,
        "package_records": package_records,
        "changes": changes,
        "received_utc": received_utc,
    }


def _check_identity(conn, header):
    warehouse_id = get_meta(conn, META_WAREHOUSE_ID)
    if header["warehouse_id"] != warehouse_id:
        raise MirrorError(
            f"the package comes from source warehouse"
            f" {header['warehouse_id']!r} but this mirror follows"
            f" {warehouse_id!r}")
    subscription = get_meta(conn, META_SUBSCRIPTION)
    if canonical_json(header["subscription"]) != subscription:
        raise MirrorError(
            f"the package subscription"
            f" {canonical_json(header['subscription'])} does not match"
            f" the mirror subscription {subscription}")


def _flush_rows(conn, buf):
    try:
        conn.executemany(
            "INSERT INTO mirror_row(region, collector, generation, ts,"
            " revision, cuts, counts, total, first_version, first_batch,"
            " first_line) VALUES (?,?,?,?,?,?,?,?,?,?,?)", buf)
    except sqlite3.IntegrityError as exc:
        raise MirrorError(
            f"base package conflicts with the mirror state: {exc}") from exc
    buf.clear()


def _apply_base(conn, package_path, expected_digest, wipe):
    """Second streaming pass: install every row of a base package."""
    if wipe:
        conn.execute("DELETE FROM mirror_row")
    n = 0
    buf = []
    for kind, rec in iter_package(package_path):
        if kind == "row":
            buf.append((rec["region"], rec["collector"],
                        rec["generation"], rec["ts"], rec["revision"],
                        rec["cuts"], rec["counts"], rec["total"],
                        rec["first_version"], rec["first_batch"],
                        rec["first_line"]))
            n += 1
            if len(buf) >= 4096:
                _flush_rows(conn, buf)
        elif kind == "trailer" and rec["sha256"] != expected_digest:
            raise MirrorError(
                "the package file changed between verification and"
                " application; refusing to install it")
    if buf:
        _flush_rows(conn, buf)
    return n


def _apply_change(conn, rec):
    key = (rec["region"], rec["collector"], rec["generation"], rec["ts"])
    if rec["old_revision"] is None:
        try:
            conn.execute(
                "INSERT INTO mirror_row(region, collector, generation,"
                " ts, revision, cuts, counts, total, first_version,"
                " first_batch, first_line) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (rec["region"], rec["collector"], rec["generation"],
                 rec["ts"], rec["revision"], rec["cuts"], rec["counts"],
                 rec["total"], rec["first_version"], rec["first_batch"],
                 rec["first_line"]))
        except sqlite3.IntegrityError:
            row = conn.execute(
                "SELECT revision FROM mirror_row WHERE region = ?"
                " AND collector = ? AND generation = ? AND ts = ?",
                key).fetchone()
            raise MirrorError(
                f"change for endpoint {key} expects it to be absent,"
                f" but the mirror holds revision"
                f" {row[0] if row else '?'}")
        return
    cur = conn.execute(
        "UPDATE mirror_row SET revision = ?, cuts = ?, counts = ?,"
        " total = ?, first_version = ?, first_batch = ?, first_line = ?"
        " WHERE region = ? AND collector = ? AND generation = ?"
        " AND ts = ? AND revision = ?",
        (rec["revision"], rec["cuts"], rec["counts"], rec["total"],
         rec["first_version"], rec["first_batch"], rec["first_line"],
         rec["region"], rec["collector"], rec["generation"], rec["ts"],
         rec["old_revision"]))
    if cur.rowcount == 0:
        row = conn.execute(
            "SELECT revision FROM mirror_row WHERE region = ?"
            " AND collector = ? AND generation = ? AND ts = ?",
            key).fetchone()
        if row is None:
            raise MirrorError(
                f"change for endpoint {key} expects revision"
                f" {rec['old_revision']} but the endpoint is absent")
        raise MirrorError(
            f"change for endpoint {key} expects revision"
            f" {rec['old_revision']} but the mirror holds revision"
            f" {row[0]}")


def _apply_delta(conn, package_path, expected_digest):
    """Second streaming pass: apply every change of a delta package."""
    n = 0
    for kind, rec in iter_package(package_path):
        if kind == "change":
            _apply_change(conn, rec)
            n += 1
        elif kind == "trailer" and rec["sha256"] != expected_digest:
            raise MirrorError(
                "the package file changed between verification and"
                " application; refusing to advance the mirror")
    return n


def receive_package(mirror_path, package_path, rebuild=False):
    """Receive one package file into a mirror database.

    The file is validated and hashed in a first streaming pass; the
    idempotency check, identity checks and the application of every row
    or change then happen inside one immediate transaction, so mirror
    rows, the cursor and the receipt become visible together or not at
    all.  Returns the (new or existing) receipt.
    """
    header, trailer = read_package(package_path)
    digest = trailer["sha256"]
    conn = connect_mirror(mirror_path, create=True)
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            generation = int(get_meta(conn, META_GENERATION, "0"))
            existing = conn.execute(
                _RECEIPT_SQL
                + " WHERE generation = ? AND package_sha256 = ?",
                (generation, digest)).fetchone()
            if existing is not None:
                conn.execute("ROLLBACK")
                return {"reused": True, "receipt": _receipt_dict(existing)}
            if generation == 0:
                if header["kind"] != "base":
                    raise MirrorError(
                        "the mirror is empty: the first package must be"
                        " a base package, not a delta")
                changes = _apply_base(conn, package_path, digest,
                                      wipe=False)
                new_generation = 1
                cursor = header["version"]
                _set_meta(conn, META_WAREHOUSE_ID, header["warehouse_id"])
                _set_meta(conn, META_SUBSCRIPTION,
                          canonical_json(header["subscription"]))
            else:
                _check_identity(conn, header)
                cursor = int(get_meta(conn, META_CURSOR))
                if header["kind"] == "base":
                    if not rebuild:
                        raise MirrorError(
                            "the package is a base package but the"
                            f" mirror already tracks generation"
                            f" {generation} (cursor {cursor}); re-run"
                            " with --rebuild to replace the mirror with"
                            " a new generation")
                    if header["version"] <= cursor:
                        raise MirrorError(
                            f"base package version {header['version']}"
                            f" is not newer than the mirror cursor"
                            f" {cursor}")
                    changes = _apply_base(conn, package_path, digest,
                                          wipe=True)
                    new_generation = generation + 1
                    cursor = header["version"]
                else:
                    if header["base_version"] != cursor:
                        raise MirrorError(
                            f"the package starts at version"
                            f" {header['base_version']} but the mirror"
                            f" cursor is {cursor}; overlapping or gapped"
                            f" ranges are never spliced")
                    changes = _apply_delta(conn, package_path, digest)
                    new_generation = generation
                    cursor = header["end_version"]
            _set_meta(conn, META_GENERATION, str(new_generation))
            _set_meta(conn, META_CURSOR, str(cursor))
            if header["kind"] == "base":
                base_version = end_version = header["version"]
            else:
                base_version = header["base_version"]
                end_version = header["end_version"]
            cur = conn.execute(
                "INSERT INTO receipt(generation, kind, warehouse_id,"
                " subscription, base_version, end_version,"
                " package_sha256, package_records, changes, received_utc)"
                " VALUES (?,?,?,?,?,?,?,?,?,?)",
                (new_generation, header["kind"], header["warehouse_id"],
                 canonical_json(header["subscription"]), base_version,
                 end_version, digest, trailer["records"], changes,
                 _utcnow()))
            receipt_id = cur.lastrowid
            if _TEST_HOOK is not None:
                _TEST_HOOK("before_commit")
            conn.execute("COMMIT")
        except BaseException:
            _rollback_quiet(conn)
            raise
        row = conn.execute(_RECEIPT_SQL + " WHERE id = ?",
                           (receipt_id,)).fetchone()
        return {"reused": False, "receipt": _receipt_dict(row)}
    finally:
        conn.close()


def mirror_status(db_path):
    """Offline mirror state: source, subscription, generation, cursor,
    endpoint count and every accepted-package receipt."""
    conn = connect_mirror(db_path, create=False)
    try:
        has_meta = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
            " AND name = 'meta'").fetchone()
        if has_meta is None or get_meta(conn, META_MIRROR_SCHEMA) is None:
            raise MirrorError(f"{db_path} is not a mirror database")
        generation = int(get_meta(conn, META_GENERATION, "0"))
        cursor = get_meta(conn, META_CURSOR)
        subscription = get_meta(conn, META_SUBSCRIPTION)
        return {
            "schema_version": MIRROR_SCHEMA_VERSION,
            "warehouse_id": get_meta(conn, META_WAREHOUSE_ID),
            "subscription": (json.loads(subscription)
                             if subscription is not None else None),
            "generation": generation,
            "cursor": int(cursor) if cursor is not None else None,
            "endpoints": conn.execute(
                "SELECT COUNT(*) FROM mirror_row").fetchone()[0],
            "receipts": [
                _receipt_dict(row) for row in conn.execute(
                    _RECEIPT_SQL + " ORDER BY id")],
        }
    finally:
        conn.close()
