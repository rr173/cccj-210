"""Warehouse storage, batch loading and retention management.

Schema
------
meta(key, value)
    'current_version': the visible warehouse version (0 = empty).
    Retention keys (only in an explicitly initialised managed
    warehouse, see below): 'warehouse_id', 'retention_watermark',
    'policy_generation', 'policy_gc', 'schema_version'.
batch(batch_id, file_sha256, version, loaded_at, rows)
    One row per committed batch.  Used for idempotent re-sends.
checkpoint(region, collector, generation, ts, revision,
           cuts, counts, total, batch_id, line_no, version)
    Every revision ever accepted.  `cuts`/`counts` are canonical JSON
    arrays, `total` is decimal TEXT (counts/totals may reach 2**64-1,
    which does not fit a SQLite signed 64-bit INTEGER).  `version` is
    the warehouse version that introduced the row.
receipt(...)
    Minimal acceptance credentials for revisions whose payload has
    been evicted, plus retirement receipts.  An evicted revision is
    recognised as an equivalent retransmission (same identity, same
    first version/provenance, same canonical-content SHA-256) without
    keeping the payload; differing content is still rejected with the
    original provenance.
retention_plan(...)
    Persisted rehearsal plans (immutable: a plan's stored body and its
    plan id cannot be changed without detection).

Indexes
-------
The checkpoint PRIMARY KEY (region, collector, generation, ts,
revision) doubles as the endpoint-lookup index: all revisions of one
checkpoint point are consecutive and ordered by revision, so
"max revision with version <= V" is an O(log n) index probe and never
a scan of the checkpoint history.

Retention
---------
A warehouse becomes *managed* only through ``init_retention`` (the
``retire init`` command): it records a stable warehouse identifier, a
watermark of 0, no exceptions and policy generation 0.  Initialising an
already managed warehouse is a no-op (it never resets state or the
identifier); an uninitialised warehouse keeps behaving exactly as
before, with no retention filtering at all.

Loading
-------
A batch is streamed line by line (never fully materialised in memory)
into a connection-local TEMP staging table in chunks; temp storage is
file-backed (PRAGMA temp_store=1) so memory stays bounded regardless
of batch size.  Only after the whole file is parsed and hashed does a
single BEGIN IMMEDIATE transaction validate it against existing data
and commit every row plus the version bump.  Any bad line, intra-batch
or cross-batch conflict, or read failure therefore leaves the visible
version untouched, and all historical versions remain recomputable.
A retransmission of an evicted revision is validated against its
acceptance receipt, and never resurrects the evicted payload row.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import datetime, timezone

from .common import (HistowareError, parse_checkpoint, payload_sha256)

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS batch(
  batch_id    TEXT PRIMARY KEY,
  file_sha256 TEXT NOT NULL,
  version     INTEGER NOT NULL,
  loaded_at   TEXT NOT NULL,
  rows        INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS checkpoint(
  region     TEXT NOT NULL,
  collector  TEXT NOT NULL,
  generation TEXT NOT NULL,
  ts         INTEGER NOT NULL,
  revision   INTEGER NOT NULL,
  cuts       TEXT NOT NULL,
  counts     TEXT NOT NULL,
  total      TEXT NOT NULL,
  batch_id   TEXT NOT NULL,
  line_no    INTEGER NOT NULL,
  version    INTEGER NOT NULL,
  PRIMARY KEY(region, collector, generation, ts, revision)
);
CREATE TABLE IF NOT EXISTS receipt(
  id                INTEGER PRIMARY KEY,
  kind              TEXT NOT NULL
                    CHECK(kind IN ('content','retirement')),
  warehouse_id      TEXT,
  region            TEXT,
  collector         TEXT,
  generation        TEXT,
  ts                INTEGER,
  revision          INTEGER,
  first_batch       TEXT,
  first_line        INTEGER,
  first_version     INTEGER,
  content_sha256    TEXT,
  content_bytes     INTEGER,
  plan_id           TEXT,
  base_version      INTEGER,
  evicted_rows      INTEGER,
  evicted_bytes     INTEGER,
  policy_generation INTEGER,
  detail_json       TEXT,
  created_utc       TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_receipt_content
  ON receipt(region, collector, generation, ts, revision)
  WHERE kind = 'content';
CREATE UNIQUE INDEX IF NOT EXISTS ux_receipt_plan
  ON receipt(plan_id) WHERE kind = 'retirement';
-- Mirror exports (histoware.mirror) read one read-only snapshot and
-- must not enumerate the whole warehouse per version: the delta export
-- range-scans checkpoint rows by introducing version and looks batches
-- up by version.  Both indexes are created once, here, for every
-- warehouse opened with create=True; exports themselves stay read-only.
CREATE INDEX IF NOT EXISTS ix_checkpoint_version ON checkpoint(version);
CREATE INDEX IF NOT EXISTS ix_batch_version ON batch(version);
"""

# Keys that mark a warehouse as explicitly managed.  Absence of
# 'warehouse_id' means "legacy warehouse": no retention behaviour.
META_WAREHOUSE_ID = "warehouse_id"
META_WATERMARK = "retention_watermark"
META_POLICY_GEN = "policy_generation"
META_POLICY_GC = "policy_gc"
META_SCHEMA_VERSION = "schema_version"

RETENTION_SCHEMA_VERSION = "2"

_STAGE_COLUMNS = "region, collector, generation, ts, revision, cuts, counts, total, line_no"
_STAGE_SQL = f"INSERT INTO stage({_STAGE_COLUMNS}) VALUES (?,?,?,?,?,?,?,?,?)"
_KEY_JOIN = (
    "p.region = s.region AND p.collector = s.collector AND "
    "p.generation = s.generation AND p.ts = s.ts AND p.revision = s.revision"
)
_CHUNK = 1000


class BatchError(HistowareError):
    """A batch is invalid or conflicts with existing data."""


def connect_warehouse(path, create=False):
    if not create and not os.path.exists(path):
        raise HistowareError(f"warehouse not found: {path}")
    conn = sqlite3.connect(path, isolation_level=None)
    conn.execute("PRAGMA busy_timeout = 30000")
    if create:
        # Several processes/threads may race to initialise the same new
        # file.  Switching the journal mode needs an exclusive lock and
        # does not always honour busy_timeout; retry briefly instead of
        # surfacing a spurious "database is locked".
        _retry_locked(lambda: conn.execute("PRAGMA journal_mode = WAL"))
        conn.execute("PRAGMA temp_store = 1")   # file-backed temp tables
        _retry_locked(lambda: conn.executescript(SCHEMA))
        conn.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES('current_version', '0')")
    return conn


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


def get_current_version(conn) -> int:
    row = conn.execute(
        "SELECT value FROM meta WHERE key = 'current_version'").fetchone()
    return int(row[0]) if row else 0


def get_meta(conn, key, default=None):
    row = conn.execute("SELECT value FROM meta WHERE key = ?",
                       (key,)).fetchone()
    return row[0] if row else default


def is_managed(conn) -> bool:
    """True only for an explicitly initialised (retention-managed) warehouse."""
    return get_meta(conn, META_WAREHOUSE_ID) is not None


def read_policy(conn) -> dict:
    """Current retention policy.  Legacy warehouses read as watermark 0,
    no exceptions and policy generation 0, but remain unmanaged."""
    wid = get_meta(conn, META_WAREHOUSE_ID)
    gc = json.loads(get_meta(conn, META_POLICY_GC, "{}"))
    return {
        "managed": wid is not None,
        "warehouse_id": wid,
        "watermark": int(get_meta(conn, META_WATERMARK, "0")),
        "exceptions": [{"work_order": name, "version": int(version)}
                       for name, version in sorted(gc.items())],
        "policy_generation": int(get_meta(conn, META_POLICY_GEN, "0")),
        "schema_version": get_meta(conn, META_SCHEMA_VERSION),
    }


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _rollback_quiet(conn) -> None:
    try:
        conn.execute("ROLLBACK")
    except sqlite3.Error:
        pass


def load_batch(db_path, batch_id, stream) -> dict:
    """Stream one JSONL batch file into the warehouse.

    `stream` must yield raw lines as bytes.  Returns a result dict with
    the allocated (or previously allocated, for idempotent re-sends)
    warehouse version.  Raises BatchError / LineError / OSError on
    failure; the visible version is never advanced by a failed batch.
    """
    conn = connect_warehouse(db_path, create=True)
    try:
        conn.execute(
            "CREATE TEMP TABLE stage("
            "region TEXT, collector TEXT, generation TEXT, ts INTEGER,"
            " revision INTEGER, cuts TEXT, counts TEXT, total TEXT,"
            " line_no INTEGER)")
        hasher = hashlib.sha256()
        n_lines = 0
        buf = []
        # Parse + stage outside the write transaction: no lock is held
        # while the (possibly large) file is read.
        for line_no, raw in enumerate(stream, 1):
            hasher.update(raw)
            cp = parse_checkpoint(raw, line_no)
            buf.append((cp.region, cp.collector, cp.generation, cp.ts,
                        cp.revision, cp.cuts_json, cp.counts_json,
                        cp.total_str, line_no))
            n_lines += 1
            if len(buf) >= _CHUNK:
                conn.executemany(_STAGE_SQL, buf)
                buf.clear()
        if buf:
            conn.executemany(_STAGE_SQL, buf)
            buf.clear()
        digest = hasher.hexdigest()

        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT file_sha256, version FROM batch WHERE batch_id = ?",
                (batch_id,)).fetchone()
            if row is not None:
                if row[0] != digest:
                    raise BatchError(
                        f"batch {batch_id!r} was already loaded from a "
                        f"different file (warehouse version {row[1]}); "
                        f"refusing to reuse the batch id")
                conn.execute("ROLLBACK")
                return {"batch": batch_id, "version": row[1], "reused": True,
                        "lines": n_lines, "new_checkpoints": 0,
                        "duplicate_checkpoints": 0,
                        "retransmitted_evicted": 0}

            # Collapse identical retransmissions inside the batch,
            # keeping the first line number.
            conn.execute(
                "CREATE TEMP TABLE stage_dedup AS "
                "SELECT region, collector, generation, ts, revision, cuts,"
                "       counts, total, MIN(line_no) AS line_no "
                "FROM stage "
                "GROUP BY region, collector, generation, ts, revision,"
                "         cuts, counts, total")
            bad = conn.execute(
                "SELECT region, collector, generation, ts, revision "
                "FROM stage_dedup "
                "GROUP BY region, collector, generation, ts, revision "
                "HAVING COUNT(*) > 1 LIMIT 1").fetchone()
            if bad is not None:
                raise BatchError(
                    "conflicting contents within batch for checkpoint "
                    f"region={bad[0]!r} collector={bad[1]!r} "
                    f"generation={bad[2]!r} ts={bad[3]} revision={bad[4]}")
            bad = conn.execute(
                "SELECT s.region, s.collector, s.generation, s.ts, s.revision,"
                "       p.batch_id, p.line_no "
                "FROM stage_dedup s JOIN checkpoint p ON " + _KEY_JOIN +
                " WHERE s.cuts <> p.cuts OR s.counts <> p.counts"
                "    OR s.total <> p.total LIMIT 1").fetchone()
            if bad is not None:
                raise BatchError(
                    "checkpoint region={!r} collector={!r} generation={!r} "
                    "ts={} revision={} conflicts with the revision already "
                    "loaded by batch {!r} line {}".format(*bad))
            # Revisions whose payload was evicted keep only an acceptance
            # receipt.  An equivalent retransmission is a duplicate and
            # must never resurrect the row; differing content is rejected
            # while naming its first provenance.
            conflicts = _check_evicted_retransmissions(conn)
            if conflicts is not None:
                raise BatchError(conflicts)
            total_staged = conn.execute(
                "SELECT COUNT(*) FROM stage_dedup").fetchone()[0]
            version = get_current_version(conn) + 1
            cur = conn.execute(
                "INSERT INTO checkpoint(region, collector, generation, ts,"
                " revision, cuts, counts, total, batch_id, line_no, version)"
                " SELECT s.region, s.collector, s.generation, s.ts,"
                "        s.revision, s.cuts, s.counts, s.total, ?, s.line_no, ?"
                " FROM stage_dedup s"
                " WHERE NOT EXISTS(SELECT 1 FROM checkpoint p WHERE "
                + _KEY_JOIN + ")"
                "   AND NOT EXISTS(SELECT 1 FROM receipt r WHERE "
                " r.kind = 'content' AND r.region = s.region"
                " AND r.collector = s.collector"
                " AND r.generation = s.generation AND r.ts = s.ts"
                " AND r.revision = s.revision)",
                (batch_id, version))
            new_rows = cur.rowcount
            retransmitted = conn.execute(
                "SELECT COUNT(*) FROM stage_dedup s JOIN receipt r"
                " ON r.kind = 'content' AND r.region = s.region"
                " AND r.collector = s.collector"
                " AND r.generation = s.generation AND r.ts = s.ts"
                " AND r.revision = s.revision"
                " WHERE NOT EXISTS(SELECT 1 FROM checkpoint p WHERE "
                + _KEY_JOIN + ")").fetchone()[0]
            conn.execute(
                "INSERT INTO batch(batch_id, file_sha256, version, loaded_at,"
                " rows) VALUES (?,?,?,?,?)",
                (batch_id, digest, version, _utcnow(), new_rows))
            conn.execute(
                "INSERT INTO meta(key, value) VALUES('current_version', ?)"
                " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (str(version),))
            conn.execute("COMMIT")
        except BaseException:
            _rollback_quiet(conn)
            raise
        return {"batch": batch_id, "version": version, "reused": False,
                "lines": n_lines, "new_checkpoints": new_rows,
                "duplicate_checkpoints": total_staged - new_rows
                - retransmitted,
                "intra_batch_duplicates": n_lines - total_staged,
                "retransmitted_evicted": retransmitted}
    finally:
        conn.close()


def _check_evicted_retransmissions(conn):
    """Compare staged revisions that only have a content receipt.

    Returns None when every such retransmission is byte-equivalent to the
    originally accepted payload, else a BatchError message naming the
    first provenance.  Only rows without a live checkpoint row are
    considered, so the cost is proportional to retransmissions of evicted
    revisions, not to the size of the batch or the warehouse.
    """
    rows = conn.execute(
        "SELECT s.region, s.collector, s.generation, s.ts, s.revision,"
        "       s.cuts, s.counts, s.total, r.first_batch, r.first_line,"
        "       r.first_version, r.content_sha256"
        " FROM stage_dedup s JOIN receipt r"
        " ON r.kind = 'content' AND r.region = s.region"
        " AND r.collector = s.collector"
        " AND r.generation = s.generation AND r.ts = s.ts"
        " AND r.revision = s.revision"
        " WHERE NOT EXISTS(SELECT 1 FROM checkpoint p WHERE "
        + _KEY_JOIN + ")").fetchall()
    for (region, collector, generation, ts, revision, cuts, counts, total,
         first_batch, first_line, first_version, sha) in rows:
        if payload_sha256(region, collector, generation, ts, revision,
                          cuts, counts, total) != sha:
            return (
                "checkpoint region={!r} collector={!r} generation={!r} "
                "ts={} revision={} conflicts with the revision first loaded"
                " by batch {!r} line {} (warehouse version {}) whose"
                " payload has since been retired".format(
                    region, collector, generation, ts, revision,
                    first_batch, first_line, first_version))
    return None


def list_batches(db_path) -> dict:
    conn = connect_warehouse(db_path, create=False)
    try:
        rows = conn.execute(
            "SELECT batch_id, version, file_sha256, loaded_at, rows"
            " FROM batch ORDER BY version").fetchall()
        return {
            "current_version": get_current_version(conn),
            "batches": [
                {"batch_id": r[0], "version": r[1], "file_sha256": r[2],
                 "loaded_at": r[3], "new_checkpoints": r[4]}
                for r in rows
            ],
        }
    finally:
        conn.close()
