"""Summary generation.

Given a warehouse version, a duty roster and a set of named shifts,
produce a self-contained SQLite result database.

Semantics
---------
* Each shift is intersected with the duty intervals of its region;
  every positive-length intersection needs the checkpoints at its exact
  start and end second, taken from the duty interval's generation, at
  the max revision visible at the chosen warehouse version.  Missing
  endpoints are never synthesised (no zero-fill, no nearest-row
  substitution); interior gaps do not matter.
* total_delta = end.total - start.total.  A negative delta, a
  non-monotonic diff at common cut points (implicit 0/+inf included),
  or an empty clipped interval for a query cut marks the intersection
  'conflict'; the revisions used are recorded, never silently replaced
  by older ones.
* For a query cut not present at both ends, each end's cumulative count
  is bracketed by its adjacent known cuts (implicit 0 -> count 0 and
  +inf -> total included); the delta interval
  [end.lo - start.hi, end.hi - start.lo] is clipped to
  [0, total_delta].  No interpolation, no bucket splitting, and a
  finite upper bound is never reported as exact.
* A shift is 'complete' only when every due intersection is usable;
  otherwise overall metrics stay NULL and only clearly marked covered
  subtotals (status 'partial') are published.  A shift with no duty
  coverage at all is 'no_duty', distinct from a complete zero-traffic
  shift.

Consistency / atomicity
-----------------------
All warehouse reads run inside one WAL snapshot transaction and are
additionally filtered by `version <= V`, so batches committed while a
summary runs can never leak newer endpoints into the result.  The
result database is written to a temporary file and atomically renamed
over the target only after a full successful commit, so a failure can
never replace an existing result with a partial one.

Resource bounds
---------------
* Endpoint lookups are O(log n) PRIMARY KEY probes (see warehouse.py),
  never history scans; a bounded LRU cache (default 65536 entries) lets
  adjacent shifts reuse shared endpoints.
* The duty roster lives in a file-backed TEMP table with its own index.
* Shifts are processed one at a time and result rows are flushed in
  fixed-size chunks; per-shift working memory is proportional to the
  number of duty intervals overlapping that one shift, never to the
  number of shifts or the checkpoint history length.
* Actual endpoint lookups / cache reuse and written row counts are
  reported in the run statistics (stdout and result meta 'stats_json').
"""
from __future__ import annotations

import bisect
import hashlib
import json
import os
import sqlite3
from collections import OrderedDict
from datetime import datetime, timezone
from typing import NamedTuple

from .common import HistowareError, parse_duty, parse_shift, TOOL_VERSION
from .warehouse import connect_warehouse, get_current_version

RESULT_SCHEMA_VERSION = "1"

RESULT_SCHEMA = """
CREATE TABLE meta(
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
-- One row per requested shift.  Counts are decimal TEXT (they may
-- exceed 2^63-1).  total/lo/hi are NULL unless status='complete';
-- covered_* always carry the subtotal over usable intersections and
-- are only authoritative when status='complete'.
CREATE TABLE shift(
  name                  TEXT PRIMARY KEY,
  region                TEXT NOT NULL,
  start_ts              INTEGER NOT NULL,
  end_ts                INTEGER NOT NULL,
  status                TEXT NOT NULL
                        CHECK(status IN ('complete','partial','no_duty')),
  due_intersections     INTEGER NOT NULL,
  covered_intersections INTEGER NOT NULL,
  total                 TEXT,
  covered_total         TEXT NOT NULL
);
CREATE TABLE shift_cut(
  shift       TEXT NOT NULL REFERENCES shift(name),
  cut_ms      INTEGER NOT NULL,
  lo          TEXT,          -- NULL unless shift complete
  hi          TEXT,          -- NULL unless shift complete
  exact       INTEGER,       -- 1 when lo=hi and shift complete
  covered_lo  TEXT NOT NULL, -- subtotal bounds over usable intersections
  covered_hi  TEXT NOT NULL,
  PRIMARY KEY(shift, cut_ms)
);
-- One row per due intersection (shift x duty-interval overlap).
CREATE TABLE intersection(
  id               INTEGER PRIMARY KEY,
  shift            TEXT NOT NULL,
  region           TEXT NOT NULL,
  collector        TEXT NOT NULL,
  generation       TEXT NOT NULL,
  start_ts         INTEGER NOT NULL,
  end_ts           INTEGER NOT NULL,
  status           TEXT NOT NULL
                   CHECK(status IN ('ok','missing_start','missing_end',
                                    'missing_both','conflict')),
  conflict_reason  TEXT,
  conflict_cut_ms  INTEGER,   -- cut involved in the conflict, if any
  start_revision   INTEGER,   -- endpoint lineage: generation is on this
  start_batch      TEXT,      -- row, revision/batch/line per end
  start_line       INTEGER,
  start_total      TEXT,
  end_revision     INTEGER,
  end_batch        TEXT,
  end_line         INTEGER,
  end_total        TEXT,
  total_delta      TEXT
);
CREATE INDEX idx_intersection_shift ON intersection(shift);
-- Per query cut, the raw adjacent cuts/counts used to bound each end.
-- Cut 0 is the implicit zero cut (count 0); a NULL cut is the implicit
-- +inf cut (count = that end's total).
CREATE TABLE intersection_cut(
  intersection_id INTEGER NOT NULL REFERENCES intersection(id),
  cut_ms          INTEGER NOT NULL,
  lo              TEXT,       -- NULL when the intersection is not ok
  hi              TEXT,
  s_lo_cut        INTEGER,
  s_lo_count      TEXT,
  s_hi_cut        INTEGER,
  s_hi_count      TEXT,
  e_lo_cut        INTEGER,
  e_lo_count      TEXT,
  e_hi_cut        INTEGER,
  e_hi_count      TEXT,
  PRIMARY KEY(intersection_id, cut_ms)
);
"""

_INSERTS = {
    "shift": "INSERT INTO shift(name, region, start_ts, end_ts, status,"
             " due_intersections, covered_intersections, total,"
             " covered_total) VALUES (?,?,?,?,?,?,?,?,?)",
    "shift_cut": "INSERT INTO shift_cut(shift, cut_ms, lo, hi, exact,"
                 " covered_lo, covered_hi) VALUES (?,?,?,?,?,?,?)",
    "intersection": "INSERT INTO intersection(id, shift, region, collector,"
                    " generation, start_ts, end_ts, status, conflict_reason,"
                    " conflict_cut_ms, start_revision, start_batch,"
                    " start_line, start_total, end_revision, end_batch,"
                    " end_line, end_total, total_delta)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
    "intersection_cut": "INSERT INTO intersection_cut(intersection_id,"
                        " cut_ms, lo, hi, s_lo_cut, s_lo_count, s_hi_cut,"
                        " s_hi_count, e_lo_cut, e_lo_count, e_hi_cut,"
                        " e_hi_count) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
}

_ENDPOINT_SQL = """
SELECT revision, cuts, counts, total, batch_id, line_no
FROM checkpoint
WHERE region = ? AND collector = ? AND generation = ? AND ts = ?
  AND version <= ?
ORDER BY revision DESC
LIMIT 1
"""


class SummaryError(HistowareError):
    """The summary request is invalid or cannot be completed."""


class Ckpt(NamedTuple):
    revision: int
    cuts: tuple
    counts: tuple
    total: int
    batch_id: str
    line_no: int


class EndpointIndex:
    """Exact-second endpoint lookups with a bounded LRU cache.

    Every miss is one O(log n) PRIMARY KEY probe against the checkpoint
    table; hits serve adjacent shifts that share an endpoint.  Memory
    is bounded by `capacity` parsed checkpoints.
    """

    def __init__(self, conn, version, capacity=65536):
        self._conn = conn
        self._version = version
        self._capacity = capacity
        self._cache = OrderedDict()
        self.sql_queries = 0
        self.cache_hits = 0

    def lookup(self, region, collector, generation, ts):
        key = (region, collector, generation, ts)
        try:
            value = self._cache[key]
            self._cache.move_to_end(key)
            self.cache_hits += 1
            return value
        except KeyError:
            pass
        self.sql_queries += 1
        row = self._conn.execute(
            _ENDPOINT_SQL,
            (region, collector, generation, ts, self._version)).fetchone()
        if row is None:
            value = None
        else:
            value = Ckpt(revision=row[0], cuts=tuple(json.loads(row[1])),
                         counts=tuple(json.loads(row[2])), total=int(row[3]),
                         batch_id=row[4], line_no=row[5])
        self._cache[key] = value
        if len(self._cache) > self._capacity:
            self._cache.popitem(last=False)
        return value


def bracket(ckpt: Ckpt, q: int):
    """Bracket the cumulative count at query cut `q`.

    Returns (lo_cut, lo_count, hi_cut, hi_count) where cut 0 is the
    implicit zero cut (count 0) and a None cut is the implicit +inf cut
    (count = total).  When `q` is an explicit cut the bracket is exact
    (lo == hi == q).
    """
    i = bisect.bisect_left(ckpt.cuts, q)
    if i < len(ckpt.cuts) and ckpt.cuts[i] == q:
        return (q, ckpt.counts[i], q, ckpt.counts[i])
    lo_cut, lo_count = (ckpt.cuts[i - 1], ckpt.counts[i - 1]) if i else (0, 0)
    hi_cut, hi_count = ((ckpt.cuts[i], ckpt.counts[i])
                        if i < len(ckpt.cuts) else (None, ckpt.total))
    return (lo_cut, lo_count, hi_cut, hi_count)


def compute_intersection(s: Ckpt, e: Ckpt, qcuts):
    """Evaluate one intersection with both endpoints present.

    Returns ("ok", total_delta, {q: (lo, hi, s_bracket, e_bracket)}) or
    ("conflict", reason, conflict_cut, evidence) where evidence is
    (q, s_bracket, e_bracket) for an empty-interval conflict and None
    otherwise.
    """
    td = e.total - s.total
    if td < 0:
        return ("conflict",
                f"total regressed: start total {s.total} > end total {e.total}",
                None, None)
    smap = dict(zip(s.cuts, s.counts))
    emap = dict(zip(e.cuts, e.counts))
    prev = 0
    for cut in sorted(set(smap) & set(emap)):
        d = emap[cut] - smap[cut]
        if d < prev:
            return ("conflict",
                    f"common-cut diff not monotonic at cut {cut} ms "
                    f"(diff {d} < previous {prev})",
                    cut, None)
        prev = d
    if td < prev:
        return ("conflict",
                f"total delta {td} below common-cut diff {prev}",
                None, None)
    out = {}
    for q in qcuts:
        sb = bracket(s, q)
        eb = bracket(e, q)
        lo = eb[1] - sb[3]
        hi = eb[3] - sb[1]
        if lo < 0:
            lo = 0
        if hi > td:
            hi = td
        if lo > hi:
            return ("conflict",
                    f"empty interval at query cut {q} ms "
                    f"([{eb[1] - sb[3]}, {eb[3] - sb[1]}] clipped to "
                    f"[0, {td}])",
                    q, (q, sb, eb))
        out[q] = (lo, hi, sb, eb)
    return ("ok", td, out)


class OutWriter:
    """Chunked INSERT writer; memory bounded by `flush_at` rows per table."""

    def __init__(self, conn, flush_at=4096):
        self._conn = conn
        self._flush_at = flush_at
        self._buf = {table: [] for table in _INSERTS}
        self.rows_written = {table: 0 for table in _INSERTS}
        self._intersection_id = 0

    def next_intersection_id(self):
        self._intersection_id += 1
        return self._intersection_id

    def add(self, table, row):
        buf = self._buf[table]
        buf.append(row)
        if len(buf) >= self._flush_at:
            self._flush(table)

    def _flush(self, table):
        buf = self._buf[table]
        if buf:
            self._conn.executemany(_INSERTS[table], buf)
            self.rows_written[table] += len(buf)
            buf.clear()

    def flush(self):
        for table in self._buf:
            self._flush(table)


def _file_sha256(path):
    hasher = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _rollback_quiet(conn):
    try:
        conn.execute("ROLLBACK")
    except sqlite3.Error:
        pass


def _load_roster(conn, path):
    """Load the duty roster into a file-backed TEMP table and validate it."""
    conn.execute(
        "CREATE TEMP TABLE duty("
        "region TEXT NOT NULL, collector TEXT NOT NULL,"
        " generation TEXT NOT NULL, start_ts INTEGER NOT NULL,"
        " end_ts INTEGER NOT NULL)")
    buf = []
    with open(path, "rb") as fh:
        for line_no, raw in enumerate(fh, 1):
            d = parse_duty(raw, line_no)
            buf.append((d.region, d.collector, d.generation, d.start, d.end))
            if len(buf) >= 1000:
                conn.executemany("INSERT INTO duty VALUES (?,?,?,?,?)", buf)
                buf.clear()
    if buf:
        conn.executemany("INSERT INTO duty VALUES (?,?,?,?,?)", buf)
    conn.execute("CREATE INDEX idx_duty_region ON duty(region, start_ts)")
    # Overlap check, streaming over the sorted rows (O(1) memory).
    prev_key = None
    max_end = -1
    for region, collector, start, end in conn.execute(
            "SELECT region, collector, start_ts, end_ts FROM duty"
            " ORDER BY region, collector, start_ts"):
        key = (region, collector)
        if key != prev_key:
            prev_key = key
            max_end = -1
        if start < max_end:
            raise SummaryError(
                f"overlapping duty intervals for region={region!r}"
                f" collector={collector!r}")
        if end > max_end:
            max_end = end


def _process_shift(conn, index, shift, writer, stats):
    duties = conn.execute(
        "SELECT collector, generation, start_ts, end_ts FROM duty"
        " WHERE region = ? AND start_ts < ? AND end_ts > ?"
        " ORDER BY start_ts, collector, generation",
        (shift.region, shift.end, shift.start))
    due = 0
    ok = 0
    sum_total = 0
    sum_lo = {q: 0 for q in shift.cuts}
    sum_hi = {q: 0 for q in shift.cuts}
    for collector, generation, ds, de in duties:
        a = ds if ds > shift.start else shift.start
        b = de if de < shift.end else shift.end
        if a >= b:
            continue
        due += 1
        s = index.lookup(shift.region, collector, generation, a)
        e = index.lookup(shift.region, collector, generation, b)
        iid = writer.next_intersection_id()
        base = (iid, shift.name, shift.region, collector, generation, a, b)
        if s is None or e is None:
            if s is None and e is None:
                status = "missing_both"
            elif s is None:
                status = "missing_start"
            else:
                status = "missing_end"
            writer.add("intersection", base + (
                status, None, None,
                s.revision if s else None,
                s.batch_id if s else None,
                s.line_no if s else None,
                str(s.total) if s else None,
                e.revision if e else None,
                e.batch_id if e else None,
                e.line_no if e else None,
                str(e.total) if e else None,
                None))
            stats["intersections"][status] += 1
            continue
        result = compute_intersection(s, e, shift.cuts)
        if result[0] == "conflict":
            _, reason, conflict_cut, evidence = result
            writer.add("intersection", base + (
                "conflict", reason, conflict_cut,
                s.revision, s.batch_id, s.line_no, str(s.total),
                e.revision, e.batch_id, e.line_no, str(e.total),
                str(e.total - s.total)))
            if evidence is not None:
                q, sb, eb = evidence
                writer.add("intersection_cut", (
                    iid, q, None, None,
                    sb[0], str(sb[1]), sb[2], str(sb[3]),
                    eb[0], str(eb[1]), eb[2], str(eb[3])))
            stats["intersections"]["conflict"] += 1
            continue
        _, td, cut_results = result
        ok += 1
        sum_total += td
        writer.add("intersection", base + (
            "ok", None, None,
            s.revision, s.batch_id, s.line_no, str(s.total),
            e.revision, e.batch_id, e.line_no, str(e.total), str(td)))
        for q in shift.cuts:
            lo, hi, sb, eb = cut_results[q]
            sum_lo[q] += lo
            sum_hi[q] += hi
            writer.add("intersection_cut", (
                iid, q, str(lo), str(hi),
                sb[0], str(sb[1]), sb[2], str(sb[3]),
                eb[0], str(eb[1]), eb[2], str(eb[3])))
        stats["intersections"]["ok"] += 1
    if due == 0:
        status = "no_duty"
    elif ok == due:
        status = "complete"
    else:
        status = "partial"
    writer.add("shift", (
        shift.name, shift.region, shift.start, shift.end, status, due, ok,
        str(sum_total) if status == "complete" else None, str(sum_total)))
    for q in shift.cuts:
        if status == "complete":
            lo, hi = sum_lo[q], sum_hi[q]
            writer.add("shift_cut", (
                shift.name, q, str(lo), str(hi), 1 if lo == hi else 0,
                str(lo), str(hi)))
        else:
            writer.add("shift_cut", (
                shift.name, q, None, None, None,
                str(sum_lo[q]), str(sum_hi[q])))
    stats["shifts"] += 1
    stats["intersections_due"] += due


def run_summary(db_path, version, roster_path, request_path, out_path):
    """Run one summary and return the run statistics dict."""
    roster_digest = _file_sha256(roster_path)
    request_digest = _file_sha256(request_path)
    wh = connect_warehouse(db_path, create=False)
    tmp_out = None
    try:
        wh.execute("PRAGMA temp_store = 1")
        _load_roster(wh, roster_path)
        # One snapshot for the whole run: batches committed while we
        # work are invisible, and the version<=V filter pins the view.
        wh.execute("BEGIN")
        try:
            current = get_current_version(wh)
            v = current if version is None else version
            if v < 0 or v > current:
                raise SummaryError(
                    f"warehouse version {v} not available"
                    f" (current version {current})")
            tmp_out = f"{out_path}.tmp-{os.getpid()}"
            try:
                os.unlink(tmp_out)
            except FileNotFoundError:
                pass
            out = sqlite3.connect(tmp_out, isolation_level=None)
            committed = False
            try:
                out.execute("PRAGMA journal_mode = OFF")
                out.execute("PRAGMA synchronous = OFF")
                out.executescript(RESULT_SCHEMA)
                out.execute("BEGIN")
                writer = OutWriter(out)
                index = EndpointIndex(wh, v)
                stats = {
                    "warehouse_version": v,
                    "shifts": 0,
                    "intersections_due": 0,
                    "intersections": {"ok": 0, "missing_start": 0,
                                      "missing_end": 0, "missing_both": 0,
                                      "conflict": 0},
                }
                meta = {
                    "schema_version": RESULT_SCHEMA_VERSION,
                    "tool": f"histoware {TOOL_VERSION}",
                    "warehouse_version": str(v),
                    "warehouse_current_version_at_run": str(current),
                    "roster_sha256": roster_digest,
                    "request_sha256": request_digest,
                    "created_utc": datetime.now(timezone.utc)
                    .isoformat(timespec="seconds"),
                }
                for key, value in meta.items():
                    out.execute("INSERT INTO meta(key, value) VALUES (?,?)",
                                (key, value))
                seen = set()
                with open(request_path, "rb") as fh:
                    for line_no, raw in enumerate(fh, 1):
                        shift = parse_shift(raw, line_no)
                        if shift.name in seen:
                            raise SummaryError(
                                f"line {line_no}: duplicate shift name"
                                f" {shift.name!r}")
                        seen.add(shift.name)
                        _process_shift(wh, index, shift, writer, stats)
                writer.flush()
                stats["endpoint_sql_queries"] = index.sql_queries
                stats["endpoint_cache_hits"] = index.cache_hits
                stats["rows_written"] = dict(writer.rows_written)
                out.execute(
                    "INSERT INTO meta(key, value) VALUES ('stats_json', ?)",
                    (json.dumps(stats, sort_keys=True),))
                out.execute("COMMIT")
                committed = True
            finally:
                if not committed:
                    _rollback_quiet(out)
                out.close()
                if not committed:
                    try:
                        os.unlink(tmp_out)
                    except OSError:
                        pass
                    tmp_out = None
            os.replace(tmp_out, out_path)
            tmp_out = None
            wh.execute("COMMIT")
            return stats
        except BaseException:
            _rollback_quiet(wh)
            raise
    finally:
        wh.close()
        if tmp_out is not None:
            try:
                os.unlink(tmp_out)
            except OSError:
                pass
