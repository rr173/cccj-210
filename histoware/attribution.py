"""Shift regression attribution.

Given two already-built summary result databases (never the raw
checkpoint warehouse and never a recomputation of either shift summary)
and an explicit pairing manifest, build a self-contained SQLite
comparison database that attributes the change in over-threshold request
counts between a baseline shift and a candidate shift.

Pairing rules
-------------
* The manifest names, per pair, the baseline shift, the candidate
  shift and the exact cut point to compare.  Matching is by shift name
  plus the exact cut and the exact region; row order and name
  similarity are never used.
* The two sides must actually contain the same cut and the same
  region, otherwise the pair is recorded as not comparable with the
  precise reason.
* Within a pair, collectors are aligned by region + collector identity
  using the intersection detail stored in the result databases.  A
  collector is usable only when every one of its due intersections is
  usable ('ok') on both sides and carries bounds for the requested cut.

Interval arithmetic
-------------------
For a collector, over-threshold requests at cut c are

    over = total_requests - requests_strictly_below_c

Each side therefore contributes an interval [total - below_hi,
total - below_lo]; the candidate-minus-baseline change interval is the
independent interval difference [cand.lo - base.hi, cand.hi - base.lo].
Bounds keep their original meaning: midpoints are never treated as
observations and the two sides' uncertainties are never assumed to
cancel.  Everything stays in arbitrary-precision integers stored as
decimal TEXT; percentage-point changes are stored as exact
numerator/denominator fractions, never binary floats.

A collector change interval gets one of three verdicts:
* 'confirmed' when its lower bound is strictly positive;
* 'possible'  when its upper bound is positive but its lower bound is
               not strictly positive;
* 'unproven'  when its upper bound is not positive.

Comparability
-------------
Zero denominators, incomplete coverage on either side, mismatched
collector sets, missing cuts, missing shifts or region mismatches never
produce a confident overall attribution: the pair is marked
'not_comparable' or 'partial', with machine-readable reasons, the
comparable scope and every source intersection preserved.  Collector
intervals always sum to the interval of the scope they cover; anything
outside the comparable scope is reported as an explicit unallocated
interval and never spread onto a collector.

Atomicity
---------
The comparison database is built in a temporary file and atomically
renamed over the target only after a full successful commit, so an
invalid input, a pairing conflict or a write failure can never replace
an existing database with partial results.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from collections import OrderedDict
from datetime import datetime, timezone
from fractions import Fraction
from typing import NamedTuple

from .common import (HistowareError, TOOL_VERSION, parse_pair)

ATTRIBUTION_SCHEMA_VERSION = "1"

# Pair statuses.
COMPARABLE = "comparable"
PARTIAL = "partial"
NOT_COMPARABLE = "not_comparable"

# Collector verdicts.
CONFIRMED = "confirmed"
POSSIBLE = "possible"
UNPROVEN = "unproven"

SIDES = ("baseline", "candidate")

ATTRIBUTION_SCHEMA = """
CREATE TABLE meta(
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
-- One row per input: the two result databases and the manifest.
CREATE TABLE input_file(
  role            TEXT PRIMARY KEY
                  CHECK(role IN ('baseline','candidate','manifest')),
  path            TEXT NOT NULL,
  sha256          TEXT NOT NULL,
  size_bytes      INTEGER NOT NULL,
  schema_version  TEXT,
  tool            TEXT,
  source_meta_json TEXT
);
-- The explicit pairing requests, verbatim (canonical raw JSON kept).
CREATE TABLE pair_request(
  pair_id         TEXT PRIMARY KEY,
  line_no         INTEGER NOT NULL,
  baseline_shift  TEXT NOT NULL,
  candidate_shift  TEXT NOT NULL,
  cut_ms          INTEGER NOT NULL,
  raw_json        TEXT NOT NULL
);
-- One row per evaluated pair.  Full-shift totals/over-threshold values
-- are NULL unless both source shifts are complete and carry the cut;
-- scoped_* values always describe the comparable collector subset
-- (zero intervals when the subset is empty).  All counts are decimal
-- TEXT; percentage-point bounds are exact fractions.
CREATE TABLE pair(
  pair_id            TEXT PRIMARY KEY REFERENCES pair_request(pair_id),
  cut_ms             INTEGER NOT NULL,
  baseline_shift     TEXT NOT NULL,
  candidate_shift    TEXT NOT NULL,
  baseline_region    TEXT,
  candidate_region   TEXT,
  status             TEXT NOT NULL
                     CHECK(status IN ('comparable','partial',
                                      'not_comparable')),
  baseline_status    TEXT,
  candidate_status   TEXT,
  baseline_total     TEXT,
  candidate_total    TEXT,
  baseline_over_lo   TEXT,
  baseline_over_hi   TEXT,
  candidate_over_lo  TEXT,
  candidate_over_hi  TEXT,
  change_lo          TEXT,
  change_hi          TEXT,
  exact              INTEGER,
  scoped_baseline_total    TEXT NOT NULL,
  scoped_candidate_total   TEXT NOT NULL,
  scoped_baseline_over_lo  TEXT NOT NULL,
  scoped_baseline_over_hi  TEXT NOT NULL,
  scoped_candidate_over_lo TEXT NOT NULL,
  scoped_candidate_over_hi TEXT NOT NULL,
  scoped_change_lo         TEXT NOT NULL,
  scoped_change_hi         TEXT NOT NULL,
  unallocated_lo    TEXT,
  unallocated_hi    TEXT,
  pp_change_lo_num  TEXT,
  pp_change_lo_den  TEXT,
  pp_change_hi_num  TEXT,
  pp_change_hi_den  TEXT,
  scoped_pp_change_lo_num TEXT,
  scoped_pp_change_lo_den TEXT,
  scoped_pp_change_hi_num TEXT,
  scoped_pp_change_hi_den TEXT,
  baseline_collectors    INTEGER NOT NULL,
  candidate_collectors   INTEGER NOT NULL,
  comparable_collectors  INTEGER NOT NULL
);
-- Machine-readable reasons behind every status decision.
CREATE TABLE pair_issue(
  pair_id TEXT NOT NULL REFERENCES pair(pair_id),
  code    TEXT NOT NULL,
  detail  TEXT
);
CREATE INDEX idx_pair_issue_pair ON pair_issue(pair_id);
-- One row per collector appearing on either side of a pair.  Rows for
-- non-eligible collectors keep the due/usable intersection counts but
-- carry no interval and no verdict.
CREATE TABLE pair_collector(
  pair_id        TEXT NOT NULL REFERENCES pair(pair_id),
  collector      TEXT NOT NULL,
  in_baseline    INTEGER NOT NULL,
  in_candidate   INTEGER NOT NULL,
  eligible       INTEGER NOT NULL,
  baseline_due_intersections   INTEGER NOT NULL,
  baseline_ok_intersections    INTEGER NOT NULL,
  candidate_due_intersections  INTEGER NOT NULL,
  candidate_ok_intersections   INTEGER NOT NULL,
  baseline_over_lo   TEXT,
  baseline_over_hi   TEXT,
  candidate_over_lo  TEXT,
  candidate_over_hi  TEXT,
  change_lo          TEXT,
  change_hi          TEXT,
  exact              INTEGER,
  contribution       TEXT
                     CHECK(contribution IS NULL
                        OR contribution IN ('confirmed','possible',
                                            'unproven')),
  PRIMARY KEY(pair_id, collector)
);
CREATE INDEX idx_pair_collector_collector
  ON pair_collector(collector);
-- Every source intersection involved in a pair, on both sides, with
-- the source intersection id and full checkpoint lineage so any number
-- can be traced back into the originating result database.
CREATE TABLE pair_intersection(
  pair_id        TEXT NOT NULL REFERENCES pair(pair_id),
  side           TEXT NOT NULL CHECK(side IN ('baseline','candidate')),
  source_shift   TEXT NOT NULL,
  region         TEXT NOT NULL,
  collector      TEXT NOT NULL,
  generation     TEXT NOT NULL,
  start_ts       INTEGER NOT NULL,
  end_ts         INTEGER NOT NULL,
  source_intersection_id INTEGER NOT NULL,
  status         TEXT NOT NULL,
  conflict_reason TEXT,
  conflict_cut_ms INTEGER,
  start_revision INTEGER,
  start_batch    TEXT,
  start_line     INTEGER,
  start_total    TEXT,
  end_revision   INTEGER,
  end_batch      TEXT,
  end_line       INTEGER,
  end_total      TEXT,
  total_delta    TEXT,
  cut_ms         INTEGER NOT NULL,
  cut_lo         TEXT,
  cut_hi         TEXT
);
CREATE INDEX idx_pair_intersection_pair
  ON pair_intersection(pair_id);
CREATE INDEX idx_pair_intersection_collector
  ON pair_intersection(collector);
"""

_INSERTS = {
    "input_file": "INSERT INTO input_file(role, path, sha256, size_bytes,"
                  " schema_version, tool, source_meta_json)"
                  " VALUES (?,?,?,?,?,?,?)",
    "pair_request": "INSERT INTO pair_request(pair_id, line_no,"
                    " baseline_shift, candidate_shift, cut_ms, raw_json)"
                    " VALUES (?,?,?,?,?,?)",
    "pair": "INSERT INTO pair VALUES (" + ",".join(["?"] * 39) + ")",
    "pair_issue": "INSERT INTO pair_issue(pair_id, code, detail)"
                  " VALUES (?,?,?)",
    "pair_collector": "INSERT INTO pair_collector VALUES ("
                      + ",".join(["?"] * 17) + ")",
    "pair_intersection": "INSERT INTO pair_intersection VALUES ("
                         + ",".join(["?"] * 24) + ")",
}

# Tables that every readable summary result database must contain.
_REQUIRED_SOURCE_TABLES = (
    "meta", "shift", "shift_cut", "intersection", "intersection_cut")

_FLUSH_AT = 4096


class AttributionError(HistowareError):
    """The attribution request is invalid or cannot be completed."""


class IntersectionData(NamedTuple):
    iid: int
    collector: str
    generation: str
    start_ts: int
    end_ts: int
    status: str
    conflict_reason: str
    conflict_cut_ms: int
    start_revision: int
    start_batch: str
    start_line: int
    start_total: str
    end_revision: int
    end_batch: str
    end_line: int
    end_total: str
    total_delta: str
    cut_lo: int          # None until cut bounds are attached / not ok
    cut_hi: int


class CollectorData(NamedTuple):
    due: int
    ok: int
    over_lo: int          # None unless every intersection is usable
    over_hi: int
    intersections: tuple


class ShiftData(NamedTuple):
    name: str
    region: str
    start_ts: int
    end_ts: int
    status: str
    due: int
    covered: int
    total: int                 # None unless complete
    covered_total: int
    cuts: dict                 # cut -> (lo, hi, covered_lo, covered_hi)
    collectors: dict           # collector -> CollectorData


def _file_digest(path):
    hasher = hashlib.sha256()
    size = 0
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            hasher.update(chunk)
            size += len(chunk)
    return hasher.hexdigest(), size


def _as_int(value):
    """Decimal TEXT -> arbitrary-precision int; None passes through."""
    if value is None:
        return None
    return int(value)


def _rate(num, den):
    """Exact percentage rate 100*num/den as a Fraction (den > 0)."""
    return Fraction(100 * num, den)


def _frac_parts(value):
    if value is None:
        return None, None
    return str(value.numerator), str(value.denominator)


def _pp_interval(cand_num_lo, cand_num_hi, cand_den,
                 base_num_lo, base_num_hi, base_den):
    """Percentage-point interval for a rate change.

    Rates are increasing in their numerator, so the lowest change uses
    the candidate lower bound against the baseline upper bound.
    Returns None when either denominator is zero.
    """
    if cand_den <= 0 or base_den <= 0:
        return None
    lo = (_rate(cand_num_lo, cand_den)
          - _rate(base_num_hi, base_den))
    hi = (_rate(cand_num_hi, cand_den)
          - _rate(base_num_lo, base_den))
    return lo, hi


def verdict(change_lo, change_hi):
    """Contribution label from a candidate-minus-baseline change interval."""
    if change_lo > 0:
        return CONFIRMED
    if change_hi > 0:
        return POSSIBLE
    return UNPROVEN


class ResultStore:
    """Read-only, validated access to one summary result database.

    The connection is opened in SQLite read-only mode, so attribution
    can never modify an input result database.  Shift loads are cached
    (the set of referenced shifts is bounded by the manifest size).
    """

    def __init__(self, path, role):
        self.path = path
        self.role = role
        if not os.path.exists(path):
            raise AttributionError(
                f"{role} result database not found: {path}")
        uri = ("file:" + path.replace("?", "%3F").replace("#", "%23")
               + "?mode=ro")
        try:
            self.conn = sqlite3.connect(uri, uri=True,
                                        isolation_level=None)
        except sqlite3.Error as exc:
            raise AttributionError(
                f"cannot open {role} result database {path}: {exc}") from exc
        self._cache = OrderedDict()
        self.meta = self._validate()

    def _validate(self):
        try:
            names = {
                r[0] for r in self.conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'")}
            missing = [t for t in _REQUIRED_SOURCE_TABLES if t not in names]
            if missing:
                raise AttributionError(
                    f"{self.role} result database {self.path!r} is missing "
                    f"table(s): {', '.join(missing)}; it does not look like "
                    "a histoware summary result database")
            return dict(self.conn.execute("SELECT key, value FROM meta"))
        except sqlite3.DatabaseError as exc:
            raise AttributionError(
                f"{self.role} result database {self.path!r} is not a valid"
                f" SQLite database: {exc}") from exc

    def get_shift(self, name):
        if name in self._cache:
            self._cache.move_to_end(name)
            return self._cache[name]
        data = self._load_shift(name)
        self._cache[name] = data
        return data

    def _load_shift(self, name):
        row = self.conn.execute(
            "SELECT name, region, start_ts, end_ts, status,"
            " due_intersections, covered_intersections, total,"
            " covered_total FROM shift WHERE name = ?", (name,)).fetchone()
        if row is None:
            return None
        (sname, region, start_ts, end_ts, status, due, covered,
         total, covered_total) = row
        cuts = {}
        for cut, lo, hi, _, covered_lo, covered_hi in self.conn.execute(
                "SELECT cut_ms, lo, hi, exact, covered_lo, covered_hi"
                " FROM shift_cut WHERE shift = ?", (name,)):
            cuts[cut] = (_as_int(lo), _as_int(hi),
                         _as_int(covered_lo), _as_int(covered_hi))
        i_rows = self.conn.execute(
            "SELECT id, collector, generation, start_ts, end_ts, status,"
            " conflict_reason, conflict_cut_ms, start_revision,"
            " start_batch, start_line, start_total, end_revision,"
            " end_batch, end_line, end_total, total_delta"
            " FROM intersection WHERE shift = ?"
            " ORDER BY id", (name,)).fetchall()
        per_collector = OrderedDict()
        intersections = []
        iv_by_id = {}
        for r in i_rows:
            (iid, collector, generation, its, ite, istatus, reason,
             conflict_cut, s_rev, s_batch, s_line, s_total, e_rev,
             e_batch, e_line, e_total, td) = r
            iv = IntersectionData(
                iid=iid, collector=collector, generation=generation,
                start_ts=its, end_ts=ite, status=istatus,
                conflict_reason=reason, conflict_cut_ms=conflict_cut,
                start_revision=s_rev, start_batch=s_batch,
                start_line=s_line, start_total=s_total,
                end_revision=e_rev, end_batch=e_batch, end_line=e_line,
                end_total=e_total, total_delta=td, cut_lo=None, cut_hi=None)
            intersections.append(iv)
            iv_by_id[iid] = iv
            cd = per_collector.setdefault(
                collector, {"due": 0, "ok": 0, "delta": 0, "iids": []})
            cd["due"] += 1
            if istatus == "ok":
                cd["ok"] += 1
                cd["delta"] += int(td)
            cd["iids"].append(iid)
        collectors = {}
        for collector, cd in per_collector.items():
            rows = tuple(iv_by_id[iid] for iid in cd["iids"])
            collectors[collector] = CollectorData(
                due=cd["due"], ok=cd["ok"], over_lo=None, over_hi=None,
                intersections=rows)
        return ShiftData(
            name=sname, region=region, start_ts=start_ts, end_ts=end_ts,
            status=status, due=due, covered=covered,
            total=_as_int(total), covered_total=int(covered_total),
            cuts=cuts, collectors=collectors)

    def attach_cut_bounds(self, shift_name, cut):
        """Map intersection id -> (lo, hi) for one cut of one shift.

        Only 'ok' intersections carry cut rows; every other status is
        absent from intersection_cut by construction.
        """
        rows = self.conn.execute(
            "SELECT ic.intersection_id, ic.lo, ic.hi"
            " FROM intersection_cut ic JOIN intersection i"
            " ON i.id = ic.intersection_id"
            " WHERE i.shift = ? AND ic.cut_ms = ?",
            (shift_name, cut)).fetchall()
        return {iid: (int(lo), int(hi)) for iid, lo, hi in rows}

    def close(self):
        self.conn.close()


class _Writer:
    """Chunked INSERT writer with exact written-row counters."""

    def __init__(self, conn, flush_at=_FLUSH_AT):
        self._conn = conn
        self._flush_at = flush_at
        self._buf = {table: [] for table in _INSERTS}
        self.rows_written = {table: 0 for table in _INSERTS}

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


def _over_interval(shift_total, cut_bounds):
    """[total - below_hi, total - below_lo]; keeps integer semantics."""
    below_lo, below_hi = cut_bounds
    return (shift_total - below_hi, shift_total - below_lo)


def _evaluate_pair(pair, base_store, cand_store):
    """Evaluate one pairing.  Returns (pair_row, issues, collector_rows,
    evidence_rows).  Never reads anything but the two result stores."""
    cut = pair.cut_ms
    issues = []
    base = base_store.get_shift(pair.baseline_shift)
    cand = cand_store.get_shift(pair.candidate_shift)

    if base is None:
        issues.append(("baseline_shift_missing", pair.baseline_shift))
    if cand is None:
        issues.append(("candidate_shift_missing", pair.candidate_shift))
    if base is None or cand is None:
        return _not_comparable(pair, base, cand, issues, cut, {}, {})
    if base.status == "no_duty":
        issues.append(("baseline_no_duty", pair.baseline_shift))
    if cand.status == "no_duty":
        issues.append(("candidate_no_duty", pair.candidate_shift))
    if base.region != cand.region:
        issues.append(("region_mismatch",
                       f"baseline region {base.region!r} != candidate "
                       f"region {cand.region!r}"))
    if cut not in base.cuts:
        issues.append(("baseline_cut_missing", str(cut)))
    if cut not in cand.cuts:
        issues.append(("candidate_cut_missing", str(cut)))
    if base.region != cand.region:
        return _not_comparable(pair, base, cand, issues, cut, {}, {})
    # A missing cut is only a hard blocker when the shift actually had
    # coverage; a no_duty shift is handled by the empty-scope path so
    # its evidence and the other side's intersections stay available.
    cut_hard_block = (
        (cut not in base.cuts and base.status != "no_duty")
        or (cut not in cand.cuts and cand.status != "no_duty"))
    if cut_hard_block:
        return _not_comparable(pair, base, cand, issues, cut, {}, {})

    base_cut_rows = base_store.attach_cut_bounds(base.name, cut)
    cand_cut_rows = cand_store.attach_cut_bounds(cand.name, cut)
    base_side = _build_side(base, base_cut_rows)
    cand_side = _build_side(cand, cand_cut_rows)
    return _compare(pair, base, cand, base_side, cand_side, issues, cut,
                    base_cut_rows, cand_cut_rows)


def _build_side(shift, cut_rows):
    """Return (collector -> (over_lo, over_hi) or None, intersection
    rows enriched with cut bounds).  A collector with any unusable
    intersection or a missing cut row maps to None."""
    enriched = {}
    for collector, cd in shift.collectors.items():
        over_lo = over_hi = None
        usable = cd.ok == cd.due and cd.due > 0
        if usable:
            sum_delta = sum(int(iv.total_delta) for iv in cd.intersections)
            blo = bhi = 0
            for iv in cd.intersections:
                if iv.iid not in cut_rows:
                    usable = False
                    break
                lo, hi = cut_rows[iv.iid]
                blo += lo
                bhi += hi
            if usable:
                over_lo, over_hi = _over_interval(sum_delta, (blo, bhi))
        enriched[collector] = (over_lo, over_hi)
    return enriched


def _not_comparable(pair, base, cand, issues, cut,
                    base_cut_rows, cand_cut_rows):
    """Build output rows for a pair that cannot be compared at all."""
    base_region = base.region if base else None
    cand_region = cand.region if cand else None
    base_status = base.status if base else None
    cand_status = cand.status if cand else None
    base_collectors = len(base.collectors) if base else 0
    cand_collectors = len(cand.collectors) if cand else 0
    pair_row = _pair_row(
        pair, cut, base_region, cand_region, NOT_COMPARABLE,
        base_status, cand_status,
        full_base_total=None, full_cand_total=None,
        full_base_over=None, full_cand_over=None, full_change=None,
        scoped=None, unallocated=None, full_pp=None, scoped_pp=None,
        n_base=base_collectors, n_cand=cand_collectors, n_eligible=0)
    evidence = _evidence_rows(pair, cut, base, cand,
                              base_cut_rows, cand_cut_rows)
    return pair_row, issues, [], evidence


def _compare(pair, base, cand, base_side, cand_side, issues, cut,
             base_cut_rows, cand_cut_rows):
    base_collectors = set(base.collectors)
    cand_collectors = set(cand.collectors)
    only_base = base_collectors - cand_collectors
    only_cand = cand_collectors - base_collectors
    for collector in sorted(only_base):
        issues.append(("collector_only_baseline", collector))
    for collector in sorted(only_cand):
        issues.append(("collector_only_candidate", collector))

    eligible = []
    blocked = {}
    for collector in sorted(base_collectors & cand_collectors):
        b = base_side[collector]
        c = cand_side[collector]
        if b is None or c is None or b[0] is None or c[0] is None:
            reasons = []
            if b is None or b[0] is None:
                reasons.append("baseline")
            if c is None or c[0] is None:
                reasons.append("candidate")
            blocked[collector] = reasons
            issues.append(("collector_not_comparable",
                           f"{collector}: " + " and ".join(reasons)))
        else:
            eligible.append(collector)
    for collector in blocked:
        issues.append(("collector_excluded_from_scope", collector))

    if base.status != "complete":
        issues.append(("baseline_incomplete", base.status))
    if cand.status != "complete":
        issues.append(("candidate_incomplete", cand.status))

    # Scoped aggregate over eligible collectors (exact closure by sums).
    sb_tot = sc_tot = 0
    sb_lo = sb_hi = sc_lo = sc_hi = 0
    collector_rows = []
    for collector in sorted(base_collectors | cand_collectors):
        in_b = collector in base_collectors
        in_c = collector in cand_collectors
        b_cd = base.collectors.get(collector)
        c_cd = cand.collectors.get(collector)
        is_eligible = collector in eligible
        if is_eligible:
            blo, bhi = base_side[collector]
            clo, chi = cand_side[collector]
            sb_tot += sum(int(iv.total_delta)
                          for iv in b_cd.intersections)
            sc_tot += sum(int(iv.total_delta)
                          for iv in c_cd.intersections)
            sb_lo += blo
            sb_hi += bhi
            sc_lo += clo
            sc_hi += chi
            change_lo = clo - bhi
            change_hi = chi - blo
            label = verdict(change_lo, change_hi)
            exact = 1 if change_lo == change_hi else 0
        else:
            blo = bhi = clo = chi = None
            change_lo = change_hi = None
            label = None
            exact = None
        collector_rows.append((
            pair.pair_id, collector, int(in_b), int(in_c),
            int(is_eligible),
            b_cd.due if b_cd else 0, b_cd.ok if b_cd else 0,
            c_cd.due if c_cd else 0, c_cd.ok if c_cd else 0,
            _t(blo), _t(bhi), _t(clo), _t(chi),
            _t(change_lo), _t(change_hi), exact, label))

    scoped_change_lo = sc_lo - sb_hi
    scoped_change_hi = sc_hi - sb_lo
    scoped = dict(sb_tot=sb_tot, sc_tot=sc_tot, sb_lo=sb_lo, sb_hi=sb_hi,
                  sc_lo=sc_lo, sc_hi=sc_hi,
                  change_lo=scoped_change_lo, change_hi=scoped_change_hi)

    full_base_total = base.total if base.status == "complete" else None
    full_cand_total = cand.total if cand.status == "complete" else None
    full_base_over = full_cand_over = full_change = None
    unallocated = None
    full_pp = scoped_pp = None

    # Full-shift over-threshold intervals, only when each side is
    # complete and the cut is present (checked by the caller).
    full_ok = base.status == "complete" and cand.status == "complete"
    if full_ok:
        b_below = base.cuts[cut][:2]
        c_below = cand.cuts[cut][:2]
        full_base_over = _over_interval(base.total, b_below)
        full_cand_over = _over_interval(cand.total, c_below)
        full_change = (full_cand_over[0] - full_base_over[1],
                       full_cand_over[1] - full_base_over[0])
        full_pp = _pp_interval(
            full_cand_over[0], full_cand_over[1], cand.total,
            full_base_over[0], full_base_over[1], base.total)
    # Scoped percentage-point change; NULL when a scoped denominator is 0.
    scoped_pp = _pp_interval(sc_lo, sc_hi, sc_tot,
                             sb_lo, sb_hi, sb_tot)
    if scoped_pp is None and eligible:
        issues.append(("scoped_zero_denominator",
                       f"baseline scoped total {sb_tot}, candidate "
                       f"scoped total {sc_tot}"))

    hard_blockers = (base.region != cand.region
                     or (cut not in base.cuts and base.status != "no_duty")
                     or (cut not in cand.cuts and cand.status != "no_duty")
                     or not eligible)
    # The overall percentage-point change needs both full totals as
    # denominators; a zero on either side makes it undefined.  Kept
    # symmetric so swapping the two sides never changes comparability.
    zero_denominator = full_ok and (base.total == 0 or cand.total == 0)
    if hard_blockers:
        status = NOT_COMPARABLE
        if not eligible:
            issues.append(("empty_comparable_scope",
                           "no collector usable on both sides"))
    elif full_ok and not only_base and not only_cand and not blocked \
            and not zero_denominator:
        status = COMPARABLE
    else:
        status = PARTIAL
        if zero_denominator:
            # A zero denominator makes the overall share undefined:
            # never publish a confident overall attribution.
            status = NOT_COMPARABLE
            zero_side = "baseline" if base.total == 0 else "candidate"
            issues.append(("zero_denominator",
                           f"{zero_side} shift total is 0; full-shift "
                           "percentage-point change is undefined"))

    # On a comparable pair every collector is in scope, so the summed
    # collector intervals equal the full-shift interval and the
    # unallocated remainder is exactly zero.  When the scope is
    # restricted (partial), the remainder is the conservative interval
    # difference; its own uncertainty cannot be assigned to a collector.
    if status == COMPARABLE:
        unallocated = (0, 0)
    elif full_ok:
        unallocated = (full_change[0] - scoped_change_hi,
                       full_change[1] - scoped_change_lo)
    else:
        unallocated = None

    pair_row = _pair_row(
        pair, cut, base.region, cand.region, status,
        base.status, cand.status,
        full_base_total, full_cand_total,
        full_base_over, full_cand_over, full_change,
        scoped, unallocated, full_pp, scoped_pp,
        len(base_collectors), len(cand_collectors), len(eligible))
    evidence = _evidence_rows(
        pair, cut, base, cand, base_cut_rows, cand_cut_rows)
    return pair_row, issues, collector_rows, evidence


def _t(value):
    return None if value is None else str(value)


def _pair_row(pair, cut, base_region, cand_region, status,
              base_status, cand_status,
              full_base_total, full_cand_total,
              full_base_over, full_cand_over, full_change,
              scoped, unallocated, full_pp, scoped_pp,
              n_base, n_cand, n_eligible):
    if full_change is None:
        change_lo = change_hi = None
        exact = None
    else:
        change_lo, change_hi = full_change
        exact = 1 if change_lo == change_hi else 0
    if scoped is None:
        scoped = dict(sb_tot=0, sc_tot=0, sb_lo=0, sb_hi=0,
                      sc_lo=0, sc_hi=0, change_lo=0, change_hi=0)
    pp_lo_n, pp_lo_d = _frac_parts(full_pp[0] if full_pp else None)
    pp_hi_n, pp_hi_d = _frac_parts(full_pp[1] if full_pp else None)
    spp_lo_n, spp_lo_d = _frac_parts(scoped_pp[0] if scoped_pp else None)
    spp_hi_n, spp_hi_d = _frac_parts(scoped_pp[1] if scoped_pp else None)
    return (
        pair.pair_id, cut, pair.baseline_shift, pair.candidate_shift,
        base_region, cand_region, status, base_status, cand_status,
        _t(full_base_total), _t(full_cand_total),
        _t(full_base_over[0]) if full_base_over else None,
        _t(full_base_over[1]) if full_base_over else None,
        _t(full_cand_over[0]) if full_cand_over else None,
        _t(full_cand_over[1]) if full_cand_over else None,
        _t(change_lo), _t(change_hi), exact,
        str(scoped["sb_tot"]), str(scoped["sc_tot"]),
        str(scoped["sb_lo"]), str(scoped["sb_hi"]),
        str(scoped["sc_lo"]), str(scoped["sc_hi"]),
        str(scoped["change_lo"]), str(scoped["change_hi"]),
        _t(unallocated[0]) if unallocated is not None else None,
        _t(unallocated[1]) if unallocated is not None else None,
        pp_lo_n, pp_lo_d, pp_hi_n, pp_hi_d,
        spp_lo_n, spp_lo_d, spp_hi_n, spp_hi_d,
        n_base, n_cand, n_eligible,
    )


def _evidence_rows(pair, cut, base, cand, base_cut_rows, cand_cut_rows):
    """Materialise every source intersection from both shifts.

    `cut_rows` map source intersection id to the (lo, hi) bounds the
    summary database published for this pair's cut; unusable
    intersections have no cut row by construction.
    """
    rows = []
    for side, shift, cut_rows in (
            ("baseline", base, base_cut_rows),
            ("candidate", cand, cand_cut_rows)):
        if shift is None:
            continue
        for cd in shift.collectors.values():
            for iv in cd.intersections:
                bounds = cut_rows.get(iv.iid)
                rows.append((
                    pair.pair_id, side, shift.name, shift.region,
                    iv.collector, iv.generation, iv.start_ts, iv.end_ts,
                    iv.iid, iv.status, iv.conflict_reason,
                    iv.conflict_cut_ms, iv.start_revision, iv.start_batch,
                    iv.start_line, iv.start_total, iv.end_revision,
                    iv.end_batch, iv.end_line, iv.end_total, iv.total_delta,
                    cut,
                    str(bounds[0]) if bounds else None,
                    str(bounds[1]) if bounds else None))
    rows.sort(key=lambda r: (r[1], r[3], r[4], r[8]))
    return rows

def _load_manifest(path):
    """Parse + validate the pairing manifest.  Raises on any invalid
    line or duplicate pair id; returns (requests, raw_records)."""
    requests = []
    raw_records = []
    seen = set()
    with open(path, "rb") as fh:
        for line_no, raw in enumerate(fh, 1):
            if not raw.strip():
                raise AttributionError(f"line {line_no}: blank line")
            # parse_pair -> load_json_object turns bad UTF-8 or JSON
            # into a HistowareError carrying the line number.
            req = parse_pair(raw, line_no)
            if req.pair_id in seen:
                raise AttributionError(
                    f"line {line_no}: duplicate pair id {req.pair_id!r}")
            seen.add(req.pair_id)
            requests.append(req)
            raw_records.append((line_no, raw.decode("utf-8").strip()))
    if not requests:
        raise AttributionError(
            f"pairing manifest {path!r} contains no pairs")
    return requests, raw_records


def _rollback_quiet(conn):
    try:
        conn.execute("ROLLBACK")
    except sqlite3.Error:
        pass


def run_attribution(baseline_path, candidate_path, manifest_path, out_path):
    """Build the comparison database; return the run statistics dict.

    All validation happens before the temporary output database is
    renamed, and the rename is atomic: a failure never replaces an
    existing target.
    """
    for label, path in (("baseline", baseline_path),
                        ("candidate", candidate_path),
                        ("manifest", manifest_path)):
        if not os.path.exists(path):
            raise AttributionError(f"{label} input not found: {path}")
    if os.path.abspath(out_path) in (
            os.path.abspath(baseline_path),
            os.path.abspath(candidate_path)):
        raise AttributionError(
            "output path must differ from both input result databases")
    manifest_digest, manifest_size = _file_digest(manifest_path)
    base_digest, base_size = _file_digest(baseline_path)
    cand_digest, cand_size = _file_digest(candidate_path)
    requests, raw_records = _load_manifest(manifest_path)

    base_store = ResultStore(baseline_path, "baseline")
    tmp_out = None
    try:
        cand_store = ResultStore(candidate_path, "candidate")
        try:
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
                out.executescript(ATTRIBUTION_SCHEMA)
                out.execute("BEGIN")
                writer = _Writer(out)
                stats = {"pairs": 0,
                         "statuses": {COMPARABLE: 0, PARTIAL: 0,
                                      NOT_COMPARABLE: 0},
                         "rows_written": None}
                writer.add("input_file", (
                    "baseline", baseline_path, base_digest, base_size,
                    base_store.meta.get("schema_version"),
                    base_store.meta.get("tool"),
                    json.dumps(base_store.meta, sort_keys=True)))
                writer.add("input_file", (
                    "candidate", candidate_path, cand_digest, cand_size,
                    cand_store.meta.get("schema_version"),
                    cand_store.meta.get("tool"),
                    json.dumps(cand_store.meta, sort_keys=True)))
                writer.add("input_file", (
                    "manifest", manifest_path, manifest_digest,
                    manifest_size, None, None, None))
                for req, (line_no, raw_text) in zip(requests, raw_records):
                    writer.add("pair_request", (
                        req.pair_id, line_no, req.baseline_shift,
                        req.candidate_shift, req.cut_ms, raw_text))
                for req in requests:
                    pair_row, issues, collector_rows, evidence = \
                        _evaluate_pair(req, base_store, cand_store)
                    writer.add("pair", pair_row)
                    status = pair_row[6]
                    stats["statuses"][status] += 1
                    stats["pairs"] += 1
                    for code, detail in issues:
                        writer.add("pair_issue",
                                   (req.pair_id, code,
                                    detail if detail is not None else ""))
                    for row in collector_rows:
                        writer.add("pair_collector", row)
                    for row in evidence:
                        writer.add("pair_intersection", row)
                writer.flush()
                stats["rows_written"] = dict(writer.rows_written)
                meta = {
                    "schema_version": ATTRIBUTION_SCHEMA_VERSION,
                    "tool": f"histoware {TOOL_VERSION}",
                    "created_utc": datetime.now(timezone.utc)
                    .isoformat(timespec="seconds"),
                    "baseline_sha256": base_digest,
                    "candidate_sha256": cand_digest,
                    "manifest_sha256": manifest_digest,
                    "stats_json": json.dumps(stats, sort_keys=True),
                }
                for key, value in meta.items():
                    out.execute(
                        "INSERT INTO meta(key, value) VALUES (?,?)",
                        (key, value))
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
            return stats
        finally:
            cand_store.close()
    finally:
        base_store.close()
        if tmp_out is not None:
            try:
                os.unlink(tmp_out)
            except OSError:
                pass
