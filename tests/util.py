"""Shared helpers for the histoware test suite."""
import contextlib
import io
import json
import sqlite3

from histoware.cli import main as cli_main
from histoware.summary import RESULT_SCHEMA, RESULT_SCHEMA_VERSION
from histoware.common import TOOL_VERSION


def cp(region, collector, generation, ts, revision, cuts, counts, total):
    """Build one checkpoint JSONL record as a dict."""
    return {"region": region, "collector": collector,
            "generation": generation, "ts": ts, "revision": revision,
            "cuts_ms": cuts, "counts": counts, "total": total}


def duty(region, collector, generation, start, end):
    return {"region": region, "collector": collector,
            "generation": generation, "start": start, "end": end}


def shift(name, region, start, end, cuts):
    return {"name": name, "region": region, "start": start, "end": end,
            "cuts_ms": cuts}


def write_jsonl(path, lines):
    """Write dicts (JSON-encoded) or raw strings, one per line."""
    with open(path, "w", encoding="utf-8") as fh:
        for line in lines:
            if isinstance(line, str):
                fh.write(line + "\n")
            else:
                fh.write(json.dumps(line) + "\n")
    return path


def run_cli(*argv):
    """Run the CLI in-process; returns (exit_code, stdout, stderr)."""
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        rc = cli_main([str(a) for a in argv])
    return rc, out.getvalue(), err.getvalue()


def run_cli_json(*argv):
    rc, out, err = run_cli(*argv)
    assert rc == 0, f"cli failed: {err}"
    return json.loads(out)


def load_batch(db, batch, path):
    return run_cli_json("load", "--db", str(db), "--batch", batch,
                        "--file", str(path))


def current_version(db):
    conn = sqlite3.connect(str(db))
    try:
        row = conn.execute(
            "SELECT value FROM meta WHERE key='current_version'").fetchone()
        return int(row[0])
    except sqlite3.OperationalError:
        return 0        # warehouse file never created
    finally:
        conn.close()


def table_rows(db, sql, args=()):
    conn = sqlite3.connect(str(db))
    try:
        return conn.execute(sql, args).fetchall()
    finally:
        conn.close()


def fetch_result(db_path):
    """Dump a result database into plain dicts for comparison."""
    conn = sqlite3.connect(str(db_path))
    try:
        shifts = {}
        for row in conn.execute(
                "SELECT name, region, start_ts, end_ts, status,"
                " due_intersections, covered_intersections, total,"
                " covered_total FROM shift"):
            shifts[row[0]] = row[1:]
        shift_cuts = {}
        for row in conn.execute(
                "SELECT shift, cut_ms, lo, hi, exact, covered_lo,"
                " covered_hi FROM shift_cut"):
            shift_cuts[(row[0], row[1])] = row[2:]
        intersections = {}
        id2key = {}
        for row in conn.execute(
                "SELECT id, shift, collector, generation, start_ts, end_ts,"
                " status, conflict_cut_ms, start_revision, start_batch,"
                " start_line, start_total, end_revision, end_batch,"
                " end_line, end_total, total_delta FROM intersection"):
            key = (row[1], row[2], row[3], row[4], row[5])
            id2key[row[0]] = key
            intersections[key] = row[6:]
        icuts = {}
        for row in conn.execute(
                "SELECT intersection_id, cut_ms, lo, hi, s_lo_cut,"
                " s_lo_count, s_hi_cut, s_hi_count, e_lo_cut, e_lo_count,"
                " e_hi_cut, e_hi_count FROM intersection_cut"):
            icuts[(id2key[row[0]], row[1])] = row[2:]
        meta = dict(conn.execute("SELECT key, value FROM meta"))
        return {"shift": shifts, "shift_cut": shift_cuts,
                "intersection": intersections,
                "intersection_cut": icuts, "meta": meta}
    finally:
        conn.close()


def make_result_db(path, shifts, meta_extra=None):
    """Synthesise a summary result database directly.

    `shifts` is a list of dicts describing one shift row:

      {
        "name", "region", "start_ts", "end_ts",
        "status": complete|partial|no_duty,
        "cuts": {cut_ms: (lo, hi)} or None for partial shifts
                 (covered bounds may be passed via "covered"),
        "intersections": [
          {"id", "collector", "generation", "start_ts", "end_ts",
           "status": ok|missing_start|missing_end|missing_both|conflict,
           "total_delta": str/int or None,
           "start_total","end_total": optional str/int,
           "reason","conflict_cut": optional conflict fields,
           "cuts": {cut_ms: (lo, hi) or None to publish evidence}},
          ...
        ],
      }

    This deliberately bypasses the warehouse so attribution tests can
    exercise arbitrary exact/interval combinations.  Counts are written
    exactly as given (decimal TEXT), never via floating point.
    """
    conn = sqlite3.connect(str(path))
    try:
        conn.executescript(RESULT_SCHEMA)
        next_iid = 1
        meta = {"schema_version": RESULT_SCHEMA_VERSION,
                "tool": f"histoware {TOOL_VERSION}",
                "warehouse_version": "1",
                "warehouse_current_version_at_run": "1",
                "roster_sha256": "0" * 64,
                "request_sha256": "0" * 64,
                "created_utc": "2026-11-20T00:00:00+00:00"}
        if meta_extra:
            meta.update(meta_extra)
        for key, value in meta.items():
            conn.execute("INSERT INTO meta(key, value) VALUES (?,?)",
                         (key, value))
        for sh in shifts:
            ivs = sh.get("intersections", [])
            id_map = {}
            due = len(ivs)
            ok = sum(1 for iv in ivs if iv["status"] == "ok")
            covered_total = sum(int(iv["total_delta"]) for iv in ivs
                                if iv["status"] == "ok")
            complete = sh["status"] == "complete"
            total = str(covered_total) if complete else None
            conn.execute(
                "INSERT INTO shift(name, region, start_ts, end_ts, status,"
                " due_intersections, covered_intersections, total,"
                " covered_total) VALUES (?,?,?,?,?,?,?,?,?)",
                (sh["name"], sh["region"], sh.get("start_ts", 0),
                 sh.get("end_ts", 3600), sh["status"], due, ok,
                 total, str(covered_total)))
            for cut, bounds in (sh.get("cuts") or {}).items():
                if bounds is None:
                    conn.execute(
                        "INSERT INTO shift_cut(shift, cut_ms, lo, hi, exact,"
                        " covered_lo, covered_hi) VALUES (?,?,?,?,?,?,?)",
                        (sh["name"], cut, None, None, None, "0", "0"))
                    continue
                lo, hi = bounds
                covered = sh.get("covered", {}).get(cut, bounds)
                clo, chi = covered
                conn.execute(
                    "INSERT INTO shift_cut(shift, cut_ms, lo, hi, exact,"
                    " covered_lo, covered_hi) VALUES (?,?,?,?,?,?,?)",
                    (sh["name"], cut,
                     str(lo) if complete else None,
                     str(hi) if complete else None,
                     1 if complete and lo == hi else (0 if complete else None),
                     str(clo), str(chi)))
            for iv in ivs:
                iid = next_iid
                next_iid += 1
                id_map[iv["id"]] = iid
                conn.execute(
                    "INSERT INTO intersection(id, shift, region, collector,"
                    " generation, start_ts, end_ts, status, conflict_reason,"
                    " conflict_cut_ms, start_revision, start_batch,"
                    " start_line, start_total, end_revision, end_batch,"
                    " end_line, end_total, total_delta)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (iid, sh["name"], sh["region"], iv["collector"],
                     iv.get("generation", "g0"), iv.get("start_ts", 0),
                     iv.get("end_ts", 3600), iv["status"],
                     iv.get("reason"), iv.get("conflict_cut"),
                     iv.get("start_revision", 0),
                     iv.get("start_batch", "b1"),
                     iv.get("start_line", iid * 2 - 1),
                     None if iv.get("start_total") is None
                     else str(iv["start_total"]),
                     iv.get("end_revision", 0),
                     iv.get("end_batch", "b1"),
                     iv.get("end_line", iid * 2),
                     None if iv.get("end_total") is None
                     else str(iv["end_total"]),
                     None if iv.get("total_delta") is None
                     else str(iv["total_delta"])))
                for cut, bounds in (iv.get("cuts") or {}).items():
                    if bounds is None:
                        continue
                    lo, hi = bounds
                    conn.execute(
                        "INSERT INTO intersection_cut(intersection_id,"
                        " cut_ms, lo, hi, s_lo_cut, s_lo_count, s_hi_cut,"
                        " s_hi_count, e_lo_cut, e_lo_count, e_hi_cut,"
                        " e_hi_count) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                        (iid, cut, str(lo), str(hi),
                         0, "0", None, "0", 0, "0", None, "0"))
        conn.commit()
    finally:
        conn.close()
    return path


def write_pairs(path, pairs):
    """Write a pairing manifest; each item needs id/baseline_shift/
    candidate_shift/cut_ms."""
    lines = [{"id": p[0], "baseline_shift": p[1], "candidate_shift": p[2],
              "cut_ms": p[3]} for p in pairs]
    return write_jsonl(path, lines)


def fetch_comparison(db_path):
    """Dump a comparison database into plain dicts for assertions."""
    conn = sqlite3.connect(str(db_path))
    try:
        out = {}
        for table in ("pair", "pair_issue", "pair_collector",
                      "pair_intersection", "pair_request", "input_file"):
            out[table] = conn.execute(
                f"SELECT * FROM {table}").fetchall()
        out["meta"] = dict(conn.execute("SELECT key, value FROM meta"))
        return out
    finally:
        conn.close()
