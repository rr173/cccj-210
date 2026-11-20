"""Shared helpers for the histoware test suite."""
import contextlib
import io
import json
import sqlite3

from histoware.cli import main as cli_main


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
