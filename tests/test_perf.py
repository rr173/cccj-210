"""Scale test: endpoint lookup work must depend on the number of due
intersections, not on total history length x shift count.

Generates a reproducible long history for many collectors and a set of
heavily overlapping shifts, then checks the reported lookup counters.
Large inputs and databases are created under pytest's tmp_path only.
"""
import json
import random
import sqlite3
import time

from util import fetch_result, run_cli, run_cli_json, write_jsonl

COLLECTORS = [f"c{i:02d}" for i in range(12)]
STEP = 300                      # one checkpoint every 5 minutes
SHORT_DAYS = 5
EXTRA_DAYS = 20                 # extra older history in the long warehouse
N_SHIFTS = 200
BASE = 1_700_000_000


def gen_segment(fh, collectors, start, days, seed):
    """Append one segment of history; cumulative counters restart at the
    segment start, so the same segment is byte-identical across files."""
    rng = random.Random(seed)
    for col in collectors:
        cum10 = cum50 = total = 0
        for i in range(days * 86400 // STEP):
            n = rng.randint(0, 50)
            n10 = rng.randint(0, n)
            n50 = rng.randint(n10, n)
            cum10 += n10
            cum50 += n50
            total += n
            fh.write(json.dumps(
                {"region": "r", "collector": col, "generation": "g0",
                 "ts": start + i * STEP, "revision": 0,
                 "cuts_ms": [10, 50], "counts": [cum10, cum50],
                 "total": total}) + "\n")


def make_shifts(seed, span):
    rng = random.Random(seed)
    shifts = []
    for i in range(N_SHIFTS):
        s = BASE + rng.randrange(0, span - 7200, STEP)
        e = s + rng.randrange(3600, 7200, STEP)
        shifts.append({"name": f"shift-{i:03d}", "region": "r",
                       "start": s, "end": e, "cuts_ms": [10, 50, 500]})
    return shifts


def test_lookup_work_independent_of_history_length(tmp_path):
    span = SHORT_DAYS * 86400
    roster = [{"region": "r", "collector": c, "generation": "g0",
               "start": BASE, "end": BASE + span} for c in COLLECTORS]
    roster_path = write_jsonl(tmp_path / "roster.jsonl", roster)
    request_path = write_jsonl(tmp_path / "request.jsonl",
                               make_shifts(99, span))

    results = {}
    # The recent window is one identical file/batch in both warehouses;
    # the long warehouse additionally loads older history as an earlier
    # batch, so lineage (batch/line) of the recent endpoints matches.
    recent = tmp_path / "history-recent.jsonl"
    with open(recent, "w", encoding="utf-8") as fh:
        gen_segment(fh, COLLECTORS, BASE, SHORT_DAYS, seed=7)
    old = tmp_path / "history-old.jsonl"
    with open(old, "w", encoding="utf-8") as fh:
        gen_segment(fh, COLLECTORS, BASE - EXTRA_DAYS * 86400, EXTRA_DAYS,
                    seed=8)
    for label in ("short", "long"):
        wh = str(tmp_path / f"wh-{label}.db")
        t0 = time.monotonic()
        if label == "long":
            run_cli_json("load", "--db", wh, "--batch", "old",
                         "--file", str(old))
        res = run_cli_json("load", "--db", wh, "--batch", "recent",
                           "--file", str(recent))
        load_s = time.monotonic() - t0
        version = res["version"]
        n_checkpoints = len(COLLECTORS) * (
            SHORT_DAYS if label == "short"
            else SHORT_DAYS + EXTRA_DAYS) * 86400 // STEP
        conn = sqlite3.connect(wh)
        assert conn.execute(
            "SELECT COUNT(*) FROM checkpoint").fetchone()[0] == n_checkpoints
        conn.close()
        out = str(tmp_path / f"result-{label}.db")
        t0 = time.monotonic()
        rc, stdout, err = run_cli(
            "summary", "--db", wh, "--version", str(version),
            "--roster", str(roster_path), "--request", str(request_path),
            "--out", out)
        summary_s = time.monotonic() - t0
        assert rc == 0, err
        stats = json.loads(stdout)
        print(f"\n[{label}] checkpoints={n_checkpoints}"
              f" load={load_s:.1f}s summary={summary_s:.1f}s"
              f" endpoint_sql_queries={stats['endpoint_sql_queries']}"
              f" cache_hits={stats['endpoint_cache_hits']}")
        results[label] = (stats, fetch_result(out))

    short_stats, short_res = results["short"]
    long_stats, long_res = results["long"]

    # Every due intersection needs exactly two endpoint lookups; the
    # bounded cache can only turn SQL queries into hits.
    for stats in (short_stats, long_stats):
        due = stats["intersections_due"]
        assert due == stats["intersections"]["ok"]
        assert stats["endpoint_sql_queries"] + stats["endpoint_cache_hits"] \
            == 2 * due
        assert stats["endpoint_sql_queries"] <= 2 * due

    # Lookup work and results do not depend on total history length.
    assert short_stats["endpoint_sql_queries"] == \
        long_stats["endpoint_sql_queries"]
    assert short_stats["endpoint_cache_hits"] == \
        long_stats["endpoint_cache_hits"]
    assert short_stats["rows_written"] == long_stats["rows_written"]
    for table in ("shift", "shift_cut", "intersection",
                  "intersection_cut"):
        assert short_res[table] == long_res[table]

    # Written row counts match the actual result tables.
    conn = sqlite3.connect(str(tmp_path / "result-short.db"))
    try:
        for table in ("shift", "shift_cut", "intersection",
                      "intersection_cut"):
            n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            assert short_stats["rows_written"][table] == n
    finally:
        conn.close()


def test_endpoint_lookup_uses_primary_key_index(tmp_path):
    from histoware.summary import _ENDPOINT_SQL

    f = write_jsonl(tmp_path / "b.jsonl", [
        {"region": "r", "collector": "c", "generation": "g", "ts": 100,
         "revision": 0, "cuts_ms": [10], "counts": [1], "total": 1}])
    wh = str(tmp_path / "wh.db")
    run_cli_json("load", "--db", wh, "--batch", "b1", "--file", str(f))
    conn = sqlite3.connect(wh)
    try:
        plan = conn.execute(
            "EXPLAIN QUERY PLAN " + _ENDPOINT_SQL,
            ("r", "c", "g", 100, 1)).fetchall()
    finally:
        conn.close()
    text = " ".join(row[-1] for row in plan).upper()
    assert "USING" in text and "INDEX" in text
    assert "SCAN CHECKPOINT" not in text
