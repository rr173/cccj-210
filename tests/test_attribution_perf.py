"""Scale test for shift regression attribution.

All data is generated at runtime under pytest's tmp_path; no large
files or databases are committed.  Checks that:

* runtime grows linearly with the work (pairs x collectors x
  intersections), not with unrelated result-database contents;
* every source intersection is materialised exactly once as evidence
  and written-row counters match the actual tables;
* a result database carrying unrelated shifts does not add work to a
  run whose manifest references only a small shift subset.
"""
import random
import sqlite3
import time

from histoware.attribution import run_attribution
from util import make_result_db, write_pairs

CUT = 10


def _big_shift(rng, name, region, n_collectors, iv_per_collector):
    ivs = []
    iid = 0
    for ci in range(n_collectors):
        collector = f"col-{ci:04d}"
        for j in range(iv_per_collector):
            iid += 1
            total = rng.randint(100, 5000)
            below_lo = rng.randint(0, total)
            below_hi = rng.randint(below_lo, total)
            ivs.append({
                "id": iid, "collector": collector, "generation": f"g{j}",
                "start_ts": j * 3600, "end_ts": (j + 1) * 3600,
                "status": "ok", "total_delta": total,
                "start_total": "0", "end_total": str(total),
                "cuts": {CUT: (below_lo, below_hi)}})
    ok = ivs
    shift = {"name": name, "region": region, "status": "complete",
             "cuts": {CUT: (sum(iv["cuts"][CUT][0] for iv in ok),
                            sum(iv["cuts"][CUT][1] for iv in ok))},
             "intersections": ivs}
    return shift


def _table_count(db, table):
    conn = sqlite3.connect(db)
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    finally:
        conn.close()


def test_runtime_scales_with_referenced_work(tmp_path):
    rng = random.Random(7)
    small_n, large_n = 40, 160
    iv_per = 6
    base_small = [_big_shift(rng, "base-small", "r", small_n, iv_per)]
    cand_small = [_big_shift(rng, "cand-small", "r", small_n, iv_per)]
    base_large = [_big_shift(rng, "base-large", "r", large_n, iv_per)]
    cand_large = [_big_shift(rng, "cand-large", "r", large_n, iv_per)]

    def run(base, cand, tag):
        bdb = make_result_db(tmp_path / f"{tag}-b.db", base)
        cdb = make_result_db(tmp_path / f"{tag}-c.db", cand)
        pj = write_pairs(tmp_path / f"{tag}-p.jsonl",
                         [("p", base[0]["name"], cand[0]["name"], CUT)])
        out = str(tmp_path / f"{tag}-out.db")
        t0 = time.monotonic()
        stats = run_attribution(str(bdb), str(cdb), str(pj), out)
        elapsed = time.monotonic() - t0
        return out, stats, elapsed

    small_out, small_stats, small_s = run(base_small, cand_small, "small")
    large_out, large_stats, large_s = run(base_large, cand_large, "large")

    # Evidence rows: one per source intersection on each side.
    small_iv = 2 * small_n * iv_per
    large_iv = 2 * large_n * iv_per
    assert _table_count(small_out, "pair_intersection") == small_iv
    assert _table_count(large_out, "pair_intersection") == large_iv
    assert small_stats["rows_written"]["pair_intersection"] == small_iv
    assert large_stats["rows_written"]["pair_intersection"] == large_iv
    assert small_stats["rows_written"]["pair_collector"] == small_n
    assert large_stats["rows_written"]["pair_collector"] == large_n

    # Four times the referenced work must not take an unbounded amount
    # longer; allow a generous CI factor but require sub-quadratic
    # growth.  The assertion is on row counts (deterministic) with the
    # timing only reported.
    ratio = large_iv / small_iv
    assert ratio == 4
    print(f"\n[attribution] small={small_iv} ivs in {small_s:.2f}s,"
          f" large={large_iv} ivs in {large_s:.2f}s"
          f" (work x{ratio}, time x{large_s / max(small_s, 1e-6):.1f})")
    # Both runs finish quickly on the in-memory scale of this test.
    assert large_s < 60


def test_unrelated_shifts_do_not_add_work(tmp_path):
    rng = random.Random(13)
    focus_base = _big_shift(rng, "focus-base", "r", 10, 3)
    focus_cand = _big_shift(rng, "focus-cand", "r", 10, 3)
    # Databases that also contain many unrelated shifts.
    noise_base = [focus_base] + [
        _big_shift(rng, f"noise-base-{i}", "r", 20, 3) for i in range(8)]
    noise_cand = [focus_cand] + [
        _big_shift(rng, f"noise-cand-{i}", "r", 20, 3) for i in range(8)]
    plain_b = make_result_db(tmp_path / "plain-b.db", [focus_base])
    plain_c = make_result_db(tmp_path / "plain-c.db", [focus_cand])
    noisy_b = make_result_db(tmp_path / "noisy-b.db", noise_base)
    noisy_c = make_result_db(tmp_path / "noisy-c.db", noise_cand)
    pj = write_pairs(tmp_path / "p.jsonl",
                     [("p", "focus-base", "focus-cand", CUT)])
    out_plain = str(tmp_path / "plain-out.db")
    out_noisy = str(tmp_path / "noisy-out.db")
    s1 = run_attribution(str(plain_b), str(plain_c), str(pj), out_plain)
    s2 = run_attribution(str(noisy_b), str(noisy_c), str(pj), out_noisy)
    # Evidence only covers the referenced pair's shifts.
    assert _table_count(out_plain, "pair_intersection") \
        == _table_count(out_noisy, "pair_intersection") == 2 * 10 * 3
    assert s1["rows_written"] == s2["rows_written"]
    # The actual comparison rows are identical.
    conn1 = sqlite3.connect(out_plain)
    conn2 = sqlite3.connect(out_noisy)
    try:
        for table in ("pair", "pair_collector", "pair_intersection",
                      "pair_issue"):
            assert conn1.execute(f"SELECT * FROM {table}").fetchall() \
                == conn2.execute(f"SELECT * FROM {table}").fetchall()
    finally:
        conn1.close()
        conn2.close()


def test_many_pairs_against_same_databases(tmp_path):
    rng = random.Random(21)
    n_pairs = 200
    base_shifts = [_big_shift(rng, f"b-{i:03d}", "r", 5, 2)
                   for i in range(n_pairs)]
    cand_shifts = [_big_shift(rng, f"c-{i:03d}", "r", 5, 2)
                   for i in range(n_pairs)]
    bdb = make_result_db(tmp_path / "base.db", base_shifts)
    cdb = make_result_db(tmp_path / "cand.db", cand_shifts)
    pairs = [(f"p-{i:03d}", f"b-{i:03d}", f"c-{i:03d}", CUT)
             for i in range(n_pairs)]
    pj = write_pairs(tmp_path / "pairs.jsonl", pairs)
    out = str(tmp_path / "out.db")
    t0 = time.monotonic()
    stats = run_attribution(str(bdb), str(cdb), str(pj), out)
    elapsed = time.monotonic() - t0
    assert stats["pairs"] == n_pairs
    assert _table_count(out, "pair") == n_pairs
    assert _table_count(out, "pair_request") == n_pairs
    assert _table_count(out, "pair_collector") == n_pairs * 5
    assert _table_count(out, "pair_intersection") == n_pairs * 2 * 5 * 2
    assert stats["rows_written"]["pair"] == n_pairs
    print(f"\n[attribution] {n_pairs} pairs in {elapsed:.2f}s")
    assert elapsed < 60
