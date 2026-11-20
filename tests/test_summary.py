"""Summary semantics: hand-computed bounds, conflicts, generations,
missing endpoints, version isolation, output atomicity, 64-bit sums."""
import hashlib
import json

import pytest

from util import (cp, duty, fetch_result, load_batch, run_cli, shift,
                  table_rows, write_jsonl)

BIG = 2 ** 64 - 1


def build(tmp_path, batches, roster, shifts):
    """Load batches, write roster/request files, return paths."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    wh = str(tmp_path / "wh.db")
    for i, lines in enumerate(batches, 1):
        f = write_jsonl(tmp_path / f"batch{i}.jsonl", lines)
        load_batch(wh, f"b{i}", f)
    roster_path = write_jsonl(tmp_path / "roster.jsonl", roster)
    request_path = write_jsonl(tmp_path / "request.jsonl", shifts)
    return wh, roster_path, request_path


def run_summary(tmp_path, wh, roster_path, request_path, version,
                name="result.db"):
    tmp_path.mkdir(parents=True, exist_ok=True)
    out = str(tmp_path / name)
    rc, stdout, err = run_cli(
        "summary", "--db", wh, "--version", str(version),
        "--roster", str(roster_path), "--request", str(request_path),
        "--out", out)
    assert rc == 0, err
    return out, json.loads(stdout)


def test_hand_computed_bounds(tmp_path):
    """The worked example from the specification.

    Intersection A: start total 3 with 2 below 10ms; end total 7 with
    only a 20ms cut at 4  ->  4 new requests, below-10ms in [0, 2].
    Intersection B: exactly 1 request below 10ms.
    Shift below-10ms bounds: [1, 3].
    """
    wh, roster_path, request_path = build(
        tmp_path,
        batches=[[
            cp("cn-1", "a", "g1", 1000, 0, [10], [2], 3),
            cp("cn-1", "a", "g1", 2000, 0, [20], [4], 7),
            cp("cn-1", "b", "g1", 1000, 0, [10], [0], 0),
            cp("cn-1", "b", "g1", 2000, 0, [10], [1], 1),
        ]],
        roster=[duty("cn-1", "a", "g1", 1000, 2000),
                duty("cn-1", "b", "g1", 1000, 2000)],
        shifts=[shift("s1", "cn-1", 1000, 2000, [10])])
    out, stats = run_summary(tmp_path, wh, roster_path, request_path, 1)
    res = fetch_result(out)
    # shift totals: 4 + 1 = 5 requests; below-10ms in [1, 3]
    assert res["shift"]["s1"] == ("cn-1", 1000, 2000, "complete", 2, 2,
                                  "5", "5")
    assert res["shift_cut"][("s1", 10)] == ("1", "3", 0, "1", "3")
    # per-intersection bounds and raw evidence
    ia = ("s1", "a", "g1", 1000, 2000)
    ib = ("s1", "b", "g1", 1000, 2000)
    assert res["intersection"][ia][0] == "ok"
    assert res["intersection"][ia][-1] == "4"          # total delta
    assert res["intersection"][ib][-1] == "1"
    # A: start has cut 10 exactly (2); end brackets 10 with (0 -> 0,
    # 20 -> 4): [0-2, 4-2] clipped to [0, 4] = [0, 2].
    assert res["intersection_cut"][(ia, 10)] == (
        "0", "2", 10, "2", 10, "2", 0, "0", 20, "4")
    # B: exact at both ends.
    assert res["intersection_cut"][(ib, 10)] == (
        "1", "1", 10, "0", 10, "0", 10, "1", 10, "1")
    assert stats["intersections"]["ok"] == 2
    assert stats["endpoint_sql_queries"] == 4


def test_generation_change_mid_shift_and_deleted_old_end(tmp_path):
    """Old generation's end is huge, new generation's start is non-zero.
    With the old generation's end checkpoint missing, the shift total
    must be empty -- never carried over from the new generation's start
    and never zeroed."""
    roster = [duty("r", "c", "g1", 1000, 2000),
              duty("r", "c", "g2", 2000, 3000)]
    base = [
        cp("r", "c", "g1", 1000, 0, [10], [0], 0),
        cp("r", "c", "g2", 2000, 0, [10], [100], 500),
        cp("r", "c", "g2", 3000, 0, [10], [300], 900),
    ]
    old_end = cp("r", "c", "g1", 2000, 0, [10], [900000], 1000000)
    shifts = [shift("s", "r", 1000, 3000, [10])]

    # Complete warehouse: both intersections usable.
    wh, roster_p, req_p = build(tmp_path, [base + [old_end]], roster, shifts)
    out, _ = run_summary(tmp_path, wh, roster_p, req_p, 1, "full.db")
    res = fetch_result(out)
    assert res["shift"]["s"][3] == "complete"
    assert res["shift"]["s"][6] == str(1000000 + 400)
    assert res["shift_cut"][("s", 10)][:2] == (str(900000 + 200),) * 2

    # Old generation's end deleted: overall metrics must be empty.
    wh2, roster_p2, req_p2 = build(tmp_path / "second", [base], roster, shifts)
    out2, _ = run_summary(tmp_path / "second", wh2, roster_p2, req_p2, 1)
    res2 = fetch_result(out2)
    row = res2["shift"]["s"]
    assert row[3] == "partial"
    assert row[6] is None                       # overall total empty
    assert row[7] == "400"                      # covered subtotal only
    cut = res2["shift_cut"][("s", 10)]
    assert cut[0] is None and cut[1] is None    # overall interval empty
    assert cut[3:] == ("200", "200")            # covered subtotal bounds
    i1 = res2["intersection"][("s", "c", "g1", 1000, 2000)]
    assert i1[0] == "missing_end"
    assert i1[2:5] == (0, "b1", 1)              # start end kept
    assert i1[6] is None                        # missing end has no revision


def test_same_time_shift_change_uses_two_records(tmp_path):
    wh, roster_p, req_p = build(
        tmp_path,
        batches=[[
            cp("r", "c", "g1", 1000, 0, [10], [0], 0),
            cp("r", "c", "g1", 2000, 0, [10], [5], 9),
            cp("r", "c", "g2", 2000, 0, [10], [2], 4),
            cp("r", "c", "g2", 3000, 0, [10], [7], 11),
        ]],
        roster=[duty("r", "c", "g1", 1000, 2000),
                duty("r", "c", "g2", 2000, 3000)],
        shifts=[shift("s", "r", 1000, 3000, [10])])
    out, _ = run_summary(tmp_path, wh, roster_p, req_p, 1)
    res = fetch_result(out)
    i_old = res["intersection"][("s", "c", "g1", 1000, 2000)]
    i_new = res["intersection"][("s", "c", "g2", 2000, 3000)]
    assert i_old[0] == i_new[0] == "ok"
    assert i_old[-1] == "9" and i_new[-1] == "7"
    assert res["shift"]["s"][6] == "16"
    assert res["shift_cut"][("s", 10)][:2] == ("10", "10")


def test_common_cut_diff_decreasing_is_conflict(tmp_path):
    wh, roster_p, req_p = build(
        tmp_path,
        batches=[[
            cp("r", "c", "g", 1000, 0, [10, 20], [5, 10], 10),
            cp("r", "c", "g", 2000, 0, [10, 20], [6, 8], 12),
        ]],
        roster=[duty("r", "c", "g", 1000, 2000)],
        shifts=[shift("s", "r", 1000, 2000, [10])])
    out, _ = run_summary(tmp_path, wh, roster_p, req_p, 1)
    res = fetch_result(out)
    i = res["intersection"][("s", "c", "g", 1000, 2000)]
    assert i[0] == "conflict"
    assert i[1] == 20                       # failing common cut located
    assert i[2:5] == (0, "b1", 1)           # conflicting revisions recorded
    assert i[6:9] == (0, "b1", 2)
    assert res["shift"]["s"][3] == "partial"
    assert res["shift"]["s"][6] is None


def test_total_regression_is_conflict(tmp_path):
    wh, roster_p, req_p = build(
        tmp_path,
        batches=[[
            cp("r", "c", "g", 1000, 0, [10], [5], 10),
            cp("r", "c", "g", 2000, 0, [10], [3], 7),
        ]],
        roster=[duty("r", "c", "g", 1000, 2000)],
        shifts=[shift("s", "r", 1000, 2000, [10])])
    out, _ = run_summary(tmp_path, wh, roster_p, req_p, 1)
    res = fetch_result(out)
    i = res["intersection"][("s", "c", "g", 1000, 2000)]
    assert i[0] == "conflict"
    assert i[1] is None                     # conflict on total, not a cut
    assert i[-1] == "-3"                    # negative delta recorded


def test_empty_interval_is_conflict_with_evidence(tmp_path):
    # start: 5 requests all below 10ms; end: total 6 but only 1 below
    # 20ms -> below-15ms interval is empty.
    wh, roster_p, req_p = build(
        tmp_path,
        batches=[[
            cp("r", "c", "g", 1000, 0, [10], [5], 5),
            cp("r", "c", "g", 2000, 0, [20], [1], 6),
        ]],
        roster=[duty("r", "c", "g", 1000, 2000)],
        shifts=[shift("s", "r", 1000, 2000, [15])])
    out, _ = run_summary(tmp_path, wh, roster_p, req_p, 1)
    res = fetch_result(out)
    i = res["intersection"][("s", "c", "g", 1000, 2000)]
    assert i[0] == "conflict"
    assert i[1] == 15
    ev = res["intersection_cut"][(("s", "c", "g", 1000, 2000), 15)]
    assert ev[0] is None and ev[1] is None  # no bounds published
    # raw brackets preserved: start (10 -> 5, +inf -> 5), end (0 -> 0,
    # 20 -> 1)
    assert ev[2:] == (10, "5", None, "5", 0, "0", 20, "1")


def test_revision_layout_change_two_versions(tmp_path):
    batches = [
        [cp("r", "c", "g", 100, 0, [10, 20], [3, 8], 9),
         cp("r", "c", "g", 200, 0, [10, 20], [5, 15], 16)],
        [cp("r", "c", "g", 200, 1, [5, 50], [6, 15], 16)],   # new layout
    ]
    roster = [duty("r", "c", "g", 100, 200)]
    shifts = [shift("s", "r", 100, 200, [5, 10, 15, 20, 50])]
    wh, roster_p, req_p = build(tmp_path, batches, roster, shifts)

    out1, _ = run_summary(tmp_path, wh, roster_p, req_p, 1, "v1.db")
    res1 = fetch_result(out1)
    cuts1 = {c: res1["shift_cut"][("s", c)] for c in (5, 10, 15, 20, 50)}
    assert cuts1[5][:2] == ("0", "5")
    assert cuts1[10][:2] == ("2", "2")
    assert cuts1[15][:2] == ("0", "7")
    assert cuts1[20][:2] == ("7", "7")
    assert cuts1[50][:2] == ("6", "7")
    ikey = ("s", "c", "g", 100, 200)
    assert res1["intersection"][ikey][6:9] == (0, "b1", 2)   # end rev 0

    out2, _ = run_summary(tmp_path, wh, roster_p, req_p, 2, "v2.db")
    res2 = fetch_result(out2)
    cuts2 = {c: res2["shift_cut"][("s", c)] for c in (5, 10, 15, 20, 50)}
    assert cuts2[5][:2] == ("3", "6")
    assert cuts2[10][:2] == ("3", "7")
    assert cuts2[15][:2] == ("0", "7")
    assert cuts2[20][:2] == ("0", "7")
    assert cuts2[50][:2] == ("6", "7")
    assert res2["intersection"][ikey][6:9] == (1, "b2", 1)   # end rev 1
    # Historical recompute of version 1 is logically identical.
    out1b, _ = run_summary(tmp_path, wh, roster_p, req_p, 1, "v1b.db")
    res1b = fetch_result(out1b)
    for table in ("shift", "shift_cut", "intersection", "intersection_cut"):
        assert res1[table] == res1b[table]
    meta1 = dict(res1["meta"])
    meta1b = dict(res1b["meta"])
    meta1.pop("created_utc")
    meta1b.pop("created_utc")
    assert meta1 == meta1b


def test_missing_endpoints_variants(tmp_path):
    batches = [[
        cp("r", "a", "g", 1000, 0, [10], [1], 2),   # only start for a
        cp("r", "b", "g", 2000, 0, [10], [3], 5),   # only end for b
        # collector c: neither endpoint
    ]]
    roster = [duty("r", "a", "g", 1000, 2000),
              duty("r", "b", "g", 1000, 2000),
              duty("r", "c", "g", 1000, 2000)]
    shifts = [shift("s", "r", 1000, 2000, [10])]
    wh, roster_p, req_p = build(tmp_path, batches, roster, shifts)
    out, _ = run_summary(tmp_path, wh, roster_p, req_p, 1)
    res = fetch_result(out)
    ia = res["intersection"][("s", "a", "g", 1000, 2000)]
    assert ia[0] == "missing_end"
    assert ia[2:5] == (0, "b1", 1) and ia[6] is None
    ib = res["intersection"][("s", "b", "g", 1000, 2000)]
    assert ib[0] == "missing_start"
    assert ib[2] is None and ib[6:9] == (0, "b1", 2)
    ic = res["intersection"][("s", "c", "g", 1000, 2000)]
    assert ic[0] == "missing_both"
    assert res["shift"]["s"][3] == "partial"
    assert res["shift"]["s"][6] is None
    assert res["shift"]["s"][7] == "0"      # nothing usable


def test_shift_boundary_needs_exact_checkpoint(tmp_path):
    # Shift starts mid-duty; no checkpoint exactly at 1500 -> the
    # intersection is missing its start (no nearest-row substitution).
    wh, roster_p, req_p = build(
        tmp_path,
        batches=[[cp("r", "c", "g", 1000, 0, [10], [1], 2),
                  cp("r", "c", "g", 2000, 0, [10], [4], 6)]],
        roster=[duty("r", "c", "g", 1000, 2000)],
        shifts=[shift("s", "r", 1500, 2000, [10])])
    out, _ = run_summary(tmp_path, wh, roster_p, req_p, 1)
    res = fetch_result(out)
    assert res["intersection"][("s", "c", "g", 1500, 2000)][0] == (
        "missing_start")


def test_interior_gaps_do_not_matter(tmp_path):
    wh, roster_p, req_p = build(
        tmp_path,
        batches=[[cp("r", "c", "g", 1000, 0, [10], [1], 2),
                  cp("r", "c", "g", 5000, 0, [10], [9], 12)]],
        roster=[duty("r", "c", "g", 1000, 5000)],
        shifts=[shift("s", "r", 1000, 5000, [10])])
    out, _ = run_summary(tmp_path, wh, roster_p, req_p, 1)
    res = fetch_result(out)
    assert res["shift"]["s"][3] == "complete"
    assert res["shift"]["s"][6] == "10"


def test_no_time_proportional_amortization(tmp_path):
    # One big intersection is complete; a smaller shift covering part
    # of the same interval gets nothing (no proportional split).
    wh, roster_p, req_p = build(
        tmp_path,
        batches=[[cp("r", "c", "g", 0, 0, [10], [0], 0),
                  cp("r", "c", "g", 7200, 0, [10], [40], 100)]],
        roster=[duty("r", "c", "g", 0, 7200)],
        shifts=[shift("big", "r", 0, 7200, [10]),
                shift("small", "r", 0, 3600, [10])])
    out, _ = run_summary(tmp_path, wh, roster_p, req_p, 1)
    res = fetch_result(out)
    assert res["shift"]["big"][3] == "complete"
    assert res["shift"]["big"][6] == "100"
    small = res["shift"]["small"]
    assert small[3] == "partial"
    assert small[6] is None
    assert small[7] == "0"                  # not 50


def test_no_duty_vs_complete_zero_traffic(tmp_path):
    wh, roster_p, req_p = build(
        tmp_path,
        batches=[[cp("r", "c", "g", 1000, 0, [10], [2], 3),
                  cp("r", "c", "g", 2000, 0, [10], [2], 3)]],
        roster=[duty("r", "c", "g", 1000, 2000)],
        shifts=[shift("empty-region", "nowhere", 1000, 2000, [10]),
                shift("zero-traffic", "r", 1000, 2000, [10])])
    out, _ = run_summary(tmp_path, wh, roster_p, req_p, 1)
    res = fetch_result(out)
    noduty = res["shift"]["empty-region"]
    assert noduty[3] == "no_duty"
    assert noduty[4:6] == (0, 0)
    assert noduty[6] is None
    zero = res["shift"]["zero-traffic"]
    assert zero[3] == "complete"
    assert zero[6] == "0"
    assert res["shift_cut"][("zero-traffic", 10)][:3] == ("0", "0", 1)


def test_roster_gap_and_shift_beyond_duty(tmp_path):
    wh, roster_p, req_p = build(
        tmp_path,
        batches=[[cp("r", "c", "g", 1000, 0, [10], [0], 0),
                  cp("r", "c", "g", 2000, 0, [10], [1], 1),
                  cp("r", "c", "g", 3000, 0, [10], [1], 1),
                  cp("r", "c", "g", 4000, 0, [10], [2], 2)]],
        roster=[duty("r", "c", "g", 1000, 2000),
                duty("r", "c", "g", 3000, 4000)],
        shifts=[shift("s", "r", 500, 5000, [10])])
    out, _ = run_summary(tmp_path, wh, roster_p, req_p, 1)
    res = fetch_result(out)
    keys = set(res["intersection"])
    assert keys == {("s", "c", "g", 1000, 2000),
                    ("s", "c", "g", 3000, 4000)}
    assert res["shift"]["s"][3] == "complete"
    assert res["shift"]["s"][6] == "2"


def test_roster_overlap_rejected(tmp_path):
    wh, roster_p, req_p = build(
        tmp_path,
        batches=[[]],
        roster=[duty("r", "c", "g1", 1000, 2500),
                duty("r", "c", "g2", 2000, 3000)],
        shifts=[shift("s", "r", 1000, 3000, [10])])
    rc, _, err = run_cli(
        "summary", "--db", wh, "--roster", roster_p, "--request", req_p,
        "--out", str(tmp_path / "out.db"))
    assert rc == 1
    assert "overlapping duty intervals" in err


def test_version_unavailable_and_missing_warehouse(tmp_path):
    wh, roster_p, req_p = build(tmp_path, [[]], [], [])
    rc, _, err = run_cli(
        "summary", "--db", wh, "--version", "5", "--roster", roster_p,
        "--request", req_p, "--out", str(tmp_path / "o.db"))
    assert rc == 1
    assert "not available" in err
    rc, _, err = run_cli(
        "summary", "--db", str(tmp_path / "nope.db"), "--roster", roster_p,
        "--request", req_p, "--out", str(tmp_path / "o.db"))
    assert rc == 1
    assert "warehouse not found" in err


def test_version_isolation_from_newer_batches(tmp_path):
    batches = [
        [cp("r", "c", "g", 1000, 0, [10], [0], 0),
         cp("r", "c", "g", 2000, 0, [10], [5], 9)],
        [cp("r", "c", "g", 2000, 1, [10], [50], 90)],   # newer revision
    ]
    roster = [duty("r", "c", "g", 1000, 2000)]
    shifts = [shift("s", "r", 1000, 2000, [10])]
    wh, roster_p, req_p = build(tmp_path, batches, roster, shifts)
    out1, _ = run_summary(tmp_path, wh, roster_p, req_p, 1, "v1.db")
    out2, _ = run_summary(tmp_path, wh, roster_p, req_p, 2, "v2.db")
    res1 = fetch_result(out1)
    res2 = fetch_result(out2)
    assert res1["shift"]["s"][6] == "9"     # version 1 view
    assert res2["shift"]["s"][6] == "90"    # version 2 view
    assert res1["intersection"][("s", "c", "g", 1000, 2000)][6] == 0
    assert res2["intersection"][("s", "c", "g", 1000, 2000)][6] == 1


def test_snapshot_isolation_from_batches_committed_mid_run(tmp_path):
    """A batch committed by another connection while a summary runs must
    not leak endpoints newer than the selected version into the result."""
    import histoware.summary as summary_mod
    from histoware.warehouse import load_batch as load_batch_fn

    wh, roster_p, req_p = build(
        tmp_path,
        batches=[[cp("r", "c", "g", 1000, 0, [10], [0], 0),
                  cp("r", "c", "g", 2000, 0, [10], [5], 9)]],
        roster=[duty("r", "c", "g", 1000, 2000)],
        shifts=[shift("s", "r", 1000, 2000, [10])])
    newer = write_jsonl(tmp_path / "newer.jsonl",
                        [cp("r", "c", "g", 2000, 1, [10], [50], 90)])

    # Baseline at version 1.
    out1, _ = run_summary(tmp_path, wh, roster_p, req_p, 1, "base.db")
    baseline = fetch_result(out1)

    # Re-run at version 1, committing a newer batch from another
    # connection in the middle of the summary.
    original_index = summary_mod.EndpointIndex
    state = {"injected": False}

    class InjectingIndex(original_index):
        def lookup(self, *args):
            if not state["injected"]:
                state["injected"] = True
                with open(newer, "rb") as fh:
                    load_batch_fn(wh, "b-newer", fh)
            return super().lookup(*args)

    summary_mod.EndpointIndex = InjectingIndex
    try:
        out2 = str(tmp_path / "mid-run.db")
        stats = summary_mod.run_summary(wh, 1, roster_p, req_p, out2)
    finally:
        summary_mod.EndpointIndex = original_index
    assert state["injected"]
    assert table_rows(wh, "SELECT MAX(version) FROM checkpoint")[0][0] == 2
    got = fetch_result(out2)
    for table in ("shift", "shift_cut", "intersection", "intersection_cut"):
        assert got[table] == baseline[table]
    assert got["shift"]["s"][6] == "9"      # still the version-1 view
    assert stats["warehouse_version"] == 1


def test_uint64_sums_exceeding_64bit(tmp_path):
    wh, roster_p, req_p = build(
        tmp_path,
        batches=[[
            cp("r", "a", "g", 1000, 0, [10], [0], 0),
            cp("r", "a", "g", 2000, 0, [10], [BIG], BIG),
            cp("r", "b", "g", 1000, 0, [10], [0], 0),
            cp("r", "b", "g", 2000, 0, [10], [BIG], BIG),
        ]],
        roster=[duty("r", "a", "g", 1000, 2000),
                duty("r", "b", "g", 1000, 2000)],
        shifts=[shift("s", "r", 1000, 2000, [10])])
    out, _ = run_summary(tmp_path, wh, roster_p, req_p, 1)
    res = fetch_result(out)
    expect = str(2 * BIG)                   # 2^65 - 2, beyond uint64
    assert res["shift"]["s"][6] == expect
    assert res["shift_cut"][("s", 10)][:2] == (expect, expect)


def test_output_write_failure_preserves_existing_result(tmp_path):
    wh, roster_p, req_p = build(
        tmp_path,
        batches=[[cp("r", "c", "g", 1000, 0, [10], [0], 0),
                  cp("r", "c", "g", 2000, 0, [10], [1], 1)]],
        roster=[duty("r", "c", "g", 1000, 2000)],
        shifts=[shift("s", "r", 1000, 2000, [10])])
    out, _ = run_summary(tmp_path, wh, roster_p, req_p, 1)
    digest = hashlib.sha256(open(out, "rb").read()).hexdigest()

    # 1. target directory does not exist
    rc, _, err = run_cli("summary", "--db", wh, "--roster", roster_p,
                         "--request", req_p,
                         "--out", str(tmp_path / "no-such-dir" / "r.db"))
    assert rc == 1
    # 2. bad request version
    rc, _, err = run_cli("summary", "--db", wh, "--version", "99",
                         "--roster", roster_p, "--request", req_p,
                         "--out", out)
    assert rc == 1
    # 3. request file breaks mid-run (second shift line invalid)
    bad_req = write_jsonl(tmp_path / "bad-req.jsonl",
                          [shift("s", "r", 1000, 2000, [10]), "not json"])
    rc, _, err = run_cli("summary", "--db", wh, "--roster", roster_p,
                         "--request", str(bad_req), "--out", out)
    assert rc == 1
    assert "line 2" in err
    # The previous result is untouched in every case.
    assert hashlib.sha256(open(out, "rb").read()).hexdigest() == digest
    # No temporary files left behind.
    assert [p.name for p in tmp_path.iterdir() if ".tmp-" in p.name] == []


def test_result_meta_records_provenance(tmp_path):
    wh, roster_p, req_p = build(
        tmp_path,
        batches=[[cp("r", "c", "g", 1000, 0, [10], [0], 0),
                  cp("r", "c", "g", 2000, 0, [10], [1], 1)]],
        roster=[duty("r", "c", "g", 1000, 2000)],
        shifts=[shift("s", "r", 1000, 2000, [10])])
    out, stats = run_summary(tmp_path, wh, roster_p, req_p, 1)
    res = fetch_result(out)
    meta = res["meta"]
    assert meta["warehouse_version"] == "1"
    assert meta["warehouse_current_version_at_run"] == "1"
    assert len(meta["roster_sha256"]) == 64
    assert len(meta["request_sha256"]) == 64
    recorded = json.loads(meta["stats_json"])
    assert recorded["warehouse_version"] == 1
    assert recorded["rows_written"]["shift"] == 1
    assert stats["endpoint_sql_queries"] == 2
