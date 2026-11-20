"""Differential testing of attribution against an independent reference.

Random result-database contents (random collector sets, totals,
below-cut intervals and occasional missing/conflicted intersections)
are generated at runtime, compared with a naive in-memory reference
that shares no code with histoware.attribution.  The reference checks:

* over-threshold interval arithmetic (total - below bounds);
* candidate-minus-baseline change intervals and closure
  (collector sums == scope interval, full vs unallocated);
* exact fraction percentage-point bounds;
* confirmed/possible/unproven verdict boundaries;
* comparability status and reversal symmetry.
No floating point is used anywhere; fractions are exact integers.
"""
import random
from fractions import Fraction

import pytest

from util import (fetch_comparison, make_result_db, run_cli, write_pairs)

CUT = 10


def _gen_side(rng, name, region, collectors, force_complete=False):
    """Build one shift dict plus the reference side structure."""
    ivs = []
    ref = {"region": region, "status": "complete", "collectors": {}}
    n_iv = 0
    for collector in collectors:
        n_iv += 1
        roll = rng.random()
        if not force_complete and roll < 0.12:
            status = rng.choice(["missing_start", "missing_end",
                                 "missing_both", "conflict"])
            iv = {"id": n_iv, "collector": collector, "status": status,
                  "total_delta": None,
                  "start_total": str(rng.randint(0, 50)),
                  "end_total": None if status != "conflict"
                  else str(rng.randint(0, 50)),
                  "cuts": None}
            ivs.append(iv)
            ref["collectors"][collector] = {"usable": False, "total": 0,
                                            "below": None}
            ref["status"] = "partial"
            continue
        total = rng.randint(0, 200)
        below_lo = rng.randint(0, total)
        below_hi = rng.randint(below_lo, total)
        ivs.append({"id": n_iv, "collector": collector, "status": "ok",
                    "total_delta": total,
                    "start_total": "0", "end_total": str(total),
                    "cuts": {CUT: (below_lo, below_hi)}})
        ref["collectors"][collector] = {
            "usable": True, "total": total,
            "below": (below_lo, below_hi)}
    shift = {"name": name, "region": region,
             "status": ref["status"], "cuts": {}, "intersections": ivs}
    ok = [iv for iv in ivs if iv["status"] == "ok"]
    if ref["status"] == "complete":
        shift["cuts"][CUT] = (
            sum(iv["cuts"][CUT][0] for iv in ok),
            sum(iv["cuts"][CUT][1] for iv in ok))
    else:
        shift["cuts"][CUT] = None
        shift["covered"] = {CUT: (
            sum(iv["cuts"][CUT][0] for iv in ok),
            sum(iv["cuts"][CUT][1] for iv in ok))}
    return shift, ref


def _ref_over(coll):
    t = coll["total"]
    lo, hi = coll["below"]
    return (t - hi, t - lo)


def reference_pair(base_ref, cand_ref, cut_present_base=True,
                   cut_present_cand=True):
    """Independent expected outcome for one pair.  Returns a dict."""
    issues = []
    if base_ref["region"] != cand_ref["region"]:
        issues.append("region_mismatch")
    if not cut_present_base:
        issues.append("baseline_cut_missing")
    if not cut_present_cand:
        issues.append("candidate_cut_missing")
    bcols = set(base_ref["collectors"])
    ccols = set(cand_ref["collectors"])
    if bcols - ccols:
        issues.append("collector_only_baseline")
    if ccols - bcols:
        issues.append("collector_only_candidate")
    eligible = []
    for k in sorted(bcols & ccols):
        if base_ref["collectors"][k]["usable"] \
                and cand_ref["collectors"][k]["usable"]:
            eligible.append(k)
        else:
            issues.append("collector_not_comparable")
    base_complete = base_ref["status"] == "complete"
    cand_complete = cand_ref["status"] == "complete"
    if not base_complete:
        issues.append("baseline_incomplete")
    if not cand_complete:
        issues.append("candidate_incomplete")

    # scoped aggregates
    sb_tot = sum(base_ref["collectors"][k]["total"] for k in eligible)
    sc_tot = sum(cand_ref["collectors"][k]["total"] for k in eligible)
    sb_over = [0, 0]
    sc_over = [0, 0]
    collector = {}
    for k in eligible:
        bo = _ref_over(base_ref["collectors"][k])
        co = _ref_over(cand_ref["collectors"][k])
        sb_over[0] += bo[0]
        sb_over[1] += bo[1]
        sc_over[0] += co[0]
        sc_over[1] += co[1]
        ch = (co[0] - bo[1], co[1] - bo[0])
        label = ("confirmed" if ch[0] > 0 else
                 "possible" if ch[1] > 0 else "unproven")
        collector[k] = {"base_over": bo, "cand_over": co,
                        "change": ch, "label": label}
    scoped_change = (sc_over[0] - sb_over[1], sc_over[1] - sb_over[0])

    hard = ("region_mismatch" in issues
            or ("baseline_cut_missing" in issues and base_complete)
            or ("candidate_cut_missing" in issues and cand_complete)
            or not eligible)
    full_ok = base_complete and cand_complete and cut_present_base \
        and cut_present_cand
    if hard:
        status = "not_comparable"
    elif full_ok and bcols == ccols and not any(
            i == "collector_not_comparable" for i in issues):
        status = "comparable"
    else:
        status = "partial"

    full = None
    unallocated = None
    full_pp = scoped_pp = None
    base_total = cand_total = None
    if full_ok:
        bt = sum(c["total"] for c in base_ref["collectors"].values())
        ct = sum(c["total"] for c in cand_ref["collectors"].values())
        base_total, cand_total = bt, ct
        b_below = [sum(c["below"][j]
                       for c in base_ref["collectors"].values())
                   for j in (0, 1)]
        c_below = [sum(c["below"][j]
                       for c in cand_ref["collectors"].values())
                   for j in (0, 1)]
        b_over = (bt - b_below[1], bt - b_below[0])
        c_over = (ct - c_below[1], ct - c_below[0])
        full_change = (c_over[0] - b_over[1], c_over[1] - b_over[0])
        full = {"base_total": bt, "cand_total": ct,
                "base_over": b_over, "cand_over": c_over,
                "change": full_change}
        if bt > 0 and ct > 0:
            full_pp = (
                Fraction(100 * c_over[0], ct)
                - Fraction(100 * b_over[1], bt),
                Fraction(100 * c_over[1], ct)
                - Fraction(100 * b_over[0], bt))
        if status == "comparable":
            unallocated = (0, 0)
        else:
            unallocated = (full_change[0] - scoped_change[1],
                           full_change[1] - scoped_change[0])
        if bt == 0 and bcols == ccols and eligible:
            status = "not_comparable"
            issues.append("zero_denominator")
    if eligible and sb_tot > 0 and sc_tot > 0:
        scoped_pp = (
            Fraction(100 * sc_over[0], sc_tot)
            - Fraction(100 * sb_over[1], sb_tot),
            Fraction(100 * sc_over[1], sc_tot)
            - Fraction(100 * sb_over[0], sb_tot))
    return {
        "status": status, "issues": issues, "eligible": eligible,
        "collector": collector,
        "scoped": {"sb_tot": sb_tot, "sc_tot": sc_tot,
                   "sb_over": tuple(sb_over), "sc_over": tuple(sc_over),
                   "change": scoped_change, "pp": scoped_pp},
        "full": full, "unallocated": unallocated, "full_pp": full_pp,
        "n_base": len(bcols), "n_cand": len(ccols)}


@pytest.mark.parametrize("seed", [11, 23, 37, 51, 68])
def test_random_attribution_against_reference(seed, tmp_path):
    rng = random.Random(seed)
    region = "r1"
    pool = [f"c{i}" for i in range(8)]
    rng.shuffle(pool)
    base_cols = sorted(pool[:rng.randint(2, 6)])
    # candidate mostly overlaps, sometimes adds/drops a collector
    shared = [c for c in base_cols if rng.random() < 0.85]
    extra = [c for c in pool if c not in base_cols
             and rng.random() < 0.4][:2]
    cand_cols = sorted(set(shared) | set(extra))
    if not cand_cols:
        cand_cols = base_cols[:1]

    base_shift, base_ref = _gen_side(rng, "b-shift", region, base_cols)
    cand_shift, cand_ref = _gen_side(rng, "c-shift", region, cand_cols)
    base_db = make_result_db(tmp_path / f"base-{seed}.db", [base_shift])
    cand_db = make_result_db(tmp_path / f"cand-{seed}.db", [cand_shift])
    pair_path = write_pairs(tmp_path / f"pairs-{seed}.jsonl",
                            [("p", "b-shift", "c-shift", CUT)])
    out = str(tmp_path / f"cmp-{seed}.db")
    rc, _, err = run_cli("attribute", "--baseline", str(base_db),
                         "--candidate", str(cand_db),
                         "--pairs", str(pair_path), "--out", out)
    assert rc == 0, err
    comp = fetch_comparison(out)
    expect = reference_pair(base_ref, cand_ref)
    pair = {r[0]: r for r in comp["pair"]}["p"]
    assert pair[6] == expect["status"]

    # scoped aggregate
    assert (pair[18], pair[19]) == (
        str(expect["scoped"]["sb_tot"]),
        str(expect["scoped"]["sc_tot"]))
    assert (pair[20], pair[21]) == tuple(map(str, expect["scoped"]["sb_over"]))
    assert (pair[22], pair[23]) == tuple(map(str, expect["scoped"]["sc_over"]))
    assert (pair[24], pair[25]) == tuple(map(str, expect["scoped"]["change"]))

    # per-collector rows and closure over the eligible set
    got_cols = {(r[0], r[1]): r for r in comp["pair_collector"]}
    sum_lo = sum_hi = 0
    for k in expect["eligible"]:
        row = got_cols[("p", k)]
        exp = expect["collector"][k]
        assert row[4] == 1
        assert (row[9], row[10]) == tuple(map(str, exp["base_over"]))
        assert (row[11], row[12]) == tuple(map(str, exp["cand_over"]))
        assert (row[13], row[14]) == tuple(map(str, exp["change"]))
        assert row[16] == exp["label"]
        sum_lo += exp["change"][0]
        sum_hi += exp["change"][1]
    assert (sum_lo, sum_hi) == expect["scoped"]["change"]
    # non-eligible rows carry no verdict
    for (pid, k), row in got_cols.items():
        if k not in expect["eligible"]:
            assert row[4] == 0 and row[16] is None and row[13] is None

    # full-shift values
    if expect["full"] is None:
        assert pair[9] is None or pair[10] is None
        assert pair[26] is None and pair[28] is None
    else:
        f = expect["full"]
        assert (pair[9], pair[10]) == (
            str(f["base_total"]), str(f["cand_total"]))
        assert (pair[11], pair[12]) == tuple(map(str, f["base_over"]))
        assert (pair[13], pair[14]) == tuple(map(str, f["cand_over"]))
        assert (pair[15], pair[16]) == tuple(map(str, f["change"]))
        assert (pair[26], pair[27]) == tuple(
            map(str, expect["unallocated"]))
    # exact fraction percentage points
    if expect["full_pp"] is None:
        assert pair[28] is None and pair[30] is None
    else:
        assert Fraction(int(pair[28]), int(pair[29])) == expect["full_pp"][0]
        assert Fraction(int(pair[30]), int(pair[31])) == expect["full_pp"][1]
    if expect["scoped"]["pp"] is None:
        assert pair[32] is None and pair[34] is None
    else:
        assert Fraction(int(pair[32]), int(pair[33])) \
            == expect["scoped"]["pp"][0]
        assert Fraction(int(pair[34]), int(pair[35])) \
            == expect["scoped"]["pp"][1]
    # evidence: every source intersection is present once per side
    ev = comp["pair_intersection"]
    base_iv_count = len(base_shift["intersections"])
    cand_iv_count = len(cand_shift["intersections"])
    assert sum(1 for r in ev if r[1] == "baseline") == base_iv_count
    assert sum(1 for r in ev if r[1] == "candidate") == cand_iv_count
    # source ids are preserved
    assert {r[8] for r in ev if r[1] == "baseline"} == \
        {iv["id"] for iv in base_shift["intersections"]}


def test_random_reversal_symmetry(tmp_path):
    seed = 44
    rng = random.Random(seed)
    cols = ["a", "b", "c", "d"]
    base_shift, base_ref = _gen_side(rng, "b", "r", cols,
                                     force_complete=True)
    cand_shift, cand_ref = _gen_side(rng, "c", "r", cols,
                                     force_complete=True)
    fwd_db_b = make_result_db(tmp_path / "fb.db", [base_shift])
    fwd_db_c = make_result_db(tmp_path / "fc.db", [cand_shift])
    write_pairs(tmp_path / "fp.jsonl", [("p", "b", "c", CUT)])
    fwd_out = str(tmp_path / "fwd.db")
    rc, _, err = run_cli("attribute", "--baseline", str(fwd_db_b),
                         "--candidate", str(fwd_db_c),
                         "--pairs", str(tmp_path / "fp.jsonl"),
                         "--out", fwd_out)
    assert rc == 0, err
    rev_db_b = make_result_db(tmp_path / "rb.db", [cand_shift])
    rev_db_c = make_result_db(tmp_path / "rc.db", [base_shift])
    write_pairs(tmp_path / "rp.jsonl", [("p", "c", "b", CUT)])
    rev_out = str(tmp_path / "rev.db")
    rc, _, err = run_cli("attribute", "--baseline", str(rev_db_b),
                         "--candidate", str(rev_db_c),
                         "--pairs", str(tmp_path / "rp.jsonl"),
                         "--out", rev_out)
    assert rc == 0, err
    fwd = fetch_comparison(fwd_out)
    rev = fetch_comparison(rev_out)
    pf = {r[0]: r for r in fwd["pair"]}["p"]
    pr = {r[0]: r for r in rev["pair"]}["p"]
    assert pf[6] == pr[6]
    assert int(pr[15]) == -int(pf[16]) and int(pr[16]) == -int(pf[15])
    assert int(pr[24]) == -int(pf[25]) and int(pr[25]) == -int(pf[24])
    fcols = {r[1]: r for r in fwd["pair_collector"]}
    rcols = {r[1]: r for r in rev["pair_collector"]}
    for k in cols:
        assert int(rcols[k][13]) == -int(fcols[k][14])
        assert int(rcols[k][14]) == -int(fcols[k][13])
    # percentage-point sign reversal via exact fractions
    if pf[28] is not None:
        assert Fraction(int(pr[28]), int(pr[29])) \
            == -Fraction(int(pf[30]), int(pf[31]))
        assert Fraction(int(pr[30]), int(pr[31])) \
            == -Fraction(int(pf[28]), int(pf[29]))
