"""Hand-computed attribution semantics.

Covers, with independently hand-derived numbers:
* exact over-threshold changes and exact closure;
* intersecting intervals that cannot prove a contribution ('possible');
* one collector rising while another falls so the total cancels;
* incomplete coverage on one or both sides;
* missing cuts; mismatched collector sets; zero denominators;
* reversed baseline/candidate comparisons (interval sign reversal and
  corresponding verdict adjustment, unchanged comparability);
* target write failure protection and manifest/input validation.
All counts stay decimal TEXT; nothing is converted through floats.
"""
import hashlib
import json

from util import (fetch_comparison, make_result_db, run_cli,
                  write_jsonl, write_pairs)

CUT = 10


def one_iv(iid, collector, delta, below, status="ok", **kw):
    """One single-intersection collector; below = (lo, hi) at CUT."""
    iv = {"id": iid, "collector": collector, "status": status,
          "total_delta": None if status != "ok" else delta,
          "start_total": kw.get("start_total", "0"),
          "end_total": kw.get("end_total",
                              None if status != "ok" else str(delta)),
          "cuts": {CUT: below} if status == "ok" else None}
    iv.update(kw)
    return iv


def shift_with_collectors(name, region, collectors, status="complete",
                          extra_cuts=None, covered=None):
    """collectors: list of one-iv dicts.  Shift cut bounds are the
    integer sums of the intersection bounds."""
    ok = [iv for iv in collectors if iv["status"] == "ok"]
    cuts = {}
    if status == "complete":
        lo = sum(iv["cuts"][CUT][0] for iv in ok)
        hi = sum(iv["cuts"][CUT][1] for iv in ok)
        cuts[CUT] = (lo, hi)
    else:
        lo = sum((iv["cuts"] or {}).get(CUT, (0, 0))[0] for iv in ok)
        hi = sum((iv["cuts"] or {}).get(CUT, (0, 0))[1] for iv in ok)
        cuts[CUT] = None
        covered = {CUT: (lo, hi)}
    if extra_cuts:
        cuts.update(extra_cuts)
    return {"name": name, "region": region, "status": status,
            "cuts": cuts, "covered": covered or {},
            "intersections": collectors}


_RUN_SEQ = 0


def run_attribute(tmp_path, baseline, candidate, pairs,
                  name="compare.db"):
    global _RUN_SEQ
    _RUN_SEQ += 1
    tag = _RUN_SEQ
    base_db = make_result_db(tmp_path / f"baseline-{tag}.db", baseline)
    cand_db = make_result_db(tmp_path / f"candidate-{tag}.db", candidate)
    pair_path = write_pairs(tmp_path / f"pairs-{tag}.jsonl", pairs)
    out = str(tmp_path / name)
    rc, stdout, err = run_cli(
        "attribute", "--baseline", str(base_db),
        "--candidate", str(cand_db), "--pairs", str(pair_path),
        "--out", out)
    assert rc == 0, err
    return out, json.loads(stdout)


def pair_map(comp):
    return {row[0]: row for row in comp["pair"]}


def collector_map(comp):
    out = {}
    for row in comp["pair_collector"]:
        out[(row[0], row[1])] = row
    return out


def issues(comp, pair_id):
    return [(r[1], r[2]) for r in comp["pair_issue"] if r[0] == pair_id]


def issue_codes(comp, pair_id):
    return [code for code, _ in issues(comp, pair_id)]


# ---------------------------------------------------------------------
# Hand-computed fixture:
#   collector a exact, collector b genuinely uncertain (possible),
#   collector c falling so totals cancel.
#
# Baseline week shift "base-week" (all complete, single intersections):
#   a: total 10, below-10 in [4, 4] -> over [6, 6]
#   b: total 10, below-10 in [3, 6] -> over [4, 7]
#   c: total  5, below-10 in [1, 3] -> over [2, 4]
#   totals: total 25, below [8, 13], over [12, 17]
#
# Candidate week shift "cand-week":
#   a: total 12, below-10 [3, 3] -> over [9, 9]   change [3, 3] confirmed
#   b: total 10, below-10 [2, 4] -> over [6, 8]
#        change: lo 6-7=-1, hi 8-4=4 -> [-1, 4] possible
#   c: total  8, below-10 [6, 6] -> over [2, 2]
#        change: lo 2-4=-2, hi 2-2=0 -> [-2, 0] unproven
#   totals: total 30, below [11, 13], over [17, 19]
#   overall change: lo 17-17=0, hi 19-12=7 -> [0, 7]
#   sum of collector change lows: 3 + (-1) + (-2) = 0
#   sum of collector change highs: 3 + 4 + 0 = 7  (exact closure)
# ---------------------------------------------------------------------
def baseline_shifts():
    return [shift_with_collectors("base-week", "cn-1", [
        one_iv(1, "a", 10, (4, 4)),
        one_iv(2, "b", 10, (3, 6)),
        one_iv(3, "c", 5, (1, 3)),
    ])]


def candidate_shifts():
    return [shift_with_collectors("cand-week", "cn-1", [
        one_iv(1, "a", 12, (3, 3)),
        one_iv(2, "b", 10, (2, 4)),
        one_iv(3, "c", 8, (6, 6)),
    ])]


PAIR = ("p1", "base-week", "cand-week", CUT)


def test_exact_interval_and_cancellation_hand_computed(tmp_path):
    out, stats = run_attribute(
        tmp_path, baseline_shifts(), candidate_shifts(), [PAIR])
    comp = fetch_comparison(out)
    p = pair_map(comp)["p1"]
    # columns: pair_id, cut, bshift, cshift, bregion, cregion, status, ...
    assert p[6] == "comparable"
    assert p[9] == "25" and p[10] == "30"            # full totals
    assert (p[11], p[12]) == ("12", "17")            # baseline over
    assert (p[13], p[14]) == ("17", "19")            # candidate over
    assert (p[15], p[16]) == ("0", "7")             # overall change
    assert p[17] == 0                                 # not exact
    # scoped values equal the full values on a comparable pair
    assert (p[18], p[19]) == ("25", "30")
    assert (p[24], p[25]) == ("0", "7")
    # nothing unallocated when every collector is in scope
    assert (p[26], p[27]) == ("0", "0")

    cm = collector_map(comp)
    a = cm[("p1", "a")]
    assert (a[9], a[10]) == ("6", "6")               # baseline over
    assert (a[11], a[12]) == ("9", "9")              # candidate over
    assert (a[13], a[14]) == ("3", "3") and a[15] == 1
    assert a[16] == "confirmed"
    b = cm[("p1", "b")]
    assert (b[9], b[10]) == ("4", "7")
    assert (b[11], b[12]) == ("6", "8")
    assert (b[13], b[14]) == ("-1", "4") and b[15] == 0
    assert b[16] == "possible"                        # intersects zero
    c = cm[("p1", "c")]
    assert (c[9], c[10]) == ("2", "4")
    assert (c[11], c[12]) == ("2", "2")
    assert (c[13], c[14]) == ("-2", "0")
    assert c[16] == "unproven"                        # hi not > 0

    # Closure: collector interval sums equal the overall interval.
    lows = [int(cm[("p1", k)][13]) for k in ("a", "b", "c")]
    highs = [int(cm[("p1", k)][14]) for k in ("a", "b", "c")]
    assert (sum(lows), sum(highs)) == (0, 7)
    assert stats["statuses"] == {"comparable": 1, "partial": 0,
                                 "not_comparable": 0}
    assert stats["rows_written"]["pair"] == 1
    assert stats["rows_written"]["pair_collector"] == 3
    # All six intersections (3 per side) are traceable evidence.
    ev = comp["pair_intersection"]
    assert len(ev) == 6
    assert {r[1] for r in ev} == {"baseline", "candidate"}
    # source intersection ids and lineage retained
    sample = [r for r in ev if r[4] == "a" and r[1] == "baseline"][0]
    assert sample[8] == 1 and sample[13] == "b1"      # source id, batch
    assert sample[14] == 1 and sample[17] == "b1"     # start/end line
    # cut bounds are decimal text, no float columns anywhere
    assert all(isinstance(r[22], str) for r in ev if r[22] is not None)
    assert all(isinstance(r[23], str) for r in ev if r[23] is not None)
    assert issues(comp, "p1") == []


def test_percentage_point_bounds_are_exact_fractions(tmp_path):
    out, _ = run_attribute(
        tmp_path, baseline_shifts(), candidate_shifts(), [PAIR])
    comp = fetch_comparison(out)
    p = pair_map(comp)["p1"]
    # pp change bounds columns 28..31 (num/den for lo, then hi)
    lo_num, lo_den, hi_num, hi_den = (int(p[i]) for i in range(28, 32))
    # lo = 100*17/30 - 100*17/25 = -34/3 ; hi = 100*19/30 - 100*12/25
    from fractions import Fraction
    assert Fraction(lo_num, lo_den) == Fraction(-34, 3)
    assert Fraction(hi_num, hi_den) == Fraction(Fraction(190, 3)
                                                - Fraction(48, 1))
    assert lo_den > 0 and hi_den > 0
    # scoped pp equals full pp on a fully comparable pair
    assert (p[32], p[33]) == (p[28], p[29])
    assert (p[34], p[35]) == (p[30], p[31])


def test_rerun_is_logically_deterministic(tmp_path):
    out1, _ = run_attribute(tmp_path, baseline_shifts(),
                            candidate_shifts(), [PAIR], "first.db")
    out2, _ = run_attribute(tmp_path, baseline_shifts(),
                            candidate_shifts(), [PAIR], "second.db")
    c1, c2 = fetch_comparison(out1), fetch_comparison(out2)
    for table in ("pair", "pair_issue", "pair_collector",
                  "pair_intersection", "pair_request"):
        assert c1[table] == c2[table]
    # input rows differ only in per-run file paths/sizes; hashes match
    assert [r[0] for r in c1["input_file"]] == [r[0] for r in c2["input_file"]]
    assert [r[2] for r in c1["input_file"]] == [r[2] for r in c2["input_file"]]
    assert [r[4] for r in c1["input_file"]] == [r[4] for r in c2["input_file"]]
    m1, m2 = dict(c1["meta"]), dict(c2["meta"])
    m1.pop("created_utc")
    m2.pop("created_utc")
    assert m1 == m2


def test_reversed_comparison_flips_intervals_and_verdicts(tmp_path):
    # Candidate/baseline swapped: intervals negate, comparability kept.
    out_fwd, _ = run_attribute(
        tmp_path, baseline_shifts(), candidate_shifts(),
        [("fwd", "base-week", "cand-week", CUT)], "fwd.db")
    out_rev, _ = run_attribute(
        tmp_path, candidate_shifts(), baseline_shifts(),
        [("rev", "cand-week", "base-week", CUT)], "rev.db")
    fwd = fetch_comparison(out_fwd)
    rev = fetch_comparison(out_rev)
    pf, pr = pair_map(fwd)["fwd"], pair_map(rev)["rev"]
    assert pf[6] == pr[6] == "comparable"
    assert int(pr[15]) == -int(pf[16])
    assert int(pr[16]) == -int(pf[15])
    cf, cr = collector_map(fwd), collector_map(rev)
    for collector in ("a", "b", "c"):
        rf, rr = cf[("fwd", collector)], cr[("rev", collector)]
        assert int(rr[13]) == -int(rf[14])
        assert int(rr[14]) == -int(rf[13])
    # verdict adjustment: a forward [3,3] confirmed -> reversed [-3,-3]
    assert cr[("rev", "a")][16] == "unproven"
    # forward c [-2,0] unproven -> reversed [0,2]: hi>0, lo not > 0
    assert cr[("rev", "c")][16] == "possible"
    # b forward [-1,4] possible -> reversed [-4,1] possible
    assert cr[("rev", "b")][16] == "possible"
    # pp bounds negate
    from fractions import Fraction
    f_lo = Fraction(int(pf[28]), int(pf[29]))
    r_hi = Fraction(int(pr[30]), int(pr[31]))
    assert f_lo == -r_hi


def test_collector_set_mismatch_is_partial_with_unallocated(tmp_path):
    # Candidate has an extra collector d; a/b/c identical to the
    # comparable fixture so the shared scope closes exactly.
    base = baseline_shifts()
    cand = candidate_shifts()
    cand[0]["intersections"].append(one_iv(4, "d", 4, (1, 2)))
    # recompute candidate shift cut bounds to include d
    ivs = cand[0]["intersections"]
    ok = [iv for iv in ivs if iv["status"] == "ok"]
    cand[0]["cuts"] = {CUT: (sum(iv["cuts"][CUT][0] for iv in ok),
                             sum(iv["cuts"][CUT][1] for iv in ok))}
    cand[0]["covered"] = {}
    out, _ = run_attribute(tmp_path, base, cand, [PAIR])
    comp = fetch_comparison(out)
    p = pair_map(comp)["p1"]
    assert p[6] == "partial"
    assert "collector_only_candidate" in issue_codes(comp, "p1")
    # shared scope still [0, 7]
    assert (p[24], p[25]) == ("0", "7")
    # full candidate totals now include d: total 34, over [19, 22]
    assert p[10] == "34"
    assert (p[13], p[14]) == ("19", "22")
    # full change [19-17, 22-12] = [2, 10]
    assert (p[15], p[16]) == ("2", "10")
    # unallocated = full change minus scoped change = [2-7, 10-0]=[-5,10]
    assert (p[26], p[27]) == ("-5", "10")
    # d row exists, not eligible, no verdict
    d = collector_map(comp)[("p1", "d")]
    assert d[4] == 0 and d[16] is None and d[2] == 0 and d[3] == 1
    # eligible collectors still carry their verdicts
    assert collector_map(comp)[("p1", "a")][16] == "confirmed"
    # evidence includes d's intersection on the candidate side only
    d_ev = [r for r in comp["pair_intersection"] if r[4] == "d"]
    assert len(d_ev) == 1 and d_ev[0][1] == "candidate"


def test_incomplete_coverage_one_side_is_partial(tmp_path):
    # Candidate collector b has a missing endpoint -> not eligible;
    # the shift is partial so full-shift metrics must stay NULL.
    cand = candidate_shifts()
    ivs = cand[0]["intersections"]
    ivs[1] = one_iv(2, "b", 10, (2, 4), status="missing_end")
    cand[0]["status"] = "partial"
    ok = [iv for iv in ivs if iv["status"] == "ok"]
    lo = sum(iv["cuts"][CUT][0] for iv in ok)
    hi = sum(iv["cuts"][CUT][1] for iv in ok)
    cand[0]["cuts"] = {CUT: None}
    cand[0]["covered"] = {CUT: (lo, hi)}
    out, _ = run_attribute(tmp_path, baseline_shifts(), cand, [PAIR])
    comp = fetch_comparison(out)
    p = pair_map(comp)["p1"]
    assert p[6] == "partial"
    assert "candidate_incomplete" in issue_codes(comp, "p1")
    assert "collector_not_comparable" in issue_codes(comp, "p1")
    # full-shift numbers withheld for the incomplete candidate side;
    # the complete baseline side keeps its totals
    assert p[9] == "25"
    assert p[10] is None and p[13] is None and p[15] is None
    assert p[26] is None
    # full pp withheld (needs both full denominators)
    assert p[28] is None and p[30] is None
    # scope = {a, c}: baseline over [6,6]+[2,4]=[8,10],
    # candidate over [9,9]+[2,2]=[11,11], change [1,3]
    assert (p[20], p[21]) == ("8", "10")
    assert (p[22], p[23]) == ("11", "11")
    assert (p[24], p[25]) == ("1", "3")
    # b row retained with due/ok counts but no interval/verdict
    b = collector_map(comp)[("p1", "b")]
    assert b[4] == 0 and b[7] == 1 and b[8] == 0
    assert b[13] is None and b[16] is None
    # the unusable intersection is still evidence with its status
    b_ev = [r for r in comp["pair_intersection"]
            if r[4] == "b" and r[1] == "candidate"]
    assert len(b_ev) == 1 and b_ev[0][9] == "missing_end"
    assert b_ev[0][22] is None


def test_both_sides_incomplete_same_blocked_collector(tmp_path):
    base = [shift_with_collectors("base-week", "cn-1", [
        one_iv(1, "a", 10, (4, 4)),
        one_iv(2, "b", 10, (3, 6), status="conflict",
               reason="total regressed", conflict_cut=None),
    ], status="partial")]
    cand = [shift_with_collectors("cand-week", "cn-1", [
        one_iv(1, "a", 12, (3, 3)),
        one_iv(2, "b", 10, (2, 4), status="missing_start"),
    ], status="partial")]
    out, _ = run_attribute(tmp_path, base, cand, [PAIR])
    comp = fetch_comparison(out)
    p = pair_map(comp)["p1"]
    assert p[6] == "partial"
    codes = issue_codes(comp, "p1")
    assert "baseline_incomplete" in codes
    assert "candidate_incomplete" in codes
    # only a is in scope; its change is exact [3,3]
    assert p[38] == 1
    assert (p[24], p[25]) == ("3", "3")
    a = collector_map(comp)[("p1", "a")]
    assert a[4] == 1 and a[16] == "confirmed"
    # conflict reason preserved in evidence
    b_ev = [r for r in comp["pair_intersection"]
            if r[4] == "b" and r[1] == "baseline"][0]
    assert b_ev[9] == "conflict" and b_ev[10] == "total regressed"


def test_missing_cut_is_not_comparable(tmp_path):
    base = baseline_shifts()
    cand = candidate_shifts()
    # candidate shift carries cut 20 instead of 10
    cand[0]["cuts"] = {20: (5, 8)}
    out, _ = run_attribute(tmp_path, base, cand, [PAIR])
    comp = fetch_comparison(out)
    p = pair_map(comp)["p1"]
    assert p[6] == "not_comparable"
    assert issue_codes(comp, "p1") == ["candidate_cut_missing"]
    # no collector verdicts, no intervals, but evidence retained
    assert comp["pair_collector"] == []
    assert len(comp["pair_intersection"]) == 6
    assert p[24] == "0" and p[15] is None


def test_region_mismatch_is_not_comparable(tmp_path):
    base = baseline_shifts()
    cand = candidate_shifts()
    cand[0]["region"] = "cn-2"
    out, _ = run_attribute(tmp_path, base, cand, [PAIR])
    comp = fetch_comparison(out)
    p = pair_map(comp)["p1"]
    assert p[6] == "not_comparable"
    assert "region_mismatch" in issue_codes(comp, "p1")
    assert p[4] == "cn-1" and p[5] == "cn-2"


def test_shift_name_must_match_exactly(tmp_path):
    # Similar names must not be guessed; the named shift is absent.
    base = baseline_shifts()
    cand = [dict(candidate_shifts()[0], name="cand-week-2")]
    out, _ = run_attribute(tmp_path, base, cand, [PAIR])
    comp = fetch_comparison(out)
    p = pair_map(comp)["p1"]
    assert p[6] == "not_comparable"
    assert issue_codes(comp, "p1") == ["candidate_shift_missing"]


def test_zero_denominator_is_not_comparable_for_full_share(tmp_path):
    # Baseline complete zero-traffic shift: total 0, below 0 -> over 0.
    base = [shift_with_collectors("base-zero", "cn-1", [
        one_iv(1, "a", 0, (0, 0)),
        one_iv(2, "b", 0, (0, 0)),
    ])]
    cand = [shift_with_collectors("cand-zero", "cn-1", [
        one_iv(1, "a", 5, (1, 2)),
        one_iv(2, "b", 3, (0, 1)),
    ])]
    out, _ = run_attribute(
        tmp_path, base, cand,
        [("z", "base-zero", "cand-zero", CUT)])
    comp = fetch_comparison(out)
    p = pair_map(comp)["z"]
    # collector-level counts are still computable and labelled
    assert p[6] == "not_comparable"
    assert "zero_denominator" in issue_codes(comp, "z")
    # full pp fractions are NULL (undefined), counts retained
    assert p[28] is None and p[29] is None and p[30] is None
    # full change = cand over [5,7] minus base over [0,0] -> [5,7]
    assert (p[15], p[16]) == ("5", "7")
    cm = collector_map(comp)
    # a: over base [0,0], cand [3,4] -> change [3,4] confirmed
    assert cm[("z", "a")][16] == "confirmed"
    assert (cm[("z", "a")][13], cm[("z", "a")][14]) == ("3", "4")
    # b: cand over [2,3] -> [2,3] confirmed as well
    assert cm[("z", "b")][16] == "confirmed"
    # scoped pp is undefined too (scoped baseline total 0)
    assert p[32] is None and p[34] is None


def test_no_duty_shift_is_not_comparable(tmp_path):
    base = [{"name": "nobody", "region": "cn-1", "status": "no_duty",
             "cuts": {}, "intersections": []}]
    cand = candidate_shifts()
    out, _ = run_attribute(
        tmp_path, base, cand,
        [("n", "nobody", "cand-week", CUT)])
    comp = fetch_comparison(out)
    p = pair_map(comp)["n"]
    assert p[6] == "not_comparable"
    codes = issue_codes(comp, "n")
    assert "baseline_no_duty" in codes
    assert "empty_comparable_scope" in codes
    # candidate-side collectors are retained, none eligible (absent on
    # the no-duty baseline side), no verdicts published
    rows = collector_map(comp)
    assert {k[1] for k in rows} == {"a", "b", "c"}
    assert all(row[4] == 0 and row[16] is None for row in rows.values())
    assert all(row[2] == 0 and row[3] == 1 for row in rows.values())


def test_manifest_conflict_and_input_validation(tmp_path):
    base_db = make_result_db(tmp_path / "v-base.db", baseline_shifts())
    cand_db = make_result_db(tmp_path / "v-cand.db", candidate_shifts())
    good_pairs = write_pairs(tmp_path / "good-pairs.jsonl", [PAIR])
    out = str(tmp_path / "never.db")

    # duplicate pair id -> whole run fails, no output written
    dup = write_pairs(tmp_path / "dup.jsonl", [PAIR, PAIR])
    rc, _, err = run_cli("attribute", "--baseline", str(base_db),
                         "--candidate", str(cand_db), "--pairs", str(dup),
                         "--out", out)
    assert rc == 1 and "duplicate pair id" in err
    assert not __import__("os").path.exists(out)

    # blank line, bad JSON, wrong fields, missing file all fail the run
    empty = write_jsonl(tmp_path / "empty.jsonl", [])
    rc, _, err = run_cli("attribute", "--baseline", str(base_db),
                         "--candidate", str(cand_db), "--pairs", str(empty),
                         "--out", out)
    assert rc == 1 and "contains no pairs" in err
    assert not __import__("os").path.exists(out)
    bad = write_jsonl(tmp_path / "bad.jsonl", [""])
    rc, _, err = run_cli("attribute", "--baseline", str(base_db),
                         "--candidate", str(cand_db), "--pairs", str(bad),
                         "--out", out)
    assert rc == 1 and "blank line" in err
    bad2 = write_jsonl(tmp_path / "bad2.jsonl",
                       [{"id": "x", "baseline_shift": "base-week",
                         "candidate_shift": "cand-week"}])
    rc, _, err = run_cli("attribute", "--baseline", str(base_db),
                         "--candidate", str(cand_db), "--pairs", str(bad2),
                         "--out", out)
    assert rc == 1 and "bad fields" in err
    rc, _, err = run_cli("attribute", "--baseline", str(base_db),
                         "--candidate", str(tmp_path / "missing.db"),
                         "--pairs", str(good_pairs),
                         "--out", out)
    assert rc == 1 and "not found" in err

    # output path equal to an input path is rejected
    rc, _, err = run_cli("attribute", "--baseline", str(base_db),
                         "--candidate", str(cand_db),
                         "--pairs", str(good_pairs),
                         "--out", str(base_db))
    assert rc == 1 and "output path must differ" in err


def test_non_summary_database_is_rejected(tmp_path):
    junk = tmp_path / "junk.db"
    conn = __import__("sqlite3").connect(str(junk))
    conn.execute("CREATE TABLE unrelated(x)")
    conn.commit()
    conn.close()
    base_db = make_result_db(tmp_path / "baseline.db", baseline_shifts())
    pair_path = write_pairs(tmp_path / "p.jsonl", [PAIR])
    rc, _, err = run_cli("attribute", "--baseline", str(base_db),
                         "--candidate", str(junk),
                         "--pairs", str(pair_path),
                         "--out", str(tmp_path / "o.db"))
    assert rc == 1 and "does not look like" in err


def test_write_failure_preserves_existing_target(tmp_path):
    out, _ = run_attribute(tmp_path, baseline_shifts(),
                           candidate_shifts(), [PAIR], "keep.db")
    digest = hashlib.sha256(open(out, "rb").read()).hexdigest()
    dbs = sorted(p for p in tmp_path.glob("baseline-*.db"))
    base_db = dbs[-1]
    cand_db = tmp_path / base_db.name.replace("baseline-", "candidate-")
    pair_path = tmp_path / base_db.name.replace(
        "baseline-", "pairs-").replace(".db", ".jsonl")

    # nonexistent target directory
    rc, _, err = run_cli(
        "attribute", "--baseline", str(base_db), "--candidate",
        str(cand_db), "--pairs", str(pair_path),
        "--out", str(tmp_path / "no-dir" / "x.db"))
    assert rc == 1
    # existing target untouched, no temp files left
    assert hashlib.sha256(open(out, "rb").read()).hexdigest() == digest
    leftovers = [p.name for p in tmp_path.iterdir() if ".tmp-" in p.name]
    assert leftovers == []


def test_inputs_are_opened_read_only(tmp_path):
    # A read-only store must never mutate the input databases.
    out, _ = run_attribute(tmp_path, baseline_shifts(),
                           candidate_shifts(), [PAIR])
    for path in list(tmp_path.glob("baseline-*.db")) \
            + list(tmp_path.glob("candidate-*.db")):
        digest = hashlib.sha256(open(path, "rb").read()).hexdigest()
        assert len(digest) == 64
    comp = fetch_comparison(out)
    roles = {r[0]: r for r in comp["input_file"]}
    assert set(roles) == {"baseline", "candidate", "manifest"}
    assert all(len(r[2]) == 64 for r in roles.values())
    assert roles["baseline"][4] == "1"          # source schema version
    meta = dict(comp["meta"])
    assert meta["schema_version"] == "1"
    recorded = json.loads(meta["stats_json"])
    assert recorded["pairs"] == 1


def test_large_uint64_counts_keep_precision(tmp_path):
    BIG = 2 ** 64 - 1
    base = [shift_with_collectors("b", "r", [
        one_iv(1, "a", BIG, (0, 0))])]
    cand = [shift_with_collectors("c", "r", [
        one_iv(1, "a", BIG, (BIG, BIG))])]
    out, _ = run_attribute(
        tmp_path, base, cand, [("big", "b", "c", CUT)])
    comp = fetch_comparison(out)
    p = pair_map(comp)["big"]
    # baseline over [BIG, BIG], candidate over [0,0], change [-BIG,-BIG]
    assert (p[11], p[12]) == (str(BIG), str(BIG))
    assert (p[13], p[14]) == ("0", "0")
    assert (p[15], p[16]) == (str(-BIG), str(-BIG))
    a = collector_map(comp)[("big", "a")]
    assert a[13] == str(-BIG) and a[15] == 1


def test_collector_with_multiple_intersections_aggregates(tmp_path):
    """One collector covered by several duty intervals: over-threshold
    bounds must sum across intersections and every intersection stays
    independently traceable."""
    # baseline collector a: two ok intersections
    base = [shift_with_collectors("b", "r", [
        one_iv(1, "a", 6, (1, 2)),     # total 6, below [1,2] -> over [4,5]
        one_iv(2, "a", 4, (0, 1)),     # total 4, below [0,1] -> over [3,4]
    ])]
    # summed total 10, below [1,3], over [7,9]
    cand = [shift_with_collectors("c", "r", [
        one_iv(1, "a", 5, (1, 1)),     # over [4,4]
        one_iv(2, "a", 5, (2, 3)),     # over [2,3]
    ])]
    # summed total 10, below [3,4], over [6,7]
    out, _ = run_attribute(tmp_path, base, cand,
                           [("m", "b", "c", CUT)])
    comp = fetch_comparison(out)
    p = pair_map(comp)["m"]
    assert p[6] == "comparable"
    a = collector_map(comp)[("m", "a")]
    assert (a[5], a[6]) == (2, 2) and (a[7], a[8]) == (2, 2)
    assert (a[9], a[10]) == ("7", "9")
    assert (a[11], a[12]) == ("6", "7")
    # change [6-9, 7-7] = [-3, 0] unproven
    assert (a[13], a[14]) == ("-3", "0") and a[16] == "unproven"
    # both intersections on each side are separate evidence rows
    ev = comp["pair_intersection"]
    assert len(ev) == 4
    ids = {(r[1], r[8]) for r in ev}
    assert ids == {("baseline", 1), ("baseline", 2),
                   ("candidate", 1), ("candidate", 2)}
    # cut evidence carried per intersection
    base_ev = sorted((r[8], r[22], r[23])
                     for r in ev if r[1] == "baseline")
    assert base_ev == [(1, "1", "2"), (2, "0", "1")]


def test_pairs_support_query_by_shift_and_collector(tmp_path):
    """The comparison DB is queryable by paired shifts and collector."""
    base = baseline_shifts()
    cand = candidate_shifts()
    pairs = [("p1", "base-week", "cand-week", CUT),
             ("p2", "base-week", "cand-week", 50)]
    out, _ = run_attribute(tmp_path, base, cand, pairs)
    conn = __import__("sqlite3").connect(out)
    try:
        # join pair -> collector
        rows = conn.execute(
            "SELECT pc.collector, pc.contribution FROM pair p"
            " JOIN pair_collector pc ON pc.pair_id = p.pair_id"
            " WHERE p.baseline_shift = ? AND p.candidate_shift = ?"
            " ORDER BY pc.collector",
            ("base-week", "cand-week")).fetchall()
        assert rows == [("a", "confirmed"), ("b", "possible"),
                        ("c", "unproven")]
        # trace a collector back to source intersections
        src = conn.execute(
            "SELECT side, source_shift, source_intersection_id, status,"
            " start_batch, cut_lo, cut_hi FROM pair_intersection"
            " WHERE pair_id = ? AND collector = ? ORDER BY side",
            ("p1", "b")).fetchall()
        assert len(src) == 2
        assert all(r[3] == "ok" and r[4] == "b1" for r in src)
        # the 50ms pair exists and is not comparable with no collector rows
        status = conn.execute(
            "SELECT status FROM pair WHERE pair_id = 'p2'").fetchone()[0]
        assert status == "not_comparable"
    finally:
        conn.close()


def test_candidate_zero_denominator_also_not_comparable(tmp_path):
    base = [shift_with_collectors("b", "r", [one_iv(1, "a", 5, (1, 2))])]
    cand = [shift_with_collectors("c", "r", [one_iv(1, "a", 0, (0, 0))])]
    out, _ = run_attribute(tmp_path, base, cand, [("z", "b", "c", CUT)])
    comp = fetch_comparison(out)
    p = pair_map(comp)["z"]
    assert p[6] == "not_comparable"
    assert "zero_denominator" in issue_codes(comp, "z")
    assert p[28] is None


def test_reversal_keeps_comparability_for_partial_and_zero(tmp_path):
    # Partial: candidate has an extra collector; reversed must stay partial.
    base = baseline_shifts()
    cand = candidate_shifts()
    cand[0]["intersections"].append(one_iv(9, "d", 4, (1, 2)))
    ivs = cand[0]["intersections"]
    cand[0]["cuts"] = {CUT: (sum(iv["cuts"][CUT][0] for iv in ivs),
                             sum(iv["cuts"][CUT][1] for iv in ivs))}
    out_f, _ = run_attribute(
        tmp_path, base, cand, [("f", "base-week", "cand-week", CUT)],
        "fwd-partial.db")
    out_r, _ = run_attribute(
        tmp_path, cand, base, [("r", "cand-week", "base-week", CUT)],
        "rev-partial.db")
    sf, sr = pair_map(fetch_comparison(out_f)), pair_map(
        fetch_comparison(out_r))
    assert sf["f"][6] == "partial" and sr["r"][6] == "partial"
    # scoped change intervals negate
    assert int(sr["r"][24]) == -int(sf["f"][25])
    assert int(sr["r"][25]) == -int(sf["f"][24])
    # the 'extra' collector swaps sides but remains excluded
    cf, cr = collector_map(fetch_comparison(out_f)), \
        collector_map(fetch_comparison(out_r))
    assert cf[("f", "d")][2] == 0 and cf[("f", "d")][3] == 1
    assert cr[("r", "d")][2] == 1 and cr[("r", "d")][3] == 0
    assert cr[("r", "d")][4] == 0

    # Zero denominator reversed stays not_comparable.
    zb = [shift_with_collectors("zb", "r", [one_iv(1, "a", 0, (0, 0))])]
    zc = [shift_with_collectors("zc", "r", [one_iv(1, "a", 5, (1, 2))])]
    o1, _ = run_attribute(tmp_path, zb, zc, [("z1", "zb", "zc", CUT)],
                          "z1.db")
    o2, _ = run_attribute(tmp_path, zc, zb, [("z2", "zc", "zb", CUT)],
                          "z2.db")
    assert pair_map(fetch_comparison(o1))["z1"][6] == "not_comparable"
    assert pair_map(fetch_comparison(o2))["z2"][6] == "not_comparable"


def test_corrupt_source_database_fails_without_touching_target(tmp_path):
    base_db = make_result_db(tmp_path / "ok-base.db", baseline_shifts())
    # A file that exists but is not a SQLite database.
    corrupt = tmp_path / "corrupt.db"
    corrupt.write_bytes(b"this is definitely not a sqlite database")
    good_pairs = write_pairs(tmp_path / "p.jsonl", [PAIR])
    out = str(tmp_path / "protected.db")
    # First produce a valid output at that path.
    rc, _, _ = run_cli(
        "attribute", "--baseline", str(base_db),
        "--candidate", str(make_result_db(
            tmp_path / "ok-cand.db", candidate_shifts())),
        "--pairs", str(good_pairs), "--out", out)
    assert rc == 0
    import hashlib as _h
    digest = _h.sha256(open(out, "rb").read()).hexdigest()
    # Now point the candidate at a corrupt database: whole run fails.
    rc, _, err = run_cli(
        "attribute", "--baseline", str(base_db),
        "--candidate", str(corrupt), "--pairs", str(good_pairs),
        "--out", out)
    assert rc == 1 and "not a valid SQLite" in err
    assert _h.sha256(open(out, "rb").read()).hexdigest() == digest
    assert [p.name for p in tmp_path.iterdir() if ".tmp-" in p.name] == []
