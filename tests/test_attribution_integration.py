"""End-to-end integration: real load -> summary -> attribute chain.

Unlike the synthetic result-database tests, this drives the complete
pipeline from checkpoint JSONL through a warehouse and summary into
attribution, proving the feature consumes genuine summary output
without ever reopening a checkpoint warehouse.
"""
import json
import sqlite3

from util import (cp, duty, fetch_comparison, load_batch, run_cli,
                  shift, write_jsonl, write_pairs)

CUT = 10


def _pipeline(tmp_path, tag, checkpoints, roster, shift_req):
    wh = str(tmp_path / f"{tag}-wh.db")
    cp_path = write_jsonl(tmp_path / f"{tag}-cp.jsonl", checkpoints)
    load_batch(wh, f"{tag}-b1", cp_path)
    roster_path = write_jsonl(tmp_path / f"{tag}-roster.jsonl", roster)
    req_path = write_jsonl(tmp_path / f"{tag}-shifts.jsonl", [shift_req])
    result = str(tmp_path / f"{tag}-result.db")
    rc, stdout, err = run_cli(
        "summary", "--db", wh, "--version", "1",
        "--roster", roster_path, "--request", req_path, "--out", result)
    assert rc == 0, err
    return wh, result


def test_full_pipeline_attribution(tmp_path):
    t0 = 1700000000
    t1 = t0 + 3600
    # Baseline: collector a exact 4/10 below; b bracketed [3,6] of 10.
    base_cps = [
        cp("cn-1", "a", "g1", t0, 0, [10], [0], 0),
        cp("cn-1", "a", "g1", t1, 0, [10], [4], 10),
        cp("cn-1", "b", "g1", t0, 0, [10], [0], 0),
        cp("cn-1", "b", "g1", t1, 0, [5, 20], [3, 6], 10),
    ]
    # Candidate: a exact 3/12 below; b bracketed [2,4] of 10.
    u0 = 1700090000
    u1 = u0 + 3600
    cand_cps = [
        cp("cn-1", "a", "g1", u0, 0, [10], [0], 0),
        cp("cn-1", "a", "g1", u1, 0, [10], [3], 12),
        cp("cn-1", "b", "g1", u0, 0, [10], [0], 0),
        cp("cn-1", "b", "g1", u1, 0, [5, 20], [2, 4], 10),
    ]
    base_roster = [duty("cn-1", "a", "g1", t0, t1),
                   duty("cn-1", "b", "g1", t0, t1)]
    cand_roster = [duty("cn-1", "a", "g1", u0, u1),
                   duty("cn-1", "b", "g1", u0, u1)]
    base_wh, base_result = _pipeline(
        tmp_path, "base", base_cps, base_roster,
        shift("base-week", "cn-1", t0, t1, [CUT]))
    cand_wh, cand_result = _pipeline(
        tmp_path, "cand", cand_cps, cand_roster,
        shift("cand-week", "cn-1", u0, u1, [CUT]))

    pair_path = write_pairs(
        tmp_path / "pairs.jsonl",
        [("e2e", "base-week", "cand-week", CUT)])
    out = str(tmp_path / "compare.db")
    rc, stdout, err = run_cli(
        "attribute", "--baseline", base_result, "--candidate",
        cand_result, "--pairs", pair_path, "--out", out)
    assert rc == 0, err
    stats = json.loads(stdout)
    assert stats["statuses"]["comparable"] == 1

    comp = fetch_comparison(out)
    pair = {r[0]: r for r in comp["pair"]}["e2e"]
    # baseline total 20, below [7,10] -> over [10,13]
    assert (pair[9], pair[11], pair[12]) == ("20", "10", "13")
    # candidate total 22, below [5,7] -> over [15,17]
    assert (pair[10], pair[13], pair[14]) == ("22", "15", "17")
    # change [15-13, 17-10] = [2,7]
    assert (pair[15], pair[16]) == ("2", "7")
    assert (pair[26], pair[27]) == ("0", "0")      # exact closure
    cols = {(r[1]): r for r in comp["pair_collector"]}
    # a: over 6 -> 9, change +3 confirmed
    assert (cols["a"][13], cols["a"][14], cols["a"][16]) == \
        ("3", "3", "confirmed")
    # b: over [4,7] -> [6,8], change [-1,4] possible
    assert (cols["b"][13], cols["b"][14], cols["b"][16]) == \
        ("-1", "4", "possible")
    # closure
    assert (sum(int(cols[k][13]) for k in cols),
            sum(int(cols[k][14]) for k in cols)) == (2, 7)
    # evidence lineage points at the real batch id used at load time
    ev = comp["pair_intersection"]
    assert {r[13] for r in ev} == {"base-b1", "cand-b1"}
    assert all(r[14] is not None for r in ev)       # source line numbers

    # The warehouse files must not have been opened for writing by
    # attribution: their checkpoint count is unchanged and the input
    # result hashes are recorded in the comparison meta.
    for wh in (base_wh, cand_wh):
        conn = sqlite3.connect(wh)
        try:
            assert conn.execute(
                "SELECT COUNT(*) FROM checkpoint").fetchone()[0] == 4
        finally:
            conn.close()
    roles = {r[0]: r[2] for r in comp["input_file"]}
    assert all(len(h) == 64 for h in roles.values())
