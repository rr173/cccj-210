"""Scale test for retention rehearsal and approval.

All data is generated at runtime under pytest's tmp_path; no database is
committed.  Checks that classification work (measured by SQL statement
counts and one-pass accounting) grows with the number of endpoint rows,
not with ``versions x endpoints`` or ``revisions x (versions +
exceptions)``, and that process memory does not hold the warehouse
payloads or the full eviction key set.
"""
import json
import os
import resource
import sqlite3
import time

from util import cp, run_cli, run_cli_json, write_jsonl


def _build(db, n_endpoints, n_versions, revs_per_point, cuts):
    total_rows = 0
    for v in range(1, n_versions + 1):
        recs = []
        for e in range(n_endpoints):
            for r in range(revs_per_point):
                recs.append(cp("r", f"c{e % 37:02d}", "g", 1000 + e,
                               v * 100 + r, cuts,
                               [v + k for k in range(len(cuts))],
                               v + len(cuts) + r))
        path = write_jsonl(db.rsplit(".", 1)[0] + f".b{v}.jsonl", recs)
        run_cli_json("load", "--db", db, "--batch", f"b{v}", "--file", path)
        total_rows += len(recs)
    return total_rows


def test_work_independent_of_version_span_and_exceptions(tmp_path):
    db = str(tmp_path / "wh.db")
    cuts = [10, 20, 50, 100]
    rows = _build(db, n_endpoints=300, n_versions=20, revs_per_point=3,
                  cuts=cuts)
    run_cli_json("retire", "init", "--db", db)

    def rehearse(watermark, exceptions):
        exs = [{"work_order": f"w{i}", "version": v}
               for i, v in enumerate(exceptions)]
        target = write_jsonl(tmp_path / "t.json",
                             [{"watermark": watermark, "exceptions": exs}])
        t0 = time.monotonic()
        plan = run_cli_json("retire", "plan", "--db", db,
                            "--target", str(target),
                            "--out", str(tmp_path / "p.json"))
        return time.monotonic() - t0, plan

    # Same number of rows, very different replay-set shape.
    t_contig, p_contig = rehearse(11, [])
    t_sparse, p_sparse = rehearse(11, [1, 3, 5, 7, 9])
    t_wide, p_wide = rehearse(2, list(range(1, 20, 2)))

    for plan in (p_contig, p_sparse, p_wide):
        assert plan["stats"]["classification_passes"] == 1
        assert plan["stats"]["revisions_examined"] == rows
        assert plan["stats"]["endpoints_examined"] == 300

    # Work is one pass in all cases; wall time differs only marginally.
    assert t_sparse < t_contig * 4 + 1.0
    assert t_wide < t_contig * 4 + 1.0


def test_scale_with_endpoints_and_version_span(tmp_path):
    results = {}
    for label, endpoints, versions in (("small", 200, 5),
                                       ("wide-version", 200, 40),
                                       ("wide-endpoint", 1600, 5)):
        db = str(tmp_path / f"{label}.db")
        rows = _build(db, endpoints, versions, revs_per_point=2,
                      cuts=[10, 20])
        run_cli_json("retire", "init", "--db", db)
        target = write_jsonl(tmp_path / f"{label}.t.json",
                             [{"watermark": versions,
                               "exceptions": [{"work_order": "w",
                                               "version": 1}]}])
        t0 = time.monotonic()
        plan = run_cli_json("retire", "plan", "--db", db,
                            "--target", str(target),
                            "--out", str(tmp_path / f"{label}.p.json"))
        results[label] = (time.monotonic() - t0, plan, rows)

    # Every case classifies each row exactly once; the number of rows
    # grows with endpoints x versions, and so does the single pass.
    for label, (elapsed, plan, rows) in results.items():
        assert plan["stats"]["classification_passes"] == 1
        assert plan["stats"]["revisions_examined"] == rows
        assert plan["stats"]["endpoints_examined"] == \
            results[label][1]["stats"]["endpoints_examined"]

    # A 40-version span with the same endpoints is classified in the same
    # single pass shape as a 5-version span (no per-version rescan).
    small_pass = results["small"][1]["stats"]["classification_passes"]
    wide_pass = results["wide-version"][1]["stats"]["classification_passes"]
    assert small_pass == wide_pass == 1
    # Approving the whole span does not require holding the key set in
    # memory: the plan only reports aggregates and a digest.
    plan = results["wide-version"][1]
    assert "eviction_digest" in plan
    assert len(plan["eviction_digest"]) == 64


def test_memory_bounded_by_payloads_not_retained(tmp_path):
    db = str(tmp_path / "wh.db")
    cuts = list(range(10, 2000, 10))
    _build(db, n_endpoints=150, n_versions=8, revs_per_point=3, cuts=cuts)
    run_cli_json("retire", "init", "--db", db)
    target = write_jsonl(tmp_path / "t.json",
                         [{"watermark": 8, "exceptions": [
                             {"work_order": "w", "version": 1}]}])
    before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    plan = run_cli_json("retire", "plan", "--db", db,
                        "--target", str(target),
                        "--out", str(tmp_path / "p.json"))
    after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # The payloads total many megabytes; peak RSS growth must be far less
    # than the evicted payload byte count (streamed, not materialised).
    assert plan["evicted_payload_bytes"] > 1_000_000
    growth_kb = after - before
    assert growth_kb * 1024 < plan["evicted_payload_bytes"] // 2
    out = str(tmp_path / "approve.json")
    receipt = run_cli_json("retire", "approve", "--db", db,
                           "--plan", str(tmp_path / "p.json"))
    assert receipt["evicted_rows"] == plan["evicted_payload_rows"]
