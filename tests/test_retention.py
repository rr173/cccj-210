"""Version retirement: policy contract, eviction semantics, plans,
atomic approval, summary eligibility and continued business loading.

Every scenario builds its own warehouse under pytest's tmp_path; no
database is committed.
"""
import json
import os
import sqlite3
import subprocess
import sys
import textwrap

import pytest

from util import (cp, current_version, load_batch, run_cli, run_cli_json,
                  table_rows, write_jsonl)

from histoware import retention
from histoware.common import HistowareError

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load(db, batch, records, ts=1000):
    path = write_jsonl(db + f".{batch}.jsonl", records)
    return run_cli_json("load", "--db", db, "--batch", batch,
                        "--file", str(path))


def _line(rev, total, ts=1000, cuts=None, counts=None, region="r",
          collector="c", generation="g"):
    return cp(region, collector, generation, ts, rev,
              cuts if cuts is not None else [10],
              counts if counts is not None else [total], total)


def _revisions(db):
    return [r[0] for r in table_rows(
        db, "SELECT revision FROM checkpoint ORDER BY revision")]


def _init(db):
    return run_cli_json("retire", "init", "--db", db)


def _plan(db, target, tmp_path, name="plan.json"):
    with open(tmp_path / name, "w") as fh:
        json.dump(target, fh)
    out = tmp_path / (name + ".plan")
    res = run_cli_json("retire", "plan", "--db", db,
                       "--target", str(tmp_path / name),
                       "--out", str(out))
    with open(out) as fh:
        return json.load(fh), res


def _approve(db, plan, tmp_path, name="approve.json"):
    path = tmp_path / name
    with open(path, "w") as fh:
        json.dump(plan, fh)
    return run_cli_json("retire", "approve", "--db", db,
                        "--plan", str(path))


@pytest.fixture
def drill_wh(tmp_path):
    """The specification's worked warehouse: versions 1..5 with revisions
    20, 3, 35, 27, then 48 and 50 in one batch, explicitly initialised
    into retention management."""
    db = str(tmp_path / "wh.db")
    for i, (rev, total) in enumerate(
            [(20, 200), (3, 30), (35, 350), (27, 270), (48, 480)], 1):
        recs = [_line(rev, total)]
        if i == 5:
            recs.append(_line(50, 500))
        _load(db, f"b{i}", recs)
    _init(db)
    return db


# ---------------------------------------------------------------------------
# Initialisation contract
# ---------------------------------------------------------------------------

def test_init_is_explicit_idempotent_and_never_deletes(tmp_path):
    db = str(tmp_path / "wh.db")
    _load(db, "b1", [_line(0, 5)])
    before = table_rows(db, "SELECT COUNT(*) FROM checkpoint")[0][0]
    first = _init(db)
    assert first["watermark"] == 0
    assert first["exceptions"] == []
    assert first["policy_generation"] == 0
    assert first["warehouse_id"]
    assert table_rows(db, "SELECT COUNT(*) FROM checkpoint")[0][0] == before
    second = _init(db)
    assert second["created"] is False
    assert second["warehouse_id"] == first["warehouse_id"]
    assert second["watermark"] == 0
    assert second["policy_generation"] == 0


def test_uninitialised_warehouse_is_legacy(tmp_path):
    db = str(tmp_path / "wh.db")
    _load(db, "b1", [_line(0, 5)])
    status = run_cli_json("retire", "status", "--db", db)
    assert status["managed"] is False
    with open(tmp_path / "t.json", "w") as fh:
        json.dump({"watermark": 0, "exceptions": []}, fh)
    rc, _, err = run_cli("retire", "plan", "--db", db, "--target",
                         str(tmp_path / "t.json"), "--out",
                         str(tmp_path / "p.json"))
    assert rc == 1 and "retire init" in err
    # And normal summaries still work on a legacy warehouse.
    roster = write_jsonl(tmp_path / "roster", [
        {"region": "r", "collector": "c", "generation": "g",
         "start": 1000, "end": 2000}])
    req = write_jsonl(tmp_path / "req", [
        {"name": "s", "region": "r", "start": 1000, "end": 2000,
         "cuts_ms": [10]}])
    _line(db, 0, 5)
    _load(db, "b2", [_line(0, 5, ts=2000)])
    rc, _, err = run_cli("summary", "--db", db, "--version", "1",
                         "--roster", str(roster), "--request", str(req),
                         "--out", str(tmp_path / "out.db"))
    assert rc == 0, err


# ---------------------------------------------------------------------------
# Policy validation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("target,needle", [
    ({"watermark": 6, "exceptions": []}, "beyond"),
    ({"watermark": -1, "exceptions": []}, "non-negative"),
    ({"watermark": 4, "exceptions": [{"work_order": "", "version": 1}]},
     "non-empty"),
    ({"watermark": 4, "exceptions": [{"work_order": "a", "version": 1},
                                     {"work_order": "a", "version": 2}]},
     "duplicate"),
    ({"watermark": 4, "exceptions": [{"work_order": "a", "version": 99}]},
     "beyond"),
    ({"watermark": 4, "exceptions": [{"work_order": "a", "version": 99},
                                     {"work_order": "b", "version": 99}]},
     "beyond"),
    ({"exceptions": []}, "watermark"),
    ({"watermark": 4}, "exceptions"),
    ({"watermark": 4, "exceptions": [], "surprise": 1}, "unexpected"),
])
def test_target_policy_validation(tmp_path, drill_wh, target, needle):
    with open(tmp_path / "t.json", "w") as fh:
        json.dump(target, fh)
    rc, _, err = run_cli("retire", "plan", "--db", drill_wh,
                         "--target", str(tmp_path / "t.json"),
                         "--out", str(tmp_path / "p.json"))
    assert rc == 1, err
    assert needle in err


def test_watermark_cannot_move_backwards(tmp_path, drill_wh):
    plan, _ = _plan(drill_wh, {"watermark": 4, "exceptions": []}, tmp_path,
                    "one.json")
    _approve(drill_wh, plan, tmp_path)
    with open(tmp_path / "back.json", "w") as fh:
        json.dump({"watermark": 2, "exceptions": []}, fh)
    rc, _, err = run_cli("retire", "plan", "--db", drill_wh, "--target",
                         str(tmp_path / "back.json"), "--out",
                         str(tmp_path / "p.json"))
    assert rc == 1 and "backwards" in err


def test_exception_cannot_readopt_retired_version(tmp_path, drill_wh):
    plan, _ = _plan(drill_wh, {"watermark": 4, "exceptions": []}, tmp_path)
    _approve(drill_wh, plan, tmp_path)
    rc, _, err = run_cli(
        "retire", "plan", "--db", drill_wh,
        "--target", write_jsonl(tmp_path / "t", [
            {"watermark": 4,
             "exceptions": [{"work_order": "x", "version": 3}]}]),
        "--out", str(tmp_path / "p2.json"))
    assert rc == 1 and "re-adopted" in err


# ---------------------------------------------------------------------------
# Eviction semantics
# ---------------------------------------------------------------------------

def test_drill_watermark_4_protects_revision_20_only(tmp_path, drill_wh):
    plan, _ = _plan(drill_wh, {
        "watermark": 4,
        "exceptions": [{"work_order": "wo-1", "version": 1}]}, tmp_path)
    assert plan["newly_retired_versions"] == [[2, 3]]
    assert plan["retained_payload_rows"] == 3
    assert plan["evicted_payload_rows"] == 3
    assert plan["evicted_payload_bytes"] > 0
    receipt = _approve(drill_wh, plan, tmp_path)
    assert receipt["evicted_rows"] == 3
    assert receipt["evicted_payload_bytes"] == plan["evicted_payload_bytes"]
    # Only 20, 35, 50 remain: 35 is selected at V=4 and 50 at V=5; 48 is
    # a same-batch record that never became the visible maximum.
    assert _revisions(drill_wh) == [20, 35, 50]
    assert current_version(drill_wh) == 5           # no version allocated
    assert table_rows(drill_wh, "SELECT COUNT(*) FROM batch")[0][0] == 5


def test_retained_rows_keep_batch_and_line(tmp_path, drill_wh):
    before = table_rows(
        drill_wh,
        "SELECT region, collector, generation, ts, revision, batch_id,"
        " line_no FROM checkpoint WHERE revision IN (20, 35, 50)"
        " ORDER BY revision")
    plan, _ = _plan(drill_wh, {
        "watermark": 4,
        "exceptions": [{"work_order": "w", "version": 1}]}, tmp_path)
    _approve(drill_wh, plan, tmp_path)
    after = table_rows(
        drill_wh,
        "SELECT region, collector, generation, ts, revision, batch_id,"
        " line_no FROM checkpoint WHERE revision IN (20, 35, 50)"
        " ORDER BY revision")
    assert before == after


def test_late_low_revision_and_same_batch_judged_by_visibility(tmp_path):
    """A late low revision is evicted only when no replayable version
    would select it; the same-batch 48/50 pair keeps 50 and drops 48."""
    db = str(tmp_path / "wh.db")
    _load(db, "b1", [_line(10, 100)])          # v1: rev 10
    _load(db, "b2", [_line(40, 400)])          # v2: rev 40 (visible)
    _load(db, "b3", [_line(5, 50)])            # v3: late rev 5, not visible
    _load(db, "b4", [_line(41, 410)])          # v4: rev 41 visible
    _init(db)
    # Watermark 4 keeps v4 only: rev 10, 40, 5 all lose every selector.
    plan, _ = _plan(db, {"watermark": 4, "exceptions": []}, tmp_path)
    _approve(db, plan, tmp_path)
    assert _revisions(db) == [41]


def test_retention_decision_ignores_checkpoint_clock(tmp_path):
    db = str(tmp_path / "wh.db")
    # The older payload clock does not matter; only versions do.
    _load(db, "b1", [_line(1, 10, ts=999999)])
    _load(db, "b2", [_line(2, 20, ts=1)])
    _init(db)
    plan, _ = _plan(db, {"watermark": 2, "exceptions": []}, tmp_path)
    _approve(db, plan, tmp_path)
    assert _revisions(db) == [1, 2]


def test_evicted_bytes_is_canonical_payload_size(tmp_path, drill_wh):
    from histoware.common import canonical_payload_bytes
    expect = sum(len(canonical_payload_bytes(
        "r", "c", "g", 1000, rev, "[10]", f"[{total}]", str(total)))
        for rev, total in [(3, 30), (48, 480), (27, 270)])
    plan, _ = _plan(drill_wh, {
        "watermark": 4,
        "exceptions": [{"work_order": "w", "version": 1}]}, tmp_path)
    assert plan["evicted_payload_bytes"] == expect


def test_space_is_reusable_and_file_not_shrunk(tmp_path, drill_wh):
    size_before = os.path.getsize(drill_wh)
    plan, _ = _plan(drill_wh, {"watermark": 4, "exceptions": []}, tmp_path)
    _approve(drill_wh, plan, tmp_path)
    detail = run_cli_json("retire", "status", "--db", drill_wh)["receipts"][0]
    assert detail["detail"]["freelist_pages_after"] >= \
        detail["detail"]["freelist_pages_before"]
    assert os.path.getsize(drill_wh) >= size_before  # not shrunk


# ---------------------------------------------------------------------------
# Multiple work orders
# ---------------------------------------------------------------------------

def test_two_work_orders_release_one_by_one(tmp_path, drill_wh):
    plan, _ = _plan(drill_wh, {
        "watermark": 4,
        "exceptions": [{"work_order": "A", "version": 1},
                       {"work_order": "B", "version": 1}]}, tmp_path)
    first = _approve(drill_wh, plan, tmp_path)
    assert first["evicted_rows"] == 3
    assert 20 in _revisions(drill_wh)
    # Releasing A leaves B protecting version 1: still replayable.
    status = run_cli_json("retire", "status", "--db", drill_wh)
    assert status["exceptions"] == [{"work_order": "A", "version": 1},
                                    {"work_order": "B", "version": 1}]
    plan_b, _ = _plan(drill_wh, {
        "watermark": 4,
        "exceptions": [{"work_order": "B", "version": 1}]}, tmp_path,
        "b.json")
    rel_a = _approve(drill_wh, plan_b, tmp_path, "a.json")
    assert rel_a["evicted_rows"] == 0
    assert 20 in _revisions(drill_wh)
    plan_c, _ = _plan(drill_wh, {"watermark": 4, "exceptions": []},
                      tmp_path, "c.json")
    rel_b = _approve(drill_wh, plan_c, tmp_path, "c.json")
    assert rel_b["evicted_rows"] == 1
    assert _revisions(drill_wh) == [35, 50]


def test_protection_survives_same_batch_visibility(tmp_path):
    """Version 3 selecting the same row as version 4 must not make a
    protected version lose its payload."""
    db = str(tmp_path / "wh.db")
    _load(db, "b1", [_line(1, 10)])
    _load(db, "b2", [_line(2, 20)])
    _load(db, "b3", [_line(3, 30)])                 # rev 3 visible at v3
    _init(db)
    # Watermark 3 keeps only v3, but work order W protects v2, where rev 2
    # is visible -- even though v3 sees the same rows, rev 2 must stay.
    plan, _ = _plan(db, {"watermark": 3, "exceptions": [
        {"work_order": "w", "version": 2}]}, tmp_path)
    assert plan["base_current_version"] == 3
    _approve(db, plan, tmp_path)
    assert _revisions(db) == [2, 3]


# ---------------------------------------------------------------------------
# Plans, approval and receipts
# ---------------------------------------------------------------------------

def test_plan_is_deterministic_and_rehearsal_is_read_only(tmp_path, drill_wh):
    import hashlib
    digest = hashlib.sha256(open(drill_wh, "rb").read()).hexdigest()
    p1, _ = _plan(drill_wh, {"watermark": 4, "exceptions": [
        {"work_order": "w", "version": 1}]}, tmp_path, "a.json")
    p2, _ = _plan(drill_wh, {"watermark": 4, "exceptions": [
        {"work_order": "w", "version": 1}]}, tmp_path, "b.json")
    assert p1["plan_id"] == p2["plan_id"]
    for key in ("newly_retired_versions", "retained_payload_rows",
                "evicted_payload_rows", "evicted_payload_bytes",
                "eviction_digest"):
        assert p1[key] == p2[key]
    assert hashlib.sha256(open(drill_wh, "rb").read()).hexdigest() == digest
    # No policy was applied either.
    status = run_cli_json("retire", "status", "--db", drill_wh)
    assert status["policy_generation"] == 0


def test_plan_is_bound_to_snapshot_and_rejects_new_load(
        tmp_path, drill_wh):
    plan, _ = _plan(drill_wh, {"watermark": 4, "exceptions": []}, tmp_path)
    _load(drill_wh, "b6", [_line(60, 600, ts=1000)])
    rc, _, err = run_cli("retire", "approve", "--db", drill_wh,
                         "--plan", str(tmp_path / "plan.json.plan"))
    assert rc == 1 and "stale" in err


def test_plan_is_bound_to_policy_generation(tmp_path, drill_wh):
    plan, _ = _plan(drill_wh, {"watermark": 4, "exceptions": []}, tmp_path)
    other, _ = _plan(drill_wh, {"watermark": 2, "exceptions": []}, tmp_path,
                     "other.json")
    _approve(drill_wh, other, tmp_path, "other-approve.json")
    rc, _, err = run_cli("retire", "approve", "--db", drill_wh,
                         "--plan", str(tmp_path / "plan.json.plan"))
    assert rc == 1 and "stale" in err


def test_plan_cannot_cross_warehouses(tmp_path, drill_wh):
    plan, _ = _plan(drill_wh, {"watermark": 4, "exceptions": []}, tmp_path)
    other = str(tmp_path / "other.db")
    _load(other, "b1", [_line(0, 1)])
    _init(other)
    rc, _, err = run_cli("retire", "approve", "--db", other,
                         "--plan", str(tmp_path / "plan.json.plan"))
    assert rc == 1 and "different warehouse" in err


def test_modified_plan_reusing_its_identifier_is_rejected(tmp_path, drill_wh):
    plan, _ = _plan(drill_wh, {"watermark": 4, "exceptions": []}, tmp_path)
    tampered = dict(plan, evicted_payload_rows=0)
    rc, _, err = run_cli("retire", "approve", "--db", drill_wh,
                         "--plan", str(write_jsonl(tmp_path / "bad",
                                                   [tampered])))
    assert rc == 1 and "modified" in err
    assert run_cli_json("retire", "status", "--db", drill_wh)[
        "policy_generation"] == 0
    # The untouched plan still applies.
    _approve(drill_wh, plan, tmp_path)


def test_resubmitting_a_committed_plan_returns_original_receipt(
        tmp_path, drill_wh):
    plan, _ = _plan(drill_wh, {"watermark": 4, "exceptions": []}, tmp_path)
    first = _approve(drill_wh, plan, tmp_path)
    # Later load and later policy must not change the replayed receipt.
    _load(drill_wh, "b6", [_line(60, 600)])
    second = _approve(drill_wh, plan, tmp_path, "again.json")
    assert second["reused"] is True
    assert second["receipt"]["receipt_id"] == first["receipt"]["receipt_id"]
    assert second["receipt"]["evicted_rows"] == \
        first["receipt"]["evicted_rows"]


def test_receipt_survives_process_exit_and_is_queryable(tmp_path, drill_wh):
    plan, _ = _plan(drill_wh, {"watermark": 4, "exceptions": []}, tmp_path)
    _approve(drill_wh, plan, tmp_path)
    code = textwrap.dedent(f"""
        import json
        from histoware import retention
        r = retention.get_receipt({drill_wh!r}, {plan['plan_id']!r})
        print(json.dumps(r))
    """)
    out = subprocess.run([sys.executable, "-c", code],
                         cwd=ROOT, capture_output=True, text=True, check=True)
    receipt = json.loads(out.stdout)
    assert receipt["plan_id"] == plan["plan_id"]
    assert receipt["evicted_rows"] == plan["evicted_payload_rows"]
    assert receipt["evicted_payload_bytes"] == plan["evicted_payload_bytes"]


def _abort_after(monkeypatch, point):
    def hook(name, analysis):
        if name == point:
            raise RuntimeError("simulated crash")
    monkeypatch.setattr(retention, "_TEST_HOOK", hook)


@pytest.mark.parametrize("point", ["after_analysis", "before_policy",
                                   "before_commit"])
def test_aborted_approval_is_all_or_nothing(tmp_path, drill_wh, monkeypatch,
                                            point):
    plan, _ = _plan(drill_wh, {"watermark": 4, "exceptions": []}, tmp_path)
    _abort_after(monkeypatch, point)
    with pytest.raises(RuntimeError):
        _approve(drill_wh, plan, tmp_path)
    monkeypatch.setattr(retention, "_TEST_HOOK", None)
    status = run_cli_json("retire", "status", "--db", drill_wh)
    assert status["policy_generation"] == 0
    assert status["watermark"] == 0
    assert status["receipts"] == []
    assert len(_revisions(drill_wh)) == 6           # nothing deleted
    # The same plan applies cleanly afterwards.
    ok = _approve(drill_wh, plan, tmp_path)
    assert ok["receipt"]["evicted_rows"] == plan["evicted_payload_rows"]


def test_killed_process_before_commit_leaves_no_partial_state(
        tmp_path, drill_wh):
    plan, _ = _plan(drill_wh, {"watermark": 4, "exceptions": []}, tmp_path)
    code = textwrap.dedent(f"""
        import os, sys
        from histoware import retention
        def hook(name, analysis):
            if name == "before_commit":
                os._exit(9)
        retention._TEST_HOOK = hook
        retention.approve({drill_wh!r}, {plan!r})
    """)
    proc = subprocess.run([sys.executable, "-c", code], cwd=ROOT)
    assert proc.returncode == 9
    status = run_cli_json("retire", "status", "--db", drill_wh)
    assert status["watermark"] == 0
    assert status["policy_generation"] == 0
    assert status["receipts"] == []
    assert _revisions(drill_wh) == [3, 20, 27, 35, 48, 50]
    ok = _approve(drill_wh, plan, tmp_path)
    assert ok["receipt"]["evicted_rows"] == plan["evicted_payload_rows"]


def test_approval_and_load_are_ordered_by_persistent_commit(tmp_path,
                                                            drill_wh):
    """Whichever commits first wins.  A load committed before approval
    makes the plan stale (refused); a load committed after approval is
    not swallowed and the next rehearsal sees it."""
    plan, _ = _plan(drill_wh, {"watermark": 4, "exceptions": []}, tmp_path)
    # Load first: approval must refuse and leave the new row alone.
    late = write_jsonl(tmp_path / "late.jsonl", [_line(70, 700, ts=1001)])
    load_batch(drill_wh, "late", late)
    rc, _, err = run_cli("retire", "approve", "--db", drill_wh,
                         "--plan", str(tmp_path / "plan.json.plan"))
    assert rc == 1 and "stale" in err
    assert 70 in _revisions(drill_wh)
    assert run_cli_json("retire", "status", "--db", drill_wh)[
        "policy_generation"] == 0
    # Re-rehearse at the new state and approve, then load again: the
    # post-approval load survives and bumps the current version.
    plan2, _ = _plan(drill_wh, {"watermark": 4,
                                "exceptions": [{"work_order": "w",
                                                "version": 1}]},
                     tmp_path, "p2.json")
    _approve(drill_wh, plan2, tmp_path, "a2.json")
    _load(drill_wh, "later", [_line(80, 800, ts=1002)])
    assert current_version(drill_wh) == 7
    assert 80 in _revisions(drill_wh)
    # Replaying the committed plan returns its receipt, does no work and
    # does not remove the newer row.
    again = _approve(drill_wh, plan2, tmp_path, "a3.json")
    assert again["reused"] is True and again["evicted_rows"] == 0
    assert 80 in _revisions(drill_wh)
    assert 20 in _revisions(drill_wh)


# ---------------------------------------------------------------------------
# Summary eligibility
# ---------------------------------------------------------------------------

def _summary_files(tmp_path):
    roster = write_jsonl(tmp_path / "roster", [
        {"region": "r", "collector": "c", "generation": "g",
         "start": 1000, "end": 2000}])
    req = write_jsonl(tmp_path / "req", [
        {"name": "s", "region": "r", "start": 1000, "end": 2000,
         "cuts_ms": []}])
    return roster, req


def test_summary_rejects_retired_version_without_touching_target(
        tmp_path, drill_wh):
    roster, req = _summary_files(tmp_path)
    out = str(tmp_path / "out.db")
    rc, _, err = run_cli("summary", "--db", drill_wh, "--version", "5",
                         "--roster", str(roster), "--request", str(req),
                         "--out", out)
    assert rc == 0, err
    import hashlib
    digest = hashlib.sha256(open(out, "rb").read()).hexdigest()
    plan, _ = _plan(drill_wh, {"watermark": 4, "exceptions": [
        {"work_order": "w", "version": 1}]}, tmp_path)
    _approve(drill_wh, plan, tmp_path)
    rc, _, err = run_cli("summary", "--db", drill_wh, "--version", "3",
                         "--roster", str(roster), "--request", str(req),
                         "--out", out)
    assert rc == 2
    assert "retired" in err
    assert hashlib.sha256(open(out, "rb").read()).hexdigest() == digest
    # Version 3 was retired even though version 4 selects the same rows.
    rc, _, err = run_cli("summary", "--db", drill_wh, "--version", "4",
                         "--roster", str(roster), "--request", str(req),
                         "--out", str(tmp_path / "v4.db"))
    # v4 is short one endpoint (the endpoint at ts=2000 is not loaded in
    # the drill warehouse) -> it may be partial, but not a retirement error.
    assert "retired" not in err


def test_retained_version_summary_results_unchanged(tmp_path):
    """A summary of a still-replayable version before and after eviction
    of other versions yields identical checkpoint results and sources."""
    db = str(tmp_path / "wh.db")
    _load(db, "b1", [_line(0, 0, ts=1000), _line(0, 10, ts=2000)])
    _load(db, "b2", [_line(1, 5, ts=1000), _line(1, 20, ts=2000)])
    _load(db, "b3", [_line(2, 7, ts=1000), _line(2, 30, ts=2000)])
    roster, req = _summary_files(tmp_path)
    before = str(tmp_path / "before.db")
    rc, _, err = run_cli("summary", "--db", db, "--version", "2",
                         "--roster", str(roster), "--request", str(req),
                         "--out", before)
    assert rc == 0, err
    _init(db)
    plan, _ = _plan(db, {"watermark": 2, "exceptions": []}, tmp_path)
    _approve(db, plan, tmp_path)
    after = str(tmp_path / "after.db")
    rc, _, err = run_cli("summary", "--db", db, "--version", "2",
                         "--roster", str(roster), "--request", str(req),
                         "--out", after)
    assert rc == 0, err

    def dump(path):
        conn = sqlite3.connect(path)
        try:
            return [conn.execute(f"SELECT * FROM {t}").fetchall()
                    for t in ("shift", "shift_cut", "intersection",
                              "intersection_cut")]
        finally:
            conn.close()
    assert dump(before) == dump(after)


def test_summary_snapshot_spanning_approval_uses_one_view(tmp_path):
    """A summary that already holds its snapshot completes on the old
    view even if a retirement commits mid-run."""
    from histoware import summary as summary_mod
    db = str(tmp_path / "wh.db")
    _load(db, "b1", [_line(0, 0, ts=1000), _line(0, 10, ts=2000)])
    _load(db, "b2", [_line(1, 5, ts=1000), _line(1, 20, ts=2000)])
    _init(db)
    roster, req = _summary_files(tmp_path)
    plan, _ = _plan(db, {"watermark": 2, "exceptions": []}, tmp_path)
    state = {"fired": False}
    original = summary_mod.EndpointIndex

    class Hooking(original):
        def lookup(self, *args, **kwargs):
            if not state["fired"]:
                state["fired"] = True
                retention.approve(db, plan)
            return super().lookup(*args, **kwargs)

    summary_mod.EndpointIndex = Hooking
    try:
        out = str(tmp_path / "out.db")
        stats = summary_mod.run_summary(db, 1, roster, req, out)
    finally:
        summary_mod.EndpointIndex = original
    assert state["fired"]
    assert stats["warehouse_version"] == 1


# ---------------------------------------------------------------------------
# Continued business loading
# ---------------------------------------------------------------------------

def test_historical_batch_resend_returns_original_version_after_eviction(
        tmp_path, drill_wh):
    _init(drill_wh)
    plan, _ = _plan(drill_wh, {"watermark": 4, "exceptions": []}, tmp_path)
    _approve(drill_wh, plan, tmp_path)
    # Re-send batch b2 verbatim (its revision 3 was evicted).
    res = run_cli_json("load", "--db", drill_wh, "--batch", "b2",
                       "--file", str(write_jsonl(tmp_path / "b2.jsonl",
                                                 [_line(3, 30)])))
    assert res["reused"] is True
    assert res["version"] == 2
    assert 3 not in _revisions(drill_wh)


def test_equivalent_retransmission_of_evicted_revision_is_duplicate(
        tmp_path, drill_wh):
    _init(drill_wh)
    plan, _ = _plan(drill_wh, {"watermark": 4, "exceptions": []}, tmp_path)
    _approve(drill_wh, plan, tmp_path)
    res = _load(drill_wh, "new", [_line(3, 30), _line(99, 990)])
    assert res["new_checkpoints"] == 1        # only revision 99 is new
    assert res["retransmitted_evicted"] == 1
    assert 3 not in _revisions(drill_wh)
    assert 99 in _revisions(drill_wh)


def test_conflicting_retransmission_of_evicted_revision_names_source(
        tmp_path, drill_wh):
    _init(drill_wh)
    plan, _ = _plan(drill_wh, {"watermark": 4, "exceptions": []}, tmp_path)
    _approve(drill_wh, plan, tmp_path)
    rc, _, err = run_cli(
        "load", "--db", drill_wh, "--batch", "bad",
        "--file", str(write_jsonl(tmp_path / "bad.jsonl", [_line(3, 999)])))
    assert rc == 1
    assert "b2" in err and "line 1" in err and "retired" in err
    assert current_version(drill_wh) == 5
    assert 3 not in _revisions(drill_wh)


def test_new_checkpoints_accepted_after_retirement_and_rehearsable(
        tmp_path, drill_wh):
    _init(drill_wh)
    plan, _ = _plan(drill_wh, {"watermark": 4, "exceptions": []}, tmp_path)
    _approve(drill_wh, plan, tmp_path)
    res = _load(drill_wh, "b6", [_line(60, 600, ts=1001)])
    assert res["version"] == 6 and res["new_checkpoints"] == 1
    plan2, _ = _plan(drill_wh, {"watermark": 5, "exceptions": []}, tmp_path,
                     "second.json")
    assert plan2["base_current_version"] == 6
    # Moving the watermark 4 -> 5 retires exactly version 4.
    assert plan2["newly_retired_versions"] == [[4, 4]]
    _approve(drill_wh, plan2, tmp_path, "second-approve.json")


# ---------------------------------------------------------------------------
# Independent small-history enumeration (no missed / no spurious evictions)
# ---------------------------------------------------------------------------

def _visible_after_evictions(rows, watermark, exceptions, current=None):
    """Reference keep set, computed naively from the selection rule.

    `rows` is ``[(revision, intro_version)]`` for one endpoint.  At a
    version V the visible revision is the greatest revision with intro
    version <= V; a row is kept when some replayable V selects it.
    `current` is the warehouse's current version (defaults to the
    endpoint's highest intro version, which is enough when the endpoint
    history reaches the current version).
    """
    if not rows:
        return set()
    if current is None:
        current = max(v for _, v in rows)
    protected = {e["version"] for e in exceptions if e["version"] < watermark}
    replay = sorted(set(range(watermark, current + 1)) | protected)
    keep = set()
    for rev, intro in rows:
        for V in replay:
            candidates = [r for r, i in rows if i <= V]
            if candidates and max(candidates) == rev:
                keep.add(rev)
                break
    return keep


@pytest.mark.parametrize("seed", range(12))
def test_sparse_and_contiguous_policies_against_reference(tmp_path, seed):
    """One endpoint, multiple revisions per batch, late low revisions and
    same-batch revisions: the kept set must equal a naive enumeration of
    "selected by some replayable version"."""
    import random
    rng = random.Random(seed)
    n = rng.randint(1, 8)
    batches = [[] for _ in range(rng.randint(1, 6))]
    for rev in sorted(rng.sample(range(0, 30), n)):
        batches[rng.randrange(len(batches))].append(rev)
    db = str(tmp_path / "wh.db")
    rows = []                       # (revision, intro version)
    for i, revs in enumerate(batches, 1):
        if not revs:
            continue                # an unloaded batch allocates no version
        res = _load(db, f"b{i}", [_line(rev, rev + 100) for rev in revs])
        for rev in revs:
            rows.append((rev, res["version"]))
    current = current_version(db)
    if current == 0:
        pytest.skip("no rows generated")
    _init(db)
    wm = rng.randint(0, current)
    prot = [v for v in range(1, wm) if rng.random() < 0.3]
    exceptions = [{"work_order": f"w{i}", "version": v}
                  for i, v in enumerate(prot)]
    plan, _ = _plan(db, {"watermark": wm, "exceptions": exceptions}, tmp_path)
    _approve(db, plan, tmp_path)
    actual = set(_revisions(db))
    expected = _visible_after_evictions(rows, wm, exceptions, current)
    assert actual == expected


@pytest.mark.parametrize("seed", range(8))
def test_multi_endpoint_reference(tmp_path, seed):
    """Several endpoints with interleaved revision histories: eviction is
    decided per endpoint and never bleeds across endpoints."""
    import random
    rng = random.Random(1000 + seed)
    db = str(tmp_path / "wh.db")
    rows = {}                       # endpoint -> [(rev, intro)]
    versions = rng.randint(1, 6)
    for v in range(1, versions + 1):
        recs = []
        staged = []                 # (endpoint, rev)
        for e in range(rng.randint(1, 4)):
            endpoint = ("r", f"c{e}", "g", 1000 + e)
            for _ in range(rng.randint(0, 2)):
                rev = rng.randrange(0, 12)
                if any(r == rev for r, _ in rows.get(endpoint, [])):
                    continue
                if (endpoint, rev) in staged:
                    continue
                recs.append(_line(rev, rev + 1, ts=1000 + e,
                                  collector=f"c{e}"))
                staged.append((endpoint, rev))
        if not recs:
            continue
        res = _load(db, f"b{v}", recs)
        for endpoint, rev in staged:
            rows.setdefault(endpoint, []).append((rev, res["version"]))
    current = current_version(db)
    if current == 0:
        pytest.skip("no rows generated")
    _init(db)
    wm = rng.randint(0, current)
    prot = [v for v in range(1, wm) if rng.random() < 0.3]
    exceptions = [{"work_order": f"w{i}", "version": v}
                  for i, v in enumerate(prot)]
    plan, _ = _plan(db, {"watermark": wm, "exceptions": exceptions}, tmp_path)
    _approve(db, plan, tmp_path)
    actual = set()
    for collector, ts, rev in table_rows(
            db, "SELECT collector, ts, revision FROM checkpoint"):
        actual.add((collector, ts, rev))
    expected = set()
    for endpoint, history in rows.items():
        expected |= {(endpoint[1], endpoint[3], rev)
                     for rev in _visible_after_evictions(
                         history, wm, exceptions, current)}
    assert actual == expected


def test_approval_is_atomic_to_single_snapshot_readers(tmp_path):
    """A reader holding one snapshot sees either the whole pre-state or the
    whole post-state - never a policy update without its row deletions (or
    a receipt without them)."""
    import threading
    db = str(tmp_path / "wh.db")
    for i, (rev, total) in enumerate(
            [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)], 1):
        _load(db, f"b{i}", [_line(rev, total)])
    _init(db)
    plan, _ = _plan(db, {"watermark": 4, "exceptions": []}, tmp_path)
    stop = {"flag": False}
    seen = set()
    errors = []

    def reader():
        while not stop["flag"]:
            try:
                conn = sqlite3.connect(db, isolation_level=None)
                conn.execute("BEGIN")
                wm = conn.execute(
                    "SELECT value FROM meta WHERE key='retention_watermark'"
                ).fetchone()[0]
                gen = conn.execute(
                    "SELECT value FROM meta WHERE key='policy_generation'"
                ).fetchone()[0]
                n = conn.execute(
                    "SELECT COUNT(*) FROM receipt WHERE kind='retirement'"
                ).fetchone()[0]
                rows = conn.execute(
                    "SELECT COUNT(*) FROM checkpoint").fetchone()[0]
                conn.execute("ROLLBACK")
                conn.close()
                seen.add((wm, gen, n, rows))
            except Exception as exc:        # pragma: no cover - diagnostic
                errors.append(exc)

    thread = threading.Thread(target=reader)
    thread.start()
    try:
        retention.approve(db, plan)
    finally:
        stop["flag"] = True
        thread.join(10)
    assert not errors
    allowed = {("0", "0", 0, 5), ("4", "1", 1, 2)}
    assert seen <= allowed, seen - allowed


def test_no_false_eviction_when_two_work_orders_share_a_version(
        tmp_path):
    db = str(tmp_path / "wh.db")
    _load(db, "b1", [_line(1, 10)])
    _load(db, "b2", [_line(2, 20)])
    _init(db)
    plan, _ = _plan(db, {"watermark": 2, "exceptions": [
        {"work_order": "a", "version": 1},
        {"work_order": "b", "version": 1}]}, tmp_path)
    _approve(db, plan, tmp_path)
    assert _revisions(db) == [1, 2]


def test_eviction_digest_identifies_the_removal_set(tmp_path):
    """The digest is order-independent, stable for the same removal set,
    and changes when an evicted payload changes.  Two warehouses with the
    same rows but different identifiers yield different plan ids."""
    def plan_for(tmp, tag, payload_bump=0):
        db = str(tmp / f"{tag}.db")
        for i, (rev, total) in enumerate(
                [(20, 200), (3, 30), (35, 350), (27, 270), (48, 480)], 1):
            _load(db, f"b{i}", [_line(rev, total + payload_bump
                                      if rev == 3 else total)])
        _init(db)
        plan, _ = _plan(db, {"watermark": 4, "exceptions": []},
                        tmp_path, f"{tag}.json")
        return plan

    a = plan_for(tmp_path, "a")
    b = plan_for(tmp_path, "b")
    # Same rows and target, different warehouse identifier: the digest
    # (which covers revision identity + canonical payload hash) matches,
    # while the plan id (which includes the warehouse id) does not.
    assert a["eviction_digest"] == b["eviction_digest"]
    assert a["plan_id"] != b["plan_id"]
    # Changing one evicted payload changes the digest.
    c = plan_for(tmp_path, "c", payload_bump=1)
    assert c["eviction_digest"] != a["eviction_digest"]
    assert c["evicted_payload_rows"] == a["evicted_payload_rows"]


def test_approval_rejects_plan_whose_digest_lied(tmp_path, drill_wh):
    """A plan that keeps its id but carries business figures altered in a
    way the id would still cover is impossible; a plan whose fields were
    edited fails the id check, and an untouched plan with a forged count
    cannot reach the execution path."""
    plan, _ = _plan(drill_wh, {"watermark": 4, "exceptions": []}, tmp_path)
    import hashlib
    from histoware.retention import plan_id_for
    forged = {k: v for k, v in plan.items()
              if k not in ("plan_id", "created_utc")}
    forged["evicted_payload_rows"] = 0
    forged["plan_id"] = plan_id_for(forged)   # attacker recomputes the id
    with pytest.raises(HistowareError):
        # Approval recomputes the eviction set and refuses the mismatch.
        retention.approve(drill_wh, forged)
    assert retention.get_status(drill_wh)["policy_generation"] == 0
