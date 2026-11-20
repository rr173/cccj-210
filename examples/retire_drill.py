#!/usr/bin/env python3
"""Re-runnable drill for histoware version retirement.

Everything is generated under a fresh temporary directory (no committed
databases).  The script walks through the specification's worked example
and the surrounding guarantees, printing a PASS/FAIL line per check and
exiting non-zero on the first failure.

Run:  python3 -m examples.retire_drill
  or: python3 examples/retire_drill.py
"""
import hashlib
import json
import os
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from histoware import retention                      # noqa: E402
from histoware.common import HistowareError, RetiredVersionError   # noqa: E402
from histoware.warehouse import load_batch           # noqa: E402
from histoware import summary as summary_mod         # noqa: E402

FAILURES = []
CHECKS = [0]


def check(label, condition, detail=""):
    CHECKS[0] += 1
    mark = "PASS" if condition else "FAIL"
    print(f"[{mark}] {label}" + (f"  ({detail})" if detail else ""))
    if not condition:
        FAILURES.append(label)


def line(rev, total, ts=1000, region="r", collector="c", generation="g",
         cuts=None, counts=None):
    return json.dumps({
        "region": region, "collector": collector, "generation": generation,
        "ts": ts, "revision": rev,
        "cuts_ms": cuts if cuts is not None else [10],
        "counts": counts if counts is not None else [total],
        "total": total}) + "\n"


def load(db, batch, text):
    lines = [ln.encode() for ln in text.splitlines(keepends=True)]
    return load_batch(db, batch, iter(lines))


def revisions(db):
    import sqlite3
    conn = sqlite3.connect(db)
    try:
        return [r[0] for r in conn.execute(
            "SELECT revision FROM checkpoint ORDER BY revision")]
    finally:
        conn.close()


def roster_request(tmp):
    roster = os.path.join(tmp, "roster.jsonl")
    with open(roster, "w") as fh:
        fh.write(json.dumps({"region": "r", "collector": "c",
                             "generation": "g", "start": 1000, "end": 2000})
                 + "\n")
    request = os.path.join(tmp, "request.jsonl")
    with open(request, "w") as fh:
        fh.write(json.dumps({"name": "s", "region": "r", "start": 1000,
                             "end": 2000, "cuts_ms": []}) + "\n")
    return roster, request


# ---------------------------------------------------------------------------
# 1. Worked example: revisions 20, 3, 35, 27 at versions 1..4, then 48/50
# ---------------------------------------------------------------------------

def drill_worked_example(tmp):
    print("\n== 1. Worked example: waterline 4, work order protecting v1 ==")
    db = os.path.join(tmp, "drill.db")
    for i, (rev, total) in enumerate(
            [(20, 200), (3, 30), (35, 350), (27, 270), (48, 480)], 1):
        text = line(rev, total)
        if i == 5:
            text += line(50, 500)
        load(db, f"b{i}", text)
    status_before = retention.get_status(db)
    check("legacy warehouse is unmanaged before init",
          status_before["managed"] is False)
    init = retention.init_retention(db)
    check("init sets watermark 0 / no exceptions / generation 0",
          init["watermark"] == 0 and init["exceptions"] == []
          and init["policy_generation"] == 0)
    check("init never deletes payloads",
          len(revisions(db)) == 6, f"{len(revisions(db))} rows")
    again = retention.init_retention(db)
    check("re-init is a no-op (identifier and state kept)",
          again["created"] is False
          and again["warehouse_id"] == init["warehouse_id"]
          and again["policy_generation"] == 0)

    target = {"watermark": 4,
              "exceptions": [{"work_order": "wo-1", "version": 1}]}
    plan = retention.rehearse(db, target)
    check("pushback: only versions 2..3 newly retired",
          plan["newly_retired_versions"] == [[2, 3]],
          str(plan["newly_retired_versions"]))
    check("pushback: reports retained / evicted payload rows",
          (plan["retained_payload_rows"], plan["evicted_payload_rows"])
          == (3, 3))
    check("pushback: reports normalised payload bytes, not file delta",
          plan["evicted_payload_bytes"] > 0,
          f"{plan['evicted_payload_bytes']} bytes")
    # Rehearsal is read-only and deterministic.
    plan2 = retention.rehearse(db, target)
    check("pushback: same snapshot + target -> same plan id and figures",
          plan2["plan_id"] == plan["plan_id"]
          and plan2["eviction_digest"] == plan["eviction_digest"])

    receipt = retention.approve(db, plan)
    check("approve: actual eviction matches the plan",
          receipt["evicted_rows"] == plan["evicted_payload_rows"]
          and receipt["evicted_payload_bytes"]
          == plan["evicted_payload_bytes"])
    check("keep 20 (protected v1), 35 (selected at v4), 50 (selected at v5)",
          revisions(db) == [20, 35, 50], str(revisions(db)))
    check("48 (same batch, never the visible maximum) was evicted",
          48 not in revisions(db))
    check("eviction allocates no new warehouse version",
          retention.get_status(db)["current_version"] == 5)
    check("eviction keeps the batch ledger intact",
          _count(db, "SELECT COUNT(*) FROM batch") == 5)
    check("re-submitting the committed plan returns the same receipt",
          retention.approve(db, plan)["receipt"]["receipt_id"]
          == receipt["receipt"]["receipt_id"])

    # Version 3 is retired even though version 4 selects the same row.
    roster, request = roster_request(tmp)
    out = os.path.join(tmp, "v3.db")
    try:
        summary_mod.run_summary(db, 3, roster, request, out)
        check("summary of retired v3 is refused", False)
    except RetiredVersionError as exc:
        check("summary of retired v3 raises a distinct error",
              "retired" in str(exc))
    check("refused summary leaves no target file", not os.path.exists(out))
    try:
        summary_mod.run_summary(db, 4, roster, request,
                                os.path.join(tmp, "v4.db"))
        check("summary of replayable v4 is allowed", True)
    except HistowareError as exc:
        check("summary of replayable v4 is allowed", False, str(exc))
    return db


def _count(db, sql):
    import sqlite3
    conn = sqlite3.connect(db)
    try:
        return conn.execute(sql).fetchone()[0]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 2. Two work orders pointing at one version release independently
# ---------------------------------------------------------------------------

def drill_two_work_orders(tmp):
    print("\n== 2. Two work orders on version 1, released one at a time ==")
    db = os.path.join(tmp, "two-wo.db")
    for i, (rev, total) in enumerate(
            [(20, 200), (3, 30), (35, 350), (27, 270), (48, 480)], 1):
        text = line(rev, total) + (line(50, 500) if i == 5 else "")
        load(db, f"b{i}", text)
    retention.init_retention(db)
    p = retention.rehearse(db, {"watermark": 4, "exceptions": [
        {"work_order": "A", "version": 1},
        {"work_order": "B", "version": 1}]})
    retention.approve(db, p)
    check("both work orders protect v1: revision 20 stays",
          20 in revisions(db))
    p = retention.rehearse(db, {"watermark": 4, "exceptions": [
        {"work_order": "B", "version": 1}]})
    r = retention.approve(db, p)
    check("lifting A does not lift B: revision 20 still replayable",
          20 in revisions(db) and r["evicted_rows"] == 0)
    p = retention.rehearse(db, {"watermark": 4, "exceptions": []})
    r = retention.approve(db, p)
    check("lifting the last work order retires revision 20",
          20 not in revisions(db) and r["evicted_rows"] == 1,
          str(revisions(db)))
    try:
        retention.rehearse(db, {"watermark": 4, "exceptions": [
            {"work_order": "C", "version": 1}]})
        check("a retired version cannot be re-adopted", False)
    except HistowareError:
        check("a retired version cannot be re-adopted", True)


# ---------------------------------------------------------------------------
# 3. Independent enumeration of visible revisions, sparse vs contiguous
# ---------------------------------------------------------------------------

def reference_keep(rows, watermark, exceptions):
    protected = {e["version"] for e in exceptions if e["version"] < watermark}
    if not rows:
        return set()
    current = max(v for _, v in rows)
    replay = sorted(set(range(watermark, current + 1)) | protected)
    keep = set()
    for rev, _ in rows:
        for V in replay:
            cand = [r for r, intro in rows if intro <= V]
            if cand and max(cand) == rev:
                keep.add(rev)
                break
    return keep


def drill_enumeration(tmp):
    print("\n== 3. Independent per-version enumeration (no missed / no"
          " spurious evictions) ==")
    import random
    failures = 0
    scenarios = 0
    for seed in range(40):
        rng = random.Random(seed)
        work = os.path.join(tmp, f"enum-{seed}")
        os.makedirs(work, exist_ok=True)
        db = os.path.join(work, "wh.db")
        seen_revs = set()
        loaded = []            # (revision, intro version)
        n_batches = rng.randint(1, 6)
        for i in range(1, n_batches + 1):
            recs = []
            for _ in range(rng.randint(0, 4)):
                rev = rng.randrange(0, 40)
                if rev in seen_revs:
                    continue
                seen_revs.add(rev)
                recs.append(line(rev, rev + 1))
            if not recs:
                continue
            res = load(db, f"b{i}", "".join(recs))
            for rec in recs:
                obj = json.loads(rec)
                loaded.append((obj["revision"], res["version"]))
        current = retention.get_status(db)["current_version"] \
            if os.path.exists(db) else 0
        if current == 0:
            continue
        retention.init_retention(db)
        wm = rng.randint(0, current)
        # Alternate sparse and contiguous replay sets.
        if rng.random() < 0.5:
            prot = []
        else:
            prot = sorted(rng.sample(range(1, wm), rng.randint(0, wm - 1))) \
                if wm > 1 else []
        exceptions = [{"work_order": f"w{i}", "version": v}
                      for i, v in enumerate(prot)]
        plan = retention.rehearse(db, {"watermark": wm,
                                        "exceptions": exceptions})
        retention.approve(db, plan)
        expected = reference_keep(loaded, wm, exceptions)
        actual = set(revisions(db))
        scenarios += 1
        if expected != actual:
            failures += 1
            print(f"   mismatch seed={seed} wm={wm} prot={prot}"
                  f" expected={sorted(expected)} actual={sorted(actual)}")
    check("40 randomized sparse/contiguous scenarios match the reference",
          failures == 0, f"{scenarios} scenarios, {failures} mismatches")


# ---------------------------------------------------------------------------
# 4. Retained-version summaries keep their checkpoint results and sources
# ---------------------------------------------------------------------------

def drill_retained_summary(tmp):
    print("\n== 4. Retained-version summaries unchanged across eviction ==")
    db = os.path.join(tmp, "retained.db")
    load(db, "b1", line(0, 0, ts=1000) + line(0, 10, ts=2000))
    load(db, "b2", line(1, 5, ts=1000) + line(1, 20, ts=2000))
    load(db, "b3", line(2, 7, ts=1000) + line(2, 30, ts=2000))
    roster, request = roster_request(tmp)
    before = os.path.join(tmp, "before.db")
    summary_mod.run_summary(db, 2, roster, request, before)
    retention.init_retention(db)
    plan = retention.rehearse(db, {"watermark": 2, "exceptions": []})
    retention.approve(db, plan)
    after = os.path.join(tmp, "after.db")
    summary_mod.run_summary(db, 2, roster, request, after)

    def dump(path):
        import sqlite3
        conn = sqlite3.connect(path)
        try:
            return [conn.execute(f"SELECT * FROM {t}").fetchall()
                    for t in ("shift", "shift_cut", "intersection",
                              "intersection_cut")]
        finally:
            conn.close()
    check("summary of v2 is byte-identical in results and sources",
          dump(before) == dump(after))


# ---------------------------------------------------------------------------
# 5. Continued business loading
# ---------------------------------------------------------------------------

def drill_reloading(tmp):
    print("\n== 5. Old batch re-send, equivalent re-send, conflict ==")
    db = os.path.join(tmp, "reload.db")
    for i, (rev, total) in enumerate(
            [(20, 200), (3, 30), (35, 350), (27, 270), (48, 480)], 1):
        text = line(rev, total) + (line(50, 500) if i == 5 else "")
        load(db, f"b{i}", text)
    retention.init_retention(db)
    plan = retention.rehearse(db, {"watermark": 4, "exceptions": []})
    retention.approve(db, plan)

    # Historical batch b2 (revision 3, now evicted) re-sent verbatim.
    res = load(db, "b2", line(3, 30))
    check("historical batch re-send returns its original version",
          res["reused"] is True and res["version"] == 2)
    # A new batch carrying an equivalent copy of the evicted revision.
    res = load(db, "b9", line(3, 30) + line(99, 990))
    check("equivalent re-send of an evicted revision is deduplicated",
          res["new_checkpoints"] == 1
          and res["retransmitted_evicted"] == 1
          and 3 not in revisions(db) and 99 in revisions(db))
    # Differing content for the evicted revision is rejected with source.
    try:
        load(db, "bad", line(3, 999))
        check("conflicting re-send is rejected with first provenance", False)
    except Exception as exc:
        msg = str(exc)
        check("conflicting re-send is rejected with first provenance",
              "b2" in msg and "line 1" in msg)


# ---------------------------------------------------------------------------
# 6. Plan staleness, snapshot across approval, crash safety
# ---------------------------------------------------------------------------

def drill_plans_and_crash(tmp):
    print("\n== 6. Staleness, open snapshot, crash atomically rolled back ==")
    db = os.path.join(tmp, "plans.db")
    for i, (rev, total) in enumerate(
            [(20, 200), (3, 30), (35, 350), (27, 270), (48, 480)], 1):
        load(db, f"b{i}", line(rev, total))
    retention.init_retention(db)
    plan = retention.rehearse(db, {"watermark": 4, "exceptions": []})
    # Another batch commits -> the plan is stale.
    load(db, "late", line(60, 600, ts=1001))
    try:
        retention.approve(db, plan)
        check("a plan overtaken by a new batch is refused", False)
    except HistowareError as exc:
        check("a plan overtaken by a new batch is refused",
              "stale" in str(exc))
    check("the new batch is not swallowed",
          60 in revisions(db)
          and retention.get_status(db)["policy_generation"] == 0)

    # A summary that already holds its snapshot finishes on the old view
    # even if a retirement commits mid-run.
    db2 = os.path.join(tmp, "snap.db")
    load(db2, "b1", line(0, 0, ts=1000) + line(0, 10, ts=2000))
    load(db2, "b2", line(1, 5, ts=1000) + line(1, 20, ts=2000))
    retention.init_retention(db2)
    plan2 = retention.rehearse(db2, {"watermark": 2, "exceptions": []})
    roster, request = roster_request(tmp)
    state = {"fired": False}
    original = summary_mod.EndpointIndex

    class Hooked(original):
        def lookup(self, *a, **kw):
            if not state["fired"]:
                state["fired"] = True
                retention.approve(db2, plan2)
            return super().lookup(*a, **kw)
    summary_mod.EndpointIndex = Hooked
    try:
        out = os.path.join(tmp, "snap-out.db")
        stats = summary_mod.run_summary(db2, 1, roster, request, out)
    finally:
        summary_mod.EndpointIndex = original
    check("open read snapshot completes across an approval",
          state["fired"] and stats["warehouse_version"] == 1)

    # Process killed inside the approval transaction: nothing partial.
    db3 = os.path.join(tmp, "crash.db")
    for i, (rev, total) in enumerate(
            [(20, 200), (3, 30), (35, 350), (27, 270), (48, 480)], 1):
        load(db3, f"b{i}", line(rev, total))
    retention.init_retention(db3)
    plan3 = retention.rehearse(db3, {"watermark": 4, "exceptions": []})
    code = (
        "import os, sys\n"
        "sys.path.insert(0, %r)\n"
        "from histoware import retention\n"
        "def hook(name, analysis):\n"
        "    if name == 'before_commit': os._exit(9)\n"
        "retention._TEST_HOOK = hook\n"
        "retention.approve(%r, %r)\n" % (ROOT, db3, plan3))
    proc = subprocess.run([sys.executable, "-c", code])
    status = retention.get_status(db3)
    check("killed before commit: policy and receipts unchanged",
          proc.returncode == 9 and status["watermark"] == 0
          and status["policy_generation"] == 0 and status["receipts"] == [])
    check("killed before commit: no payload was removed",
          revisions(db3) == [3, 20, 27, 35, 48])
    ok = retention.approve(db3, plan3)
    check("the same plan then applies cleanly",
          ok["receipt"]["evicted_rows"] == plan3["evicted_payload_rows"])


# ---------------------------------------------------------------------------

def main():
    tmp = tempfile.mkdtemp(prefix="histoware-drill-")
    print(f"scratch directory: {tmp}")
    try:
        drill_worked_example(tmp)
        drill_two_work_orders(tmp)
        drill_enumeration(tmp)
        drill_retained_summary(tmp)
        drill_reloading(tmp)
        drill_plans_and_crash(tmp)
    finally:
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)
    print(f"\n{CHECKS[0] - len(FAILURES)}/{CHECKS[0]} checks passed")
    if FAILURES:
        print("FAILURES:")
        for f in FAILURES:
            print("  -", f)
        return 1
    print("ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
