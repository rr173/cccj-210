#!/usr/bin/env python3
"""Re-runnable drill for histoware checkpoint mirroring.

Everything is generated under a fresh temporary directory (no committed
databases or packages).  The script walks from central loading, through
export, to offline field receive — the centre is never contacted by the
field side — printing a PASS/FAIL line per check and exiting non-zero
on the first failure.

Run:  python3 -m examples.mirror_drill
  or: python3 examples/mirror_drill.py
"""
import json
import os
import random
import sqlite3
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from histoware import mirror                      # noqa: E402
from histoware import retention                   # noqa: E402
from histoware.common import HistowareError       # noqa: E402
from histoware.warehouse import load_batch        # noqa: E402

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


def worked_warehouse(tmp, name="wh.db"):
    """The specification's worked example: v1 rev 4; v2 revs 8+10 in one
    batch; v3 a late 9; v4 rev 11."""
    db = os.path.join(tmp, name)
    load(db, "b1", line(4, 7, cuts=[10, 20], counts=[2, 5]))
    load(db, "b2", line(8, 9, cuts=[10, 20], counts=[3, 6])
         + line(10, 11, cuts=[10, 20], counts=[4, 7]))
    load(db, "b3", line(9, 9, cuts=[10, 20], counts=[3, 6]))
    load(db, "b4", line(11, 13, cuts=[10, 20], counts=[5, 8]))
    retention.init_retention(db)
    return db


def mirror_rows(db):
    conn = sqlite3.connect(db)
    try:
        return {tuple(r[:4]): r[4:] for r in conn.execute(
            "SELECT region, collector, generation, ts, revision, cuts,"
            " counts, total, first_version, first_batch, first_line"
            " FROM mirror_row")}
    finally:
        conn.close()


def package_events(path):
    events = []
    with open(path, "rb") as fh:
        for raw in fh:
            obj = json.loads(raw.decode("utf-8"))
            events.append((obj["record"], obj))
    return events


# ---------------------------------------------------------------------------
# 1. Worked example: central load -> export -> offline receive
# ---------------------------------------------------------------------------

def drill_worked_example(tmp):
    print("\n== 1. Worked example: rev 4; 8+10 same batch; late 9; 11 ==")
    db = worked_warehouse(tmp)
    base_pkg = os.path.join(tmp, "base1.pkg")
    delta_pkg = os.path.join(tmp, "d14.pkg")
    res = mirror.export_base(db, None, 1, base_pkg)
    check("base export at version 1 carries the one endpoint",
          res["rows"] == 1 and res["version"] == 1)
    res = mirror.export_delta(db, None, 1, 4, delta_pkg)
    check("delta 1->4 has 3 commit units and 2 changes",
          res["units"] == 3 and res["changes"] == 2, str(res["stats"]))

    events = package_events(delta_pkg)
    units = [e[1] for e in events if e[0] == "unit"]
    changes = [e[1] for e in events if e[0] == "change"]
    check("units are versions 2,3,4 with batches b2,b3,b4",
          [(u["version"], u["batch_id"]) for u in units]
          == [(2, "b2"), (3, "b3"), (4, "b4")])
    check("batch 2 replaces with 10, batch 3 empty, batch 4 replaces"
          " with 11",
          [u["changes"] for u in units] == [1, 0, 1])
    check("same-batch 8 and 10 publish only the final 10",
          changes[0]["old_revision"] == 4 and changes[0]["revision"] == 10)
    check("revision 10 keeps its first provenance (v2, b2, line 2)",
          (changes[0]["first_version"], changes[0]["first_batch"],
           changes[0]["first_line"]) == (2, "b2", 2))
    check("revision 11 replaces 10 with provenance (v4, b4, line 1)",
          changes[1]["old_revision"] == 10
          and changes[1]["revision"] == 11
          and (changes[1]["first_version"], changes[1]["first_batch"],
               changes[1]["first_line"]) == (4, "b4", 1))

    # The field side only ever sees package files.
    field = os.path.join(tmp, "field")
    os.makedirs(field)
    mdb = os.path.join(field, "mirror.db")
    r1 = mirror.receive_package(mdb, base_pkg)
    check("first install takes the base package",
          r1["reused"] is False and r1["receipt"]["generation"] == 1)
    r2 = mirror.receive_package(mdb, delta_pkg)
    check("catch-up advances the cursor to 4",
          r2["receipt"]["end_version"] == 4)
    rows = mirror_rows(mdb)
    check("mirror holds revision 11 with its own provenance",
          rows[("r", "c", "g", 1000)][0] == 11
          and rows[("r", "c", "g", 1000)][4:] == (4, "b4", 1),
          str(rows))
    status = mirror.mirror_status(mdb)
    check("status is offline: cursor 4, generation 1, two receipts",
          status["cursor"] == 4 and status["generation"] == 1
          and len(status["receipts"]) == 2)

    again = mirror.receive_package(mdb, delta_pkg)
    check("re-sending the same package returns the existing receipt",
          again["reused"] is True
          and again["receipt"]["receipt_id"] == r2["receipt"]["receipt_id"])
    check("re-send did not move the cursor",
          mirror.mirror_status(mdb)["cursor"] == 4)

    # Determinism: the same range re-exports the same logical package.
    redo = os.path.join(tmp, "d14-again.pkg")
    mirror.export_delta(db, None, 1, 4, redo)
    with open(delta_pkg, "rb") as fh:
        original = fh.read()
    with open(redo, "rb") as fh:
        check("re-export is byte-identical (no paths or timestamps)",
              fh.read() == original)
    return db


# ---------------------------------------------------------------------------
# 2. Subscriptions and new generations
# ---------------------------------------------------------------------------

def drill_subscriptions(tmp):
    print("\n== 2. Region filter, late regions, new process generation ==")
    db = os.path.join(tmp, "regions.db")
    load(db, "b1", line(1, 10, region="r1") + line(1, 20, region="r2"))
    load(db, "b2", line(2, 11, region="r1") + line(2, 21, region="r2"))
    load(db, "b3", line(1, 30, region="r3")
         + line(1, 40, region="r1", generation="g2", ts=1001))
    retention.init_retention(db)

    pkg = os.path.join(tmp, "r1.pkg")
    res = mirror.export_delta(db, ["r1"], 1, 3, pkg)
    events = package_events(pkg)
    changes = [e[1] for e in events if e[0] == "change"]
    check("explicit subscription exports only its regions",
          res["changes"] == 2
          and {c["region"] for c in changes} == {"r1"})
    check("a new process generation appears as a first appearance",
          any(c["generation"] == "g2" and c["old_revision"] is None
              for c in changes))
    check("every commit version gets its unit, in order",
          [e[1]["version"] for e in events if e[0] == "unit"] == [2, 3])

    all_pkg = os.path.join(tmp, "all.pkg")
    mirror.export_base(db, None, 3, all_pkg)
    rows = [e[1] for e in package_events(all_pkg) if e[0] == "row"]
    check("full subscription includes the late region r3",
          {r["region"] for r in rows} == {"r1", "r2", "r3"})

    # A mirror subscribed to r1 rejects a full-subscription package.
    mdb = os.path.join(tmp, "m-r1.db")
    r1_base = os.path.join(tmp, "r1-base.pkg")
    mirror.export_base(db, ["r1"], 1, r1_base)
    mirror.receive_package(mdb, r1_base)
    try:
        mirror.receive_package(mdb, all_pkg, rebuild=True)
        check("mismatched subscription is rejected", False)
    except HistowareError as exc:
        check("mismatched subscription is rejected",
              "subscription" in str(exc))
    # Region lists compare as sets.
    same = os.path.join(tmp, "r1-base2.pkg")
    mirror.export_base(db, ["r1"], 1, same)
    with open(r1_base, "rb") as fh:
        a = fh.read()
    with open(same, "rb") as fh:
        check("same set re-exports the identical package", fh.read() == a)


# ---------------------------------------------------------------------------
# 3. Reader robustness: truncation, corruption, ordering
# ---------------------------------------------------------------------------

def drill_reader_robustness(tmp):
    print("\n== 3. Truncated, corrupt and out-of-order packages ==")
    db = worked_warehouse(tmp)
    base_pkg = os.path.join(tmp, "b1.pkg")
    delta_pkg = os.path.join(tmp, "d.pkg")
    mirror.export_base(db, None, 1, base_pkg)
    mirror.export_delta(db, None, 1, 4, delta_pkg)
    mdb = os.path.join(tmp, "m.db")
    mirror.receive_package(mdb, base_pkg)

    data = open(delta_pkg, "rb").read()
    lines = data.splitlines(keepends=True)
    bad_cases = {
        "tail-truncated": data[:len(data) - 10],
        "no-trailer": b"".join(lines[:-1]),
        "corrupt-byte": data[:120] + b"X" + data[121:],
        "trailing-garbage": data + b"{}\n",
    }
    for name, blob in bad_cases.items():
        bad = os.path.join(tmp, f"bad-{name}.pkg")
        with open(bad, "wb") as fh:
            fh.write(blob)
        try:
            mirror.receive_package(mdb, bad)
            check(f"{name} is rejected", False)
        except HistowareError:
            check(f"{name} is rejected", True)
    check("rejections left the cursor untouched",
          mirror.mirror_status(mdb)["cursor"] == 1)

    # Out-of-order and overlapping packages.
    d24 = os.path.join(tmp, "d24.pkg")
    d34 = os.path.join(tmp, "d34.pkg")
    mirror.export_delta(db, None, 2, 4, d24)
    mirror.export_delta(db, None, 3, 4, d34)
    try:
        mirror.receive_package(mdb, d24)      # gap: cursor is 1
        check("gapped range is rejected", False)
    except HistowareError as exc:
        check("gapped range is rejected", "cursor" in str(exc))
    d13 = os.path.join(tmp, "d13.pkg")
    mirror.export_delta(db, None, 1, 3, d13)
    mirror.receive_package(mdb, d13)
    try:
        mirror.receive_package(mdb, d24)      # overlap: cursor is 3
        check("overlapping range is rejected (no splicing)", False)
    except HistowareError as exc:
        check("overlapping range is rejected (no splicing)",
              "cursor" in str(exc))
    mirror.receive_package(mdb, d34)
    check("the exact continuation still attaches",
          mirror.mirror_status(mdb)["cursor"] == 4)


# ---------------------------------------------------------------------------
# 4. Competing receivers and a killed receiver
# ---------------------------------------------------------------------------

def drill_concurrency(tmp):
    print("\n== 4. Two receivers, and a receiver killed mid-way ==")
    import threading
    db = worked_warehouse(tmp)
    base_pkg = os.path.join(tmp, "b1.pkg")
    delta_pkg = os.path.join(tmp, "d.pkg")
    mirror.export_base(db, None, 1, base_pkg)
    mirror.export_delta(db, None, 1, 4, delta_pkg)
    mdb = os.path.join(tmp, "m.db")
    mirror.receive_package(mdb, base_pkg)

    results = []

    def receive():
        try:
            results.append(("ok", mirror.receive_package(mdb, delta_pkg)))
        except HistowareError as exc:
            results.append(("err", exc))

    threads = [threading.Thread(target=receive) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    oks = [r for kind, r in results if kind == "ok"]
    check("two receivers of one package: both finish, one reuses",
          len(oks) == 2 and sorted(r["reused"] for r in oks)
          == [False, True])
    check("exactly one receipt for the package",
          len(mirror.mirror_status(mdb)["receipts"]) == 2)

    # A receiver killed before commit leaves no partial state.
    mdb2 = os.path.join(tmp, "m2.db")
    mirror.receive_package(mdb2, base_pkg)
    before = mirror_rows(mdb2)
    code = (
        "import os, sys\n"
        "sys.path.insert(0, %r)\n"
        "from histoware import mirror\n"
        "mirror._TEST_HOOK = lambda p: os._exit(9) \\\n"
        "    if p == 'before_commit' else None\n"
        "mirror.receive_package(%r, %r)\n" % (ROOT, mdb2, delta_pkg))
    proc = subprocess.run([sys.executable, "-c", code])
    check("killed receiver: exit code propagated", proc.returncode == 9)
    check("killed receiver: no partial rows, cursor or receipt",
          mirror_rows(mdb2) == before
          and mirror.mirror_status(mdb2)["cursor"] == 1
          and len(mirror.mirror_status(mdb2)["receipts"]) == 1)
    mirror.receive_package(mdb2, delta_pkg)
    check("restarted receive completes",
          mirror.mirror_status(mdb2)["cursor"] == 4)


# ---------------------------------------------------------------------------
# 5. Retention gap: protected start, retired middle, rebuild, catch up
# ---------------------------------------------------------------------------

def drill_retention_gap(tmp):
    print("\n== 5. Protected start, retired middle: rebuild then catch up ==")
    db = worked_warehouse(tmp)
    load(db, "b5", line(12, 15, cuts=[10, 20], counts=[6, 9]))
    base1 = os.path.join(tmp, "b1.pkg")
    mirror.export_base(db, None, 1, base1)
    mdb = os.path.join(tmp, "m.db")
    mirror.receive_package(mdb, base1)

    plan = retention.rehearse(db, {"watermark": 4, "exceptions": [
        {"work_order": "wo-1", "version": 1}]})
    retention.approve(db, plan)
    try:
        mirror.export_delta(db, None, 1, 5, os.path.join(tmp, "d15.pkg"))
        check("delta across a retired middle is refused", False)
    except HistowareError as exc:
        check("delta across a retired middle reports the first gap",
              "version 2" in str(exc) and "first" in str(exc), str(exc))
    check("the field mirror still serves its original cursor",
          mirror.mirror_status(mdb)["cursor"] == 1
          and mirror_rows(mdb)[("r", "c", "g", 1000)][0] == 4)

    base4 = os.path.join(tmp, "b4.pkg")
    d45 = os.path.join(tmp, "d45.pkg")
    mirror.export_base(db, None, 4, base4)
    mirror.export_delta(db, None, 4, 5, d45)
    try:
        mirror.receive_package(mdb, base4)     # no --rebuild
        check("rebuild needs the explicit flag", False)
    except HistowareError as exc:
        check("rebuild needs the explicit flag", "rebuild" in str(exc))
    rec = mirror.receive_package(mdb, base4, rebuild=True)
    check("rebuild starts generation 2 at version 4",
          rec["receipt"]["generation"] == 2
          and rec["receipt"]["end_version"] == 4)
    mirror.receive_package(mdb, d45)
    status = mirror.mirror_status(mdb)
    check("caught up to 5 in generation 2",
          status["cursor"] == 5 and status["generation"] == 2)
    check("old-generation receipts remain for audit",
          [r["generation"] for r in status["receipts"]] == [1, 2, 2])
    try:
        mirror.receive_package(mdb, base1)     # old generation's package
        check("old-generation package is not re-accepted", False)
    except HistowareError:
        check("old-generation package is not re-accepted", True)


# ---------------------------------------------------------------------------
# 6. Independent per-version enumeration
# ---------------------------------------------------------------------------

def ref_selected(batches, version, regions=None):
    best = {}
    for v, batch_id, recs in batches:
        if v > version:
            break
        for i, rec in enumerate(recs):
            if regions is not None and rec["region"] not in regions:
                continue
            key = (rec["region"], rec["collector"], rec["generation"],
                   rec["ts"])
            if key not in best or rec["revision"] > best[key][0]["revision"]:
                best[key] = (rec, v, batch_id, i + 1)
    return best


def drill_enumeration(tmp):
    print("\n== 6. Independent per-version enumeration cross-check ==")
    rng = random.Random(20261121)
    regions = ["r1", "r2"]
    db = os.path.join(tmp, "enum.db")
    batches = []
    used = {}
    for v in range(1, 7):
        recs = []
        for _ in range(rng.randint(0, 4)):
            key = (rng.choice(regions), f"c{rng.randrange(3)}",
                   f"g{rng.randrange(2)}", rng.randrange(1000, 1005))
            rev = rng.randrange(0, 20)
            if rev in used.setdefault(key, set()):
                continue
            used[key].add(rev)
            recs.append({"region": key[0], "collector": key[1],
                         "generation": key[2], "ts": key[3],
                         "revision": rev, "cuts_ms": [10, 50],
                         "counts": [rev, rev + 1], "total": rev + 2})
        batches.append((v, f"b{v}", recs))
        load(db, f"b{v}", "".join(json.dumps(r) + "\n" for r in recs))
    retention.init_retention(db)

    mismatches = 0
    for subscription in (None, {"r1"}):
        for version in range(0, 7):
            pkg = os.path.join(tmp, f"e-{subscription}-{version}.pkg")
            mirror.export_base(db, subscription, version, pkg)
            rows = [e[1] for e in package_events(pkg) if e[0] == "row"]
            expected = ref_selected(batches, version, subscription)
            got = {(r["region"], r["collector"], r["generation"], r["ts"]):
                   (r["revision"], r["first_version"], r["first_batch"],
                    r["first_line"]) for r in rows}
            want = {k: (rec["revision"], v, batch_id, line_no)
                    for k, (rec, v, batch_id, line_no) in expected.items()}
            if got != want:
                mismatches += 1
        # Receive a base + the full chain of one-version deltas.
        mdb = os.path.join(tmp, f"em-{subscription}.db")
        base_pkg = os.path.join(tmp, f"eb-{subscription}.pkg")
        mirror.export_base(db, subscription, 0, base_pkg)
        mirror.receive_package(mdb, base_pkg)
        for v in range(1, 7):
            pkg = os.path.join(tmp, f"ed-{subscription}-{v}.pkg")
            mirror.export_delta(db, subscription, v - 1, v, pkg)
            mirror.receive_package(mdb, pkg)
        rows = mirror_rows(mdb)
        expected = ref_selected(batches, 6, subscription)
        if len(rows) != len(expected):
            mismatches += 1
        for key, (rec, v, batch_id, line_no) in expected.items():
            row = rows.get(key)
            if row is None or row[0] != rec["revision"] \
                    or row[4:] != (v, batch_id, line_no):
                mismatches += 1
    check("base packages and chained one-version deltas match the"
          " independent enumeration", mismatches == 0,
          f"{mismatches} mismatches")


def main():
    tmp = tempfile.mkdtemp(prefix="histoware-mirror-drill-")
    print(f"scratch directory: {tmp}")
    try:
        for i, drill in enumerate((drill_worked_example,
                                   drill_subscriptions,
                                   drill_reader_robustness,
                                   drill_concurrency,
                                   drill_retention_gap,
                                   drill_enumeration), 1):
            section = os.path.join(tmp, f"s{i}")
            os.makedirs(section)
            drill(section)
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
