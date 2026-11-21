"""Checkpoint mirroring: export eligibility, package format, offline
receive, idempotency, rebuilds, retention gaps and crash safety.

Every scenario builds its own warehouse, packages and mirror databases
under pytest's tmp_path; nothing is committed.
"""
import json
import os
import subprocess
import sys
import threading

import pytest

from util import (cp, load_batch, run_cli, run_cli_json, table_rows,
                  write_jsonl)

from histoware import mirror
from histoware.common import HistowareError

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _line(rev, total, ts=1000, region="r", collector="c", generation="g",
          cuts=None, counts=None):
    return cp(region, collector, generation, ts, rev,
              cuts if cuts is not None else [10],
              counts if counts is not None else [total], total)


def _load(db, batch, records):
    path = write_jsonl(db + f".{batch}.jsonl", records)
    return load_batch(db, batch, path)


def _worked_wh(tmp_path):
    """The specification's worked example: v1 rev 4; v2 revs 8+10 in one
    batch; v3 a late 9; v4 rev 11."""
    db = str(tmp_path / "wh.db")
    _load(db, "b1", [_line(4, 7, cuts=[10, 20], counts=[2, 5])])
    _load(db, "b2", [_line(8, 9, cuts=[10, 20], counts=[3, 6]),
                     _line(10, 11, cuts=[10, 20], counts=[4, 7])])
    _load(db, "b3", [_line(9, 9, cuts=[10, 20], counts=[3, 6])])
    _load(db, "b4", [_line(11, 13, cuts=[10, 20], counts=[5, 8])])
    run_cli_json("retire", "init", "--db", db)
    return db


def _export_base(db, version, out, regions="all"):
    return run_cli_json("mirror", "export-base", "--db", db,
                        "--regions", regions, "--version", str(version),
                        "--out", str(out))


def _export_delta(db, base, end, out, regions="all"):
    return run_cli_json("mirror", "export-delta", "--db", db,
                        "--regions", regions, "--from", str(base),
                        "--to", str(end), "--out", str(out))


def _receive(db, package, rebuild=False):
    argv = ["mirror", "receive", "--db", str(db), "--package", str(package)]
    if rebuild:
        argv.append("--rebuild")
    return run_cli_json(*argv)


def _receive_error(db, package, rebuild=False):
    argv = ["mirror", "receive", "--db", str(db), "--package", str(package)]
    if rebuild:
        argv.append("--rebuild")
    rc, out, err = run_cli(*argv)
    assert rc != 0, f"receive unexpectedly succeeded: {out}"
    return err


def _status(db):
    return run_cli_json("mirror", "status", "--db", str(db))


def _package_events(path):
    """Parse a package file into (header, [(kind, record), ...]) with plain
    JSON only — deliberately independent of histoware.mirror's reader."""
    header = None
    events = []
    with open(path, "rb") as fh:
        for raw in fh:
            obj = json.loads(raw.decode("utf-8"))
            if obj["record"] == "header":
                header = obj
            else:
                events.append((obj["record"], obj))
    return header, events


def _mirror_rows(db):
    return {tuple(r[:4]): r[4:] for r in table_rows(
        db, "SELECT region, collector, generation, ts, revision, cuts,"
            " counts, total, first_version, first_batch, first_line"
            " FROM mirror_row")}


def _make_package(path, records):
    """Hand-craft a syntactically valid package (correct trailer digest)
    from raw record dicts, using the exporter's own writer."""
    writer = mirror._PackageWriter(str(path))
    for rec in records:
        writer.write_record(rec)
    kind = records[0]["kind"]
    extra = {"rows": sum(1 for r in records if r["record"] == "row")} \
        if kind == "base" else {
            "units": sum(1 for r in records if r["record"] == "unit"),
            "changes": sum(1 for r in records if r["record"] == "change")}
    return writer.finish(extra)


# ---------------------------------------------------------------------------
# The worked example from the specification
# ---------------------------------------------------------------------------

def test_worked_example_end_to_end(tmp_path):
    db = _worked_wh(tmp_path)
    base_pkg = tmp_path / "base1.pkg"
    delta_pkg = tmp_path / "d14.pkg"
    res = _export_base(db, 1, base_pkg)
    assert res["rows"] == 1 and res["version"] == 1
    res = _export_delta(db, 1, 4, delta_pkg)
    assert res["units"] == 3 and res["changes"] == 2

    header, events = _package_events(delta_pkg)
    assert header["kind"] == "delta" and header["base_version"] == 1
    assert header["end_version"] == 4
    units = [e for e in events if e[0] == "unit"]
    changes = [e for e in events if e[0] == "change"]
    # Batch by batch: replace with 10, no change, replace with 11.
    assert [(u[1]["version"], u[1]["batch_id"], u[1]["changes"])
            for u in units] == [(2, "b2", 1), (3, "b3", 0), (4, "b4", 1)]
    c1, c2 = changes[0][1], changes[1][1]
    # Same-batch 8 and 10 publish only the final selected 10.
    assert c1["old_revision"] == 4 and c1["revision"] == 10
    # Revision 10 keeps its first provenance (version 2, batch b2, line 2).
    assert (c1["first_version"], c1["first_batch"], c1["first_line"]) \
        == (2, "b2", 2)
    assert c2["old_revision"] == 10 and c2["revision"] == 11
    assert (c2["first_version"], c2["first_batch"], c2["first_line"]) \
        == (4, "b4", 1)

    mdb = str(tmp_path / "mirror.db")
    r1 = _receive(mdb, base_pkg)
    assert r1["reused"] is False and r1["receipt"]["kind"] == "base"
    r2 = _receive(mdb, delta_pkg)
    assert r2["receipt"]["end_version"] == 4
    rows = _mirror_rows(mdb)
    key = ("r", "c", "g", 1000)
    assert list(rows) == [key]
    (revision, cuts, counts, total, fv, fb, fl) = rows[key]
    assert revision == 11 and cuts == "[10,20]" and counts == "[5,8]"
    assert total == "13" and (fv, fb, fl) == (4, "b4", 1)

    status = _status(mdb)
    assert status["cursor"] == 4 and status["generation"] == 1
    assert status["endpoints"] == 1
    assert [(r["kind"], r["base_version"], r["end_version"])
            for r in status["receipts"]] == [("base", 1, 1),
                                             ("delta", 1, 4)]

    # Re-sending the same package returns the existing receipt, even
    # after the cursor has advanced past it.
    again = _receive(mdb, base_pkg)
    assert again["reused"] is True
    assert again["receipt"]["receipt_id"] == r1["receipt"]["receipt_id"]
    again = _receive(mdb, delta_pkg)
    assert again["reused"] is True
    assert again["receipt"]["receipt_id"] == r2["receipt"]["receipt_id"]
    assert _status(mdb)["cursor"] == 4


def test_export_requires_managed_warehouse(tmp_path):
    db = str(tmp_path / "plain.db")
    _load(db, "b1", [_line(0, 5)])
    rc, out, err = run_cli("mirror", "export-base", "--db", db,
                           "--regions", "all", "--version", "1",
                           "--out", str(tmp_path / "p.pkg"))
    assert rc == 1
    assert "retire init" in err
    assert not (tmp_path / "p.pkg").exists()
    rc, out, err = run_cli("mirror", "export-delta", "--db", db,
                           "--regions", "all", "--from", "0", "--to", "1",
                           "--out", str(tmp_path / "p.pkg"))
    assert rc == 1 and "retire init" in err


def test_base_version_zero_empty_warehouse(tmp_path):
    db = str(tmp_path / "wh.db")
    _load(db, "b1", [_line(3, 30)])
    run_cli_json("retire", "init", "--db", db)
    pkg = tmp_path / "base0.pkg"
    res = _export_base(db, 0, pkg)
    assert res["rows"] == 0
    mdb = str(tmp_path / "m.db")
    rec = _receive(mdb, pkg)
    assert rec["receipt"]["end_version"] == 0
    assert _status(mdb)["cursor"] == 0
    # A delta from 0 attaches at cursor 0.
    d = tmp_path / "d01.pkg"
    _export_delta(db, 0, 1, d)
    _receive(mdb, d)
    assert _status(mdb)["cursor"] == 1
    assert _mirror_rows(mdb)[("r", "c", "g", 1000)][0] == 3


def test_export_does_not_touch_retention_policy(tmp_path):
    db = _worked_wh(tmp_path)
    before = run_cli_json("retire", "status", "--db", db)
    _export_base(db, 2, tmp_path / "b.pkg")
    _export_delta(db, 1, 4, tmp_path / "d.pkg")
    after = run_cli_json("retire", "status", "--db", db)
    assert before["watermark"] == after["watermark"]
    assert before["policy_generation"] == after["policy_generation"]
    assert before["current_version"] == after["current_version"]


def test_export_is_deterministic_across_reexports_and_new_batches(tmp_path):
    db = _worked_wh(tmp_path)
    p1 = tmp_path / "d1.pkg"
    p2 = tmp_path / "d2.pkg"
    r1 = _export_delta(db, 1, 4, p1)
    r2 = _export_delta(db, 1, 4, p2)
    assert r1["package_sha256"] == r2["package_sha256"]
    assert p1.read_bytes() == p2.read_bytes()
    # A later batch does not change an already exportable range.
    _load(db, "b5", [_line(12, 15)])
    p3 = tmp_path / "d3.pkg"
    r3 = _export_delta(db, 1, 4, p3)
    assert r3["package_sha256"] == r1["package_sha256"]
    assert p3.read_bytes() == p1.read_bytes()


def test_export_snapshot_isolated_from_concurrent_commit_and_eviction(
        tmp_path, monkeypatch):
    db = _worked_wh(tmp_path)
    ref = tmp_path / "ref.pkg"
    _export_delta(db, 1, 4, ref)
    # Prepare a retirement that would retire versions 2..3 once approved.
    target = write_jsonl(tmp_path / "t.json",
                         [{"watermark": 3,
                           "exceptions": [{"work_order": "w", "version": 1}]}])
    plan = run_cli_json("retire", "plan", "--db", db,
                        "--target", str(target),
                        "--out", str(tmp_path / "plan.json"))
    fired = []

    def hook(point):
        if point != "export_snapshot" or fired:
            return
        fired.append(point)
        # Approve the retirement, then commit a new batch: the export's
        # snapshot must still deliver the original view.
        run_cli_json("retire", "approve", "--db", db,
                     "--plan", str(tmp_path / "plan.json"))
        _load(db, "b5", [_line(12, 15)])

    monkeypatch.setattr(mirror, "_TEST_HOOK", hook)
    out = tmp_path / "out.pkg"
    _export_delta(db, 1, 4, out)
    monkeypatch.setattr(mirror, "_TEST_HOOK", None)
    assert fired == ["export_snapshot"]
    assert out.read_bytes() == ref.read_bytes()
    # The changes really happened outside the snapshot.
    status = run_cli_json("retire", "status", "--db", db)
    assert status["watermark"] == 3 and status["current_version"] == 5


# ---------------------------------------------------------------------------
# Subscriptions
# ---------------------------------------------------------------------------

def test_region_filter_and_full_subscription(tmp_path):
    db = str(tmp_path / "wh.db")
    _load(db, "b1", [_line(1, 10, region="r1"), _line(1, 20, region="r2")])
    _load(db, "b2", [_line(2, 11, region="r1"), _line(2, 21, region="r2")])
    run_cli_json("retire", "init", "--db", db)

    pkg = tmp_path / "r1.pkg"
    res = _export_delta(db, 1, 2, pkg, regions="r1")
    header, events = _package_events(pkg)
    assert header["subscription"] == {"regions": ["r1"]}
    assert res["changes"] == 1
    change = [e for e in events if e[0] == "change"][0][1]
    assert change["region"] == "r1" and change["revision"] == 2

    # Region lists compare as sets: "r2,r1" equals "r1,r2".
    pkg2 = tmp_path / "r12.pkg"
    _export_base(db, 1, pkg2, regions="r2,r1")
    header2, _ = _package_events(pkg2)
    assert header2["subscription"] == {"regions": ["r1", "r2"]}

    # A full subscription includes regions that appear later.
    _load(db, "b3", [_line(1, 30, region="r3")])
    all_pkg = tmp_path / "all.pkg"
    _export_base(db, 3, all_pkg, regions="all")
    _, events = _package_events(all_pkg)
    rows = [e[1] for e in events if e[0] == "row"]
    assert {r["region"] for r in rows} == {"r1", "r2", "r3"}
    r1_pkg = tmp_path / "only-r1.pkg"
    _export_base(db, 3, r1_pkg, regions="r1")
    _, events = _package_events(r1_pkg)
    assert {e[1]["region"] for e in events if e[0] == "row"} == {"r1"}


def test_subscription_mismatch_is_rejected(tmp_path):
    db = str(tmp_path / "wh.db")
    _load(db, "b1", [_line(1, 10, region="r1"), _line(1, 20, region="r2")])
    run_cli_json("retire", "init", "--db", db)
    pkg_r1 = tmp_path / "r1.pkg"
    pkg_all = tmp_path / "all.pkg"
    _export_base(db, 1, pkg_r1, regions="r1")
    _export_base(db, 1, pkg_all, regions="all")
    mdb = str(tmp_path / "m.db")
    _receive(mdb, pkg_r1)
    err = _receive_error(mdb, pkg_all, rebuild=True)
    assert "subscription" in err
    # The same subscription re-exported is the same logical package:
    # re-sending it returns the existing receipt.
    pkg_r1b = tmp_path / "r1b.pkg"
    _export_base(db, 1, pkg_r1b, regions="r1")
    ok = _receive(mdb, pkg_r1b)
    assert ok["reused"] is True


def test_cross_warehouse_package_is_rejected(tmp_path):
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    db1 = _worked_wh(tmp_path / "a")
    db2 = _worked_wh(tmp_path / "b")
    p1 = tmp_path / "p1.pkg"
    p2 = tmp_path / "p2.pkg"
    _export_base(db1, 1, p1)
    _export_base(db2, 1, p2)
    # Same business content, different warehouse identifiers.
    h1, _ = _package_events(p1)
    h2, _ = _package_events(p2)
    assert h1["warehouse_id"] != h2["warehouse_id"]
    mdb = str(tmp_path / "m.db")
    _receive(mdb, p1)
    err = _receive_error(mdb, p2, rebuild=True)
    assert "source warehouse" in err


# ---------------------------------------------------------------------------
# Receive ordering and overlap rules
# ---------------------------------------------------------------------------

def test_out_of_order_gap_and_overlap_are_rejected(tmp_path):
    db = _worked_wh(tmp_path)
    base1 = tmp_path / "base1.pkg"
    d12 = tmp_path / "d12.pkg"
    d13 = tmp_path / "d13.pkg"
    d24 = tmp_path / "d24.pkg"
    d34 = tmp_path / "d34.pkg"
    _export_base(db, 1, base1)
    _export_delta(db, 1, 2, d12)
    _export_delta(db, 1, 3, d13)
    _export_delta(db, 2, 4, d24)
    _export_delta(db, 3, 4, d34)

    mdb = str(tmp_path / "m.db")
    # Delta before any base.
    err = _receive_error(mdb, d12)
    assert "first package must be a base" in err
    _receive(mdb, base1)
    # Gap: cursor is 1, package starts at 2.
    err = _receive_error(mdb, d24)
    assert "cursor" in err
    _receive(mdb, d13)
    assert _status(mdb)["cursor"] == 3
    # Overlap: cursor is 3, package starts at 2 — no partial splicing.
    err = _receive_error(mdb, d24)
    assert "cursor" in err
    assert _status(mdb)["cursor"] == 3
    _receive(mdb, d34)
    assert _status(mdb)["cursor"] == 4


def test_base_package_on_established_mirror_needs_rebuild_flag(tmp_path):
    db = _worked_wh(tmp_path)
    base1 = tmp_path / "base1.pkg"
    base2 = tmp_path / "base2.pkg"
    base3 = tmp_path / "base3.pkg"
    _export_base(db, 1, base1)
    _export_base(db, 2, base2)
    _export_base(db, 3, base3)
    mdb = str(tmp_path / "m.db")
    _receive(mdb, base1)
    err = _receive_error(mdb, base3)
    assert "--rebuild" in err
    # Rebuild must move forward: version 2 is not newer than cursor 3.
    _receive(mdb, base3, rebuild=True)
    err = _receive_error(mdb, base2, rebuild=True)
    assert "not newer" in err
    assert _status(mdb)["cursor"] == 3
    assert _status(mdb)["generation"] == 2


def test_rebuild_replaces_rows_and_keeps_old_receipts(tmp_path):
    db = str(tmp_path / "wh.db")
    _load(db, "b1", [_line(1, 10, region="r1"), _line(1, 20, region="r2")])
    _load(db, "b2", [_line(2, 11, region="r1")])
    _load(db, "b3", [_line(3, 12, region="r1")])
    run_cli_json("retire", "init", "--db", db)
    base1 = tmp_path / "b1.pkg"
    base3 = tmp_path / "b3.pkg"
    _export_base(db, 1, base1)
    _export_base(db, 3, base3)
    mdb = str(tmp_path / "m.db")
    _receive(mdb, base1)
    assert len(_mirror_rows(mdb)) == 2
    _receive(mdb, base3, rebuild=True)
    rows = _mirror_rows(mdb)
    assert rows[("r1", "c", "g", 1000)][0] == 3
    assert rows[("r2", "c", "g", 1000)][0] == 1
    status = _status(mdb)
    assert status["generation"] == 2
    # Old receipts remain for audit.
    assert [r["generation"] for r in status["receipts"]] == [1, 2]
    # The rebuild replaces the endpoint set wholesale; it never merges:
    # a (hand-crafted, same-identity) newer base that no longer carries
    # an endpoint removes it from the mirror.
    header = {"record": "header", "format": mirror.PACKAGE_FORMAT,
              "format_version": 1, "kind": "base",
              "warehouse_id": status["warehouse_id"],
              "subscription": {"regions": "all"}, "version": 5}
    row = {"record": "row", "region": "r1", "collector": "c",
           "generation": "g", "ts": 1000, "revision": 5,
           "cuts_ms": [10], "counts": [12], "total": 12,
           "first_version": 5, "first_batch": "b5", "first_line": 1}
    crafted = tmp_path / "crafted-base5.pkg"
    _make_package(crafted, [header, row])
    _receive(mdb, crafted, rebuild=True)
    rows = _mirror_rows(mdb)
    assert list(rows) == [("r1", "c", "g", 1000)]
    assert rows[("r1", "c", "g", 1000)][0] == 5
    # Old-generation packages are not accepted under the new generation.
    err = _receive_error(mdb, base1)
    assert "not newer" in err or "--rebuild" in err


# ---------------------------------------------------------------------------
# Package reader robustness
# ---------------------------------------------------------------------------

def test_truncated_and_corrupt_packages_are_rejected(tmp_path):
    db = _worked_wh(tmp_path)
    pkg = tmp_path / "d.pkg"
    _export_delta(db, 1, 4, pkg)
    data = pkg.read_bytes()
    lines = data.splitlines(keepends=True)

    mdb = str(tmp_path / "m.db")
    base = tmp_path / "base.pkg"
    _export_base(db, 1, base)
    _receive(mdb, base)

    for name, blob in (
            ("cut-mid-line", data[:len(data) - 10]),
            ("no-trailer", b"".join(lines[:-1])),
            ("no-final-newline", data.rstrip(b"\n")),
            ("flipped-byte", data[:100] + b"X" + data[101:]),
            ("trailing-garbage", data + b"{}\n")):
        bad = tmp_path / f"bad-{name}.pkg"
        bad.write_bytes(blob)
        err = _receive_error(mdb, bad)
        assert err.strip(), name
    assert _status(mdb)["cursor"] == 1


def test_reader_rejects_structural_violations(tmp_path):
    db = _worked_wh(tmp_path)
    good = tmp_path / "d.pkg"
    _export_delta(db, 1, 4, good)
    header, events = _package_events(good)
    units = [e[1] for e in events if e[0] == "unit"]
    changes = [e[1] for e in events if e[0] == "change"]
    mdb = str(tmp_path / "m.db")
    base = tmp_path / "base.pkg"
    _export_base(db, 1, base)
    _receive(mdb, base)

    def reject(records, needle, name):
        pkg = tmp_path / f"bad-{name}.pkg"
        _make_package(pkg, records)
        err = _receive_error(mdb, pkg)
        assert needle in err, f"{name}: {err}"

    # Duplicate endpoint key within one commit unit.
    dup = [header, units[0], changes[0], changes[0], units[1], units[2],
           changes[1]]
    reject(dup, "duplicate", "dup-key")
    # A missing commit unit (version 3 dropped).
    missing = [header, units[0], changes[0], units[2], changes[1]]
    reject(missing, "missing or out of order", "missing-unit")
    # Units out of order.
    reordered = [header, units[1], units[0], changes[0], units[2],
                 changes[1]]
    reject(reordered, "missing or out of order", "reordered-units")
    # A change whose provenance does not match its unit.
    bad_prov = dict(changes[0])
    bad_prov["first_batch"] = "bX"
    reject([header, units[0], bad_prov, units[1], units[2], changes[1]],
           "provenance", "bad-provenance")
    # A change that is not a payload the warehouse would accept.
    bad_payload = dict(changes[0])
    bad_payload["counts"] = [9, 1]
    reject([header, units[0], bad_payload, units[1], units[2], changes[1]],
           "counts", "bad-payload")
    # Unknown format version.
    bad_header = dict(header)
    bad_header["format_version"] = 99
    reject([bad_header] + [e[1] for e in events], "format version",
           "bad-version")
    assert _status(mdb)["cursor"] == 1


def test_expected_old_revision_mismatch_is_rejected(tmp_path):
    db = _worked_wh(tmp_path)
    base = tmp_path / "base.pkg"
    _export_base(db, 1, base)
    mdb = str(tmp_path / "m.db")
    _receive(mdb, base)
    # Hand-craft a delta 1->2 that assumes the wrong replaced revision
    # (well-formed: the assumed revision is below the new one, it just
    # does not match what the mirror actually holds).
    header = {"record": "header", "format": mirror.PACKAGE_FORMAT,
              "format_version": 1, "kind": "delta",
              "warehouse_id": _status(mdb)["warehouse_id"],
              "subscription": {"regions": "all"},
              "base_version": 1, "end_version": 2}
    unit = {"record": "unit", "version": 2, "batch_id": "b2", "changes": 1}
    change = {"record": "change", "region": "r", "collector": "c",
              "generation": "g", "ts": 1000, "old_revision": 3,
              "revision": 10, "cuts_ms": [10, 20], "counts": [4, 7],
              "total": 11, "first_version": 2, "first_batch": "b2",
              "first_line": 2}
    pkg = tmp_path / "crafted.pkg"
    _make_package(pkg, [header, unit, change])
    err = _receive_error(mdb, pkg)
    assert "expects revision 3" in err and "holds revision 4" in err
    assert _status(mdb)["cursor"] == 1
    # An appear for an endpoint that already exists is also refused.
    appear = dict(change)
    appear["old_revision"] = None
    pkg2 = tmp_path / "crafted2.pkg"
    _make_package(pkg2, [header, unit, appear])
    err = _receive_error(mdb, pkg2)
    assert "expects it to be absent" in err
    assert _status(mdb)["cursor"] == 1


# ---------------------------------------------------------------------------
# Appear vs replace, late revisions, new generations
# ---------------------------------------------------------------------------

def test_appear_replace_and_new_generation(tmp_path):
    db = str(tmp_path / "wh.db")
    _load(db, "b1", [_line(1, 10, ts=1000)])
    _load(db, "b2", [_line(1, 20, ts=1001, generation="g2"),   # new endpoint
                     _line(2, 11, ts=1000)])                   # replace
    _load(db, "b3", [_line(0, 5, ts=1000)])                    # late low rev
    run_cli_json("retire", "init", "--db", db)
    base = tmp_path / "b.pkg"
    delta = tmp_path / "d.pkg"
    _export_base(db, 1, base)
    _export_delta(db, 1, 3, delta)
    _, events = _package_events(delta)
    by_version = {}
    current = None
    for kind, rec in events:
        if kind == "unit":
            current = rec["version"]
            by_version[current] = []
        elif kind == "change":
            by_version[current].append(rec)
    # v2: one appear (new generation g2) and one replace (1 -> 2).
    v2 = sorted(by_version[2], key=lambda r: r["ts"])
    assert v2[0]["old_revision"] == 1 and v2[0]["revision"] == 2
    assert v2[1]["old_revision"] is None and v2[1]["generation"] == "g2"
    # v3: the late revision 0 is not a change.
    assert by_version[3] == []

    mdb = str(tmp_path / "m.db")
    _receive(mdb, base)
    _receive(mdb, delta)
    rows = _mirror_rows(mdb)
    assert rows[("r", "c", "g", 1000)][0] == 2
    assert rows[("r", "c", "g2", 1001)][0] == 1


def test_big_counts_stay_exact(tmp_path):
    big = (1 << 64) - 1
    db = str(tmp_path / "wh.db")
    _load(db, "b1", [_line(1, big, cuts=[10, 20], counts=[big - 2, big - 1])])
    _load(db, "b2", [_line(2, big - 1, cuts=[10, 20],
                           counts=[big - 3, big - 2])])
    run_cli_json("retire", "init", "--db", db)
    base = tmp_path / "b.pkg"
    delta = tmp_path / "d.pkg"
    _export_base(db, 1, base)
    _export_delta(db, 1, 2, delta)
    mdb = str(tmp_path / "m.db")
    _receive(mdb, base)
    rows = _mirror_rows(mdb)
    assert rows[("r", "c", "g", 1000)][3] == str(big)
    assert rows[("r", "c", "g", 1000)][2] == f"[{big - 2},{big - 1}]"
    _receive(mdb, delta)
    rows = _mirror_rows(mdb)
    assert rows[("r", "c", "g", 1000)][3] == str(big - 1)
    assert rows[("r", "c", "g", 1000)][2] == f"[{big - 3},{big - 2}]"


# ---------------------------------------------------------------------------
# Retention gaps
# ---------------------------------------------------------------------------

def test_retention_gap_blocks_delta_but_protected_start_exports(tmp_path):
    db = _worked_wh(tmp_path)
    _load(db, "b5", [_line(12, 15)])
    target = write_jsonl(tmp_path / "t.json",
                         [{"watermark": 4,
                           "exceptions": [{"work_order": "wo", "version": 1}]}])
    run_cli_json("retire", "plan", "--db", db, "--target", str(target),
                 "--out", str(tmp_path / "plan.json"))
    run_cli_json("retire", "approve", "--db", db,
                 "--plan", str(tmp_path / "plan.json"))
    # Replayable now: {1, 4, 5}.  Delta 1->5 must report the first gap.
    rc, out, err = run_cli("mirror", "export-delta", "--db", db,
                           "--regions", "all", "--from", "1", "--to", "5",
                           "--out", str(tmp_path / "d15.pkg"))
    assert rc == 1
    assert "version 2" in err and "first" in err
    assert not (tmp_path / "d15.pkg").exists()
    # The protected orphan start and a fully replayable range still work.
    _export_base(db, 1, tmp_path / "b1.pkg")
    _export_base(db, 4, tmp_path / "b4.pkg")
    _export_delta(db, 4, 5, tmp_path / "d45.pkg")
    # A retired base version is refused distinctly (exit code 2).
    rc, out, err = run_cli("mirror", "export-base", "--db", db,
                           "--regions", "all", "--version", "2",
                           "--out", str(tmp_path / "b2.pkg"))
    assert rc == 2 and "retired" in err


def test_gap_recovery_rebuild_then_catch_up(tmp_path):
    db = _worked_wh(tmp_path)
    _load(db, "b5", [_line(12, 15)])
    base1 = tmp_path / "b1.pkg"
    _export_base(db, 1, base1)
    mdb = str(tmp_path / "m.db")
    _receive(mdb, base1)
    # The centre retires versions 2..3 (protecting 1); no delta can
    # advance the field mirror any more.
    target = write_jsonl(tmp_path / "t.json",
                         [{"watermark": 4,
                           "exceptions": [{"work_order": "wo", "version": 1}]}])
    run_cli_json("retire", "plan", "--db", db, "--target", str(target),
                 "--out", str(tmp_path / "plan.json"))
    run_cli_json("retire", "approve", "--db", db,
                 "--plan", str(tmp_path / "plan.json"))
    rc, out, err = run_cli("mirror", "export-delta", "--db", db,
                           "--regions", "all", "--from", "1", "--to", "5",
                           "--out", str(tmp_path / "d.pkg"))
    assert rc == 1 and "version 2" in err
    # The field database keeps serving its original cursor meanwhile.
    assert _status(mdb)["cursor"] == 1
    assert _mirror_rows(mdb)[("r", "c", "g", 1000)][0] == 4
    # Recovery: an explicit rebuild from a newer base of the same source
    # and subscription, then a normal catch-up.
    base4 = tmp_path / "b4.pkg"
    d45 = tmp_path / "d45.pkg"
    _export_base(db, 4, base4)
    _export_delta(db, 4, 5, d45)
    _receive(mdb, base4, rebuild=True)
    _receive(mdb, d45)
    status = _status(mdb)
    assert status["generation"] == 2 and status["cursor"] == 5
    assert _mirror_rows(mdb)[("r", "c", "g", 1000)][0] == 12
    # Receipts of generation 1 remain queryable but authorise nothing.
    assert [r["generation"] for r in status["receipts"]] == [1, 2, 2]


# ---------------------------------------------------------------------------
# Concurrency and crash safety
# ---------------------------------------------------------------------------

def test_two_receivers_same_package(tmp_path):
    db = _worked_wh(tmp_path)
    base = tmp_path / "b.pkg"
    delta = tmp_path / "d.pkg"
    _export_base(db, 1, base)
    _export_delta(db, 1, 4, delta)
    mdb = str(tmp_path / "m.db")
    _receive(mdb, base)

    results = []
    errors = []

    def receive():
        try:
            results.append(mirror.receive_package(mdb, str(delta)))
        except Exception as exc:                # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=receive) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert len(results) == 2
    assert sorted(r["reused"] for r in results) == [False, True]
    assert _status(mdb)["cursor"] == 4
    assert len(_status(mdb)["receipts"]) == 2


def test_two_receivers_competing_packages(tmp_path):
    db = _worked_wh(tmp_path)
    base = tmp_path / "b.pkg"
    d12 = tmp_path / "d12.pkg"
    d13 = tmp_path / "d13.pkg"
    _export_base(db, 1, base)
    _export_delta(db, 1, 2, d12)
    _export_delta(db, 1, 3, d13)
    mdb = str(tmp_path / "m.db")
    _receive(mdb, base)

    outcomes = []

    def receive(pkg):
        try:
            outcomes.append(("ok", mirror.receive_package(mdb, str(pkg))))
        except Exception as exc:                # noqa: BLE001
            outcomes.append(("err", exc))

    threads = [threading.Thread(target=receive, args=(d12,)),
               threading.Thread(target=receive, args=(d13,))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    oks = [o for o in outcomes if o[0] == "ok"]
    errs = [o for o in outcomes if o[0] == "err"]
    # Exactly one package wins; the loser is refused, never spliced.
    assert len(oks) == 1 and len(errs) == 1
    cursor = _status(mdb)["cursor"]
    assert cursor in (2, 3)
    assert len(_status(mdb)["receipts"]) == 2


def test_killed_receiver_leaves_no_partial_state(tmp_path):
    db = _worked_wh(tmp_path)
    base = tmp_path / "b.pkg"
    delta = tmp_path / "d.pkg"
    _export_base(db, 1, base)
    _export_delta(db, 1, 4, delta)
    mdb = str(tmp_path / "m.db")
    _receive(mdb, base)
    before_rows = _mirror_rows(mdb)

    code = (
        "import os, sys\n"
        "sys.path.insert(0, %r)\n"
        "from histoware import mirror\n"
        "mirror._TEST_HOOK = lambda point: os._exit(9) \\\n"
        "    if point == 'before_commit' else None\n"
        "mirror.receive_package(%r, %r)\n" % (ROOT, mdb, str(delta)))
    proc = subprocess.run([sys.executable, "-c", code])
    assert proc.returncode == 9
    # Nothing partial: rows, cursor and receipts are exactly as before.
    assert _mirror_rows(mdb) == before_rows
    status = _status(mdb)
    assert status["cursor"] == 1 and len(status["receipts"]) == 1
    # Reopening and receiving the same package works.
    res = mirror.receive_package(mdb, str(delta))
    assert res["reused"] is False
    assert _status(mdb)["cursor"] == 4


def test_killed_rebuild_leaves_old_generation_intact(tmp_path):
    db = _worked_wh(tmp_path)
    base1 = tmp_path / "b1.pkg"
    base4 = tmp_path / "b4.pkg"
    _export_base(db, 1, base1)
    _export_base(db, 4, base4)
    mdb = str(tmp_path / "m.db")
    _receive(mdb, base1)
    before_rows = _mirror_rows(mdb)

    code = (
        "import os, sys\n"
        "sys.path.insert(0, %r)\n"
        "from histoware import mirror\n"
        "mirror._TEST_HOOK = lambda point: os._exit(9) \\\n"
        "    if point == 'before_commit' else None\n"
        "mirror.receive_package(%r, %r, rebuild=True)\n"
        % (ROOT, mdb, str(base4)))
    proc = subprocess.run([sys.executable, "-c", code])
    assert proc.returncode == 9
    assert _mirror_rows(mdb) == before_rows
    status = _status(mdb)
    assert status["generation"] == 1 and status["cursor"] == 1
    res = mirror.receive_package(mdb, str(base4), rebuild=True)
    assert res["receipt"]["generation"] == 2
    assert _status(mdb)["cursor"] == 4


# ---------------------------------------------------------------------------
# Independent per-version enumeration
# ---------------------------------------------------------------------------

def _ref_selected(batches, version, regions=None):
    """Straightforward enumeration of the selected row per endpoint at a
    version, computed from the original load batches."""
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


def _ref_units(batches, base, end, regions=None):
    """Per commit unit in (base, end]: {key: (old_rev, rec, v, batch, line)}."""
    prev = _ref_selected(batches, base, regions)
    units = []
    for v in range(base + 1, end + 1):
        batch_id = batches[v - 1][1]
        cur = dict(prev)
        for i, rec in enumerate(batches[v - 1][2]):
            if regions is not None and rec["region"] not in regions:
                continue
            key = (rec["region"], rec["collector"], rec["generation"],
                   rec["ts"])
            if key not in cur or rec["revision"] > cur[key][0]["revision"]:
                cur[key] = (rec, v, batch_id, i + 1)
        changes = {}
        for key, (rec, fv, fb, fl) in cur.items():
            old = prev.get(key)
            if old is None or old[0]["revision"] != rec["revision"]:
                changes[key] = (None if old is None else old[0]["revision"],
                                rec, fv, fb, fl)
        units.append((v, batch_id, changes))
        prev = cur
    return units


def _check_base_package(pkg, batches, version, regions=None):
    header, events = _package_events(pkg)
    assert header["kind"] == "base" and header["version"] == version
    expected = _ref_selected(batches, version, regions)
    rows = [e[1] for e in events if e[0] == "row"]
    assert len(rows) == len(expected)
    for row in rows:
        key = (row["region"], row["collector"], row["generation"],
               row["ts"])
        rec, v, batch_id, line = expected.pop(key)
        assert row["revision"] == rec["revision"]
        assert row["cuts_ms"] == rec["cuts_ms"]
        assert row["counts"] == rec["counts"]
        assert row["total"] == rec["total"]
        assert (row["first_version"], row["first_batch"],
                row["first_line"]) == (v, batch_id, line)
    assert not expected


def _check_delta_package(pkg, batches, base, end, regions=None):
    header, events = _package_events(pkg)
    assert header["kind"] == "delta"
    assert header["base_version"] == base and header["end_version"] == end
    expected = _ref_units(batches, base, end, regions)
    seen_units = [e[1] for e in events if e[0] == "unit"]
    seen_changes = [e[1] for e in events if e[0] == "change"]
    assert [(u["version"], u["batch_id"]) for u in seen_units] == \
        [(v, batch_id) for v, batch_id, _ in expected]
    want_changes = []
    for v, batch_id, changes in expected:
        for key in sorted(changes):
            want_changes.append((v, key, changes[key]))
    assert len(seen_changes) == len(want_changes)
    for got, (v, key, (old_rev, rec, fv, fb, fl)) in \
            zip(seen_changes, want_changes):
        assert (got["region"], got["collector"], got["generation"],
                got["ts"]) == key
        assert got["old_revision"] == old_rev
        assert got["revision"] == rec["revision"]
        assert got["cuts_ms"] == rec["cuts_ms"]
        assert got["counts"] == rec["counts"]
        assert got["total"] == rec["total"]
        assert (got["first_version"], got["first_batch"],
                got["first_line"]) == (fv, fb, fl)


def _check_mirror(mdb, batches, version, regions=None):
    expected = _ref_selected(batches, version, regions)
    rows = _mirror_rows(mdb)
    assert len(rows) == len(expected)
    for key, (rec, v, batch_id, line) in expected.items():
        (revision, cuts, counts, total, fv, fb, fl) = rows[key]
        assert revision == rec["revision"]
        assert cuts == json.dumps(rec["cuts_ms"], separators=(",", ":"))
        assert counts == json.dumps(rec["counts"], separators=(",", ":"))
        assert total == str(rec["total"])
        assert (fv, fb, fl) == (v, batch_id, line)


@pytest.mark.parametrize("seed", [1, 2, 3, 4, 5])
def test_randomized_against_independent_enumeration(tmp_path, seed):
    import random
    rng = random.Random(seed)
    regions = ["r1", "r2", "r3"]
    db = str(tmp_path / "wh.db")
    batches = []
    used = {}
    n_versions = 8
    for v in range(1, n_versions + 1):
        recs = []
        for _ in range(rng.randint(0, 5)):
            key = (rng.choice(regions), f"c{rng.randrange(3)}",
                   f"g{rng.randrange(2)}", rng.randrange(1000, 1006))
            rev = rng.randrange(0, 24)
            if rev in used.setdefault(key, set()):
                continue
            used[key].add(rev)
            total = rng.randrange(0, 500)
            n_cuts = rng.randint(1, 3)
            cuts = sorted(rng.sample(range(1, 100), n_cuts))
            counts = sorted(rng.randrange(0, total + 1)
                            for _ in range(n_cuts))
            recs.append(cp(key[0], key[1], key[2], key[3], rev, cuts,
                           counts, total))
        batches.append((v, f"b{v}", recs))
        _load(db, f"b{v}", recs)
    run_cli_json("retire", "init", "--db", db)

    for subscription in (None, {"r1"}, {"r2", "r3"}):
        label = "all" if subscription is None else ",".join(
            sorted(subscription))
        # Base packages at several versions.
        for version in sorted(rng.sample(range(0, n_versions + 1), 3)):
            pkg = tmp_path / f"base-{label}-{version}.pkg"
            _export_base(db, version, pkg,
                         regions="all" if subscription is None else label)
            _check_base_package(pkg, batches, version, subscription)
        # Delta packages over several ranges, then a field receive.
        base_v = rng.randrange(0, n_versions - 1)
        end_v = rng.randrange(base_v + 1, n_versions + 1)
        base_pkg = tmp_path / f"recv-base-{label}.pkg"
        delta_pkg = tmp_path / f"recv-delta-{label}.pkg"
        _export_base(db, base_v, base_pkg,
                     regions="all" if subscription is None else label)
        _export_delta(db, base_v, end_v, delta_pkg,
                      regions="all" if subscription is None else label)
        _check_delta_package(delta_pkg, batches, base_v, end_v, subscription)
        mdb = str(tmp_path / f"mirror-{label}.db")
        _receive(mdb, base_pkg)
        _receive(mdb, delta_pkg)
        assert _status(mdb)["cursor"] == end_v
        _check_mirror(mdb, batches, end_v, subscription)


# ---------------------------------------------------------------------------
# Misc guards
# ---------------------------------------------------------------------------

def test_mirror_db_is_not_a_warehouse(tmp_path):
    db = _worked_wh(tmp_path)
    # A warehouse must not be accepted as a mirror database.
    rc, out, err = run_cli("mirror", "status", "--db", db)
    assert rc == 1 and "not a mirror" in err
    pkg = tmp_path / "b.pkg"
    _export_base(db, 1, pkg)
    rc, out, err = run_cli("mirror", "receive", "--db", db,
                           "--package", str(pkg))
    assert rc == 1 and "not a mirror" in err
    # Nor does the mirror answer warehouse commands.
    mdb = str(tmp_path / "m.db")
    _receive(mdb, pkg)
    assert table_rows(
        mdb, "SELECT value FROM meta WHERE key = 'current_version'") == []


def test_failed_export_never_replaces_target(tmp_path):
    db = _worked_wh(tmp_path)
    out = tmp_path / "pkg.out"
    out.write_bytes(b"previous content")
    # Version beyond current: fails; the old file must survive.
    rc, _, err = run_cli("mirror", "export-base", "--db", db,
                         "--regions", "all", "--version", "99",
                         "--out", str(out))
    assert rc == 1
    assert out.read_bytes() == b"previous content"
    # A gap failure likewise leaves the target alone.  With watermark 3
    # and no exceptions, versions 1..2 are retired and the first gap of
    # the closed interval [1, 4] is version 1.
    target = write_jsonl(tmp_path / "t.json",
                         [{"watermark": 3, "exceptions": []}])
    run_cli_json("retire", "plan", "--db", db, "--target", str(target),
                 "--out", str(tmp_path / "plan.json"))
    run_cli_json("retire", "approve", "--db", db,
                 "--plan", str(tmp_path / "plan.json"))
    rc, _, err = run_cli("mirror", "export-delta", "--db", db,
                         "--regions", "all", "--from", "1", "--to", "4",
                         "--out", str(out))
    assert rc == 1 and "version 1" in err
    assert out.read_bytes() == b"previous content"


def test_delta_requires_base_less_than_end(tmp_path):
    db = _worked_wh(tmp_path)
    for args in (("2", "2"), ("3", "1"), ("-1", "2")):
        rc, out, err = run_cli("mirror", "export-delta", "--db", db,
                               "--regions", "all", "--from", args[0],
                               "--to", args[1],
                               "--out", str(tmp_path / "x.pkg"))
        assert rc == 1
    rc, out, err = run_cli("mirror", "export-delta", "--db", db,
                           "--regions", "all", "--from", "1", "--to", "99",
                           "--out", str(tmp_path / "x.pkg"))
    assert rc == 1 and "beyond" in err
