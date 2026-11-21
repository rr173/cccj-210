"""Scale test for mirror exports and receives.

All data is generated at runtime under pytest's tmp_path; no database
or package is committed.  Checks that delta-export work (rows examined,
endpoint probes) grows with the batches and changes involved — not with
the warehouse size — once the supporting indexes exist, and that
neither export nor receive holds the whole package or mirror in memory.
Base exports and one-time index builds are allowed to traverse.
"""
import json
import os
import resource
import sqlite3
import time

from util import cp, run_cli_json, write_jsonl

from histoware import mirror


def _batch(db, batch, records):
    path = write_jsonl(db + f".{batch}.jsonl", records)
    return run_cli_json("load", "--db", db, "--batch", batch,
                        "--file", str(path))


def _endpoint(e, rev, cuts=(10,), total=1):
    return cp("r", f"c{e:04d}", "g", 1000 + e, rev, list(cuts),
              [total] * len(cuts), total)


def _build_shared(db, n_endpoints, n_versions, changes_per_version):
    """v1: every endpoint at revision 0.  v2..: a rotating set of
    endpoints gets a new revision per version; all other endpoints
    never change."""
    _batch(db, "b1", [_endpoint(e, 0) for e in range(n_endpoints)])
    for v in range(2, n_versions + 1):
        recs = []
        for k in range(changes_per_version):
            e = (v * changes_per_version + k) % n_endpoints
            recs.append(_endpoint(e, v, total=v))
        _batch(db, f"b{v}", recs)


def _build_wide(db, n_endpoints, n_versions, changes_per_version,
                extra_endpoints, extra_versions):
    """The shared history plus, afterwards, many more endpoints and
    versions that the measured export range never touches."""
    _build_shared(db, n_endpoints, n_versions, changes_per_version)
    for v in range(n_versions + 1, n_versions + extra_versions + 1):
        recs = []
        for k in range(extra_endpoints // extra_versions):
            e = n_endpoints + (v - n_versions - 1) * (
                extra_endpoints // extra_versions) + k
            recs.append(_endpoint(e, 1))
        _batch(db, f"b{v}", recs)
    run_cli_json("retire", "init", "--db", db)


def _export_delta(db, base, end, out):
    return run_cli_json("mirror", "export-delta", "--db", db,
                        "--regions", "all", "--from", str(base),
                        "--to", str(end), "--out", str(out))


def test_delta_export_work_tracks_involved_batches_not_warehouse_size(
        tmp_path):
    small = str(tmp_path / "small.db")
    wide = str(tmp_path / "wide.db")
    _build_wide(small, n_endpoints=300, n_versions=20,
                changes_per_version=4, extra_endpoints=0,
                extra_versions=0)
    _build_wide(wide, n_endpoints=300, n_versions=20,
                changes_per_version=4, extra_endpoints=900,
                extra_versions=20)

    results = {}
    for label, db in (("small", small), ("wide", wide)):
        out = str(tmp_path / f"d-{label}.pkg")
        t0 = time.monotonic()
        res = _export_delta(db, 10, 15, out)
        results[label] = (time.monotonic() - t0, res, out)

    (t_small, res_small, out_small) = results["small"]
    (t_wide, res_wide, out_wide) = results["wide"]
    # The exported range has 5 versions x 4 changed endpoints; the wide
    # warehouse's extra 900 endpoints and 20 extra versions add no work.
    for res in (res_small, res_wide):
        assert res["stats"]["rows_examined"] == 5 * 4
        assert res["stats"]["endpoint_probes"] == 5 * 4
        assert res["stats"]["changes"] == 5 * 4
    # Untouched history is invisible to the package content: with the
    # (deliberately different) source warehouse ids and the digest that
    # covers them normalised away, the two packages are identical.
    def normalised(path):
        records = []
        with open(path, "rb") as fh:
            for raw in fh:
                obj = json.loads(raw)
                obj.pop("warehouse_id", None)
                obj.pop("sha256", None)
                records.append(obj)
        return records
    assert normalised(out_small) == normalised(out_wide)
    assert t_wide < t_small * 4 + 1.0

    # A longer interval scales with its own versions and changes only
    # (versions 3..20 = 18 commit units).
    res = _export_delta(wide, 2, 20, str(tmp_path / "long.pkg"))
    assert res["stats"]["rows_examined"] == 18 * 4
    assert res["stats"]["endpoint_probes"] == 18 * 4
    assert res["stats"]["changes"] == 18 * 4
    assert res["units"] == 18


def test_delta_export_uses_supporting_indexes(tmp_path):
    db = str(tmp_path / "wh.db")
    _build_shared(db, n_endpoints=50, n_versions=5, changes_per_version=2)
    run_cli_json("retire", "init", "--db", db)
    conn = sqlite3.connect(db)
    try:
        plan = conn.execute(
            "EXPLAIN QUERY PLAN " + mirror._VERSION_ROWS_SQL
            + " ORDER BY region, collector, generation, ts, revision",
            (2,)).fetchall()
        text = " ".join(row[-1] for row in plan).upper()
        assert "IX_CHECKPOINT_VERSION" in text
        assert "SCAN CHECKPOINT" not in text
        plan = conn.execute(
            "EXPLAIN QUERY PLAN SELECT version, batch_id FROM batch"
            " WHERE version > ? AND version <= ?", (1, 4)).fetchall()
        text = " ".join(row[-1] for row in plan).upper()
        assert "IX_BATCH_VERSION" in text
        plan = conn.execute(
            "EXPLAIN QUERY PLAN " + mirror._PREV_REVISION_SQL,
            ("r", "c0001", "g", 1001, 3)).fetchall()
        text = " ".join(row[-1] for row in plan).upper()
        assert "INDEX" in text and "SCAN CHECKPOINT" not in text
    finally:
        conn.close()


def test_export_and_receive_stream_big_packages(tmp_path):
    db = str(tmp_path / "wh.db")
    n_versions = 12
    cuts = list(range(10, 10 + 13 * 40000, 13))      # ~40k cuts per row
    counts = list(range(1, len(cuts) + 1))
    _batch(db, "b1", [_endpoint(e, 0) for e in range(50)])
    for v in range(2, n_versions + 1):
        recs = []
        for k in range(2):
            e = (v * 2 + k) % 50
            recs.append(cp("r", f"c{e:04d}", "g", 1000 + e, v, cuts,
                           counts, len(cuts) + 1))
        _batch(db, f"b{v}", recs)
    run_cli_json("retire", "init", "--db", db)

    out = str(tmp_path / "big.pkg")
    before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    res = _export_delta(db, 1, n_versions, out)
    after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    package_size = os.path.getsize(out)
    # The package carries many megabytes of payload; peak RSS growth
    # must stay far below it (streamed, never materialised).
    assert package_size > 5_000_000
    assert res["changes"] == (n_versions - 1) * 2
    assert (after - before) * 1024 * 2 < package_size

    mdb = str(tmp_path / "mirror.db")
    base_pkg = str(tmp_path / "base.pkg")
    run_cli_json("mirror", "export-base", "--db", db, "--regions", "all",
                 "--version", "1", "--out", base_pkg)
    run_cli_json("mirror", "receive", "--db", mdb, "--package", base_pkg)
    before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    rec = run_cli_json("mirror", "receive", "--db", mdb,
                       "--package", out)
    after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    assert rec["receipt"]["changes"] == (n_versions - 1) * 2
    assert (after - before) * 1024 * 2 < package_size
    status = run_cli_json("mirror", "status", "--db", mdb)
    assert status["cursor"] == n_versions
    assert status["endpoints"] == 50


def test_base_export_traverses_once_and_receive_installs(tmp_path):
    db = str(tmp_path / "wh.db")
    _build_shared(db, n_endpoints=2000, n_versions=10,
                  changes_per_version=3)
    run_cli_json("retire", "init", "--db", db)
    out = str(tmp_path / "base.pkg")
    res = run_cli_json("mirror", "export-base", "--db", db,
                       "--regions", "all", "--version", "10",
                       "--out", out)
    # One selected row per endpoint, however long the history.
    assert res["rows"] == 2000
    assert res["stats"]["rows_examined"] == 2000
    mdb = str(tmp_path / "mirror.db")
    rec = run_cli_json("mirror", "receive", "--db", mdb, "--package", out)
    assert rec["receipt"]["changes"] == 2000
    status = run_cli_json("mirror", "status", "--db", mdb)
    assert status["endpoints"] == 2000 and status["cursor"] == 10
