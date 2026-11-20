"""Batch loading: validation, idempotency, conflicts, concurrency, bounds."""
import json
import sqlite3
import threading

import pytest

from util import (cp, current_version, load_batch, run_cli, run_cli_json,
                  table_rows, write_jsonl)

BIG = 2 ** 64 - 1


def test_basic_load_and_versions(tmp_path):
    wh = str(tmp_path / "wh.db")
    f1 = write_jsonl(tmp_path / "b1.jsonl",
                     [cp("r", "c", "g", 100, 0, [10], [1], 2)])
    res = load_batch(wh, "b1", f1)
    assert res["version"] == 1 and res["new_checkpoints"] == 1
    assert res["reused"] is False
    f2 = write_jsonl(tmp_path / "b2.jsonl",
                     [cp("r", "c", "g", 200, 0, [10], [1], 2),
                      cp("r", "c", "g", 300, 0, [10], [2], 3)])
    res = load_batch(wh, "b2", f2)
    assert res["version"] == 2 and res["new_checkpoints"] == 2
    listing = run_cli_json("batches", "--db", wh)
    assert listing["current_version"] == 2
    assert [b["batch_id"] for b in listing["batches"]] == ["b1", "b2"]
    assert [b["version"] for b in listing["batches"]] == [1, 2]


def test_idempotent_resend_same_file_returns_original_version(tmp_path):
    wh = str(tmp_path / "wh.db")
    f1 = write_jsonl(tmp_path / "b1.jsonl",
                     [cp("r", "c", "g", 100, 0, [10], [1], 2)])
    first = load_batch(wh, "b1", f1)
    again = load_batch(wh, "b1", f1)
    assert again["reused"] is True
    assert again["version"] == first["version"] == 1
    assert current_version(wh) == 1
    assert table_rows(wh, "SELECT COUNT(*) FROM checkpoint")[0][0] == 1


def test_same_batch_id_different_file_rejected(tmp_path):
    wh = str(tmp_path / "wh.db")
    f1 = write_jsonl(tmp_path / "b1.jsonl",
                     [cp("r", "c", "g", 100, 0, [10], [1], 2)])
    load_batch(wh, "b1", f1)
    f2 = write_jsonl(tmp_path / "b2.jsonl",
                     [cp("r", "c", "g", 200, 0, [10], [1], 2)])
    rc, _, err = run_cli("load", "--db", wh, "--batch", "b1",
                         "--file", str(f2))
    assert rc == 1
    assert "different file" in err
    assert current_version(wh) == 1


def test_intra_batch_identical_duplicates_deduped(tmp_path):
    wh = str(tmp_path / "wh.db")
    line = cp("r", "c", "g", 100, 0, [10], [1], 2)
    f = write_jsonl(tmp_path / "b.jsonl", [line, line, line])
    res = load_batch(wh, "b", f)
    assert res["new_checkpoints"] == 1
    assert res["intra_batch_duplicates"] == 2
    rows = table_rows(wh, "SELECT line_no FROM checkpoint")
    assert rows == [(1,)]   # first position kept


def test_intra_batch_conflict_rejected_then_fixed_resend_ok(tmp_path):
    wh = str(tmp_path / "wh.db")
    a = cp("r", "c", "g", 100, 0, [10], [1], 2)
    b = cp("r", "c", "g", 100, 0, [10], [5], 9)   # same key, other content
    f = write_jsonl(tmp_path / "bad.jsonl", [a, b])
    rc, _, err = run_cli("load", "--db", wh, "--batch", "b1",
                         "--file", str(f))
    assert rc == 1
    assert "conflicting contents within batch" in err
    assert current_version(wh) == 0
    assert table_rows(wh, "SELECT COUNT(*) FROM checkpoint")[0][0] == 0
    # The failed batch left no record: the same batch id may be reused
    # with a corrected file.
    fixed = write_jsonl(tmp_path / "fixed.jsonl", [a, b | {"ts": 200}])
    res = load_batch(wh, "b1", fixed)
    assert res["version"] == 1 and res["new_checkpoints"] == 2


def test_identical_retransmission_keeps_original_position(tmp_path):
    wh = str(tmp_path / "wh.db")
    line = cp("r", "c", "g", 100, 0, [10], [1], 2)
    f1 = write_jsonl(tmp_path / "b1.jsonl", [line])
    load_batch(wh, "b1", f1)
    # Same checkpoint+revision, same content, in a later batch: no
    # second revision, original batch/line preserved.
    f2 = write_jsonl(tmp_path / "b2.jsonl",
                     [line, cp("r", "c", "g", 200, 0, [10], [2], 4)])
    res = load_batch(wh, "b2", f2)
    assert res["new_checkpoints"] == 1
    assert res["duplicate_checkpoints"] == 1
    rows = table_rows(
        wh, "SELECT batch_id, line_no, version FROM checkpoint"
            " WHERE ts = 100")
    assert rows == [("b1", 1, 1)]


def test_conflicting_existing_revision_rejected(tmp_path):
    wh = str(tmp_path / "wh.db")
    f1 = write_jsonl(tmp_path / "b1.jsonl",
                     [cp("r", "c", "g", 100, 0, [10], [1], 2)])
    load_batch(wh, "b1", f1)
    f2 = write_jsonl(tmp_path / "b2.jsonl",
                     [cp("r", "c", "g", 100, 0, [10], [9], 9)])
    rc, _, err = run_cli("load", "--db", wh, "--batch", "b2",
                         "--file", str(f2))
    assert rc == 1
    assert "conflicts with the revision already loaded" in err
    assert current_version(wh) == 1


def test_higher_revision_layout_change_and_late_lower_revision(tmp_path):
    wh = str(tmp_path / "wh.db")
    f1 = write_jsonl(tmp_path / "b1.jsonl",
                     [cp("r", "c", "g", 100, 0, [10, 20], [1, 2], 3)])
    load_batch(wh, "b1", f1)
    # Higher revision may change the bucket layout.
    f2 = write_jsonl(tmp_path / "b2.jsonl",
                     [cp("r", "c", "g", 100, 2, [5, 50], [1, 2], 3)])
    load_batch(wh, "b2", f2)
    # A late, previously unseen lower revision is stored but never
    # replaces the higher one.
    f3 = write_jsonl(tmp_path / "b3.jsonl",
                     [cp("r", "c", "g", 100, 1, [7], [1], 3)])
    res = load_batch(wh, "b3", f3)
    assert res["new_checkpoints"] == 1
    rows = table_rows(
        wh, "SELECT revision FROM checkpoint WHERE ts = 100"
            " ORDER BY revision")
    assert [r[0] for r in rows] == [0, 1, 2]
    # Max-revision visibility at each version.
    conn = sqlite3.connect(wh)
    for version, expect_rev in ((1, 0), (2, 2), (3, 2)):
        row = conn.execute(
            "SELECT revision FROM checkpoint"
            " WHERE ts = 100 AND version <= ?"
            " ORDER BY revision DESC LIMIT 1", (version,)).fetchone()
        assert row[0] == expect_rev
    conn.close()


def test_uint64_boundary(tmp_path):
    wh = str(tmp_path / "wh.db")
    f = write_jsonl(tmp_path / "b.jsonl",
                    [cp("r", "c", "g", 100, 0, [10, 20], [BIG - 1, BIG], BIG)])
    res = load_batch(wh, "b", f)
    assert res["new_checkpoints"] == 1
    rows = table_rows(wh, "SELECT counts, total FROM checkpoint")
    assert rows == [(f"[{BIG - 1},{BIG}]", str(BIG))]


@pytest.mark.parametrize("field,value", [("counts", [2 ** 64]),
                                         ("total", 2 ** 64)])
def test_above_uint64_rejected(tmp_path, field, value):
    wh = str(tmp_path / "wh.db")
    rec = cp("r", "c", "g", 100, 0, [10], [1], 2)
    if field == "counts":
        rec["counts"] = value
        rec["total"] = 2 ** 64
    else:
        rec["total"] = value
    f = write_jsonl(tmp_path / "b.jsonl", [rec])
    rc, _, err = run_cli("load", "--db", wh, "--batch", "b", "--file", str(f))
    assert rc == 1
    assert "2^64-1" in err or str(BIG) in err


BAD_LINES = [
    "not json",
    "[1, 2]",
    json.dumps({"region": "r", "collector": "c", "generation": "g",
                "ts": 1, "revision": 0, "cuts_ms": [10],
                "counts": [1]}),                                  # missing total
    json.dumps({"region": "r", "collector": "c", "generation": "g",
                "ts": 1, "revision": 0, "cuts_ms": [10], "counts": [1],
                "total": 1, "extra": 1}),                         # unknown field
    '{"region":"a","region":"b","collector":"c","generation":"g","ts":1,'
    '"revision":0,"cuts_ms":[10],"counts":[1],"total":1}',        # duplicate key
    json.dumps(cp("r", "c", "g", 1.5, 0, [10], [1], 1)),          # ts float
    json.dumps(cp("r", "c", "g", True, 0, [10], [1], 1)),         # ts bool
    json.dumps(cp("r", "c", "g", 1, -1, [10], [1], 1)),           # negative revision
    json.dumps(cp("r", "c", "g", 1, 0, [0], [1], 1)),             # zero cut
    json.dumps(cp("r", "c", "g", 1, 0, [10, 10], [1, 1], 1)),     # cut not increasing
    json.dumps(cp("r", "c", "g", 1, 0, [20, 10], [1, 1], 1)),     # cuts decreasing
    json.dumps(cp("r", "c", "g", 1, 0, [10, 20], [1], 1)),        # length mismatch
    json.dumps(cp("r", "c", "g", 1, 0, [10], [-1], 1)),           # negative count
    json.dumps(cp("r", "c", "g", 1, 0, [10, 20], [5, 3], 9)),     # counts not monotonic
    json.dumps(cp("r", "c", "g", 1, 0, [10], [1.5], 2)),          # float count
    json.dumps(cp("r", "c", "g", 1, 0, [10], ["1"], 1)),          # string count
    json.dumps(cp("r", "c", "g", 1, 0, [10], [1], -1)),           # negative total
    json.dumps(cp("r", "c", "g", 1, 0, [10], [5], 4)),            # total < max count
    "",                                                           # blank line
]


@pytest.mark.parametrize("bad", BAD_LINES)
def test_bad_line_rejected_and_version_unchanged(tmp_path, bad):
    wh = str(tmp_path / "wh.db")
    good = cp("r", "c", "g", 100, 0, [10], [1], 2)
    f = write_jsonl(tmp_path / "b.jsonl", [good, bad])
    rc, _, err = run_cli("load", "--db", wh, "--batch", "b1",
                         "--file", str(f))
    assert rc == 1
    assert "line 2" in err
    assert current_version(wh) == 0
    assert table_rows(wh, "SELECT COUNT(*) FROM checkpoint")[0][0] == 0


def test_unreadable_file_leaves_version_unchanged(tmp_path):
    wh = str(tmp_path / "wh.db")
    rc, _, err = run_cli("load", "--db", wh, "--batch", "b1",
                         "--file", str(tmp_path / "does-not-exist.jsonl"))
    assert rc == 1
    assert current_version(wh) == 0


def test_interrupted_read_leaves_version_unchanged(tmp_path):
    from histoware.warehouse import load_batch as load_batch_fn

    wh = str(tmp_path / "wh.db")
    f1 = write_jsonl(tmp_path / "b1.jsonl",
                     [cp("r", "c", "g", 100, 0, [10], [1], 2)])
    load_batch(wh, "b1", f1)

    class BrokenStream:
        def __init__(self):
            self.n = 0

        def __iter__(self):
            return self

        def __next__(self):
            self.n += 1
            if self.n == 1:
                return (json.dumps(cp("r", "c", "g", 200, 0, [10], [1], 2))
                        .encode() + b"\n")
            raise OSError("simulated read failure")

    with pytest.raises(OSError):
        load_batch_fn(wh, "b2", BrokenStream())
    assert current_version(wh) == 1
    assert table_rows(wh, "SELECT COUNT(*) FROM checkpoint")[0][0] == 1


def test_empty_batch_file_allocates_version(tmp_path):
    wh = str(tmp_path / "wh.db")
    f = write_jsonl(tmp_path / "empty.jsonl", [])
    res = load_batch(wh, "b1", f)
    assert res["version"] == 1 and res["new_checkpoints"] == 0
    assert current_version(wh) == 1


def test_concurrent_distinct_batches(tmp_path):
    from histoware.warehouse import load_batch as load_batch_fn

    wh = str(tmp_path / "wh.db")
    files = [write_jsonl(tmp_path / f"b{i}.jsonl",
                         [cp("r", "c", "g", 1000 + i, 0, [10], [1], 1)])
             for i in range(8)]
    results, errors = {}, []
    barrier = threading.Barrier(8)

    def worker(i):
        try:
            barrier.wait()
            with open(files[i], "rb") as fh:
                results[i] = load_batch_fn(wh, f"b{i}", fh)
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    versions = sorted(results[i]["version"] for i in range(8))
    assert versions == list(range(1, 9))
    assert current_version(wh) == 8


def test_concurrent_same_batch_same_file(tmp_path):
    from histoware.warehouse import load_batch as load_batch_fn

    wh = str(tmp_path / "wh.db")
    f = write_jsonl(tmp_path / "b.jsonl",
                    [cp("r", "c", "g", 100, 0, [10], [1], 1)])
    results, errors = [], []
    barrier = threading.Barrier(4)

    def worker():
        try:
            barrier.wait()
            with open(f, "rb") as fh:
                results.append(load_batch_fn(wh, "b", fh))
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert {res["version"] for res in results} == {1}
    assert current_version(wh) == 1
    assert table_rows(wh, "SELECT COUNT(*) FROM checkpoint")[0][0] == 1
