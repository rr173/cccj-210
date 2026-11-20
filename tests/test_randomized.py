"""Differential test: randomized scenarios (shuffled record order and
JSON key order) compared against the independent reference."""
import json
import random

import pytest

from reference import compare_results, reference_result
from util import fetch_result, run_cli, run_cli_json, write_jsonl

FINE = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000]


def _dumps_shuffled(rng, obj):
    items = list(obj.items())
    rng.shuffle(items)
    return "{" + ",".join(
        json.dumps(k) + ":" + json.dumps(v) for k, v in items) + "}"


def generate(seed, tmp_path):
    rng = random.Random(seed)
    base = 1_700_000_000
    span = 43200
    regions = {"r1": ["c1", "c2", "c3"], "r2": ["c4", "c5"]}
    roster = []
    lines = []
    for region, collectors in regions.items():
        for col in collectors:
            t = base
            gi = 0
            while t < base + span:
                dur = rng.choice([1500, 1800, 2700, 3600])
                de = min(t + dur, base + span)
                gen = f"g{gi}"
                roster.append({"region": region, "collector": col,
                               "generation": gen, "start": t, "end": de})
                cum = [0] * len(FINE)
                total = 0
                layout = sorted(rng.sample(FINE, rng.randint(1, 6)))
                rev = 0
                report_times = []
                ts = t
                while True:
                    report_times.append(ts)
                    if ts >= de:
                        break
                    ts = min(ts + 300, de)
                gen_lines = []
                for i, rts in enumerate(report_times):
                    if i > 0:
                        for _ in range(rng.randint(0, 30)):
                            lat = rng.randint(1, 6000)
                            total += 1
                            for k, f in enumerate(FINE):
                                if lat < f:
                                    cum[k] += 1
                    interior = 0 < i < len(report_times) - 1
                    if interior and rng.random() < 0.08:
                        continue            # missing interior report
                    if rng.random() < 0.06:
                        rev += 1            # layout change, new revision
                        layout = sorted(rng.sample(FINE, rng.randint(1, 6)))
                    counts = [cum[FINE.index(c)] for c in layout]
                    gen_lines.append({
                        "region": region, "collector": col,
                        "generation": gen, "ts": rts, "revision": rev,
                        "cuts_ms": layout, "counts": counts,
                        "total": total})
                # occasionally the final report of a generation is lost
                if gen_lines and rng.random() < 0.15:
                    gen_lines.pop()
                lines.extend(gen_lines)
                t = de + rng.choice([0, 0, 0, 300])
                gi += 1
    # inject one total regression (process reset misreported mid-generation)
    candidates = [ln for ln in lines if ln["total"] >= 10]
    if candidates:
        victim = rng.choice(candidates)
        victim["total"] //= 2
        victim["counts"] = [c // 2 for c in victim["counts"]]
    # identical retransmissions (dedup must keep the first position)
    lines += [dict(ln) for ln in lines if rng.random() < 0.02]
    rng.shuffle(lines)
    third = max(1, len(lines) // 3)
    batch_lines = [lines[:third], lines[third:2 * third], lines[2 * third:]]
    batch_files = []
    for i, chunk in enumerate(batch_lines, 1):
        path = tmp_path / f"batch{i}.jsonl"
        with open(path, "w", encoding="utf-8") as fh:
            for obj in chunk:
                fh.write(_dumps_shuffled(rng, obj) + "\n")
        batch_files.append((f"batch-{i}", str(path)))
    shifts = []
    for i in range(24):
        s = base + rng.randint(0, span - 3600)
        e = s + rng.randint(600, 5400)
        region = rng.choice(list(regions) + ["r9"])
        shifts.append({"name": f"shift-{i:02d}", "region": region,
                       "start": s, "end": e,
                       "cuts_ms": sorted(rng.sample(FINE, rng.randint(1, 5)))})
    roster_path = write_jsonl(tmp_path / "roster.jsonl", roster)
    request_path = write_jsonl(tmp_path / "request.jsonl", shifts)
    return batch_files, str(roster_path), str(request_path)


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_randomized_against_reference(seed, tmp_path):
    batch_files, roster_path, request_path = generate(seed, tmp_path)
    wh = str(tmp_path / "wh.db")
    for batch_id, path in batch_files:
        res = run_cli_json("load", "--db", wh, "--batch", batch_id,
                           "--file", path)
        assert res["version"] >= 1
    for version in (1, 2, 3):
        out = tmp_path / f"result-v{version}.db"
        rc, _, err = run_cli(
            "summary", "--db", wh, "--version", str(version),
            "--roster", roster_path, "--request", request_path,
            "--out", str(out))
        assert rc == 0, err
        ref = reference_result(batch_files, roster_path, request_path,
                               version)
        got = fetch_result(out)
        compare_results(ref, got)
