"""Independent reference implementation for differential testing.

Deliberately naive: everything in memory, brute-force scans, no SQLite.
Shares no code with the histoware package; the randomized test compares
its output against the tool's result database.
"""
import bisect
import json


def read_jsonl(path):
    out = []
    with open(path, "rb") as fh:
        for raw in fh:
            out.append(json.loads(raw))
    return out


def load_checkpoints(batch_files):
    """batch_files: [(batch_id, path)] in load order.

    Applies the warehouse rules: first identical retransmission wins
    (original batch/line kept); returns {key: record} where key is
    (region, collector, generation, ts, revision).
    """
    store = {}
    for version, (batch_id, path) in enumerate(batch_files, 1):
        seen = set()
        for line_no, obj in enumerate(read_jsonl(path), 1):
            key = (obj["region"], obj["collector"], obj["generation"],
                   obj["ts"], obj["revision"])
            if key in seen or key in store:
                continue
            seen.add(key)
            store[key] = dict(obj, batch_id=batch_id, line_no=line_no,
                              version=version)
    return store


def bracket(cuts, counts, total, q):
    i = bisect.bisect_left(cuts, q)
    if i < len(cuts) and cuts[i] == q:
        return (q, counts[i], q, counts[i])
    lo = (cuts[i - 1], counts[i - 1]) if i else (0, 0)
    hi = (cuts[i], counts[i]) if i < len(cuts) else (None, total)
    return (lo[0], lo[1], hi[0], hi[1])


def _intersection(s, e, qcuts):
    """Returns dict with status and, for ok, per-cut results."""
    td = e["total"] - s["total"]
    if td < 0:
        return {"status": "conflict", "cut": None, "td": td, "rows": {}}
    sm = dict(zip(s["cuts_ms"], s["counts"]))
    em = dict(zip(e["cuts_ms"], e["counts"]))
    prev = 0
    for c in sorted(set(sm) & set(em)):
        d = em[c] - sm[c]
        if d < prev:
            return {"status": "conflict", "cut": c, "td": td, "rows": {}}
        prev = d
    if td < prev:
        return {"status": "conflict", "cut": None, "td": td, "rows": {}}
    rows = {}
    for q in qcuts:
        sb = bracket(s["cuts_ms"], s["counts"], s["total"], q)
        eb = bracket(e["cuts_ms"], e["counts"], e["total"], q)
        lo = max(eb[1] - sb[3], 0)
        hi = min(eb[3] - sb[1], td)
        if lo > hi:
            return {"status": "conflict", "cut": q, "td": td,
                    "rows": {q: (None, None, sb, eb)}}
        rows[q] = (lo, hi, sb, eb)
    return {"status": "ok", "td": td, "rows": rows}


def _end_tuple(rec):
    if rec is None:
        return (None, None, None, None)
    return (rec["revision"], rec["batch_id"], rec["line_no"],
            str(rec["total"]))


def _cut_row(bounds):
    lo, hi, sb, eb = bounds
    return ((None if lo is None else str(lo)),
            (None if hi is None else str(hi)),
            sb[0], str(sb[1]), sb[2], str(sb[3]),
            eb[0], str(eb[1]), eb[2], str(eb[3]))


def reference_result(batch_files, roster_path, request_path, version):
    """Compute the expected result tables (minus meta) for `version`."""
    cps = load_checkpoints(batch_files)
    roster = read_jsonl(roster_path)
    shifts = read_jsonl(request_path)
    endpoints = {}
    for rec in cps.values():
        if rec["version"] > version:
            continue
        key = (rec["region"], rec["collector"], rec["generation"],
               rec["ts"])
        cur = endpoints.get(key)
        if cur is None or rec["revision"] > cur["revision"]:
            endpoints[key] = rec

    result = {"shift": {}, "shift_cut": {}, "intersection": {},
              "intersection_cut": {}}
    for sh in shifts:
        name = sh["name"]
        qcuts = sorted(set(sh["cuts_ms"]))
        due = ok = 0
        sum_total = 0
        sum_lo = {q: 0 for q in qcuts}
        sum_hi = {q: 0 for q in qcuts}
        for d in roster:
            if d["region"] != sh["region"]:
                continue
            a = max(d["start"], sh["start"])
            b = min(d["end"], sh["end"])
            if a >= b:
                continue
            due += 1
            ikey = (name, d["collector"], d["generation"], a, b)
            s = endpoints.get((sh["region"], d["collector"],
                               d["generation"], a))
            e = endpoints.get((sh["region"], d["collector"],
                               d["generation"], b))
            if s is None or e is None:
                status = ("missing_both" if s is None and e is None
                          else "missing_start" if s is None
                          else "missing_end")
                result["intersection"][ikey] = (
                    status, None) + _end_tuple(s) + _end_tuple(e) + (None,)
                continue
            r = _intersection(s, e, qcuts)
            if r["status"] == "conflict":
                result["intersection"][ikey] = (
                    "conflict", r["cut"]) + _end_tuple(s) + _end_tuple(e) + (
                    str(r["td"]),)
                for q, bounds in r["rows"].items():
                    result["intersection_cut"][(ikey, q)] = _cut_row(bounds)
                continue
            ok += 1
            sum_total += r["td"]
            result["intersection"][ikey] = (
                "ok", None) + _end_tuple(s) + _end_tuple(e) + (str(r["td"]),)
            for q, (lo, hi, sb, eb) in r["rows"].items():
                sum_lo[q] += lo
                sum_hi[q] += hi
                result["intersection_cut"][(ikey, q)] = _cut_row(
                    (lo, hi, sb, eb))
        status = ("no_duty" if due == 0
                  else "complete" if ok == due else "partial")
        result["shift"][name] = (
            sh["region"], sh["start"], sh["end"], status, due, ok,
            str(sum_total) if status == "complete" else None,
            str(sum_total))
        for q in qcuts:
            if status == "complete":
                lo, hi = sum_lo[q], sum_hi[q]
                result["shift_cut"][(name, q)] = (
                    str(lo), str(hi), 1 if lo == hi else 0,
                    str(lo), str(hi))
            else:
                result["shift_cut"][(name, q)] = (
                    None, None, None, str(sum_lo[q]), str(sum_hi[q]))
    return result


def compare_results(ref, got):
    for table in ("shift", "shift_cut", "intersection", "intersection_cut"):
        r, g = ref[table], got[table]
        assert set(r) == set(g), (
            f"{table}: key sets differ: only-ref={sorted(set(r) - set(g))[:3]}"
            f" only-got={sorted(set(g) - set(r))[:3]}")
        for key in r:
            assert r[key] == g[key], (
                f"{table}[{key}]:\n  ref={r[key]}\n  got={g[key]}")
