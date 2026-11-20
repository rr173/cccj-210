#!/bin/sh
# Runnable demo for shift regression attribution.
#
# Two independent pipelines (baseline week / candidate week) each go
# through `load` + `summary`; attribution then reads ONLY the two
# result databases plus the explicit pairing manifest.  It never opens
# a checkpoint warehouse and never recomputes a shift summary.
#
# Hand-computed expectations at the 10ms cut (over = total - below 10):
#   baseline: a over 6 (exact), b over [4,7], c over [2,4]
#             totals 25, over [12,17]
#   candidate:a over 9 (exact), b over [6,8], c over 2 (exact)
#             totals 30, over [17,19]
#   changes:  a +3 confirmed; b [-1,4] possible; c [-2,0] unproven
#             overall change [0,7], exact closure (unallocated 0)
# The second pair asks for a 50ms cut neither side carries, so it is
# recorded not_comparable with reason candidate/baseline_cut_missing
# while all source evidence stays traceable.
set -eu
cd "$(dirname "$0")/.."
rm -f examples/attr_*.db examples/attr_*.db-wal examples/attr_*.db-shm

# --- baseline week pipeline ---
python3 -m histoware load \
    --db examples/attr_base_wh.db --batch base-b1 \
    --file examples/attr_base_checkpoints.jsonl
python3 -m histoware summary \
    --db examples/attr_base_wh.db --version 1 \
    --roster examples/attr_base_roster.jsonl \
    --request examples/attr_base_shifts.jsonl \
    --out examples/attr_base_result.db

# --- candidate week pipeline ---
python3 -m histoware load \
    --db examples/attr_cand_wh.db --batch cand-b1 \
    --file examples/attr_cand_checkpoints.jsonl
python3 -m histoware summary \
    --db examples/attr_cand_wh.db --version 1 \
    --roster examples/attr_cand_roster.jsonl \
    --request examples/attr_cand_shifts.jsonl \
    --out examples/attr_cand_result.db

# --- attribution: result DBs + explicit pairs only ---
python3 -m histoware attribute \
    --baseline examples/attr_base_result.db \
    --candidate examples/attr_cand_result.db \
    --pairs examples/attr_pairs.jsonl \
    --out examples/attr_compare.db

echo "--- pair status ---"
python3 - <<'EOF'
import sqlite3
conn = sqlite3.connect("examples/attr_compare.db")
for row in conn.execute(
        "SELECT pair_id, status, baseline_total, candidate_total,"
        " change_lo, change_hi, unallocated_lo, unallocated_hi FROM pair"):
    print(row)
print("--- issues ---")
for row in conn.execute("SELECT pair_id, code FROM pair_issue ORDER BY 1,2"):
    print(row)
print("--- per-collector contributions ---")
for row in conn.execute(
        "SELECT pair_id, collector, eligible, change_lo, change_hi,"
        " contribution FROM pair_collector ORDER BY 1,2"):
    print(row)
print("--- traceable source intersections (count per pair/side) ---")
for row in conn.execute(
        "SELECT pair_id, side, COUNT(*) FROM pair_intersection"
        " GROUP BY 1,2 ORDER BY 1,2"):
    print(row)
conn.close()
EOF
