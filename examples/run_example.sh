#!/bin/sh
# Runnable demo: load one batch, then summarise one shift.
# Expected: shift "morning" complete, total 5, below-10ms in [1, 3].
set -eu
cd "$(dirname "$0")/.."
rm -f examples/wh.db examples/wh.db-wal examples/wh.db-shm examples/result.db

python3 -m histoware load \
    --db examples/wh.db --batch 2026-11-20T00 \
    --file examples/checkpoints.jsonl

python3 -m histoware batches --db examples/wh.db

python3 -m histoware summary \
    --db examples/wh.db --version 1 \
    --roster examples/roster.jsonl \
    --request examples/shifts.jsonl \
    --out examples/result.db

echo "--- shift totals ---"
python3 - <<'EOF'
import sqlite3
conn = sqlite3.connect("examples/result.db")
for row in conn.execute("SELECT name, status, total, covered_total FROM shift"):
    print(row)
for row in conn.execute("SELECT shift, cut_ms, lo, hi, exact FROM shift_cut"):
    print(row)
EOF
