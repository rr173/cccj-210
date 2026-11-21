#!/bin/sh
# End-to-end checkpoint-mirror example: central load -> export -> offline
# field receive -> offline query.  Everything lands in a fresh temporary
# directory; nothing here is committed to the repository.
#
# The worked example: version 1 selects revision 4; version 2's batch
# carries revisions 8 and 10 (only the final 10 is published); version 3
# brings a late 9 (not a change); version 4 brings 11.
set -eu
cd "$(dirname "$0")/.."
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

cat > "$TMP/b1.jsonl" <<'EOF'
{"region":"cn-1","collector":"a","generation":"g1","ts":1700000000,"revision":4,"cuts_ms":[10,20],"counts":[2,5],"total":7}
EOF
cat > "$TMP/b2.jsonl" <<'EOF'
{"region":"cn-1","collector":"a","generation":"g1","ts":1700000000,"revision":8,"cuts_ms":[10,20],"counts":[3,6],"total":9}
{"region":"cn-1","collector":"a","generation":"g1","ts":1700000000,"revision":10,"cuts_ms":[10,20],"counts":[4,7],"total":11}
EOF
cat > "$TMP/b3.jsonl" <<'EOF'
{"region":"cn-1","collector":"a","generation":"g1","ts":1700000000,"revision":9,"cuts_ms":[10,20],"counts":[3,6],"total":9}
EOF
cat > "$TMP/b4.jsonl" <<'EOF'
{"region":"cn-1","collector":"a","generation":"g1","ts":1700000000,"revision":11,"cuts_ms":[10,20],"counts":[5,8],"total":13}
EOF

echo "== central: load four batches and bring the warehouse under management =="
for i in 1 2 3 4; do
    python3 -m histoware load --db "$TMP/wh.db" --batch "b$i" --file "$TMP/b$i.jsonl"
done
python3 -m histoware retire init --db "$TMP/wh.db"

echo "== central: export base@1 and catch-up 1->4 =="
python3 -m histoware mirror export-base --db "$TMP/wh.db" --regions all \
    --version 1 --out "$TMP/base1.pkg"
python3 -m histoware mirror export-delta --db "$TMP/wh.db" --regions all \
    --from 1 --to 4 --out "$TMP/d14.pkg"

echo "== the catch-up package (one unit per batch: replace with 10 / none / replace with 11) =="
cat "$TMP/d14.pkg"

echo "== field (offline): receive, advance, query =="
python3 -m histoware mirror receive --db "$TMP/mirror.db" --package "$TMP/base1.pkg"
python3 -m histoware mirror receive --db "$TMP/mirror.db" --package "$TMP/d14.pkg"
python3 -m histoware mirror status --db "$TMP/mirror.db"

echo "== field: stable SQLite query interface (current row + first provenance) =="
python3 - "$TMP/mirror.db" <<'PY'
import sqlite3, sys
conn = sqlite3.connect(sys.argv[1])
for row in conn.execute(
        "SELECT region, collector, generation, ts, revision, cuts, counts,"
        " total, first_version, first_batch, first_line FROM mirror_row"):
    print(row)
print("cursor:", conn.execute(
    "SELECT value FROM meta WHERE key = 'cursor'").fetchone()[0])
PY
