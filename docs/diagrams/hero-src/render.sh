#!/usr/bin/env bash
# Re-render the README hero (light and dark, animated and still) into docs/diagrams/.
set -euo pipefail
cd "$(dirname "$0")"
WORK="${WORK:-$(mktemp -d)}"
HF="npx --yes hyperframes@0.8.125"
$HF lint .
$HF render . --format png-sequence --fps 30 -o "$WORK/light"
$HF render . --format png-sequence --fps 30 --variables '{"theme":"dark"}' -o "$WORK/dark"
$HF render . --format png-sequence --fps 30 --variables '{"still":true}' -o "$WORK/light-still"
$HF render . --format png-sequence --fps 30 --variables '{"theme":"dark","still":true}' -o "$WORK/dark-still"
python3 encode.py "$WORK" ..
echo "PNG frames kept in $WORK"
