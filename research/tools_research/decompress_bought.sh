#!/usr/bin/env bash
# Decompress all *.gz files under ADVANCED/ (recursively).
# Default: restore originals and remove .gz files.
# Toggle KEEP_COMPRESSED=true to keep the .gz alongside the restored file.

set -euo pipefail

BASE_DIR="/Users/hugocoussens/git/betfair_profitbox/research/hist_data/ADVANCED"
KEEP_COMPRESSED=false  # set to true to keep the .gz after restoring

if [[ ! -d "$BASE_DIR" ]]; then
  echo "❌ Directory not found: $BASE_DIR" >&2
  exit 1
fi

echo "🔓 Decompressing .gz files under: $BASE_DIR"
echo "   Keep .gz after restore: $KEEP_COMPRESSED"
echo

count=0
while IFS= read -r -d '' gzfile; do
  if [[ "$KEEP_COMPRESSED" == true ]]; then
    # gunzip -k keeps the .gz and writes the original
    gunzip -k -- "$gzfile"
  else
    # default: remove .gz and restore original
    gunzip -- "$gzfile"
  fi
  ((count++)) || true
  echo "  • decompressed: $gzfile"
done < <(find "$BASE_DIR" -type f -name "*.gz" -print0)

echo
echo "✅ Done. Decompressed $count files."
