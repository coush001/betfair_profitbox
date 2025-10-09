#!/usr/bin/env bash
# Compress all files under ADVANCED/ (recursively) using gzip/pigz.
# Default: replace originals after compression (space savings).
# Toggle KEEP_ORIGINAL=true to keep originals alongside .gz files.

set -euo pipefail

BASE_DIR="/Users/hugocoussens/git/betfair_profitbox/research/hist_data/ADVANCED"
KEEP_ORIGINAL=false   # set to true to keep the original files too
LEVEL="-5"            # compression level (1 fastest, 9 smallest)

if [[ ! -d "$BASE_DIR" ]]; then
  echo "❌ Directory not found: $BASE_DIR" >&2
  exit 1
fi

if command -v pigz >/dev/null 2>&1; then
  COMPRESSOR=(pigz "$LEVEL")
else
  COMPRESSOR=(gzip "$LEVEL")
fi

echo "📦 Compressing files under: $BASE_DIR"
echo "   Using: ${COMPRESSOR[*]}"
echo "   Keep originals: $KEEP_ORIGINAL"
echo

count=0
# BSD/macOS find supports -print0; iterate safely for spaces/newlines
while IFS= read -r -d '' file; do
  # Skip already-compressed files
  [[ "$file" == *.gz ]] && continue

  if [[ "$KEEP_ORIGINAL" == true ]]; then
    # -k keeps original for gzip; pigz also supports -k
    "${COMPRESSOR[@]}" -k -- "$file"
  else
    # default: replace original (no -k)
    "${COMPRESSOR[@]}" -- "$file"
  fi
  ((count++)) || true
  echo "  • compressed: $file"
done < <(find "$BASE_DIR" -type f -print0)

echo
echo "✅ Done. Compressed $count files."
