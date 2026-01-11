#!/bin/bash
# decompress_mine.sh — macOS-compatible (Bash 3.2), no 'mapfile', no unbound arrays
# Decompress self_recorded data with optional day window and sport filtering.
#
# Usage:
#   ./tools_research/decompress_mine.sh [--nprev N] [--sport "4" | "1,4" | "1,2,4"]
#
# Examples:
#   ./tools_research/decompress_mine.sh
#   ./tools_research/decompress_mine.sh --nprev 2
#   ./tools_research/decompress_mine.sh --sport 4
#   ./tools_research/decompress_mine.sh --nprev 5 --sport 1,4

set -euo pipefail

BASE_DIR="/Users/hcoussens/git/s3_data/"
N_PREV=""
SPORT_FILTER=""

# -------- Parse arguments --------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --nprev)
      N_PREV="${2:-}"; shift 2;;
    --sport)
      SPORT_FILTER="${2:-}"; shift 2;;
    -h|--help)
      echo "Usage: $0 [--nprev N] [--sport \"4\"|\"1,4\"|\"1,2,4\"]"; exit 0;;
    *)
      echo "Unknown arg: $1" >&2; exit 1;;
  esac
done

if [[ ! -d "$BASE_DIR" ]]; then
  echo "❌ Base dir not found: $BASE_DIR" >&2
  exit 1
fi

# Split comma-separated list into array (always define it to avoid unbound)
IFS=',' read -r -a SPORT_IDS <<< "${SPORT_FILTER:-}"

# -------- Helper: check if date is within last N days --------
within_window() {
  local date_dir="$1"
  [[ -z "${N_PREV:-}" ]] && return 0  # if no limit, accept all
  local target_epoch today_epoch
  target_epoch=$(date -j -f "%Y-%m-%d" "$date_dir" "+%s" 2>/dev/null || echo "")
  [[ -z "$target_epoch" ]] && return 1
  today_epoch=$(date "+%s")
  local diff=$(( (today_epoch - target_epoch) / 86400 ))
  [[ $diff -ge 0 && $diff -le ${N_PREV} ]]
}

# -------- Main logic --------
count=0
echo "🔓 Decompressing under: $BASE_DIR"
[[ -n "${N_PREV:-}" ]] && echo "   Window: last $N_PREV day(s)"
[[ -n "${SPORT_FILTER:-}" ]] && echo "   Sports: $SPORT_FILTER (IDs)"
echo

for datedir in "$BASE_DIR"/*; do
  [[ -d "$datedir" ]] || continue
  datebase="$(basename "$datedir")"
  [[ "$datebase" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || continue
  within_window "$datebase" || continue

  # Determine which sport directories to process (always initialize array)
  SPORT_DIRS=()

  if [[ -z "${SPORT_FILTER:-}" ]]; then
    # collect all numeric subdirs (sport IDs)
    while IFS= read -r dir; do
      [[ -n "$dir" ]] && SPORT_DIRS+=( "$dir" )
    done < <(find "$datedir" -mindepth 1 -maxdepth 1 -type d -regex '.*/[0-9]+' | sort)
  else
    # specific sport ids
    for sid in "${SPORT_IDS[@]:-}"; do
      [[ -d "$datedir/$sid" ]] && SPORT_DIRS+=( "$datedir/$sid" )
    done
  fi

  # If none found, continue gracefully
  if [[ ${#SPORT_DIRS[@]:-0} -eq 0 ]]; then
    echo "ℹ️  No sport dirs for $datebase matching filter '${SPORT_FILTER:-ALL}'."
    continue
  fi

  # Decompress each sport directory
  for sdir in "${SPORT_DIRS[@]:-}"; do
    echo "➡️  Date $datebase  Sport $(basename "$sdir")"
    found_any=false
    # Iterate .gz files (if any)
    while IFS= read -r -d '' gz; do
      found_any=true
      gunzip -- "$gz"
      ((count++)) || true
      echo "   • decompressed: $gz"
    done < <(find "$sdir" -type f -name "*.gz" -print0)

    if [[ "$found_any" != true ]]; then
      echo "   (no .gz files found)"
    fi
  done
done

echo
echo "✅ Done. Decompressed $count files."
