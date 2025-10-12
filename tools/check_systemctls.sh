#!/usr/bin/env bash
# check_unit_logs.sh — show last 15 journal lines for each .service defined in /root/betting/systemd_files
# Usage: ./check_unit_logs.sh

set -euo pipefail

DIR="/root/betting/systemd_files"
LINES=15

if [[ ! -d "$DIR" ]]; then
  echo "❌ Directory not found: $DIR" >&2
  exit 1
fi

mapfile -t SERVICE_FILES < <(find "$DIR" -maxdepth 1 -type f -name "*.service" | sort)

if [[ ${#SERVICE_FILES[@]} -eq 0 ]]; then
  echo "⚠️  No .service files found in $DIR" >&2
  exit 0
fi

for file in "${SERVICE_FILES[@]}"; do
  unit="$(basename "$file")"
  echo "══════════════════════════════════════════════════════════════"
  echo "🟩 $unit — Last $LINES log lines"
  echo "══════════════════════════════════════════════════════════════"
  journalctl -u "$unit" -n $LINES -o short-iso --no-pager || echo "⚠️ No logs found for $unit"
  echo
done
