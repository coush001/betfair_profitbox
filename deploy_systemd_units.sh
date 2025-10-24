#!/usr/bin/env bash
# deploy_systemd_units.sh
# Sync unit files from /root/betfair_profitbox/systemd_files/ to /etc/systemd/system/
# then daemon-reload and enable/start them. Finally, print next run for each timer.

set -euo pipefail

SRC_DIR="/root/betfair_profitbox/systemd_files"
DEST_DIR="/etc/systemd/system"

# Must be root
if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  echo "❌ Please run as root (sudo)."
  exit 1
fi

# Validate source
if [[ ! -d "$SRC_DIR" ]]; then
  echo "❌ Source directory not found: $SRC_DIR"
  exit 1
fi

# Find unit files (service/timer/path/socket/target)
mapfile -t UNITS < <(find "$SRC_DIR" -maxdepth 1 -type f \
  -regex '.*\.\(service\|timer\|path\|socket\|target\)$' \
  -printf '%f\n' | sort)

if [[ ${#UNITS[@]} -eq 0 ]]; then
  echo "ℹ️ No unit files (*.service|*.timer|*.path|*.socket|*.target) found in $SRC_DIR"
  exit 0
fi

echo "📦 Copying ${#UNITS[@]} unit file(s) to $DEST_DIR …"
for unit in "${UNITS[@]}"; do
  install -m 0644 "$SRC_DIR/$unit" "$DEST_DIR/$unit"
  echo "  → $unit"
done

echo "🔄 Reloading systemd daemon …"
systemctl daemon-reload

echo "▶️ Enabling and starting units …"
for unit in "${UNITS[@]}"; do
  # Try enable --now; if not installable, at least start/restart it.
  if systemctl enable --now "$unit" >/dev/null 2>&1; then
    echo "  ✔ enabled & started: $unit"
  elif systemctl restart "$unit" >/dev/null 2>&1 || systemctl start "$unit" >/dev/null 2>&1; then
    echo "  ✔ started (not enabled): $unit"
  else
    echo "  ❗ could not start: $unit (check: systemctl status $unit)"
  fi

done

# --- Helper: print next run for a timer ---
print_next_for_timer() {
  local t="$1"

  # Prefer precise realtime timestamp from systemd (microseconds since epoch)
  local raw
  raw="$(systemctl show "$t" -p NextElapseUSecRealtime --value 2>/dev/null || true)"

  if [[ -n "${raw:-}" && "$raw" != "0" && "$raw" =~ ^[0-9]+$ ]]; then
    # Convert µs → seconds, then format in UTC
    local sec=$(( raw / 1000000 ))
    local human
    human="$(date -u -d "@$sec" '+%a %Y-%m-%d %H:%M:%S UTC')"
    echo "  - $t → $human"
    return
  fi

  # Fallback: parse from list-timers line
  local line
  line="$(systemctl list-timers --all --no-legend 2>/dev/null | awk -v u="$t" '$(NF-1)==u {print; exit}')"
  if [[ -n "${line:-}" ]]; then
    # Print the NEXT column (everything up to the UNIT column is hard to parse generically),
    # so show the whole line for visibility.
    echo "  - $t → $line"
  else
    echo "  - $t → (no schedule found)"
  fi
}

# Collect timers we just deployed
TIMERS=()
for unit in "${UNITS[@]}"; do
  if [[ "$unit" == *.timer ]]; then
    TIMERS+=("$unit")
  fi
done

if (( ${#TIMERS[@]} > 0 )); then
  echo "⏰ Next scheduled runs for timers:"
  for t in "${TIMERS[@]}"; do
    print_next_for_timer "$t"
  done
else
  echo "⏰ No timer units in this deployment."
fi

echo "✅ Done."
