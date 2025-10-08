#!/usr/bin/env bash
# deploy_systemd_units.sh
# Sync unit files from /root/betting/systemd_files/ to /etc/systemd/system/,
# then daemon-reload and enable/start them.

set -euo pipefail

SRC_DIR="/root/betting/systemd_files"
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

echo "✅ Done."

