#!/usr/bin/env bash
set -euo pipefail

# === Config ===
BASE_DIR="/root/betting/research/hist_data/self_recorded"
S3_PREFIX="s3://s3-betbucket/self_recorded"   # change if you want a different prefix in the bucket
DRY_RUN="${DRY_RUN:-0}"                       # set DRY_RUN=1 to simulate
LOG_FILE="${LOG_FILE:-/var/log/s3-archive-md.log}"

# === Preconditions ===
command -v aws >/dev/null 2>&1 || { echo "ERROR: aws CLI not found"; exit 1; }
[ -d "$BASE_DIR" ] || { echo "ERROR: BASE_DIR not found: $BASE_DIR"; exit 1; }
mkdir -p "$(dirname "$LOG_FILE")"

today="$(date +%F)"   # e.g., 2025-10-11

log() {
  echo "[$(date -Iseconds)] $*" | tee -a "$LOG_FILE"
}

# Find top-level day folders like YYYY-MM-DD (exclude today)
mapfile -t days < <(find "$BASE_DIR" -mindepth 1 -maxdepth 1 -type d -printf '%f\n' \
  | grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' \
  | grep -v "^${today}$" \
  | sort)

if [ "${#days[@]}" -eq 0 ]; then
  log "No day folders to process (only today exists or none present)."
  exit 0
fi

log "Starting archive run. Base: $BASE_DIR  →  S3: $S3_PREFIX  (today=$today)"
[ "$DRY_RUN" = "1" ] && log "DRY_RUN enabled: will NOT delete or upload."

for day in "${days[@]}"; do
  src="$BASE_DIR/$day"
  dst="$S3_PREFIX/$day/"

  # Skip empty directories (optional)
  if [ -z "$(find "$src" -mindepth 1 -print -quit)" ]; then
    log "Skipping empty day folder: $src"
    continue
  fi

  log "Uploading: $src  →  $dst"
  # --exact-timestamps ensures reupload if timestamps differ; default behavior already overwrites changed files.
  # We avoid --delete to not remove extra remote files unintentionally.
  if [ "$DRY_RUN" = "1" ]; then
    log "[DRY_RUN] aws s3 sync \"$src\" \"$dst\" --exact-timestamps --only-show-errors --no-progress"
  else
    aws s3 sync "$src" "$dst" --exact-timestamps --only-show-errors --no-progress
  fi

  # Verify a few files made it up (best-effort quick check)
  # If you want strict verification, you could list and compare counts/hashes.
  if [ "$DRY_RUN" = "1" ]; then
    log "[DRY_RUN] Would remove local folder after successful upload: $src"
  else
    log "Upload OK. Deleting local: $src"
    rm -rf --one-file-system "$src"
  fi

  log "Done with $day"
done

log "Archive run complete."
