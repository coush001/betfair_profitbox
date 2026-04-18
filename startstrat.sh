#!/usr/bin/env bash
set -euo pipefail

SERVICE_ACTIONS=(start stop restart status enable disable)
DEFAULT_PROMPT_STRAT="2strat"

function prompt_choice() {
  local prompt="$1"
  local default="$2"
  local result
  read -rp "$prompt" result
  if [[ -z "$result" && -n "$default" ]]; then
    result="$default"
  fi
  echo "$result"
}

function normalize_service_name() {
  local name="$1"
  if [[ "${name##*.}" != "service" ]]; then
    echo "${name}.service"
  else
    echo "$name"
  fi
}

function print_service_info() {
  local service="$1"
  echo
  echo "=== systemd unit for $service ==="
  if [[ $(id -u) -ne 0 ]]; then
    SYSTEMD_PAGER=cat sudo systemctl cat "$service" || true
  else
    SYSTEMD_PAGER=cat systemctl cat "$service" || true
  fi
  echo
  echo "=== current status for $service ==="
  if [[ $(id -u) -ne 0 ]]; then
    SYSTEMD_PAGER=cat sudo systemctl status "$service" --no-pager || true
  else
    SYSTEMD_PAGER=cat systemctl status "$service" --no-pager || true
  fi
  echo
}

function run_action() {
  local service="$1"
  local action="$2"
  local cmd
  if [[ $(id -u) -ne 0 ]]; then
    cmd=(sudo systemctl "$action" "$service")
  else
    cmd=(systemctl "$action" "$service")
  fi

  echo
  echo ">>> Running: ${cmd[*]}"
  if ! "${cmd[@]}"; then
    echo "Warning: command failed: ${cmd[*]}"
  fi
  echo
  print_service_info "$service"
}

function valid_action() {
  local action="$1"
  for a in "${SERVICE_ACTIONS[@]}"; do
    if [[ "$a" == "$action" ]]; then
      return 0
    fi
  done
  return 1
}

while true; do
  echo
  echo "Available strategies: 2strat, 3strat, 4strat, arbstrat"
  strat=$(prompt_choice "Which strat? [default: $DEFAULT_PROMPT_STRAT] " "$DEFAULT_PROMPT_STRAT")
  service_name=$(normalize_service_name "$strat")

  action=""
  while [[ -z "$action" ]]; do
    action=$(prompt_choice "Action (${SERVICE_ACTIONS[*]})? " "")
    if ! valid_action "$action"; then
      echo "Invalid action: $action"
      action=""
    fi
  done

  run_action "$service_name" "$action"

  again=$(prompt_choice "Do another action? (y/N) " "N")
  case "$again" in
    [Yy]* ) continue;;
    * ) echo "Exiting."; break;;
  esac

done
