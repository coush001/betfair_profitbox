#!/usr/bin/env bash
set -euo pipefail

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
  if [[ "$action" == "on" ]]; then
    echo
    echo ">>> Enabling and starting: $service"
    if [[ $(id -u) -ne 0 ]]; then
      sudo systemctl enable "$service" && sudo systemctl start "$service"
    else
      systemctl enable "$service" && systemctl start "$service"
    fi
  elif [[ "$action" == "off" ]]; then
    echo
    echo ">>> Disabling and stopping: $service"
    if [[ $(id -u) -ne 0 ]]; then
      sudo systemctl stop "$service" && sudo systemctl disable "$service"
    else
      systemctl stop "$service" && systemctl disable "$service"
    fi
  else
    echo "Invalid action: $action (use 'on' or 'off')"
    return 1
  fi
  echo
  print_service_info "$service"
}

while true; do
  echo
  echo "Available strategies: 2strat, 3strat, 4strat, arbstrat"
  strat=$(prompt_choice "Which strat? [default: $DEFAULT_PROMPT_STRAT] " "$DEFAULT_PROMPT_STRAT")
  service_name=$(normalize_service_name "$strat")

  action=""
  while [[ -z "$action" ]]; do
    action=$(prompt_choice "Turn [on], [off], or check [status]? (on/off/status) " "")
    case "$action" in
      on|off|status ) ;;
      * ) echo "Invalid action: $action"; action="";;
    esac
  done

  if [[ "$action" == "status" ]]; then
    print_service_info "$service_name"
  else
    run_action "$service_name" "$action"
  fi

  again=$(prompt_choice "Do another strat? (y/N) " "N")
  case "$again" in
    [Yy]* ) continue;;
    * ) echo "Exiting."; break;;
  esac

done
