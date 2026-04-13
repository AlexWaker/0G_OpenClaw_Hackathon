#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
state_dir="${OPENCLAW_STATE_DIR:-$HOME/.openclaw}"
network="${OPENCLAW_0G_NETWORK:-mainnet}"
runtime_dir="${ZEROG_KV_RUNTIME_DIR:-$state_dir/zerog-kv/$network}"
pid_file="$runtime_dir/zgs_kv.pid"
config_path="$runtime_dir/config.toml"

find_matching_pids() {
  ps -axo pid=,command= | awk -v cfg="$config_path" '
    index($0, "zgs_kv") > 0 && index($0, cfg) > 0 { print $1 }
  '
}

stop_pids() {
  local pids
  pids="$1"
  [ -z "$pids" ] && return 0

  printf '%s\n' "$pids" | while IFS= read -r pid; do
    [ -z "$pid" ] && continue
    kill "$pid" 2>/dev/null || true
  done

  for _ in $(seq 1 40); do
    if [ -z "$(find_matching_pids)" ]; then
      return 0
    fi
    sleep 0.25
  done

  printf '%s\n' "$(find_matching_pids)" | while IFS= read -r pid; do
    [ -z "$pid" ] && continue
    kill -9 "$pid" 2>/dev/null || true
  done
}

pids="$(find_matching_pids)"
if [ -z "$pids" ] && [ -f "$pid_file" ]; then
  pid="$(cat "$pid_file" 2>/dev/null || true)"
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    pids="$pid"
  fi
fi

if [ -z "$pids" ]; then
  printf 'zgs_kv was not running.\n'
  rm -f "$pid_file"
  exit 0
fi

stop_pids "$pids"
printf 'Stopped zgs_kv PID(s): %s\n' "$(printf '%s' "$pids" | tr '\n' ' ' | sed 's/[[:space:]]*$//')"
rm -f "$pid_file"
