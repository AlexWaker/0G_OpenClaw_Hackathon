#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -eq 0 ]; then
  printf 'Usage: openclaw-with-local-zerog-kv.sh <command> [args...]\n' >&2
  exit 2
fi

script_dir="$(cd "$(dirname "$0")" && pwd)"
rpc_listen_address="${ZEROG_KV_RPC_LISTEN_ADDRESS:-127.0.0.1:6789}"
"$script_dir/start-local-zerog-kv.sh"
export OPENCLAW_0G_KV_RPC_URL="http://$rpc_listen_address"
exec "$@"
