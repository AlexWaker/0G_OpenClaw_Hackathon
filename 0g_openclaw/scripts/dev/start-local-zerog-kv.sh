#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: start-local-zerog-kv.sh [--foreground] [--build-only] [--dry-run] [--no-build] [--force-restart]

Starts a local 0G Storage KV node for the current OpenClaw wallet.
USAGE
}

foreground=0
build_only=0
dry_run=0
no_build=0
force_restart=0

while [ "$#" -gt 0 ]; do
  case "$1" in
    --foreground)
      foreground=1
      ;;
    --build-only)
      build_only=1
      ;;
    --dry-run)
      dry_run=1
      ;;
    --no-build)
      no_build=1
      ;;
    --force-restart)
      force_restart=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'Unknown option: %s\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
default_source_dir="$(cd "$repo_root/.." && pwd)/0g-storage-kv-local"
source_dir="${ZEROG_KV_SOURCE_DIR:-$default_source_dir}"
state_dir="${OPENCLAW_STATE_DIR:-$HOME/.openclaw}"
wallet_path="$state_dir/credentials/ethereum-wallet.json"

network="mainnet"
if [ -n "${OPENCLAW_0G_RPC_URL:-}" ]; then
  case "${OPENCLAW_0G_RPC_URL}" in
    https://evmrpc.0g.ai)
      network="mainnet"
      ;;
    https://evmrpc-testnet.0g.ai)
      network="testnet"
      ;;
    *)
      network="custom"
      ;;
  esac
elif [ "${OPENCLAW_0G_NETWORK:-mainnet}" = "testnet" ]; then
  network="testnet"
fi

case "$network" in
  mainnet)
    default_chain_rpc="${OPENCLAW_0G_RPC_URL:-https://evmrpc.0g.ai}"
    default_indexer="${OPENCLAW_0G_STORAGE_INDEXER_URL:-https://indexer-storage-turbo.0g.ai}"
    default_flow_contract="0x62d4144db0f0a6fbbaeb6296c785c71b3d57c526"
    ;;
  testnet)
    default_chain_rpc="${OPENCLAW_0G_RPC_URL:-https://evmrpc-testnet.0g.ai}"
    default_indexer="${OPENCLAW_0G_STORAGE_INDEXER_URL:-https://indexer-storage-testnet-turbo.0g.ai}"
    default_flow_contract="0x22e03a6a89b950f1c82ec5e74f8eca321a105296"
    ;;
  custom)
    default_chain_rpc="${OPENCLAW_0G_RPC_URL:-}"
    default_indexer="${OPENCLAW_0G_STORAGE_INDEXER_URL:-}"
    default_flow_contract="${ZEROG_KV_FLOW_CONTRACT_ADDRESS:-}"
    ;;
esac

chain_rpc="${ZEROG_KV_BLOCKCHAIN_RPC_ENDPOINT:-$default_chain_rpc}"
indexer_url="${ZEROG_KV_INDEXER_URL:-$default_indexer}"
flow_contract="${ZEROG_KV_FLOW_CONTRACT_ADDRESS:-$default_flow_contract}"
rpc_listen_address="${ZEROG_KV_RPC_LISTEN_ADDRESS:-127.0.0.1:6789}"
log_sync_start_block_number="${ZEROG_KV_LOG_SYNC_START_BLOCK_NUMBER:-0}"
runtime_dir="${ZEROG_KV_RUNTIME_DIR:-$state_dir/zerog-kv/$network}"
config_path="$runtime_dir/config.toml"
log_config_path="$runtime_dir/log_config"
pid_file="$runtime_dir/zgs_kv.pid"
output_log="$runtime_dir/zgs_kv.log"
build_log="$runtime_dir/build.log"
metadata_path="$runtime_dir/openclaw-memory-stream.txt"
default_binary_path="$runtime_dir/bin/zgs_kv"
binary_path="${ZEROG_KV_BINARY:-$default_binary_path}"
release_tag="${ZEROG_KV_RELEASE_TAG:-v1.5.1}"
release_url="${ZEROG_KV_RELEASE_URL:-https://github.com/0gfoundation/0g-storage-kv/releases/download/$release_tag/zgs_kv_mac.zip}"

find_matching_pids() {
  ps -axo pid=,command= | awk -v cfg="$config_path" '
    index($0, "zgs_kv") > 0 && index($0, cfg) > 0 { print $1 }
  '
}

stop_matching_processes() {
  local pids
  pids="$(find_matching_pids)"
  if [ -z "$pids" ]; then
    return 0
  fi

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

download_binary() {
  if ! command -v curl >/dev/null 2>&1; then
    return 1
  fi
  if ! command -v unzip >/dev/null 2>&1; then
    return 1
  fi

  local tmp_dir
  local archive_path
  local extracted_path
  tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/openclaw-zgs-kv.XXXXXX")"
  archive_path="$tmp_dir/zgs_kv_mac.zip"

  rm -rf "$tmp_dir" >/dev/null 2>&1 || true
  tmp_dir="$(mktemp -d)"
  archive_path="$tmp_dir/zgs_kv_mac.zip"

  if ! curl -L --fail --output "$archive_path" "$release_url"; then
    rm -rf "$tmp_dir"
    return 1
  fi
  if ! unzip -qo "$archive_path" -d "$tmp_dir"; then
    rm -rf "$tmp_dir"
    return 1
  fi

  extracted_path="$(find "$tmp_dir" -type f -name zgs_kv | head -1)"
  if [ -z "$extracted_path" ]; then
    rm -rf "$tmp_dir"
    return 1
  fi

  mkdir -p "$(dirname "$binary_path")"
  cp "$extracted_path" "$binary_path"
  chmod +x "$binary_path"
  rm -rf "$tmp_dir"
  return 0
}

if [ ! -f "$wallet_path" ]; then
  printf 'OpenClaw wallet not found: %s\n' "$wallet_path" >&2
  exit 1
fi

if [ -z "$chain_rpc" ] || [ -z "$indexer_url" ] || [ -z "$flow_contract" ]; then
  printf 'Missing network configuration. chain_rpc=%s indexer_url=%s flow_contract=%s\n' "$chain_rpc" "$indexer_url" "$flow_contract" >&2
  exit 1
fi

mkdir -p "$runtime_dir"

wallet_info="$(
  cd "$repo_root"
  WALLET_PATH="$wallet_path" node <<'NODE'
const fs = require("node:fs");
const { Wallet, keccak256, toUtf8Bytes } = require("ethers");

const walletPath = process.env.WALLET_PATH;
const raw = JSON.parse(fs.readFileSync(walletPath, "utf8"));
const address = raw.kind === "mnemonic" ? Wallet.fromPhrase(raw.mnemonic).address : new Wallet(raw.privateKey).address;
const streamId = keccak256(toUtf8Bytes(`openclaw:0g-memory-sync:v1:${address.trim().toLowerCase()}`));
console.log(address);
console.log(streamId.slice(2));
NODE
)"

wallet_address="$(printf '%s\n' "$wallet_info" | sed -n '1p')"
stream_id="$(printf '%s\n' "$wallet_info" | sed -n '2p')"

if [ -z "$wallet_address" ] || [ -z "$stream_id" ]; then
  printf 'Failed to derive wallet address or OpenClaw stream id.\n' >&2
  exit 1
fi

cat > "$config_path" <<EOF
#######################################################################
###                   Key-Value Stream Options                      ###
#######################################################################
stream_ids = ["$stream_id"]

#######################################################################
###                     DB Config Options                           ###
#######################################################################
db_dir = "$runtime_dir/db"
kv_db_file = "$runtime_dir/kv.DB"

#######################################################################
###                     Log Sync Config Options                     ###
#######################################################################
blockchain_rpc_endpoint = "$chain_rpc"
log_contract_address = "$flow_contract"
log_sync_start_block_number = $log_sync_start_block_number

#######################################################################
###                     RPC Config Options                          ###
#######################################################################
rpc_enabled = true
rpc_listen_address = "$rpc_listen_address"
indexer_url = "$indexer_url"
zgs_node_urls = ""

#######################################################################
###                     Misc Config Options                         ###
#######################################################################
log_config_file = "$log_config_path"
EOF

printf '%s\n' "info" > "$log_config_path"
printf 'wallet=%s\nstream_id=%s\nrpc=%s\nnetwork=%s\n' "$wallet_address" "$stream_id" "http://$rpc_listen_address" "$network" > "$metadata_path"

if [ ! -x "$binary_path" ]; then
  printf 'Downloading official zgs_kv release from %s\n' "$release_url"
  if ! download_binary; then
    if [ "$no_build" -eq 1 ]; then
      printf 'zgs_kv binary missing, download failed, and --no-build was set.\n' >&2
      exit 1
    fi
    if [ ! -d "$source_dir" ]; then
      printf '0G Storage KV source checkout not found for build fallback: %s\n' "$source_dir" >&2
      exit 1
    fi
    binary_path="$source_dir/target/release/zgs_kv"
    printf 'Falling back to cargo build in %s\n' "$source_dir"
    (
      cd "$source_dir"
      cargo build --release
    ) 2>&1 | tee "$build_log"
  fi
fi

if [ "$build_only" -eq 1 ]; then
  printf 'Build complete: %s\n' "$binary_path"
  exit 0
fi

if [ -f "$pid_file" ]; then
  existing_pid="$(cat "$pid_file" 2>/dev/null || true)"
  if [ -n "$existing_pid" ] && kill -0 "$existing_pid" 2>/dev/null; then
    if [ "$force_restart" -eq 0 ]; then
      printf 'zgs_kv already running with PID %s\n' "$existing_pid"
      printf 'RPC: http://%s\n' "$rpc_listen_address"
      exit 0
    fi
    kill "$existing_pid"
    rm -f "$pid_file"
  else
    rm -f "$pid_file"
  fi
fi

stray_pids="$(find_matching_pids)"
if [ -n "$stray_pids" ]; then
  if [ "$force_restart" -eq 0 ]; then
    first_pid="$(printf '%s\n' "$stray_pids" | sed -n '1p')"
    printf 'zgs_kv already running with PID %s\n' "$first_pid"
    printf 'RPC: http://%s\n' "$rpc_listen_address"
    exit 0
  fi
  stop_matching_processes
  rm -f "$pid_file"
fi

if [ "$dry_run" -eq 1 ]; then
  printf 'Would start %s --config %s\n' "$binary_path" "$config_path"
  exit 0
fi

if [ "$foreground" -eq 1 ]; then
  exec "$binary_path" --config "$config_path"
fi

nohup "$binary_path" --config "$config_path" >> "$output_log" 2>&1 &
pid=$!
printf '%s\n' "$pid" > "$pid_file"
printf 'Started zgs_kv (PID %s)\n' "$pid"
printf 'RPC: http://%s\n' "$rpc_listen_address"
printf 'Log: %s\n' "$output_log"
printf 'Config: %s\n' "$config_path"
