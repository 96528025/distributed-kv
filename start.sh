#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_DIR="${KV_RUN_DIR:-$ROOT_DIR/.run}"
DATA_DIR="${KV_DATA_DIR:-$ROOT_DIR/.demo-data}"
PORTS=(5001 5002 5003)

usage() {
  echo "Usage: ./start.sh {start|stop|status}"
}

pid_file() {
  echo "$RUN_DIR/node-$1.pid"
}

is_running() {
  local port="$1"
  local file
  file="$(pid_file "$port")"
  [[ -f "$file" ]] || return 1

  local pid
  pid="$(<"$file")"
  kill -0 "$pid" 2>/dev/null || return 1

  local command
  command="$(ps -p "$pid" -o command= 2>/dev/null || true)"
  [[ "$command" == *"node_raft_sharded.py"* ]]
}

start_node() {
  local port="$1"
  shift

  local -a node_command=(
    python3 "$ROOT_DIR/node_raft_sharded.py"
    "$port" "$@"
    --backend=wal
    --data-dir="$DATA_DIR"
  )
  if [[ "${KV_FSYNC:-0}" == "1" ]]; then
    node_command+=(--fsync)
  fi

  "${node_command[@]}" >"$RUN_DIR/node-$port.log" 2>&1 &
  echo "$!" >"$(pid_file "$port")"
}

wait_until_ready() {
  local port="$1"
  local deadline=$((SECONDS + 15))
  while (( SECONDS < deadline )); do
    if curl --silent --fail --max-time 1 "http://127.0.0.1:$port/health" >/dev/null; then
      return 0
    fi
    sleep 0.2
  done
  return 1
}

wait_until_elected() {
  local deadline=$((SECONDS + 15))
  while (( SECONDS < deadline )); do
    local cluster_ready=1
    for port in "${PORTS[@]}"; do
      if ! curl --silent --fail --max-time 1 \
        "http://127.0.0.1:$port/health" | \
        python3 -c 'import json, sys; data = json.load(sys.stdin); raise SystemExit(0 if all(s["leader"] is not None for s in data["shards"].values()) else 1)'
      then
        cluster_ready=0
        break
      fi
    done
    if [[ "$cluster_ready" == "1" ]]; then
      return 0
    fi
    sleep 0.2
  done
  return 1
}

start_cluster() {
  mkdir -p "$RUN_DIR" "$DATA_DIR"

  for port in "${PORTS[@]}"; do
    if is_running "$port"; then
      echo "Node $port is already running. Use './start.sh stop' first."
      exit 1
    fi
  done

  echo "Starting a three-node sharded KV cluster with the WAL backend..."
  start_node 5001 5002 5003
  start_node 5002 5001 5003
  start_node 5003 5001 5002

  for port in "${PORTS[@]}"; do
    if ! wait_until_ready "$port"; then
      echo "Node $port did not become ready. See $RUN_DIR/node-$port.log."
      stop_cluster
      exit 1
    fi
  done

  if ! wait_until_elected; then
    echo "The cluster did not elect a leader for every shard within 15 seconds."
    stop_cluster
    exit 1
  fi

  echo "Cluster ready: http://127.0.0.1:{5001,5002,5003}"
  echo "Write:   curl http://127.0.0.1:5001/set -d '{\"key\":\"hello\",\"value\":\"world\"}'"
  echo "Read:    curl 'http://127.0.0.1:5002/get?key=hello'"
  echo "Metrics: curl http://127.0.0.1:5001/metrics"
  echo "Stop:    ./start.sh stop"
}

stop_cluster() {
  local stopped=0
  for port in "${PORTS[@]}"; do
    local file
    file="$(pid_file "$port")"
    if is_running "$port"; then
      local pid
      pid="$(<"$file")"
      kill "$pid"
      stopped=1
      echo "Stopped node $port (PID $pid)."
    fi
    rm -f "$file"
  done

  if [[ "$stopped" == "0" ]]; then
    echo "No managed nodes are running."
  fi
}

show_status() {
  for port in "${PORTS[@]}"; do
    if is_running "$port"; then
      echo "node $port: running (PID $(<"$(pid_file "$port")"))"
    else
      echo "node $port: stopped"
    fi
  done
}

case "${1:-}" in
  start) start_cluster ;;
  stop) stop_cluster ;;
  status) show_status ;;
  *) usage; exit 2 ;;
esac
