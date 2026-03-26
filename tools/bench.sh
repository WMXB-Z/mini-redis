#!/usr/bin/env bash
set -euo pipefail

HOST=${HOST:-127.0.0.1}
PORT=${PORT:-6379}
DUR=${DUR:-10}
PIPE=${PIPE:-1}

have_rb() {
  command -v redis-benchmark >/dev/null 2>&1
}

echo "[bench] target ${HOST}:${PORT} duration=${DUR}s pipeline=${PIPE}"

if have_rb; then
  echo "[bench] redis-benchmark found"
  redis-benchmark -h "$HOST" -p "$PORT" -d 8 -t set,get -P "$PIPE" -q -n 100000 || true
  redis-benchmark -h "$HOST" -p "$PORT" -d 8 -q -n 50000 -P "$PIPE" SET key:__rand_int__ value:__rand_int__ || true
else
  echo "[bench] redis-benchmark not found, using nc fallback"
  start=$(date +%s)
  total=0
  while (( $(date +%s) - start < DUR )); do
    printf '*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n' | nc -w 1 "$HOST" "$PORT" >/dev/null || true
    total=$((total+1))
  done
  echo "[bench] approx ops: $total in ${DUR}s"
fi
