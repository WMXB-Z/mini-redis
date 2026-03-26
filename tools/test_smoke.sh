#!/usr/bin/env bash
set -euo pipefail

HOST=${HOST:-127.0.0.1}
PORT=${PORT:-6379}

have_cli() { command -v redis-cli >/dev/null 2>&1; }

red() { printf "\e[31m%s\e[0m\n" "$*"; }
green() { printf "\e[32m%s\e[0m\n" "$*"; }

fail() { red "[FAIL] $*"; exit 1; }
ok() { green "[OK] $*"; }

if have_cli; then
  rc() { redis-cli --raw -h "$HOST" -p "$PORT" "$@"; }
  check_eq() { local exp="$1"; shift; local got; got=$(rc "$@" 2>/dev/null | tr -d '\r'); [[ "$got" == "$exp" ]] || fail "$* expected='$exp' got='$got'"; }
  check_in() { local exp="$1"; shift; local got; got=$(rc "$@" 2>/dev/null | tr -d '\r'); [[ "$got" == *"$exp"* ]] || fail "$* expect-contains='$exp' got='$got'"; }

  # ensure clean state
  rc FLUSHALL >/dev/null || true

  # Base
  check_eq PONG PING
  check_eq hello ECHO hello

  # String
  check_eq OK SET k v
  check_eq v GET k
  rc DEL k >/dev/null || true
  check_eq 0 EXISTS k
  check_eq OK SET k v
  check_eq 1 EXPIRE k 10
  # TTL returns >=0 or -1/-2; just ensure integer
  ttlv=$(rc TTL k | tr -d '\r'); [[ "$ttlv" =~ ^-?[0-9]+$ ]] || fail "TTL not integer: $ttlv"; ok "TTL integer: $ttlv"

  # Hash
  check_eq 1 HSET h f1 v1
  check_eq v1 HGET h f1
  check_eq 1 HLEN h
  check_eq 1 HDEL h f1
  check_eq 0 HLEN h

  # ZSet
  check_eq 1 ZADD z 1.0 a
  check_eq 1 ZADD z 0.5 b
  zr=$(rc ZRANGE z 0 1 | tr -d '\r'); [[ "$zr" == *$'a'* && "$zr" == *$'b'* ]] || fail "ZRANGE unexpected: $zr"; ok "ZRANGE contains a,b"
  zs=$(rc ZSCORE z a | tr -d '\r')
  [[ "$zs" == "1" || "$zs" == "1.0" || "$zs" == "1.00" || "$zs" == "1.000000" ]] || fail "ZSCORE expect 1 got='$zs'"
  check_eq 1 ZREM z b

  # KEYS & FLUSHALL
  rc SET foo bar >/dev/null
  rc HSET h f v >/dev/null
  rc ZADD z 2 c >/dev/null
  keys=$(rc KEYS '*'); [[ "$keys" == *$'foo'* && "$keys" == *$'h'* && "$keys" == *$'z'* ]] || fail "KEYS missing entries: $keys"; ok "KEYS lists foo,h,z"
  check_eq OK FLUSHALL
  k2=$(rc KEYS '*'); [[ -z "$k2" ]] || fail "FLUSHALL not empty: $k2"; ok "FLUSHALL emptied keys"

  # BGSAVE
  check_eq OK BGSAVE
  ok "All tests passed via redis-cli"
else
  # Fallback minimal nc checks
  nc_send() { printf "%s" "$1" | nc -w 1 "$HOST" "$PORT"; }
  pong=$(nc_send $'*1\r\n$4\r\nPING\r\n' | tr -d '\r'); [[ "$pong" == "+PONG"* ]] || fail "PING failed"; ok "PING"
  setok=$(nc_send $'*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n' | tr -d '\r'); [[ "$setok" == "+OK"* ]] || fail "SET failed"; ok "SET"
  getv=$(nc_send $'*2\r\n$3\r\nGET\r\n$1\r\nk\r\n' | tr -d '\r'); [[ "$getv" == *$'\n'v* ]] || fail "GET failed: $getv"; ok "GET"
  ok "Basic tests passed via nc (limited coverage)"
fi

