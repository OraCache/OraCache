#!/usr/bin/env bash
# CLI smoke + kill-leader failover against the running PoC Compose project.
#   docker compose -f compose.poc.yaml up -d --build
#   go build -o bin/oracache ./cmd/oracache
#   scripts/poc-smoke.sh            # add --no-kill to skip the failover part
# Kills the ACTUAL elected leader container; it is not restarted (no persistence).
set -euo pipefail
cd "$(dirname "$0")/.."

export ORACACHE_ENDPOINT="${ORACACHE_ENDPOINT:-127.0.0.1:28080}"
CLI="${ORACACHE_CLI:-bin/oracache}"
COMPOSE=(docker compose -f compose.poc.yaml)
STATUS_PORTS=("${ORACACHE_POC_NODE1_PORT:-28091}" "${ORACACHE_POC_NODE2_PORT:-28092}" "${ORACACHE_POC_NODE3_PORT:-28093}")
KEY="smoke:$(date +%s)"

cli() { "$CLI" --timeout 20s "$@"; }
field() { python3 -c 'import sys,json; print(json.load(sys.stdin).get(sys.argv[1],""))' "$1"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
expect() { # expect <what> <got> <want>
	[ "$2" = "$3" ] || fail "$1: got '$2', want '$3'"
	echo "ok   $1 = $2"
}

# Print the node number (1..3) whose own status says it is leader, excluding $1.
leader_node() {
	local i out
	for i in 0 1 2; do
		[ "$((i + 1))" = "${1:-0}" ] && continue
		out=$(curl -fsS --max-time 2 "http://127.0.0.1:${STATUS_PORTS[$i]}/status" 2>/dev/null) || continue
		if [ "$(printf '%s' "$out" | field Role)" = StateLeader ] && [ "$(printf '%s' "$out" | field Ready)" = True ]; then
			echo "$((i + 1))"
			return 0
		fi
	done
	return 1
}
wait_leader() {
	local n
	for _ in $(seq 1 60); do
		if n=$(leader_node "${1:-0}"); then echo "$n"; return 0; fi
		sleep 1
	done
	return 1
}

echo "== waiting for a leader"
L1=$(wait_leader) || fail "no leader elected"
echo "leader: node-$L1"

echo "== CLI via gateway $ORACACHE_ENDPOINT, key $KEY"
expect "get absent" "$(cli get "$KEY" | field status)" GET_STATUS_MISS
out=$(cli put "$KEY" hello --expected-rev 0)
expect "create-only put" "$(printf '%s' "$out" | field status)" PUT_STATUS_OK
R1=$(printf '%s' "$out" | field rev)
out=$(cli put "$KEY" other --expected-rev 0)
expect "duplicate create-only put" "$(printf '%s' "$out" | field status)" PUT_STATUS_CONFLICT
expect "conflict reports latest rev" "$(printf '%s' "$out" | field rev)" "$R1"
out=$(cli put "$KEY" hello2 --expected-rev "$R1")
expect "conditional put" "$(printf '%s' "$out" | field status)" PUT_STATUS_OK
R2=$(printf '%s' "$out" | field rev)
[ "$R2" -gt "$R1" ] || fail "revision did not increase: $R1 -> $R2"
out=$(cli get "$KEY")
expect "get latest" "$(printf '%s' "$out" | field status)" GET_STATUS_HIT
expect "get latest data (base64 hello2)" "$(printf '%s' "$out" | field data)" aGVsbG8y
expect "get old revision data (base64 hello)" "$(cli get "$KEY" --rev "$R1" | field data)" aGVsbG8=

[ "${1:-}" = "--no-kill" ] && { echo "SMOKE PASSED (failover skipped)"; exit 0; }

echo "== killing the elected leader: node-$L1"
"${COMPOSE[@]}" kill "node-$L1" >/dev/null
L2=$(wait_leader "$L1") || fail "no new leader after killing node-$L1"
[ "$L2" != "$L1" ] || fail "leader did not change"
echo "new leader: node-$L2"

echo "== same gateway endpoint after failover"
out=$(cli get "$KEY")
expect "get after failover" "$(printf '%s' "$out" | field status)" GET_STATUS_HIT
expect "confirmed data survived" "$(printf '%s' "$out" | field rev)" "$R2"
out=$(cli put "$KEY" hello3 --expected-rev "$R2")
expect "put after failover" "$(printf '%s' "$out" | field status)" PUT_STATUS_OK
R3=$(printf '%s' "$out" | field rev)
[ "$R3" -gt "$R2" ] || fail "revision did not increase after failover: $R2 -> $R3"
expect "stale conditional put conflicts" "$(cli put "$KEY" late --expected-rev "$R2" | field status)" PUT_STATUS_CONFLICT
expect "delete" "$(cli delete "$KEY" | field status)" DELETE_STATUS_DELETED
expect "get after delete" "$(cli get "$KEY" | field status)" GET_STATUS_MISS
echo "SMOKE PASSED: leader node-$L1 killed, node-$L2 serves the same gateway endpoint"
