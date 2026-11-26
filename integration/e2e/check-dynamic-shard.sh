#!/usr/bin/env bash
# Real membership mutation uses the standalone administrative API from #101.
set -euo pipefail
cd "$(dirname "$0")/../.."

project="oracache-discovery-$(date +%s)-$$"
mkdir -p .reports
tmp=$(mktemp -d "$PWD/.reports/dynamic-shard.XXXXXXXX")
export ORACACHE_PROMETHEUS_TARGETS_DIR="$tmp/targets"
export ORACACHE_PROMETHEUS_PORT=0 ORACACHE_GRAFANA_PORT=0
mkdir "$ORACACHE_PROMETHEUS_TARGETS_DIR"

cat > "$tmp/cluster.yaml" <<'YAML'
x-storage: &standalone
  entrypoint:
    - /storage-node
    - --grpc-listen=0.0.0.0:28091
    - --raft-listen=0.0.0.0:28191
    - --snapshot-entries=8
    - --peer=1,node-1,node-1:28091,http://node-1:28191/raft
    - --peer=2,node-2,node-2:28091,http://node-2:28191/raft
    - --peer=3,node-3,node-3:28091,http://node-3:28191/raft
  ports: ["127.0.0.1::28191", "127.0.0.1::28091"]
services:
  node-1: *standalone
  node-2: *standalone
  node-3: *standalone
  node-4:
    <<: *standalone
    image: ${ORACACHE_CP_IMAGE:-oracache-cp:local}
    command: [--node-id=node-4, --peer-id=4, --join, "--peer=4,node-4,node-4:28091,http://node-4:28191/raft"]
    labels:
      io.oracache.metrics-address: node-4:28191
YAML

compose() { docker compose -p "$project" -f compose.cp.yaml -f compose.cp-observability.yaml -f "$tmp/cluster.yaml" "$@"; }
watcher=
cleanup() {
  local result=$?
  trap - EXIT
  if [[ -n "$watcher" ]]; then
    kill "$watcher" 2>/dev/null || true
    wait "$watcher" 2>/dev/null || true
  fi
  if (( result != 0 )); then
    compose ps -a >&2 || true
    compose logs --tail=15 >&2 || true
  fi
  compose down --volumes >&2 || true
  rm -rf "$tmp"
  exit "$result"
}
trap cleanup EXIT

if [[ -z "${ORACACHE_CP_IMAGE:-}" ]]; then
  export ORACACHE_CP_IMAGE="$project:local"
  compose build node-1 >&2
fi
go build -o "$tmp/oracache" ./cmd/oracache
compose up -d --no-deps --no-build node-1 node-2 node-3 prometheus grafana >&2
integration/e2e/watch-storage-targets.sh "$project" "$ORACACHE_PROMETHEUS_TARGETS_DIR" &
watcher=$!
prom=$(compose port prometheus 9090)
grafana=$(compose port grafana 3000)
prometheus_id=$(compose ps -q prometheus)

await() {
  local deadline=$((SECONDS + 35)) output
  while (( SECONDS < deadline )); do
    if output=$("$@"); then
      printf '%s\n' "$output"
      return
    fi
    if [[ -n "$watcher" ]]; then kill -0 "$watcher"; fi
    sleep 1
  done
  printf 'Timed out waiting for %s\n' "$*" >&2
  return 1
}

prom_query() {
  curl -fsSG --max-time 3 "http://$prom/api/v1/query" --data-urlencode "query=$1" |
    jq -ce '.data.result | map({labels: .metric, value: .value[1]}) | select('"$2"')'
}

find_leader() {
  local service http
  for service in "$@"; do
    http=$(compose port "$service" 28191)
    if curl -fsS --max-time 1 "http://$http/status" | jq -e '.Role == "StateLeader" and .Ready' >/dev/null; then
      printf '%s\n' "$service"
      return
    fi
  done
  return 1
}

change_membership() {
  local http=$1 body=$2 predicate=$3
  curl -fsS --max-time 5 "http://$http/membership" -H 'Content-Type: application/json' -d "$body" |
    jq -ce "$predicate"
}

rpc() { ORACACHE_ENDPOINT=$(compose port "$1" 28091) "$tmp/oracache" --timeout=3s "$2" shard:demo "${@:3}"; }

await prom_query 'up{job="storage-node"}' 'length == 3 and all(.[]; .value == "1")'
leader=$(await find_leader node-1 node-2 node-3)
http=$(compose port "$leader" 28191)
for value in 1 2 3 4 5 6 7 8 9 10; do
  rpc "$leader" put "$value" | jq -e '.status == "PUT_STATUS_OK"' >/dev/null
done

compose up -d --no-deps --no-build node-4 >&2
await prom_query 'up{job="storage-node",node_id="node-4"}' 'length == 1 and .[0].value == "1"'
add='{"group_id":"region-a/shard-0/poc-1","operation_id":"add-4","action":"add-learner","peer":{"id":4,"node_id":"node-4","address":"node-4:28091","raft_address":"http://node-4:28191/raft"}}'
change_membership "$http" "$add" '.membership | select(any(.members[]; .id == 4 and .learner and (.voter | not)))'

dashboard=$(curl -fsS --max-time 3 "http://$grafana/api/dashboards/uid/oracache-real-cluster")
matrix=$(jq -er '.dashboard.panels[] | select(.id == 33 and
  .fieldConfig.defaults.mappings[0].options["3"].text == "Learner") | .targets[0].expr' <<<"$dashboard")
matrix=$(sed 's/\${raft_region}/region-a/; s/\${raft_partition:regex}/0/; s/}$/,node_id="node-4"}/' <<<"$matrix")
await prom_query "$matrix" 'length == 1 and .[0].value == "3"'
curl -fsSG --max-time 3 "http://$grafana/api/datasources/proxy/uid/oracache-prometheus/api/v1/query" --data-urlencode "query=$matrix" |
  jq -e '.status == "success" and (.data.result[0].value[1] == "3")' >/dev/null

node4_http=$(compose port node-4 28191)
curl -fsS --max-time 3 "http://$node4_http/status" | jq -e '.IsLearner and (.Ready | not)' >/dev/null
if rpc node-4 get; then
  printf 'Learner served a client request\n' >&2
  exit 1
fi

promote='{"group_id":"region-a/shard-0/poc-1","operation_id":"promote-4","action":"promote","peer_id":4}'
await change_membership "$http" "$promote" '.membership | select(any(.members[]; .id == 4 and .voter and (.learner | not)))'
await prom_query "$matrix" 'length == 1 and .[0].value == "1"'

remove='{"group_id":"region-a/shard-0/poc-1","operation_id":"remove-4","action":"remove","peer_id":4}'
change_membership "$http" "$remove" '.membership | select((any(.members[]; .id == 4) | not) and (.removed_ids | index(4) != null))'
await prom_query 'up{job="storage-node",node_id="node-4"}' 'length == 1 and .[0].value == "1"'
compose stop node-4 >&2
await prom_query 'up{job="storage-node",node_id="node-4"}' 'length == 1 and .[0].value == "0"'
compose rm -f node-4 >&2
await prom_query 'up{job="storage-node",node_id="node-4"}' 'length == 0'
[[ $(compose ps -q prometheus) == "$prometheus_id" ]]

# The same mutation is currently refused when the Storage Node runs a CP Agent.
# Use a fresh cluster because its earlier standalone Raft state is in memory.
kill "$watcher"
wait "$watcher" || true
watcher=
compose down --volumes >&2
project="$project-cp"
cat > "$tmp/cluster.yaml" <<'YAML'
x-publish: &publish
  ports: ["127.0.0.1::28191"]
services:
  node-1: *publish
  node-2: *publish
  node-3: *publish
YAML
export ORACACHE_CP_PORT=0 ORACACHE_CP_STATUS_PORT=0
compose up -d --no-build cp-api node-1 node-2 node-3 api-gateway >&2
gateway=$(compose port api-gateway 28080)
write_through_gateway() {
  ORACACHE_ENDPOINT="$gateway" "$tmp/oracache" --timeout=3s put guard:demo before |
    jq -ce 'select(.status == "PUT_STATUS_OK") | {status, servedBy}'
}
await write_through_gateway
leader=$(await find_leader node-1 node-2 node-3)
http=$(compose port "$leader" 28191)
http_code=$(curl -sS --max-time 5 -o "$tmp/guard.json" -w '%{http_code}' "http://$http/membership" \
  -H 'Content-Type: application/json' -d "$add")
[[ "$http_code" == 409 ]]
jq -ce 'select(.code == "FailedPrecondition")' "$tmp/guard.json"
curl -fsS --max-time 5 "http://$http/membership" | jq -e 'all(.members[]; .id != 4)' >/dev/null
printf 'Observed standalone AddLearner plus the running CP Agent membership guard.\n'
