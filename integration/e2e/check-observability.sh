#!/usr/bin/env bash
# Exercise three isolated real Compose clusters. Keep stdout as a test report.
set -euo pipefail
cd "$(dirname "$0")/../.."

run="oracache-obs-$(date +%s)-$$"
project="$run"
network=
tmp=$(mktemp -d)
compose_files=(-f compose.cp.yaml -f compose.cp-observability.yaml)
paused=()
export ORACACHE_CP_PORT=0 ORACACHE_CP_STATUS_PORT=0 ORACACHE_PROMETHEUS_PORT=0 ORACACHE_GRAFANA_PORT=0

compose() { docker compose -p "$project" "${compose_files[@]}" "$@"; }
cleanup_stack() {
  if (( ${#paused[@]} )); then
    compose unpause "${paused[@]}" >&2 || true
    paused=()
  fi
  compose down --volumes >&2
}
cleanup() {
  local result=$?
  trap - EXIT
  if (( result != 0 )); then
    compose ps -a >&2 || true
  fi
  cleanup_stack || true
  rm -rf "$tmp"
  exit "$result"
}
trap cleanup EXIT

if [[ -z "${ORACACHE_CP_IMAGE:-}" ]]; then
  export ORACACHE_CP_IMAGE="$run:local"
  compose build cp-api >&2
fi
go build -o "$tmp/oracache" ./cmd/oracache

query() {
  curl -fsSG --max-time 3 "http://$prom/api/v1/query" --data-urlencode "query=$1" |
    jq -ec '.data.result | map({labels:.metric,value:.value[1]})'
}
await_query() {
  local expression=$1 predicate=$2 snapshot deadline=$((SECONDS + 45))
  while (( SECONDS < deadline )); do
    if snapshot=$(query "$expression") && jq -e "$predicate" <<<"$snapshot" >/dev/null; then
      printf '%s => %s\n' "$expression" "$snapshot" >&2
      printf '%s\n' "$snapshot"
      return
    fi
    sleep 1
  done
  printf 'Timed out: %s expected %s; last samples: %s\n' "$expression" "$predicate" "${snapshot:-}" >&2
  return 1
}

rpc() { ORACACHE_ENDPOINT="$gateway" "$tmp/oracache" --timeout=3s "$@"; }
expect_rpc() {
  local predicate=$1 response
  shift
  response=$(rpc "$@")
  jq -e "$predicate" <<<"$response" >/dev/null || {
    jq -c '{status,rev,servedBy}' <<<"$response" >&2
    return 1
  }
  jq -c '{status,rev,servedBy}' <<<"$response"
}
route_snapshot() { curl -fsS --max-time 3 "http://$routes/routes"; }
topology_revision() { route_snapshot | jq -er '.revision'; }
await_route() {
  local node=$1 deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    if route_snapshot | jq -e --arg node "$node" '
      .read_routes["0"].NodeID == $node and .write_routes["0"].NodeID == $node' >/dev/null; then
      return
    fi
    sleep 1
  done
  printf 'Gateway did not route partition 0 to %s\n' "$node" >&2
  return 1
}

all_up='sum(up{job=~"cp-api|api-gateway|storage-node"})'
leader_term='oracache_raft_term and (oracache_raft_is_leader == 1)'
cp_up='up{job="cp-api"}'
last_ok='oracache_gateway_config_last_success_timestamp_seconds'
cp_errors='oracache_gateway_config_refresh_total{result="rpc_error"}'
start() {
  project="$run-$1"
  compose_files=(-f compose.cp.yaml -f compose.cp-observability.yaml)
  printf '\n## %s\n' "$1"
  export ORACACHE_GATEWAY_CP_ADDR=cp-api:28081
  if [[ "$1" == cp-link-loss ]]; then
    export ORACACHE_GATEWAY_CP_ADDR=cp-link:28081
    network="${project}_cp-link"
    cat > "$tmp/cp-link.yaml" <<'YAML'
services:
  cp-api:
    networks:
      default: {}
      cp-link:
        aliases: [cp-link]
  api-gateway:
    networks:
      default: {}
      cp-link: {}
networks:
  cp-link:
    internal: true
YAML
    compose_files+=(-f "$tmp/cp-link.yaml")
  fi
  compose up -d --no-build >&2
  gateway=$(compose port api-gateway 28080)
  routes=$(compose port api-gateway 9466)
  prom=$(compose port prometheus 9090)
  grafana=$(compose port grafana 3000)
  await_query "$all_up" '.[0].value == "5"' >/dev/null
  await_query "$leader_term" 'length == 1' >/dev/null
  await_query 'oracache_gateway_partitions_without_route' 'length == 2 and all(.[]; .value == "0")' >/dev/null
}

start leader-failover
hit_rate_query=$(jq -er '.panels[] | select(.id == 16 and .fieldConfig.defaults.unit == "percentunit") | .targets[0].expr' \
  configs/monitoring/grafana/dashboards/oracache-real.json)
await_query 'min(count_over_time(oracache_cache_rpc_total{operation="get",grpc_code="OK",outcome="hit"}[1m]))' \
  'length == 1 and (.[0].value | tonumber) >= 2' >/dev/null
await_query "$hit_rate_query" 'length == 0' >/dev/null
await_query 'oracache_raft_replica_state' 'length == 3 and (map(.value) | sort) == ["1","1","2"]' >/dev/null
initial_index=$(await_query 'min(oracache_raft_commit_index)' 'length == 1' | jq '.[0].value | tonumber')
expect_rpc '.status == "GET_STATUS_MISS"' get demo:key
await_query "$hit_rate_query" 'length == 2 and all(.[]; (.value | tonumber) == 0)' >/dev/null
seed=$(expect_rpc '.status == "PUT_STATUS_OK"' put demo:key hello --expected-rev=0)
printf '%s\n' "$seed"
rev=$(jq -er '.rev' <<<"$seed")
expect_rpc ".status == \"GET_STATUS_HIT\" and .rev == \"$rev\" and .data == \"aGVsbG8=\"" get demo:key
await_query 'oracache_storage_keys' 'length == 3 and all(.[]; .value == "1")' >/dev/null
await_query 'oracache_storage_accounted_bytes' 'length == 3 and all(.[]; (.value | tonumber) > 0)' >/dev/null
expect_rpc '.status == "PUT_STATUS_CONFLICT"' put demo:key other --expected-rev=0
expect_rpc '.status == "DELETE_STATUS_DELETED"' delete demo:key
expect_rpc '.status == "GET_STATUS_MISS"' get demo:key
await_query "$hit_rate_query" 'length == 2 and all(.[]; (.value | tonumber) > 0.32 and (.value | tonumber) < 0.34)' >/dev/null
await_query 'oracache_cache_rpc_total{grpc_code="OK"} > 0' 'length == 10' >/dev/null
await_query 'sum by (job,operation) (oracache_cache_rpc_duration_seconds_count) > 0' 'length == 6' >/dev/null
await_query 'oracache_cp_rpc_duration_seconds_count > 0' 'length == 2' >/dev/null
await_query 'oracache_raft_stage_duration_seconds_count{stage="read_index"} > 0' 'length == 1' >/dev/null
await_query 'oracache_raft_follower_lag_entries' 'length == 2 and all(.[]; .value == "0")' >/dev/null
await_query 'min(oracache_raft_commit_index)' ".[0].value | tonumber > $initial_index" >/dev/null
for metric in process_cpu_seconds_total process_resident_memory_bytes go_memstats_heap_alloc_bytes go_goroutines; do
  await_query "$metric" 'length == 5' >/dev/null
done

dashboard=$(curl -fsS "http://$grafana/api/dashboards/uid/oracache-real-cluster")
jq -ec 'select(.meta.provisioned) | {dashboard:.dashboard.uid, panels:(.dashboard.panels | length)}' <<<"$dashboard"
curl -fsS "http://$grafana/api/dashboards/uid/oracache-cp-monitoring" | jq -e '.meta.provisioned' >/dev/null
curl -fsS "http://$grafana/api/datasources/proxy/uid/oracache-prometheus/api/v1/query?query=up" |
  jq -e '.status == "success" and (.data.result | length == 5)' >/dev/null
for panel in 13 14; do
  while IFS= read -r expression; do
    await_query "$expression" 'length == 3 and all(.[]; (.value | tonumber) >= 0)' >/dev/null
  done < <(jq -er --argjson panel "$panel" '.dashboard.panels[] | select(.id == $panel) | .targets[] | select(.refId == "B" or .refId == "C") | .expr' <<<"$dashboard")
done
matrix_query=$(jq -er '.dashboard.panels[] | select(.id == 33) | .targets[0].expr' <<<"$dashboard")
regional_matrix=${matrix_query//\$\{raft_region\}/region-a}
await_query "${regional_matrix//\$\{raft_partition:regex\}/.*}" \
  'length == 3 and all(.[]; .labels.region == "region-a")' >/dev/null
await_query "${regional_matrix//\$\{raft_partition:regex\}/0}" \
  'length == 3 and all(.[]; .labels.partition == "0")' >/dev/null
await_query "${regional_matrix//\$\{raft_partition:regex\}/1}" 'length == 0' >/dev/null
while IFS= read -r expression; do
  query "$expression" >/dev/null
done < <(jq -r '.dashboard.panels[] | select(.id != 33) | .targets[]?.expr' <<<"$dashboard")

# As in TestControlPlaneGatewayFailover, stop the serving node after confirmation
# and check the same Gateway with the acknowledged version and a conditional write.
seed=$(expect_rpc '.status == "PUT_STATUS_OK"' put failover:key retained --expected-rev=0)
printf '%s\n' "$seed"
leader=$(jq -er '.servedBy.instance | select(test("^node-[123]$"))' <<<"$seed")
rev=$(jq -er '.rev' <<<"$seed")
old_term=$(await_query "$leader_term" "length == 1 and .[0].labels.node_id == \"$leader\"" | jq '.[0].value | tonumber')
topology=$(topology_revision)
compose kill -s SIGKILL "$leader" >&2
await_query "up{instance=\"$leader:28191\"}" '.[0].value == "0"' >/dev/null
next=$(await_query "$leader_term" "length == 1 and .[0].labels.node_id != \"$leader\" and (.[0].value | tonumber) > $old_term" |
  jq -er '.[0].labels.node_id')
await_route "$next"
await_query 'oracache_raft_replica_state' \
  "length == 3 and any(.[]; .labels.node_id == \"$leader\" and .value == \"-1\") and any(.[]; .labels.node_id == \"$next\" and .value == \"2\")" >/dev/null
[[ "$(topology_revision)" == "$topology" ]]
expect_rpc ".status == \"GET_STATUS_HIT\" and .rev == \"$rev\" and .data == \"cmV0YWluZWQ=\" and .servedBy.instance == \"$next\"" get failover:key
expect_rpc ".status == \"PUT_STATUS_OK\" and (.rev | tonumber) > $rev and .servedBy.instance == \"$next\"" put failover:key updated --expected-rev="$rev"
cleanup_stack

start cp-link-loss
expect_rpc '.status == "PUT_STATUS_OK"' put link:key before --expected-rev=0
topology=$(topology_revision)
gateway_container=$(compose ps -q api-gateway)
before=$(await_query "$cp_errors" 'length == 1' | jq '.[0].value | tonumber')
docker network disconnect "$network" "$gateway_container"
failure=$(await_query "$cp_errors" ".[0].value | tonumber > $before" | jq '.[0].value | tonumber')
stalled=$(await_query "$last_ok" 'length == 1' | jq -r '.[0].value')
await_query "$cp_errors" ".[0].value | tonumber > $failure" >/dev/null
await_query "$last_ok" ".[0].value == \"$stalled\"" >/dev/null
await_query "$all_up" '.[0].value == "5"' >/dev/null
await_query 'oracache_gateway_partitions_without_route' 'length == 2 and all(.[]; .value == "0")' >/dev/null
[[ "$(topology_revision)" == "$topology" ]]
expect_rpc '.status == "GET_STATUS_HIT"' get link:key
expect_rpc '.status == "PUT_STATUS_OK"' put link:key during
docker network connect "$network" "$gateway_container"
await_query "$last_ok" ".[0].value | tonumber > $stalled" >/dev/null
cleanup_stack

start majority-loss
seed=$(expect_rpc '.status == "PUT_STATUS_OK"' put majority:key before --expected-rev=0)
printf '%s\n' "$seed"
leader=$(jq -er '.servedBy.instance | select(test("^node-[123]$"))' <<<"$seed")
followers=()
for node in node-1 node-2 node-3; do
  if [[ "$node" != "$leader" ]]; then followers+=("$node"); fi
done
before=$(await_query "$last_ok" 'length == 1' | jq '.[0].value | tonumber')
gateway_put_failures='oracache_cache_rpc_total{job="api-gateway",operation="put",outcome="none",grpc_code=~"Canceled|DeadlineExceeded|Unavailable"}'
failures_before=$(await_query "$gateway_put_failures" 'length == 3' | jq 'map(.value | tonumber) | add')
paused=("${followers[@]}")
compose pause "${followers[@]}" >&2
if result=$(rpc put majority:key during 2>&1); then
  printf 'Unexpected acknowledgment without majority: %s\n' "$result" >&2
  exit 1
fi
printf 'PUT with 3s deadline => %s\n' "$result"
if [[ "$result" != *"code = DeadlineExceeded"* && "$result" != *"code = Unavailable"* ]]; then
  printf 'Expected DeadlineExceeded or Unavailable from the client\n' >&2
  exit 1
fi
await_query 'sum(up{job="storage-node"})' '.[0].value == "1"' >/dev/null
await_query "$cp_up" '.[0].value == "1"' >/dev/null
await_query "$last_ok" ".[0].value | tonumber > $before" >/dev/null
await_query "$leader_term" 'length == 0' >/dev/null
await_query 'oracache_raft_follower_lag_entries' 'length == 0' >/dev/null
# The client's deadline may cancel the Gateway handler. Its server-side
# counter can record Canceled while the client observes DeadlineExceeded.
await_query "$gateway_put_failures > 0" "map(.value | tonumber) | (add // 0) > $failures_before" >/dev/null
await_query 'oracache_raft_transport_send_errors_total > 0' 'length > 0' >/dev/null
await_query 'oracache_gateway_partitions_without_route{operation="put"}' '.[0].value == "1"' >/dev/null
compose unpause "${followers[@]}" >&2
paused=()
await_query "$all_up" '.[0].value == "5"' >/dev/null
next=$(await_query "$leader_term" 'length == 1' | jq -er '.[0].labels.node_id')
await_route "$next"
printf 'PUT after restoring the majority\n'
expect_rpc ".status == \"PUT_STATUS_OK\" and .servedBy.instance == \"$next\"" put majority-recovery:key after --expected-rev=0
