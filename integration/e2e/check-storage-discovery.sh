#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

run="oracache-discovery-$(date +%s)-$$"
project="$run"
tmp=$(mktemp -d)
export ORACACHE_CP_PORT=0 ORACACHE_CP_STATUS_PORT=0 ORACACHE_PROMETHEUS_PORT=0 ORACACHE_GRAFANA_PORT=0
export ORACACHE_REPLICATION_PORT=0
compose_files=(-f compose.cp.yaml -f compose.cp-observability.yaml -f "$tmp/discovery.yaml")

cat > "$tmp/discovery.yaml" <<'YAML'
services:
  cp-api:
    ports: ["127.0.0.1::9464"]
  node-4:
    image: ${ORACACHE_CP_IMAGE:-oracache-cp:local}
    profiles: [extra-replica]
    entrypoint:
      - /storage-node
      - --cp-addr=cp-api:28081
      - --heartbeat-interval=1s
      - --grpc-listen=0.0.0.0:28091
      - --raft-listen=0.0.0.0:28191
      - --node-id=node-4
      - --peer-id=4
      - --join
      - --peer=1,node-1,node-1:28091,http://node-1:28191/raft
      - --peer=2,node-2,node-2:28091,http://node-2:28191/raft
      - --peer=3,node-3,node-3:28091,http://node-3:28191/raft
      - --peer=4,node-4,node-4:28091,http://node-4:28191/raft
    ports: ["127.0.0.1::28191"]
YAML

compose() { docker compose -p "$project" "${compose_files[@]}" "$@"; }
cleanup() {
  local result=$?
  trap - EXIT
  compose unpause >/dev/null 2>&1 || true
  if (( result != 0 )); then
    compose ps -a >&2 || true
    compose logs --tail=12 >&2 || true
  fi
  compose --profile extra-replica down --volumes >&2 || true
  rm -rf "$tmp"
  exit "$result"
}
trap cleanup EXIT

if [[ -z "${ORACACHE_CP_IMAGE:-}" ]]; then
  export ORACACHE_CP_IMAGE="$run:local"
  compose build cp-api >&2
fi
go build -o "$tmp/oracache" ./cmd/oracache

await() {
  local deadline=$((SECONDS + 50)) output
  while (( SECONDS < deadline )); do
    if output=$("$@" 2>"$tmp/last-error"); then
      printf '%s\n' "$output"
      return
    fi
    sleep 1
  done
  printf 'Timed out waiting for %s\n' "$*" >&2
  cat "$tmp/last-error" >&2
  return 1
}

prom_json() { curl -fsS --max-time 4 "http://$prom/api/v1/$1"; }
targets() {
  prom_json targets | jq -ce '[.data.activeTargets[] | select(.labels.job == "storage-node") |
    {node: .labels.node_id, region: .labels.region, url: .scrapeUrl, health}] | sort_by(.node) | select('"$1"')'
}
prom_query() {
  curl -fsSG --max-time 4 "http://$prom/api/v1/query" --data-urlencode "query=$1" |
    jq -ce '[.data.result[] | {labels: .metric, value: .value[1]}] | select('"$2"')'
}
panel() {
  local id=$1 region=$2 expression
  expression=$(jq -er --argjson id "$id" '.dashboard.panels[] | select(.id == $id) | .targets[0].expr' <<<"$dashboard")
  expression=${expression//\$\{raft_region\}/$region}
  printf '%s\n' "${expression//\$\{raft_partition:regex\}/0}"
}
grafana_query() {
  curl -fsSG --max-time 4 "http://$grafana/api/datasources/proxy/uid/oracache-prometheus/api/v1/query"     --data-urlencode "query=$1" |
    jq -ce '[.data.result[] | {labels: .metric, time: .value[0], value: .value[1]}] | select('"$2"')'
}
cp_targets() {
  curl -fsS --max-time 4 "http://$cp/discovery/storage-nodes$1" |
    jq -ce 'select('"$2"')'
}
cp_http_status() {
  [[ $(curl -sS --max-time 4 -o /dev/null -w '%{http_code}' "http://$cp/discovery/storage-nodes$1") == "$2" ]]
}
rpc() { ORACACHE_ENDPOINT="$gateway" "$tmp/oracache" --timeout=4s "$@"; }
membership() {
  local action=$1
  shift
  rpc membership "$action" --group-id=region-a/shard-0/poc-1 --region=region-a --partition-id=0 "$@"
}
ready_get() {
  rpc get discovery:key | jq -ce 'select(.status == "GET_STATUS_HIT") | {status, servedBy}'
}
prom_process() { docker inspect --format '{{.Id}} {{.State.StartedAt}}' "$(compose ps -q prometheus)"; }
discovery_failures() {
  curl -fsS --max-time 4 "http://$prom/metrics" | awk '/^prometheus_sd_http_failures_total / {print $2; found=1} END {if (!found) exit 1}'
}
new_discovery_failure() { [[ $(discovery_failures) -gt $1 ]]; }

printf '\n## CLI, Gateway, Raft, CP, Prometheus and Grafana\n'
compose up -d --no-build >&2
gateway=$(compose port api-gateway 28080)
prom=$(compose port prometheus 9090)
grafana=$(compose port grafana 3000)
cp=$(compose port cp-api 9464)
process=$(prom_process)
config=$(await prom_json status/config | jq -er '.data.yaml')
await targets 'length == 3 and all(.[]; .health == "up")'
dashboard=$(await curl -fsS --max-time 4 "http://$grafana/api/dashboards/uid/oracache-real-cluster")
jq -e '.meta.provisioned and any(.dashboard.panels[]; .id == 33 and .fieldConfig.defaults.mappings[0].options["3"].text == "Learner")' <<<"$dashboard" >/dev/null
matrix=$(panel 33 region-a)
fresh=$(panel 34 region-a)
await grafana_query "$fresh" 'length == 1 and .[0].value == "1"' >/dev/null
rpc put discovery:key retained --expected-rev=0 | jq -ce 'select(.status == "PUT_STATUS_OK") | {status, servedBy}'

compose up -d --no-deps --no-build node-4 >&2
node4=$(compose port node-4 28191)
await curl -fsS -o /dev/null --max-time 4 "http://$node4/metrics" >/dev/null
await cp_targets '?region=region-a' 'length == 3 and all(.[]; .labels.node_id != "node-4")'
await targets 'length == 3 and all(.[]; .node != "node-4")'
await grafana_query "$matrix" 'length == 3 and all(.[]; .labels.node_id != "node-4")' >/dev/null
membership add-learner --operation-id=add-4 --peer-id=4 --node-id=node-4   --address=node-4:28091 --raft-address=http://node-4:28191/raft |
  jq -ce '.membership.members[] | select(.peer.nodeId == "node-4" and .learner)'
await targets 'length == 4 and any(.[]; .node == "node-4" and .url == "http://node-4:28191/metrics" and .health == "up")'
learner=$(await grafana_query "$matrix" 'length == 4 and any(.[]; .labels.node_id == "node-4" and .labels.region == "region-a" and .labels.partition == "0" and .value == "3")')
jq -ce '.[] | select(.labels.node_id == "node-4")' <<<"$learner"
learner_time=$(jq -er '.[] | select(.labels.node_id == "node-4") | .time' <<<"$learner")

await membership promote --operation-id=promote-4 --peer-id=4 |
  jq -ce '.membership.members[] | select(.peer.nodeId == "node-4" and .voter and (.learner | not))'
await grafana_query "$matrix" 'length == 4 and any(.[]; .labels.node_id == "node-4" and (.value == "1" or .value == "2"))'
leader=$(await ready_get | jq -er '.servedBy.instance')
compose pause "$leader" >&2
await grafana_query "$matrix" "length == 4 and any(.[]; .labels.node_id == \"$leader\" and .value == \"-1\") and any(.[]; .labels.node_id != \"$leader\" and .value == \"2\")"
await ready_get | jq -ce --arg previous "$leader" 'select(.servedBy.instance != $previous)'
compose unpause "$leader" >&2
await targets 'length == 4 and all(.[]; .health == "up")' >/dev/null

compose pause node-4 >&2
await prom_query 'up{job="storage-node",node_id="node-4"}' 'length == 1 and .[0].value == "0"'
await grafana_query "$matrix" 'length == 4 and any(.[]; .labels.node_id == "node-4" and .value == "-1")'
compose pause node-1 >&2
await grafana_query "$fresh" 'length == 1 and .[0].value == "0"'
await cp_targets '?region=region-a' 'length == 4'
await targets 'length == 4 and ([.[] | select(.health == "down")] | length) == 2'
if result=$(rpc put discovery:key without-quorum 2>&1); then
  printf 'PUT unexpectedly acknowledged without quorum: %s\n' "$result" >&2
  exit 1
fi
printf 'PUT without quorum => %s\n' "$result"
[[ "$result" == *"code = DeadlineExceeded"* || "$result" == *"code = Unavailable"* ]]
compose unpause node-1 node-4 >&2
await grafana_query "$fresh" 'length == 1 and .[0].value == "1"' >/dev/null
await ready_get

current=$(await membership get)
old_id=$(jq -er '.leaderId as $leader | [.members[].peer.id | select(. != $leader and . != "4")][0]' <<<"$current")
await membership remove --operation-id=remove-bootstrap --peer-id="$old_id" |
  jq -ce --arg old "$old_id" '.membership | select((.joint | not) and (.removedIds | index($old) != null)) | {removedIds}'
await cp_targets '?region=region-a' "length == 3 and all(.[]; .labels.node_id != \"node-$old_id\")"
await targets "length == 3 and all(.[]; .node != \"node-$old_id\")"
await grafana_query "$matrix" "length == 3 and all(.[]; .labels.node_id != \"node-$old_id\")"

compose stop cp-api >&2
await grafana_query "$fresh" 'length == 1 and .[0].value == "-1"'
await targets "length == 3 and all(.[]; .node != \"node-$old_id\")" >/dev/null
# Freeze Raft before restarting CP so no heartbeat can hide a bootstrap regression.
compose pause node-1 node-2 node-3 node-4 >&2
compose start cp-api >&2
cp=$(compose port cp-api 9464)
await cp_targets '?region=region-a' "length == 3 and all(.[]; .labels.node_id != \"node-$old_id\")"
await grafana_query "$fresh" 'length == 1 and .[0].value == "0"'
await targets "length == 3 and all(.[]; .node != \"node-$old_id\")" >/dev/null
compose unpause node-1 node-2 node-3 >&2
await grafana_query "$fresh" 'length == 1 and .[0].value == "1"' >/dev/null
compose unpause node-4 >&2
compose stop node-4 >&2
await grafana_query "$matrix" 'length == 3 and any(.[]; .labels.node_id == "node-4" and .value == "-1")'
await membership remove --operation-id=remove-4 --peer-id=4 |
  jq -ce '.membership | select((.joint | not) and (.removedIds | index("4") != null)) | {removedIds}'
await targets 'length == 2 and all(.[]; .node != "node-4")'
await grafana_query "$matrix" 'length == 2 and all(.[]; .labels.node_id != "node-4")'
curl -fsSG --max-time 4 "http://$prom/api/v1/query" --data-urlencode "query=$matrix" --data-urlencode "time=$learner_time" |
  jq -ce '.data.result[] | select(.metric.node_id == "node-4" and .value[1] == "3")'
[[ $(prom_process) == "$process" ]]
[[ $(prom_json status/config | jq -er '.data.yaml') == "$config" ]]
printf 'Prometheus process and configuration unchanged\n'
compose --profile extra-replica down --volumes >&2

printf '\n## Two regions, staged startup and region filtering\n'
project="$run-regions"
compose_files=(-f compose.replication.yaml -f compose.replication-observability.yaml -f "$tmp/regions.yaml")
cat > "$tmp/regions.yaml" <<'YAML'
services:
  cp-api:
    ports: ["127.0.0.1::9464"]
YAML
compose up -d --no-build cp-api a-1 a-2 a-3 prometheus grafana >&2
prom=$(compose port prometheus 9090)
grafana=$(compose port grafana 3000)
cp=$(compose port cp-api 9464)
process=$(prom_process)
await cp_targets '?region=region-b' 'length == 3 and all(.[]; .labels.region == "region-b")'
await targets 'length == 6 and all(.[]; if .region == "region-a" then .health == "up" else .health == "down" end)'
await prom_query 'oracache_cp_raft_membership_fresh{region="region-a"}' 'length == 1 and .[0].value == "1"' >/dev/null

compose restart cp-api >&2
cp=$(compose port cp-api 9464)
await cp_http_status '?region=region-b' 503
await cp_http_status '' 503
failures=$(await discovery_failures)
await new_discovery_failure "$failures" >/dev/null
printf 'Region B and unfiltered discovery returned HTTP 503; Prometheus retained targets\n'
await cp_targets '?region=region-a' 'length == 3 and all(.[]; .labels.region == "region-a")'
await targets 'length == 6 and all(.[]; if .region == "region-a" then .health == "up" else .health == "down" end)' >/dev/null

compose up -d --no-build b-1 b-2 b-3 >&2
await cp_targets '?region=region-b' 'length == 3 and all(.[]; .labels.region == "region-b")'
await cp_targets '' 'length == 6 and (group_by(.labels.region) | map(length)) == [3,3]'
await targets 'length == 6 and all(.[]; .health == "up")'
dashboard=$(await curl -fsS --max-time 4 "http://$grafana/api/dashboards/uid/oracache-real-cluster")
for region in region-a region-b; do
  await grafana_query "$(panel 33 "$region")" "length == 3 and all(.[]; .labels.region == \"$region\")"
done
[[ $(prom_process) == "$process" ]]
