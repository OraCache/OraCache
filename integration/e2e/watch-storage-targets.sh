#!/usr/bin/env bash
# Export the Compose project's deployed storage endpoints to Prometheus file_sd.
set -euo pipefail
project=$1
directory=$2
tmp=$(mktemp "$directory/.storage-nodes.XXXXXXXX")
trap 'rm -f "$tmp"' EXIT
trap 'exit 0' INT TERM

while true; do
  if docker ps -a --filter "label=com.docker.compose.project=$project" \
    --filter 'label=io.oracache.metrics-address' --filter 'label=com.docker.compose.oneoff=False' \
    --format '{{.Label "com.docker.compose.service"}}{{"\t"}}{{.Label "io.oracache.metrics-address"}}' |
    jq -nRc '
      [inputs | split("\t") |
        if length == 2 and (.[0] | test("^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$")) and
          (.[1] | test("^[a-zA-Z0-9][a-zA-Z0-9.-]{0,252}:[0-9]{1,5}$")) and
          (.[1] | split(":")[1] | tonumber | . > 0 and . <= 65535)
        then {labels:{node_id:.[0]}, targets:[.[1]]}
        else error("invalid storage metrics service or address")
        end] | sort_by(.labels.node_id) | unique |
      if (group_by(.labels.node_id) | any(length != 1)) or (group_by(.targets) | any(length != 1))
      then error("storage metrics addresses and node IDs must be unique")
      else .
      end' > "$tmp"; then
    if ! cmp -s "$tmp" "$directory/storage-nodes.yml"; then
      chmod 644 "$tmp"
      mv -f "$tmp" "$directory/storage-nodes.yml"
    fi
  else
    printf 'Could not refresh storage metrics targets for %s\n' "$project" >&2
  fi
  sleep 2
done
