#!/bin/bash
# Each invocation starts a fresh group, seeds it and warms all GET keys.
set -eu
cd "$(dirname "$0")/../.."
first=true
for nodes in 1 3; do
  for rate in 1000 3000 6000 10000; do
    for keys in ${KEY_COUNTS:-1 2048}; do
      set -- get "$rate" --nodes "$nodes" --via direct --read linearizable \
        --keys "$keys" --warmup 2048 --seconds "${PERF_SECONDS:-6}" --seed "${SEED:-1}"
      if [ "$first" = true ]; then
        first=false
      else
        set -- "$@" --no-build
      fi
      python3 scripts/profiling/run.py "$@"
    done
  done
done
