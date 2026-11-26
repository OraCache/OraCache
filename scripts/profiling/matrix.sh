#!/bin/bash
# Fresh processes per row. The historical growing-key run intentionally exercises a fatal limit.
set -eu
cd "$(dirname "$0")/../.."
seconds=${PERF_SECONDS:-8}
first_run=true
run() {
  if [ "$first_run" = true ]; then
    python3 scripts/profiling/run.py "$@"
    first_run=false
  else
    python3 scripts/profiling/run.py "$@" --no-build
  fi
}
for via in direct gateway; do
  for rps in 500 10000; do
    run get "$rps" --phase profile --via "$via" --seconds "$seconds"
  done
  run put 500 --phase profile --via "$via" --seconds "$seconds"
done
run put 500 --via gateway --keys 0 --seconds 15
for repetition in 1 2; do
  for rps in 500 10000; do
    for read in linearizable local; do
      run get "$rps" --standalone --read "$read" --seconds "$seconds"
    done
    for via in direct gateway; do
      run get "$rps" --via "$via" --seconds "$seconds"
    done
  done
  for rps in 50 200 500; do
    run put "$rps" --seconds "$seconds"
  done
  run put 500 --via gateway --seconds "$seconds"
done
for op in get put; do
  run "$op" 500 --phase trace --seconds "$seconds"
  run "$op" 500 --nodes 3 --seconds "$seconds"
done
