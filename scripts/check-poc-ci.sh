#!/usr/bin/env bash
# Regression check: the PoC CI job must keep running the real smoke (with failover),
# use an isolated Compose project, collect diagnostics and always clean up.
set -euo pipefail
cd "$(dirname "$0")/.."
wf=.github/workflows/go.yml
fail() { echo "FAIL: $*" >&2; exit 1; }
has() { grep -qE -- "$1" <<<"$job" || fail "poc job: $2"; }

bash -n scripts/poc-smoke.sh
job=$(awk '/^  poc:/{f=1;next} f&&/^  [a-z]/{f=0} f' "$wf")
[ -n "$job" ] || fail "no poc job in $wf"

has 'COMPOSE_PROJECT_NAME: oracache-poc-ci-' "isolated project name missing"
has 'run: bash scripts/poc-smoke.sh$' "must run scripts/poc-smoke.sh"
! grep -q -- '--no-kill' <<<"$job" || fail "poc job must not pass --no-kill"
has 'go build -o bin/oracache \./cmd/oracache' "CLI build missing"
has 'file: Dockerfile\.poc' "image build missing"
has 'up -d --no-build' "must start without rebuilding"
has 'if: failure\(\)' "failure diagnostics missing"
has 'upload-artifact' "diagnostics upload missing"
has 'if: always\(\)' "always-cleanup missing"
has 'down --volumes --remove-orphans' "cleanup must run compose down"
! grep -qE 'docker (system|volume|image) prune|docker rm|docker kill|compose.* -p ' <<<"$job" || fail "poc job must only touch its own project"
echo "poc CI wiring ok"
