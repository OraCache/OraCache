#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
work=$(mktemp -d "$PWD/.smoke-XXXXXX")
pid=
cleanup() {
  if [[ -n "$pid" ]]; then kill -TERM "$pid" 2>/dev/null || true; wait "$pid" || true; fi
  rm -rf -- "$work"
}
trap cleanup EXIT
mkdir -p bin
go build -o bin/oracache ./cmd/oracache
go build -o bin/oracache-server ./cmd/oracache-server
bin/oracache-server --listen 127.0.0.1:0 >"$work/server.log" 2>&1 &
pid=$!
python3 - "$work/server.log" <<'PY'
import base64, json, os, pathlib, subprocess, sys, time
log = pathlib.Path(sys.argv[1])
endpoint = None
for _ in range(100):
    text = log.read_text()
    for line in text.splitlines():
        if line.startswith('listening '):
            endpoint = line.split()[1]
    if endpoint:
        os.environ['ORACACHE_ENDPOINT'] = endpoint
        p = subprocess.run(['bin/oracache', 'get', '--timeout=200ms', 'smoke'], capture_output=True, text=True, timeout=2)
        if p.returncode == 0:
            break
    time.sleep(.05)
else:
    raise SystemExit('server not ready: ' + log.read_text())

def cli(*args, data=None):
    p = subprocess.run(['bin/oracache', *args], input=data, capture_output=True, timeout=10)
    assert p.returncode == 0, (args, p.stderr)
    result = json.loads(p.stdout)
    print(' '.join(args), result)
    return result

assert cli('get', 'smoke')['status'] == 'GET_STATUS_MISS'
payload = b'hello\x00cache\xff'
assert cli('put', '--expected-rev=0', 'smoke', data=payload)['rev'] == '1'
hit = cli('get', 'smoke')
assert hit['status'] == 'GET_STATUS_HIT' and hit['rev'] == '1'
assert base64.b64decode(hit['data']) == payload
assert 'expiresAt' not in hit and 'hardExpiresAt' not in hit
conflict = cli('put', '--expected-rev=0', 'smoke', data=b'stale')
assert conflict['status'] == 'PUT_STATUS_CONFLICT' and conflict['rev'] == '1'
assert cli('put', '--expected-rev=1', 'smoke', data=b'new')['rev'] == '2'
assert cli('get', '--rev=1', 'smoke')['data'] == hit['data']
assert cli('delete', '--rev=2', 'smoke')['status'] == 'DELETE_STATUS_DELETED'
assert cli('get', 'smoke')['rev'] == '1'
assert cli('delete', 'smoke')['status'] == 'DELETE_STATUS_DELETED'
assert cli('get', 'smoke')['status'] == 'GET_STATUS_MISS'
assert cli('put', '--expected-rev=0', 'smoke', data=b'after delete')['rev'] == '3'
assert cli('delete', 'smoke')['status'] == 'DELETE_STATUS_DELETED'
assert cli('get', 'smoke')['status'] == 'GET_STATUS_MISS'
print('CLI smoke: PASS')
PY
