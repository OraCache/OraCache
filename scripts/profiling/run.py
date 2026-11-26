#!/usr/bin/env python3
"""Run one isolated, warmed OraCache experiment on loopback."""
import argparse
from catalog_metrics import write_results
import collections
import csv
import math
import concurrent.futures
import contextlib
import datetime
import json
import shutil
import os
from pathlib import Path
import platform
import subprocess as sp
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
BIN = ROOT / "bin/profiling"
COMPOSE = ["docker", "compose", "-f", str(ROOT / "compose.profiling.yaml")]
LOCAL = "127.0.0.1"


def addr(port):
    return f"{LOCAL}:{port}"


def output(*cmd):
    return sp.check_output(cmd, text=True).strip()


def stop(proc):
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(5)
        except sp.TimeoutExpired:
            proc.kill()
            proc.wait()


def fetch(url, post=False):
    with urllib.request.urlopen(urllib.request.Request(url, data=b"" if post else None), timeout=180) as resp:
        return resp.read()


def build(docker=False):
    BIN.mkdir(parents=True, exist_ok=True)
    sp.run(["go", "build", "-o", str(BIN) + "/", "./cmd/cp-api", "./cmd/storage-node", "./cmd/api-gateway"], check=True)
    sp.run(["go", "build", "-o", BIN / "bench", "oracache_bench.go"], check=True)
    if docker:
        arch = output("docker","version","--format","{{.Server.Arch}}")
        dest = BIN / "linux"
        dest.mkdir(exist_ok=True)
        sp.run(["go","build","-o",str(dest)+"/", "./cmd/cp-api", "./cmd/storage-node",
                "./cmd/api-gateway","./scripts/profiling/probe"], check=True,
               env=dict(os.environ, GOOS="linux", GOARCH=arch, CGO_ENABLED="0"))


def native(args, folder, stack):
    cfg = json.loads((ROOT / "scripts/profiling/one-node.json").read_text())
    peers = []
    for i in range(1, args.nodes + 1):
        name = f"node-{i}"
        client = addr(28790+i)
        peers.append(f"--peer={i},{name},{client},http://{addr(28890+i)}/raft")
        if i > 1:
            cfg["nodes"].append(dict(id=name, region="region-a", address=client))
            cfg["partitions"][0]["replica_node_ids"].append(name)
    config = folder / "config.json"
    config.write_text(json.dumps(cfg))
    def start(label, binary, flags, diag=None):
        log = stack.enter_context((folder / (label+".log")).open("wb"))
        env = dict(os.environ, ORACACHE_PPROF_ADDR=diag or "")
        proc = sp.Popen([BIN / binary, *flags], env=env, stdout=log, stderr=log)
        stack.callback(stop, proc)
        return proc
    cp = addr(28781)
    procs = []
    if not args.standalone:
        procs.append(start("cp-api", "cp-api", ["--config="+str(config), "--listen="+cp, "--metrics-listen=none"]))
    for i in range(1, args.nodes+1):
        procs.append(start(f"node-{i}", "storage-node", [f"--node-id=node-{i}", f"--peer-id={i}",
            "--grpc-listen="+addr(28790+i), "--raft-listen="+addr(28890+i), "--cp-addr="+("" if args.standalone else cp),
            "--heartbeat-interval=500ms", "--read-mode="+args.read, *peers],
            addr(28990+i) if args.phase != "baseline" else None))
    if args.standalone:
        return procs
    procs.append(start("gateway", "api-gateway", ["--node-id=gw", "--region=region-a", "--cp-addr="+cp,
        "--listen="+addr(28780), "--status-listen="+addr(28766), "--poll-interval=500ms"],
        addr(28980) if args.phase != "baseline" and args.via == "gateway" else None))
    return procs


def docker(args, folder, stack):
    # Unique Compose project, so cleanup only targets containers of this run.
    compose = [*COMPOSE, "-p", "oracache-perf-"+str(os.getpid())]
    env = dict(os.environ,
        PROFILE_NODE_ADDR=addr(28991) if args.phase != "baseline" else "",
        PROFILE_GATEWAY_ADDR=addr(28980) if args.phase != "baseline" and args.via == "gateway" else "")
    def cleanup():
        with (folder/"compose.log").open("wb") as log:
            sp.run([*compose, "logs", "--no-color"], stdout=log, env=env)
        sp.run([*compose, "down"], env=env, check=True)
    stack.callback(cleanup)
    sp.run([*compose, "up", "-d", "--build"], env=env, check=True)
    (folder/"docker.txt").write_text(output("docker", "version")+"\n"+output("docker","info","--format",
        "CPUs={{.NCPU}} RAM={{.MemTotal}} OS={{.OperatingSystem}}"))
    def retrieve(url, post=False):
        return sp.check_output([*compose, "exec", "-T", "cp-api", "/probe",
                                "POST" if post else "GET", url], env=env, timeout=185)
    return retrieve


def leader(procs, standalone):
    for _ in range(160):
        if any(p.poll() is not None for p in procs):
            raise RuntimeError("server exited during startup, inspect the run logs")
        try:
            if standalone:
                state = json.loads(fetch("http://"+addr(28891)+"/status"))
                if state["Ready"] and state["LeaderID"]:
                    return state["LeaderID"]
                time.sleep(.25)
                continue
            routes = json.loads(fetch("http://"+addr(28766)+"/routes"))
            node = (routes.get("write_routes") or {}).get("0", {}).get("NodeID")
            if node:
                return int(node.split("-")[1])
        except (KeyError, OSError, ValueError):
            pass
        time.sleep(.25)
    raise RuntimeError("leader route did not become ready")


def tops(folder):
    for profile in sorted(folder.glob("*.pprof")):
        metric = profile.name.split(".")[-2]
        index = {"allocs":"alloc_space", "heap":"inuse_space", "block":"delay", "mutex":"delay", "cpu":"cpu"}[metric]
        for sort in ("flat", "cum"):
            with profile.with_suffix("."+sort+".txt").open("w") as report:
                sp.run(["go", "tool", "pprof", "-top", "-sample_index="+index, "-"+sort, "-nodecount=25", profile],
                       stdout=report, check=True)


def run(args, folder, stack):
    procs, retrieve = [], fetch
    if args.docker:
        retrieve = docker(args, folder, stack)
    else:
        procs = native(args, folder, stack)
    peer = leader(procs, args.standalone)
    endpoint = addr(28780 if args.via == "gateway" else 28790+peer)
    diag = {f"node-{peer}": addr(28990+peer)}
    if args.via == "gateway":
        diag["gateway"] = addr(28980)
    key_flags = ["-put-keys=" + str(args.keys)]
    if args.op in ("get", "catalog"):
        key_flags = ["-get-keys=" + str(args.keys), "-seed=" + str(args.seed)]
        if args.key_sequence:
            replay = folder / "key-sequence.txt"
            shutil.copyfile(args.key_sequence, replay)
            key_flags.append("-key-sequence=" + str(replay))
    if args.op == "catalog":
        for flag in ("distribution", "get_percent", "miss_rate", "hot_traffic", "hot_keys", "zipf_s", "sizes"):
            key_flags.append("-" + flag.replace("_", "-") + "=" + str(getattr(args, flag)))

        def node_states(label, ready=False):
            deadline = time.monotonic() + 10
            while True:
                states = []
                for i in range(1, args.nodes+1):
                    try:
                        state = json.loads(retrieve("http://" + addr(28890+i) + "/status"))
                    except (OSError, ValueError, sp.SubprocessError) as err:
                        state = {"unavailable": str(err)}
                    (folder/f"node-{i}.{label}.json").write_text(json.dumps(state, indent=2))
                    states.append(state)
                if not ready:
                    return states
                watermark = states[peer-1].get("CommitIndex", 0)
                if all(s.get("Running") and s.get("Ready") and not s.get("LastError") and
                       s.get("LeaderID") == peer and s.get("AppliedIndex", -1) >= watermark for s in states):
                    return states
                if time.monotonic() >= deadline:
                    raise RuntimeError("Raft group not ready after warmup, see node status files")
                time.sleep(.1)
        stack.callback(node_states, "final")
    bench_log = stack.enter_context((folder / "bench.stderr").open("w"))
    bench = sp.Popen([BIN/"bench", "-endpoint="+endpoint, "-mode="+args.op, "-rps="+str(args.rps),
        "-seconds="+str(args.seconds), "-workers="+str(args.workers), "-bytes=10240", "-warmup=" + str(args.warmup),
        "-phase="+args.phase, "-csv="+str(folder/"latency.csv"), *key_flags,
        "-wait-for-start"], stdin=sp.PIPE, stdout=sp.PIPE, stderr=bench_log, text=True)
    stack.callback(stop, bench)
    if bench.stdout.readline().strip() != "READY":
        raise RuntimeError("seed/warmup failed: " + (folder / "bench.stderr").read_text().strip())
    if args.op == "catalog":
        node_states("after-warmup", ready=True)
    if args.op == "get" and args.keys > 1:
        for i in range(1, args.nodes + 1):
            status = json.loads(retrieve("http://" + addr(28890+i) + "/status"))
            (folder / f"node-{i}.after-warmup.json").write_text(json.dumps(status, indent=2))
            if not status["Running"] or status["LastError"]:
                raise RuntimeError(f"node-{i} failed during preparation: {status}")
    def capture(name, target, path, file):
        (folder/(name+"."+file)).write_bytes(retrieve("http://"+target+path))
    def process_snapshot(when):
        if args.op != "catalog" or args.phase == "baseline":
            return
        if procs:
            result = sp.run(["ps","-p",",".join(str(p.pid) for p in procs),
                             "-o","pid,time,rss,comm"], text=True, capture_output=True)
            (folder/f"process-{when}.txt").write_text(result.stdout + result.stderr)
        for name, target in diag.items():
            try:
                raw = retrieve("http://" + target + "/debug/pprof/goroutine?debug=1")
                (folder/f"{name}.goroutines-{when}.txt").write_bytes(raw.splitlines()[0] + b"\n")
            except (OSError, sp.SubprocessError) as err:
                (folder/f"{name}.goroutines-{when}.txt").write_text(str(err))
    if args.op == "catalog" and args.phase == "profile":
        # MemProfile can lag by two GC cycles. Flush samples from seed/checkpoints
        # before starting the alloc_space delta and reading the runtime counters.
        for target in diag.values():
            for _ in range(2):
                retrieve("http://" + target + "/debug/pprof/heap?gc=1")
    process_snapshot("before")
    with concurrent.futures.ThreadPoolExecutor() as pool:
        jobs = []
        periods = {}
        if args.phase != "baseline":
            for name, target in diag.items():
                retrieve("http://" + target + "/debug/stages/reset", True)
                capture(name, target, "/debug/runtime", "runtime-before.json")
            if args.phase == "trace":
                periods["trace"] = min(3, args.seconds - 2)
            else:
                periods = dict.fromkeys(("profile", "allocs", "mutex", "block"), args.seconds - 2)

        bench.stdin.write("\n")
        bench.stdin.flush()
        for name, target in diag.items():
            for metric, seconds in periods.items():
                file = {"profile": "cpu.pprof", "trace": "trace.out"}.get(metric, metric + ".pprof")
                path = f"/debug/pprof/{metric}?seconds={seconds}"
                jobs.append(pool.submit(capture, name, target, path, file))
        result, _ = bench.communicate(timeout=args.seconds + 20)
        if bench.returncode:
            raise RuntimeError("load generator failed: "+result)
        problems = []
        for job in jobs:
            try:
                job.result()
            except (OSError, sp.SubprocessError) as err:
                problems.append(str(err))
        process_snapshot("after")
        if args.phase != "baseline":
            for name, target in diag.items():
                for path, file in [("/debug/runtime", "runtime-after.json"), ("/debug/stages", "stages.json"),
                                   ("/debug/pprof/heap?gc=1", "heap.pprof")]:
                    try:
                        capture(name, target, path, file)
                    except (OSError, sp.SubprocessError) as err:
                        problems.append(str(err))
        if args.op == "catalog":
            if not all(s.get("Running") and not s.get("LastError") for s in node_states("after-load")):
                problems.append("Raft member unavailable after load")
        with (folder/"latency.csv").open() as source:
            failed = [r for r in csv.DictReader(source) if r["success"] == "false"]
        if args.op == "catalog":
            timing = next(line for line in result.splitlines() if line.startswith("elapsed_ns="))
            elapsed = int(timing.split()[0].split("=")[1])
            result += write_results(folder, elapsed)
        if failed:
            latencies = sorted(float(r["latency_ms"]) for r in failed)
            q = lambda pct: latencies[math.ceil(len(latencies)*pct)-1]
            result += "failed_latency_ms mean=%.3f p50=%.3f p95=%.3f p99=%.3f\n" % (
                sum(latencies)/len(latencies),q(.50),q(.95),q(.99))
            for error, count in collections.Counter(r["error"] for r in failed).most_common():
                result += f"{count} {error}\n"
        (folder/"summary.txt").write_text(result)
        print(result, end="", flush=True)
        if failed and not (args.op == "put" and args.keys == 0):
            problems.append("unexpected RPC errors, inspect summary.txt and server logs")
        if problems:
            raise RuntimeError("; ".join(problems))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("op", choices=["get", "put", "catalog"])
    p.add_argument("rps", type=int)
    p.add_argument("--phase", choices=["baseline","profile","trace"], default="baseline")
    p.add_argument("--read", choices=["linearizable","local"], default="linearizable")
    p.add_argument("--via", choices=["direct","gateway"], default="direct")
    p.add_argument("--nodes", type=int, choices=[1,3], default=1)
    p.add_argument("--seconds", type=int, default=15)
    p.add_argument("--workers", type=int, default=32)
    p.add_argument("--warmup", type=int, default=100, help="minimum warmup RPCs (GET always visits every key)")
    p.add_argument("--keys", type=int, help="working set: get 1, put 32 (0 for growing), catalog 2048 by default")
    p.add_argument("--seed", type=int, default=1, help="seed for the repeated GET permutation")
    p.add_argument("--key-sequence", help="replay zero-based GET key indices, one per line")
    p.add_argument("--standalone", action="store_true", help="omit CP and gateway for matched direct local/strong reads")
    p.add_argument("--docker", action="store_true", help="use the one-node profiling Compose lab")
    p.add_argument("--no-build", action="store_true", help="reuse previously built binaries")
    p.add_argument("--distribution", choices=["uniform", "hotspot", "zipf"], default="uniform")
    p.add_argument("--get-percent", type=float, default=100)
    p.add_argument("--miss-rate", type=float, default=0, help="percentage of catalog GETs for absent products")
    p.add_argument("--hot-traffic", type=float, default=80)
    p.add_argument("--hot-keys", type=float, default=20)
    p.add_argument("--zipf-s", type=float, default=1.1)
    p.add_argument("--sizes", default="10240:100", help="catalog size:integer-percent groups")
    args = p.parse_args()
    if args.op != "catalog" and any(getattr(args, k) != p.get_default(k) for k in
                                    ("distribution","get_percent","miss_rate","hot_traffic","hot_keys","zipf_s","sizes")):
        p.error("popularity, operation mix, misses and size options require the catalog workload")
    if args.keys is None:
        args.keys = {"get":1, "put":32, "catalog":2048}[args.op]
    if args.warmup < 0:
        p.error("--warmup must be nonnegative")
    if args.op != "put" and args.keys < 1:
        p.error("GET needs at least one key")
    if not -(2**63) <= args.seed < 2**63:
        p.error("--seed must fit in int64")
    if args.key_sequence:
        if args.op != "get":
            p.error("--key-sequence requires GET")
        args.key_sequence = str(Path(args.key_sequence).resolve())
    if args.seconds < 4 or args.workers < 1 or args.keys < 0 or not 1 <= args.rps <= 100000 or (args.docker and args.nodes != 1):
        p.error("need seconds >= 4, workers >= 1, keys >= 0, RPS 1..100000 and one node for Compose")
    if (args.standalone and (args.docker or args.via == "gateway")) or (args.read == "local" and not args.standalone):
        p.error("local reads require --standalone, which uses direct native storage without the Control Plane")
    if args.op == "catalog" and args.read != "linearizable":
        p.error("catalog revision checks require linearizable reads")
    os.chdir(ROOT)
    if not args.no_build:
        build(args.docker)
    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    folder = ROOT / ".reports/profiling" / f"{stamp}-{args.nodes}n-{'standalone' if args.standalone else args.via}-{args.read}-{args.op}-{args.rps}-{args.phase}"
    folder.mkdir(parents=True)
    print(folder, flush=True)
    metadata = dict(vars(args), commit=output("git","rev-parse","HEAD"), go=output("go","version"),
                    platform=platform.platform(), cpu_count=os.cpu_count(), build=output("go","version","-m",str(BIN/"bench")))
    metadata["python"] = platform.python_version()
    metadata["value_bytes"] = 10240 if args.op != "catalog" or args.sizes == "10240:100" else None
    metadata["cpu_arch"] = platform.machine()
    metadata["server_build"] = output("go", "version", "-m", str(BIN / ("linux/storage-node" if args.docker else "storage-node")))
    if args.op == "catalog":
        metadata["plan_version"] = "catalog-v1"
        metadata["pre_profile_gc_cycles"] = 2 if args.phase == "profile" else 0
        metadata["system"] = output("uname", "-smr")
        if platform.system() == "Darwin":
            hardware = sp.run(["sysctl", "-n", "machdep.cpu.brand_string", "hw.ncpu", "hw.memsize"],
                              capture_output=True, text=True)
            if hardware.returncode == 0:
                metadata["hardware"] = hardware.stdout.splitlines()
    (folder/"metadata.json").write_text(json.dumps(metadata, indent=2))
    try:
        with contextlib.ExitStack() as stack:
            run(args, folder, stack)
        tops(folder)
    except Exception as err:
        # A failed preparation has zero measured samples.
        if not (folder / "latency.csv").exists():
            with (folder / "latency.csv").open("x", newline="") as empty:
                fields = ["phase", "operation", "request_id", "latency_ms", "success", "error", "error_kind", "key_index"]
                if args.op == "catalog":
                    fields.extend(["absent_key","outcome","payload_bytes","revision","write_token"])
                csv.writer(empty).writerow(fields)
        with (folder / "summary.txt").open("a") as summary:
            summary.write(f"experiment_failed: {err}\n")
        raise


if __name__ == "__main__":
    main()
