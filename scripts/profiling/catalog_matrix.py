#!/usr/bin/env python3
"""Sequential catalog experiments. Full matrix requires an explicit --full."""
import argparse
import subprocess
import sys
from pathlib import Path

RUN = Path(__file__).with_name("run.py")
SHOP_SIZES = "256:60,1024:30,10240:10"
SCENARIOS = {
    "legacy-one": ["--keys", "1"],
    "uniform": [],
    "hotspot": ["--distribution", "hotspot"],
    "zipf": ["--distribution", "zipf"],
    "mixed98": ["--get-percent", "98"],
    "mixed90": ["--get-percent", "90"],
    "mixed70": ["--get-percent", "70"],
    "miss1": ["--miss-rate", "1"],
    "miss5": ["--miss-rate", "5"],
    "sizes": ["--sizes", SHOP_SIZES],
    "shop": ["--distribution", "hotspot", "--get-percent", "98",
             "--miss-rate", "1", "--sizes", SHOP_SIZES],
}
SMOKE = ["uniform", "hotspot", "zipf", "mixed98"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--scenarios", nargs="+", choices=SCENARIOS)
    parser.add_argument("--nodes", nargs="+", type=int, choices=[1, 3])
    parser.add_argument("--rates", nargs="+", type=int)
    parser.add_argument("--seconds", type=int)
    parser.add_argument("--repeats", type=int)
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--seed", type=int, default=1)
    args = parser.parse_args()

    repeats = args.repeats if args.repeats is not None else (3 if args.full else 1)
    seconds = args.seconds if args.seconds is not None else (30 if args.full else 6)
    if repeats < 1 or seconds < 4:
        parser.error("need repeats >= 1 and seconds >= 4")
    nodes = args.nodes or ([1, 3] if args.full else [1])
    rates = args.rates or ([1000, 3000, 6000, 10000] if args.full else [1000, 10000])
    scenarios = args.scenarios or (list(SCENARIOS) if args.full else SMOKE)
    built = False
    failures = []
    for rep in range(1, repeats + 1):
        for n in nodes:
            for rate in rates:
                for scenario in scenarios:
                    op = "get" if scenario == "legacy-one" else "catalog"
                    cmd = [sys.executable, str(RUN), op, str(rate),
                           "--nodes", str(n), "--workers", str(args.workers), "--seed", str(args.seed),
                           "--seconds", str(seconds), "--warmup", "2048", *SCENARIOS[scenario]]
                    if built:
                        cmd.append("--no-build")
                    print(f"repeat={rep} scenario={scenario} nodes={n} target={rate}", flush=True)
                    if subprocess.run(cmd).returncode:
                        failures.append((rep, scenario, n, rate))
                    else:
                        built = True
    if failures:
        sys.exit(f"failed experiments: {failures}")


if __name__ == "__main__":
    main()
