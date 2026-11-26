"""Offline aggregation of measured catalog RPCs. No work on the request path."""
import collections
import csv
import json
import math
from pathlib import Path


def write_results(folder: Path, elapsed_ns: int):
    with (folder / "products.csv").open() as stream:
        products = list(csv.DictReader(stream))
    sizes = collections.Counter(int(row["size"]) for row in products)
    counts = collections.defaultdict(collections.Counter)
    latencies = {"get": [], "put": []}
    errors = collections.Counter()
    attempts = collections.Counter()
    with (folder / "latency.csv").open() as stream:
        for row in csv.DictReader(stream):
            index = int(row["key_index"])
            absent = row["absent_key"] == "true"
            key = (index, absent)
            op = row["operation"]
            attempts[op] += 1
            counts[key][op] += 1
            if row["success"] == "true":
                counts[key][row["outcome"]] += 1
                latencies[op].append(float(row["latency_ms"]))
            else:
                counts[key]["errors"] += 1
                errors[row["error_kind"] + ": " + row["error"]] += 1
    fields = ["get", "put", "hit", "expected_miss", "updated", "errors"]
    totals = sum(counts.values(), collections.Counter())
    distinct = len(counts)
    with (folder / "key-frequency.csv").open("w", newline="") as out:
        writer = csv.writer(out)
        writer.writerow(["key", "index", "seed_bytes", "hot_rank_group", *fields])
        for i, product in enumerate(products):
            for absent in (False, True):
                key = ("missing:" if absent else "") + product["key"]
                writer.writerow([key, i, 0 if absent else product["size"], product["hot"],
                                 *(counts[i, absent][field] for field in fields)])
    completed_get = totals["hit"] + totals["expected_miss"]
    result = dict(seed_keys=len(products), seed_bytes=sum(k*v for k,v in sizes.items()),
                  sizes=sizes, distinct_requested_keys=distinct, elapsed_ns=elapsed_ns,
                  counts=dict(totals), error_reasons=dict(errors), operations={},
                  hit_rate=totals["hit"]/completed_get if completed_get else None,
                  miss_rate=totals["expected_miss"]/completed_get if completed_get else None)
    lines = []
    for op, values in latencies.items():
        values.sort()
        n = len(values)
        stats = dict(attempts=attempts[op], success=n, errors=attempts[op]-n,
                     actual_rps=attempts[op]*1e9/elapsed_ns, success_rps=n*1e9/elapsed_ns)
        if n:
            stats.update(mean_ms=sum(values)/n,max_ms=values[-1])
            for p in (50,95,99):
                stats[f"p{p}_ms"] = values[math.ceil(p*n/100)-1]
        result["operations"][op] = stats
        lines.append(op.upper()+" "+json.dumps(stats))
    (folder/"catalog.json").write_text(json.dumps(result, indent=2))
    return "\n".join(lines) + "\ncache_counts=" + json.dumps(dict(totals)) + "\n"
