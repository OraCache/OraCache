import csv
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from catalog_metrics import write_results


class CatalogMetricsTest(unittest.TestCase):
    def test_separate_latency_rps_and_observed_keys(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            (folder / "products.csv").write_text(
                "index,key,size,hot\n0,product:0,256,true\n1,product:1,1024,false\n")
            (folder / "latency.csv").write_text(
                "key_index,absent_key,operation,success,outcome,latency_ms,error_kind,error\n"
                "0,false,get,true,hit,1,,\n0,true,get,true,expected_miss,3,,\n"
                "0,false,get,false,,99,cache_miss,unexpected MISS\n"
                "1,false,put,true,updated,20,,\n")
            write_results(folder, 2_000_000_000)
            data = json.loads((folder / "catalog.json").read_text())
            get, put = (data["operations"][op] for op in ("get", "put"))
            self.assertEqual((get["actual_rps"], get["success_rps"], get["errors"]), (1.5, 1, 1))
            self.assertEqual([get[k] for k in ("mean_ms", "p50_ms", "p95_ms", "p99_ms", "max_ms")],
                             [2, 1, 3, 3, 3])
            self.assertEqual((put["p99_ms"], put["success_rps"]), (20, .5))
            self.assertEqual(data["error_reasons"], {"cache_miss: unexpected MISS": 1})
            self.assertEqual((data["miss_rate"], data["distinct_requested_keys"], data["seed_bytes"]),
                             (.5, 3, 1280))
            with (folder / "key-frequency.csv").open() as stream:
                rows = {row["key"]: row for row in csv.DictReader(stream)}
            self.assertEqual(rows["missing:product:0"]["expected_miss"], "1")
            self.assertEqual(rows["product:1"]["updated"], "1")
            self.assertEqual(rows["missing:product:1"]["get"], "0")


class CatalogIntegrationTest(unittest.TestCase):
    def run_catalog(self, *flags):
        root = Path(__file__).resolve().parents[2]
        return subprocess.run([sys.executable, "scripts/profiling/run.py", "catalog", "1000",
                               "--seconds", "4", "--keys", "16", *flags],
                              cwd=root, capture_output=True, text=True, timeout=180)

    def test_mixed_hot_products_and_misses(self):
        result = self.run_catalog("--get-percent", "70", "--miss-rate", "5", "--distribution", "hotspot",
                                  "--sizes", "256:50,1024:50", "--phase", "profile")
        self.assertEqual(result.returncode, 0, result.stderr)
        folder = Path(result.stdout.splitlines()[0])
        data = json.loads((folder / "catalog.json").read_text())
        counts = data["counts"]
        self.assertFalse(data["error_reasons"])
        self.assertGreater(counts["expected_miss"], 0)
        self.assertGreater(counts["updated"], 0)
        self.assertEqual(counts["hit"] + counts["expected_miss"], counts["get"])
        self.assertEqual(counts["updated"], counts["put"])
        self.assertEqual(data["sizes"], {"256": 8, "1024": 8})
        stages = json.loads((folder / "node-1.stages.json").read_text())
        self.assertEqual(stages["raft.read_barrier"]["count"], counts["get"])
        self.assertEqual(stages["raft.commit_observed"]["count"], counts["put"])
        self.assertTrue((folder / "node-1.cpu.pprof").stat().st_size)
        with (folder / "key-frequency.csv").open() as stream:
            freq = list(csv.DictReader(stream))
        self.assertEqual(sum(int(row["updated"]) for row in freq), counts["put"])

    def test_oversize_keeps_evidence_before_measurement(self):
        result = self.run_catalog("--sizes", "65536:100")
        self.assertNotEqual(result.returncode, 0)
        folder = Path(result.stdout.splitlines()[0])
        self.assertIn("PUT_STATUS_TOO_LARGE", (folder / "summary.txt").read_text())
        self.assertTrue((folder / "node-1.final.json").exists())
        with (folder / "latency.csv").open() as stream:
            self.assertEqual(list(csv.DictReader(stream)), [])


if __name__ == "__main__":
    unittest.main()
