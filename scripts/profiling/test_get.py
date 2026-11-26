"""Live tests. Run sequentially with the profiling lab's loopback ports free."""
import collections
import csv
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


class GETIntegrationTest(unittest.TestCase):
    def experiment(self, *flags, op="get", success=True):
        process = subprocess.run(
            [sys.executable, "scripts/profiling/run.py", op, "1000", "--seconds", "4", *flags],
            cwd=ROOT, capture_output=True, text=True, timeout=180,
        )
        self.assertEqual(process.returncode == 0, success, process.stderr)
        folder = Path(process.stdout.splitlines()[0])
        self.assertEqual(folder.parent, ROOT / ".reports/profiling")
        return folder

    def test_real_raft_groups(self):
        for nodes, phase in [(1, "profile"), (3, "trace")]:
            with self.subTest(nodes=nodes, phase=phase):
                folder = self.experiment("--keys", "2048", "--nodes", str(nodes), "--phase", phase)
                summary = (folder / "summary.txt").read_text()
                self.assertIn("measured_distinct=2048", summary)
                self.assertIn("warmup_calls=2048", summary)
                self.assertIn("cache_miss=0 other_errors=0", summary)
                with (folder / "latency.csv").open() as stream:
                    rows = list(csv.DictReader(stream))
                self.assertTrue(all(r["operation"] == "get" and r["success"] == "true" for r in rows))
                self.assertEqual(int(rows[0]["request_id"]), 2049)
                counts = collections.Counter(int(r["key_index"]) for r in rows)
                self.assertEqual(set(counts), set(range(2048)))
                self.assertLessEqual(max(counts.values()) - min(counts.values()), 1)
                for i in range(1, nodes + 1):
                    status = json.loads((folder / f"node-{i}.after-warmup.json").read_text())
                    self.assertTrue(status["Running"])
                    self.assertGreaterEqual(status["SnapshotIndex"], 2000)
                stages_file, = folder.glob("node-*.stages.json")
                stages = json.loads(stages_file.read_text())
                self.assertEqual(stages["grpc.handler"]["count"], len(rows))
                self.assertEqual(stages["raft.read_barrier"]["count"], len(rows))
                self.assertNotIn("raft.propose", stages)
                self.assertNotIn("raft.apply", stages)
                artifact = "*.cpu.pprof" if phase == "profile" else "*.trace.out"
                self.assertTrue(any(f.stat().st_size for f in folder.glob(artifact)))

    def test_legacy_defaults_and_replay(self):
        legacy = self.experiment()
        self.assertIn("keys=1 measured_distinct=1", (legacy / "summary.txt").read_text())
        self.assertIn("warmup_calls=100", (legacy / "summary.txt").read_text())
        put = self.experiment(op="put")
        self.assertEqual(json.loads((put / "metadata.json").read_text())["keys"], 32)
        self.assertIn("errors=0", (put / "summary.txt").read_text())
        with tempfile.TemporaryDirectory() as tmp:
            sequence = Path(tmp) / "indices"
            sequence.write_text("2\n0\n2\n1\n")
            folder = self.experiment("--keys", "3", "--key-sequence", str(sequence))
            self.assertEqual((folder / "key-sequence.txt").read_bytes(), sequence.read_bytes())
        with (folder / "latency.csv").open() as stream:
            indices = [int(row["key_index"]) for row in csv.DictReader(stream)]
        self.assertGreaterEqual(len(indices), 4)
        self.assertTrue(all(index == (2, 0, 2, 1)[i % 4] for i, index in enumerate(indices)))

    def test_invalid_replay_aborts_before_measurement(self):
        with tempfile.TemporaryDirectory() as tmp:
            sequence = Path(tmp) / "sequence.txt"
            sequence.write_text("2048\n")
            folder = self.experiment("--keys", "2048", "--key-sequence", str(sequence), success=False)
        self.assertIn("sequence line 1", (folder / "summary.txt").read_text())
        with (folder / "latency.csv").open() as stream:
            self.assertEqual(list(csv.DictReader(stream)), [])


if __name__ == "__main__":
    unittest.main()
