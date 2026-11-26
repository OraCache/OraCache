#!/usr/bin/env python3
"""Export Go pprof's interactive flame view as a local HTML file."""
import argparse
from pathlib import Path
import subprocess
import time
import urllib.request


def export(profile):
    # Empty port asks pprof to pick a free loopback port.
    proc = subprocess.Popen(["go", "tool", "pprof", "-no_browser", "-http=127.0.0.1:", profile],
                            stderr=subprocess.PIPE, text=True, start_new_session=True)
    try:
        for line in proc.stderr:
            if "Serving web UI on " in line:
                url = line.split("Serving web UI on ", 1)[1].strip() + "/ui/flamegraph"
                break
        else:
            raise RuntimeError("pprof failed to start")
        for i in range(50):
            try:
                with urllib.request.urlopen(url, timeout=10) as page:
                    html = page.read()
                if b"stackViewer(" not in html:
                    raise RuntimeError("pprof did not return a flame graph")
                profile.with_suffix(".html").write_bytes(html)
                return
            except OSError:
                if i == 49:
                    raise
                time.sleep(.05)
    finally:
        # go tool runs the pprof executable as a child process.
        import os
        import signal
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGTERM)
        proc.wait()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("profiles", nargs="+", type=Path)
    for profile in p.parse_args().profiles:
        export(profile)
