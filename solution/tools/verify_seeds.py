"""Verify the pipeline on freshly generated datasets (any seeds, small or large).

For every seed it generates data with the supplied generator, then checks that
  1. incremental ingest (drop_1 then drop_2) == one-shot ingest (parent folder),
  2. both equal the independent oracle (solution/tests/oracle.py), file by file,
  3. re-ingesting everything changes nothing and regenerates identical reports.

Usage:  python solution/tools/verify_seeds.py 11 22 33 [--scale small|large]
Exit code 0 only if every seed passes.  Uses throw-away temp directories and test keys.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from solution.tests import oracle  # noqa: E402
from solution.tests.conftest import GENERATOR, SCHEMAS, Pipeline  # noqa: E402


def check(seed: int, scale: str) -> bool:
    t0 = time.time()
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        subprocess.run([sys.executable, str(GENERATOR), "--seed", str(seed), "--scale", scale,
                        "--out", str(tmp / "data")], check=True, capture_output=True)
        data = tmp / "data"
        inc, full = Pipeline(tmp / "inc.db"), Pipeline(tmp / "full.db")
        try:
            inc.ingest(data / "drop_1")
            inc.ingest(data / "drop_2")
            full.ingest(data)
            a, b = inc.report(tmp / "a"), full.report(tmp / "b")
            want = oracle.run([data / "drop_1", data / "drop_2"], SCHEMAS)
            again = inc.ingest(data)
            c = inc.report(tmp / "c")
        finally:
            inc.close()
            full.close()
    results = {
        "incremental == one-shot": a == b,
        "incremental == oracle": a == want,
        "re-ingest is a no-op": again.files_ingested == 0 and again.events_inserted == 0,
        "reports stable after re-ingest": c == a,
    }
    ok = all(results.values())
    leads = a["lead_report.csv"].count("\n") - 1
    print(f"seed {seed:>8} [{scale}] {'PASS' if ok else 'FAIL'} ({leads} leads, {time.time() - t0:.0f}s)"
          + "".join(f"\n    {'ok ' if v else 'BAD'} {k}" for k, v in results.items() if not v))
    return ok


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("seeds", type=int, nargs="+")
    ap.add_argument("--scale", choices=["small", "large"], default="small")
    args = ap.parse_args()
    results = [check(s, args.scale) for s in args.seeds]
    print(f"{sum(results)}/{len(results)} seeds passed")
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
