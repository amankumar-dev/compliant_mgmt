
import subprocess
import sys
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent

STEPS = [
    ("Bronze ingestion", "scripts.bronze"),
    ("Silver transformation", "scripts.silver"),
    ("Silver validation", "scripts.silver_validate"),
    ("Gold view refresh", "scripts.gold_refresh"),
    ("Gold validation", "scripts.gold_validate"),
    ("Excel report generation", "scripts.gold_report"),
]


def main():
    started_at = datetime.now()
    print("=" * 60)
    print("COMPLIANCE DATA PIPELINE")
    print(f"Started: {started_at:%Y-%m-%d %H:%M:%S}")
    print("=" * 60)

    for step_name, module_name in STEPS:
        print(f"\n{'=' * 60}")
        print(f"RUNNING: {step_name}")
        print("=" * 60)

        result = subprocess.run(
            [sys.executable, "-m", module_name],
            cwd=PROJECT_ROOT,
            check=False,
        )

        if result.returncode != 0:
            print(f"\nPIPELINE STOPPED: {step_name} failed.")
            print(f"Exit code: {result.returncode}")
            sys.exit(result.returncode)

        print(f"SUCCESS: {step_name}")

    finished_at = datetime.now()

    print("\n" + "=" * 60)
    print("PIPELINE COMPLETED SUCCESSFULLY")
    print(f"Started:  {started_at:%Y-%m-%d %H:%M:%S}")
    print(f"Finished: {finished_at:%Y-%m-%d %H:%M:%S}")
    print(f"Duration: {finished_at - started_at}")
    print(f"Report:   reports/compliance_gold_report.xlsx")
    print("=" * 60)


if __name__ == "__main__":
    main()
