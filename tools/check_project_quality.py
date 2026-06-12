#!/usr/bin/env python3
"""Run the local quality gate shared with CI."""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYTHON_TARGETS = ("tg_harvest", "tests", "scripts", "tools")


@dataclass(frozen=True)
class QualityStep:
    name: str
    command: tuple[str, ...]


def build_steps(*, include_tests: bool) -> list[QualityStep]:
    steps = [
        QualityStep(
            name="Check change hygiene",
            command=(
                sys.executable,
                "tools/change_inventory.py",
                "--check",
                "--quiet",
            ),
        ),
        QualityStep(
            name="Compile Python sources",
            command=(sys.executable, "-m", "compileall", "-q", *PYTHON_TARGETS),
        ),
        QualityStep(
            name="Lint Python sources",
            command=(sys.executable, "-m", "ruff", "check", *PYTHON_TARGETS),
        ),
        QualityStep(
            name="Check static JavaScript syntax",
            command=(sys.executable, "tools/check_static_js.py"),
        ),
    ]
    if include_tests:
        steps.append(
            QualityStep(
                name="Run tests",
                command=(sys.executable, "-m", "pytest"),
            )
        )
    steps.append(
        QualityStep(
            name="Smoke check app startup",
            command=(sys.executable, "tools/smoke_check_app.py"),
        )
    )
    return steps


def run_step(step: QualityStep) -> int:
    print(f"\n==> {step.name}")
    started_at = time.monotonic()
    result = subprocess.run(step.command, cwd=ROOT, check=False)
    elapsed = time.monotonic() - started_at
    if result.returncode == 0:
        print(f"[OK] {step.name} ({elapsed:.1f}s)")
        return 0
    print(f"[FAIL] {step.name} exited with {result.returncode} ({elapsed:.1f}s)")
    return result.returncode


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the project quality gate used by CI."
    )
    parser.add_argument(
        "--skip-tests",
        action="store_true",
        help="Skip pytest for a faster local iteration check.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    total_started_at = time.monotonic()
    for step in build_steps(include_tests=not args.skip_tests):
        return_code = run_step(step)
        if return_code != 0:
            return return_code

    elapsed = time.monotonic() - total_started_at
    print(f"\n[OK] Project quality gate passed ({elapsed:.1f}s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
