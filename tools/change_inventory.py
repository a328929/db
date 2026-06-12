#!/usr/bin/env python3
"""Summarize visible git changes by maintenance theme."""

from __future__ import annotations

import argparse
import fnmatch
import subprocess
import sys
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

THEME_ORDER = (
    "CI / Dev Tooling",
    "Security / Admin Auth",
    "Admin Jobs / Runtime Recovery",
    "Frontend Admin UX / JS Safety",
    "Search / Storage / Ingest Correctness",
    "Operator Tools / Telegram Scripts",
    "Documentation",
    "Tests",
    "Other",
)

EXACT_THEMES = {
    ".github/workflows/ci.yml": "CI / Dev Tooling",
    "pyproject.toml": "CI / Dev Tooling",
    "requirements-dev.txt": "CI / Dev Tooling",
    "tools/check_project_quality.py": "CI / Dev Tooling",
    "tools/check_static_js.py": "CI / Dev Tooling",
    "tools/change_inventory.py": "CI / Dev Tooling",
    "tests/test_change_inventory.py": "CI / Dev Tooling",
    "tg_harvest/app/factory.py": "Security / Admin Auth",
    "tg_harvest/web/auth.py": "Security / Admin Auth",
    "static/admin_manage_shared.js": "Security / Admin Auth",
    "tests/test_routes_channels.py": "Security / Admin Auth",
    "tests/test_routes_misc.py": "Security / Admin Auth",
    "tests/test_runtime_failures.py": "Admin Jobs / Runtime Recovery",
    "tests/test_frontend_safety.py": "Frontend Admin UX / JS Safety",
    "tg_harvest/config.py": "Security / Admin Auth",
    "README.md": "Documentation",
}

PREFIX_THEMES = (
    ("docs/", "Documentation"),
    (".github/", "CI / Dev Tooling"),
    ("tg_harvest/app/", "Security / Admin Auth"),
    ("tg_harvest/admin_jobs/", "Admin Jobs / Runtime Recovery"),
    ("tg_harvest/search/", "Search / Storage / Ingest Correctness"),
    ("tg_harvest/storage/", "Search / Storage / Ingest Correctness"),
    ("tg_harvest/ingest/", "Search / Storage / Ingest Correctness"),
    ("tg_harvest/domain/", "Search / Storage / Ingest Correctness"),
    ("tg_harvest/runtime/", "Search / Storage / Ingest Correctness"),
    ("tg_harvest/web/", "Security / Admin Auth"),
    ("static/", "Frontend Admin UX / JS Safety"),
    ("templates/", "Frontend Admin UX / JS Safety"),
    ("scripts/", "Operator Tools / Telegram Scripts"),
    ("tools/", "Operator Tools / Telegram Scripts"),
    ("tests/", "Tests"),
)

FORBIDDEN_PREFIXES = (
    ".runtime/",
    ".venv/",
    "venv/",
    "__pycache__/",
    ".pytest_cache/",
    ".ruff_cache/",
    "media/",
    "downloads/",
)

FORBIDDEN_PATTERNS = (
    ".env",
    "*.db",
    "*.db-*",
    "*.sqlite",
    "*.sqlite-*",
    "*.sqlite3",
    "*.sqlite3-*",
    "*.session",
    "*.session-*",
    "*.log",
)


@dataclass(frozen=True)
class StatusEntry:
    status: str
    path: str
    old_path: str | None = None


def _normalize_path(path: str) -> str:
    normalized_path = path.replace("\\", "/")
    while normalized_path.startswith("./"):
        normalized_path = normalized_path[2:]
    return normalized_path


def classify_path(path: str) -> str:
    normalized_path = _normalize_path(path)
    exact_theme = EXACT_THEMES.get(normalized_path)
    if exact_theme:
        return exact_theme
    for prefix, theme in PREFIX_THEMES:
        if normalized_path.startswith(prefix):
            return theme
    return "Other"


def is_forbidden_path(path: str) -> bool:
    normalized_path = _normalize_path(path)
    if any(normalized_path.startswith(prefix) for prefix in FORBIDDEN_PREFIXES):
        return True
    return any(fnmatch.fnmatch(normalized_path, pattern) for pattern in FORBIDDEN_PATTERNS)


def parse_status_porcelain_z(raw_status: bytes) -> list[StatusEntry]:
    chunks = raw_status.decode("utf-8", errors="replace").split("\0")
    entries: list[StatusEntry] = []
    index = 0
    while index < len(chunks):
        chunk = chunks[index]
        index += 1
        if not chunk:
            continue
        if len(chunk) < 4:
            entries.append(StatusEntry(status="??", path=chunk))
            continue

        status = chunk[:2]
        path = chunk[3:]
        old_path = None
        if "R" in status or "C" in status:
            if index < len(chunks) and chunks[index]:
                old_path = chunks[index]
            index += 1
        entries.append(StatusEntry(status=status, path=path, old_path=old_path))
    return entries


def collect_git_status() -> list[StatusEntry]:
    result = subprocess.run(
        ["git", "status", "--porcelain=v1", "-z", "-uall"],
        cwd=ROOT,
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(stderr or "git status failed")
    return parse_status_porcelain_z(result.stdout)


def find_forbidden_entries(entries: Iterable[StatusEntry]) -> list[StatusEntry]:
    return [entry for entry in entries if is_forbidden_path(entry.path)]


def _display_status(status: str) -> str:
    compact_status = status.strip()
    return compact_status or "M"


def _format_entry(entry: StatusEntry) -> str:
    rendered = f"- {_display_status(entry.status)} {entry.path}"
    if entry.old_path:
        rendered += f" (from {entry.old_path})"
    return rendered


def format_inventory(entries: Iterable[StatusEntry]) -> str:
    grouped_entries: dict[str, list[StatusEntry]] = defaultdict(list)
    entry_count = 0
    for entry in sorted(entries, key=lambda item: item.path):
        grouped_entries[classify_path(entry.path)].append(entry)
        entry_count += 1

    lines = [
        "Change Inventory",
        "================",
        "",
        f"Total changed paths: {entry_count}",
    ]
    if entry_count == 0:
        lines.append("No visible git changes.")
        return "\n".join(lines)

    for theme in THEME_ORDER:
        theme_entries = grouped_entries.get(theme)
        if not theme_entries:
            continue
        lines.extend(("", f"## {theme} ({len(theme_entries)})"))
        lines.extend(_format_entry(entry) for entry in theme_entries)
    return "\n".join(lines)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Group visible git changes by maintenance theme."
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Fail if visible changes include runtime data, credentials, or cache files.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Only print check results; useful from CI and aggregate quality gates.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        entries = collect_git_status()
    except RuntimeError as exc:
        print(f"[FAIL] {exc}", file=sys.stderr)
        return 1

    if not args.quiet:
        print(format_inventory(entries))

    if not args.check:
        return 0

    forbidden_entries = find_forbidden_entries(entries)
    if forbidden_entries:
        print("[FAIL] Forbidden runtime or local-only paths are visible in git:")
        for entry in forbidden_entries:
            print(_format_entry(entry))
        return 1

    if args.quiet:
        print(f"[OK] Change hygiene passed ({len(entries)} visible path(s))")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
