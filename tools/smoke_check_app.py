#!/usr/bin/env python3
"""Minimal startup smoke check for Flask route registration."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Iterable, Set

REQUIRED_ROUTES = {
    "/",
    "/admin/manage",
    "/api/meta",
    "/api/search",
    "/api/admin/chats",
}

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _collect_routes(app) -> Set[str]:
    return {rule.rule for rule in app.url_map.iter_rules()}


def _format_missing(items: Iterable[str]) -> str:
    return ", ".join(sorted(items))


def main() -> int:
    try:
        from app import create_app
    except Exception as exc:  # pragma: no cover - runtime smoke check path
        print(f"[FAIL] import create_app failed: {exc}")
        return 1

    try:
        flask_app = create_app()
    except Exception as exc:  # pragma: no cover - runtime smoke check path
        print(f"[FAIL] create_app() failed: {exc}")
        return 1

    routes = _collect_routes(flask_app)
    missing = REQUIRED_ROUTES - routes
    if missing:
        print(f"[FAIL] missing required route(s): {_format_missing(missing)}")
        return 1

    print("[OK] create_app import and app creation succeeded")
    print(f"[OK] required routes present: {_format_missing(REQUIRED_ROUTES)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
