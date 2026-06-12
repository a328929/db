from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_ROOT = PROJECT_ROOT / ".runtime"
RUNTIME_DB_DIR = RUNTIME_ROOT / "db"
RUNTIME_SESSION_DIR = RUNTIME_ROOT / "sessions"


def ensure_runtime_layout() -> Path:
    RUNTIME_DB_DIR.mkdir(parents=True, exist_ok=True)
    RUNTIME_SESSION_DIR.mkdir(parents=True, exist_ok=True)
    return RUNTIME_ROOT


def runtime_dir() -> Path:
    ensure_runtime_layout()
    return RUNTIME_ROOT


def _resolve_relative_path(raw_name: str, *, default_dir: Path) -> str:
    ensure_runtime_layout()
    candidate = Path(raw_name)
    if candidate.is_absolute():
        candidate.parent.mkdir(parents=True, exist_ok=True)
        return str(candidate)
    if candidate.parent != Path("."):
        resolved = (PROJECT_ROOT / candidate).resolve()
        resolved.parent.mkdir(parents=True, exist_ok=True)
        return str(resolved)
    resolved = (default_dir / candidate.name).resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return str(resolved)


def resolve_db_path(raw_name: str) -> str:
    name = str(raw_name or "tg_data.db").strip() or "tg_data.db"
    return _resolve_relative_path(name, default_dir=RUNTIME_DB_DIR)


def resolve_session_name(raw_name: str) -> str:
    name = str(raw_name or "my_session").strip() or "my_session"
    if name.endswith(".session"):
        name = name[: -len(".session")]
    return _resolve_relative_path(name, default_dir=RUNTIME_SESSION_DIR)
