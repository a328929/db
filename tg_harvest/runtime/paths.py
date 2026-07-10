from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_ROOT = PROJECT_ROOT / ".runtime"
RUNTIME_DB_DIR = RUNTIME_ROOT / "db"
RUNTIME_SESSION_DIR = RUNTIME_ROOT / "sessions"

_PRIVATE_DIRECTORY_MODE = 0o700
_PRIVATE_FILE_MODE = 0o600
_SQLITE_SIDECAR_SUFFIXES = ("-journal", "-wal", "-shm")


def _ensure_private_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    path.chmod(_PRIVATE_DIRECTORY_MODE)


def secure_runtime_file(path: str | Path) -> None:
    """Restrict a runtime artifact without changing its containing directory."""
    candidate = Path(path)
    try:
        if candidate.is_file():
            candidate.chmod(_PRIVATE_FILE_MODE)
    except FileNotFoundError:
        return


def secure_sqlite_artifacts(path: str | Path) -> None:
    """Restrict a SQLite database and any sidecars that currently exist."""
    database_path = Path(path)
    secure_runtime_file(database_path)
    for suffix in _SQLITE_SIDECAR_SUFFIXES:
        secure_runtime_file(Path(f"{database_path}{suffix}"))


def secure_session_artifacts(session_name: str | Path) -> None:
    session_path = Path(session_name)
    if session_path.suffix != ".session":
        session_path = Path(f"{session_path}.session")
    secure_sqlite_artifacts(session_path)


def ensure_runtime_layout() -> Path:
    _ensure_private_directory(RUNTIME_ROOT)
    _ensure_private_directory(RUNTIME_DB_DIR)
    _ensure_private_directory(RUNTIME_SESSION_DIR)
    _ensure_private_directory(RUNTIME_ROOT / "models")
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
