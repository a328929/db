import functools
import hashlib
import secrets
import threading
import time
from collections import deque
from urllib.parse import urlencode, urlsplit

from flask import current_app, jsonify, redirect, request, session

from tg_harvest.web.responses import json_error, require_json_dict

ADMIN_LOGIN_FAILURE_LIMIT = 10
ADMIN_LOGIN_FAILURE_WINDOW_SEC = 5 * 60
ADMIN_CSRF_HEADER = "X-CSRF-Token"
ADMIN_CSRF_SESSION_KEY = "admin_csrf_token"
DEFAULT_ADMIN_PAGE_PATH = "/admin/manage"
ALLOWED_ADMIN_PAGE_PATHS = frozenset(
    {
        "/admin/manage",
        "/admin/sync",
        "/admin/channels",
        "/admin/clone",
        "/admin/clone/create",
        "/admin/clone/migrate",
        "/admin/clone/runs/manage",
        "/admin/clone/runs/detail",
        "/admin/clone/runs/messages/delete",
        "/admin/recovery",
    }
)
_admin_login_failure_tracker = {}
_admin_login_failure_lock = threading.Lock()


def _admin_login_client_key() -> str:
    return request.remote_addr or "unknown"


def _prune_expired_login_failures_locked(now: float) -> None:
    expired_keys = []
    for key, history in _admin_login_failure_tracker.items():
        while history and history[0] < now - ADMIN_LOGIN_FAILURE_WINDOW_SEC:
            history.popleft()
        if not history:
            expired_keys.append(key)
    for key in expired_keys:
        _admin_login_failure_tracker.pop(key, None)


def _admin_login_is_limited(client_key: str) -> bool:
    now = time.time()
    with _admin_login_failure_lock:
        _prune_expired_login_failures_locked(now)
        history = _admin_login_failure_tracker.setdefault(client_key, deque())
        return len(history) >= ADMIN_LOGIN_FAILURE_LIMIT


def _admin_login_record_failure(client_key: str) -> None:
    now = time.time()
    with _admin_login_failure_lock:
        _prune_expired_login_failures_locked(now)
        history = _admin_login_failure_tracker.setdefault(client_key, deque())
        history.append(now)


def _admin_login_clear_failures(client_key: str) -> None:
    with _admin_login_failure_lock:
        _admin_login_failure_tracker.pop(client_key, None)


def _get_auth_config():
    from tg_harvest.config import CFG

    return CFG


def admin_login_required(f):
    @functools.wraps(f)
    def decorated_function(*args, **kwargs):
        if not is_authenticated():
            return json_error("Unauthorized", 401, auth_required=True)
        return f(*args, **kwargs)

    return decorated_function


def normalize_admin_next_path(raw_next_path) -> str:
    next_path = str(raw_next_path or "").strip()
    if not next_path:
        return DEFAULT_ADMIN_PAGE_PATH

    parsed = urlsplit(next_path)
    if parsed.scheme or parsed.netloc or not parsed.path.startswith("/"):
        return DEFAULT_ADMIN_PAGE_PATH
    if parsed.path not in ALLOWED_ADMIN_PAGE_PATHS:
        return DEFAULT_ADMIN_PAGE_PATH

    if parsed.query:
        return f"{parsed.path}?{parsed.query}"
    return parsed.path


def admin_login_redirect_response(raw_next_path=None):
    next_path = normalize_admin_next_path(raw_next_path or request.path)
    return redirect("/admin/login?" + urlencode({"next": next_path}))


def admin_page_login_required(f):
    @functools.wraps(f)
    def decorated_function(*args, **kwargs):
        if not is_authenticated():
            raw_next_path = request.full_path or request.path
            if raw_next_path.endswith("?"):
                raw_next_path = raw_next_path[:-1]
            return admin_login_redirect_response(raw_next_path)
        return f(*args, **kwargs)

    return decorated_function


def is_authenticated():
    """检查当前请求是否已通过验证"""
    if not is_admin_auth_configured():
        logout_admin()
        return False

    auth_token = session.get("admin_token")
    expiry = session.get("admin_expiry", 0)
    auth_fp = session.get("admin_auth_fp", "")

    if not auth_token or time.time() > expiry:
        if auth_token:
            logout_admin()
        return False
    if not auth_fp or not secrets.compare_digest(
        str(auth_fp), _admin_password_fingerprint()
    ):
        logout_admin()
        return False

    return True


def is_admin_auth_configured():
    """后台密码必须显式配置；空密码一律视为未启用认证。"""
    cfg = _get_auth_config()
    return bool(str(getattr(cfg, "admin_password", "") or ""))


def _admin_password_fingerprint():
    cfg = _get_auth_config()
    raw = str(getattr(cfg, "admin_password", "") or "")
    if not raw:
        return ""
    secret_key = current_app.secret_key or ""
    secret_bytes = (
        secret_key
        if isinstance(secret_key, bytes)
        else str(secret_key).encode("utf-8", "surrogatepass")
    )
    if secret_bytes:
        secret_bytes = hashlib.blake2b(secret_bytes, digest_size=32).digest()
        return hashlib.blake2b(
            raw.encode("utf-8"), key=secret_bytes, digest_size=16
        ).hexdigest()
    return hashlib.blake2b(raw.encode("utf-8"), digest_size=16).hexdigest()


def _rotate_admin_csrf_token() -> str:
    token = secrets.token_urlsafe(32)
    session[ADMIN_CSRF_SESSION_KEY] = token
    return token


def _admin_csrf_token() -> str:
    token = session.get(ADMIN_CSRF_SESSION_KEY)
    if isinstance(token, str) and token:
        return token
    return _rotate_admin_csrf_token()


def _validate_admin_csrf_token(supplied_token) -> bool:
    expected = session.get(ADMIN_CSRF_SESSION_KEY)
    if not isinstance(expected, str) or not expected:
        return False
    if not isinstance(supplied_token, str) or not supplied_token:
        return False
    return secrets.compare_digest(supplied_token, expected)


def _request_needs_admin_csrf() -> bool:
    if request.method not in {"POST", "PUT", "PATCH", "DELETE"}:
        return False
    path = str(request.path or "")
    if not path.startswith("/api/admin/"):
        return False
    # First login cannot present a session-bound CSRF token yet.
    return path != "/api/admin/auth/login"


def _admin_csrf_payload() -> dict:
    return {"csrf_token": _admin_csrf_token()}


def login_admin(password):
    cfg = _get_auth_config()
    expected_password = str(getattr(cfg, "admin_password", "") or "")
    if not expected_password:
        return False, 0

    supplied_password = str(password or "")
    if secrets.compare_digest(supplied_password, expected_password):
        token = secrets.token_hex(16)
        session.permanent = True
        session["admin_token"] = token
        session["admin_expiry"] = time.time() + cfg.admin_session_expiry
        session["admin_auth_fp"] = _admin_password_fingerprint()
        _rotate_admin_csrf_token()
        return True, cfg.admin_session_expiry
    return False, 0


def logout_admin():
    session.clear()
    session.permanent = False


def register_auth_routes(app):
    @app.before_request
    def _protect_admin_write_requests():
        if not _request_needs_admin_csrf():
            return None

        if not is_authenticated():
            return json_error("Unauthorized", 401, auth_required=True)

        supplied_token = request.headers.get(ADMIN_CSRF_HEADER, "")
        if not _validate_admin_csrf_token(supplied_token):
            return json_error(
                "CSRF token missing or invalid",
                403,
                csrf_required=True,
            )

        return None

    def _load_json_dict():
        return require_json_dict()

    @app.route("/api/admin/auth/check", methods=["GET"])
    def api_auth_check():
        authenticated = is_authenticated()
        expiry = session.get("admin_expiry", 0)
        remaining = max(0, int(expiry - time.time())) if authenticated else 0
        payload = {
            "ok": True,
            "authenticated": authenticated,
            "remaining": remaining,
        }
        if authenticated:
            payload.update(_admin_csrf_payload())
        return jsonify(payload)

    @app.route("/api/admin/auth/login", methods=["POST"])
    def api_auth_login():
        data, error_response = _load_json_dict()
        if error_response is not None:
            return error_response
        password = data.get("password", "")
        if not isinstance(password, str):
            return json_error("password 参数必须为字符串", 400)
        if not is_admin_auth_configured():
            return json_error("后台密码未配置", 503)

        client_key = _admin_login_client_key()
        if _admin_login_is_limited(client_key):
            return json_error("登录失败次数过多，请稍后再试", 429)

        success, expiry_duration = login_admin(password)
        if success:
            _admin_login_clear_failures(client_key)
            payload = {"ok": True, "expiry_duration": expiry_duration}
            payload.update(_admin_csrf_payload())
            return jsonify(payload)

        _admin_login_record_failure(client_key)
        return json_error("密码错误", 403)

    @app.route("/api/admin/auth/logout", methods=["POST"])
    def api_auth_logout():
        logout_admin()
        return jsonify({"ok": True})
