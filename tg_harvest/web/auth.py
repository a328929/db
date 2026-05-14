# -*- coding: utf-8 -*-
import time
import secrets
import functools
import hashlib
import threading
from collections import deque
from flask import current_app, request, jsonify, session

ADMIN_LOGIN_FAILURE_LIMIT = 10
ADMIN_LOGIN_FAILURE_WINDOW_SEC = 5 * 60
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
            return (
                jsonify(
                    {"ok": False, "error": "Unauthorized", "auth_required": True}
                ),
                401,
            )
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
        return True, cfg.admin_session_expiry
    return False, 0


def logout_admin():
    session.clear()
    session.permanent = False


def register_auth_routes(app):
    def _load_json_dict():
        if not request.is_json:
            return None, (jsonify({"ok": False, "error": "请求必须为 JSON"}), 400)
        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            return None, (jsonify({"ok": False, "error": "请求 JSON 格式错误"}), 400)
        return data, None

    @app.route("/api/admin/auth/check", methods=["GET"])
    def api_auth_check():
        authenticated = is_authenticated()
        expiry = session.get("admin_expiry", 0)
        remaining = max(0, int(expiry - time.time())) if authenticated else 0
        return jsonify(
            {
                "ok": True,
                "authenticated": authenticated,
                "remaining": remaining,
            }
        )

    @app.route("/api/admin/auth/login", methods=["POST"])
    def api_auth_login():
        data, error_response = _load_json_dict()
        if error_response is not None:
            return error_response
        password = data.get("password", "")
        if not isinstance(password, str):
            return jsonify({"ok": False, "error": "password 参数必须为字符串"}), 400
        if not is_admin_auth_configured():
            return jsonify({"ok": False, "error": "后台密码未配置"}), 503

        client_key = _admin_login_client_key()
        if _admin_login_is_limited(client_key):
            return jsonify({"ok": False, "error": "登录失败次数过多，请稍后再试"}), 429

        success, expiry_duration = login_admin(password)
        if success:
            _admin_login_clear_failures(client_key)
            return jsonify({"ok": True, "expiry_duration": expiry_duration})

        _admin_login_record_failure(client_key)
        return jsonify({"ok": False, "error": "密码错误"}), 403

    @app.route("/api/admin/auth/logout", methods=["POST"])
    def api_auth_logout():
        logout_admin()
        return jsonify({"ok": True})
