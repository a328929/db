# -*- coding: utf-8 -*-
import time
import secrets
import functools
from flask import request, jsonify, session

# =========================
# 后台验证体系 (物理隔离)
# =========================

def _get_auth_config():
    """从 app.config 或 CFG 获取配置"""
    # 假设 CFG 已经注入到 app.config 中，或者直接引用
    from tg_harvest.config import CFG
    return CFG

def admin_login_required(f):
    """装饰器：保护后台 API 路由"""
    @functools.wraps(f)
    def decorated_function(*args, **kwargs):
        if not is_authenticated():
            return jsonify({"ok": False, "error": "Unauthorized", "auth_required": True}), 401
        return f(*args, **kwargs)
    return decorated_function

def is_authenticated():
    """检查当前请求是否已通过验证"""
    auth_token = session.get("admin_token")
    expiry = session.get("admin_expiry", 0)
    
    if not auth_token or time.time() > expiry:
        # 清除过期的 session
        if auth_token:
            logout_admin()
        return False
    
    # 续期：每次操作都刷新过期时间（可选，根据需求：这里按要求是固定注销时间）
    # 如果需要固定时间注销，则不刷新 expiry
    return True

def login_admin(password):
    """执行登录逻辑"""
    cfg = _get_auth_config()
    if password == cfg.admin_password:
        token = secrets.token_hex(16)
        session.permanent = True  # 核心：允许 Session 跨浏览器重启持久化
        session["admin_token"] = token
        session["admin_expiry"] = time.time() + cfg.admin_session_expiry
        return True, token, cfg.admin_session_expiry
    return False, None, 0

def logout_admin():
    """执行登出逻辑"""
    session.clear()
    session.permanent = False

def register_auth_routes(app):
    """注册认证相关的 API 路由"""

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
        return jsonify({
            "ok": True, 
            "authenticated": authenticated, 
            "remaining": remaining
        })

    @app.route("/api/admin/auth/login", methods=["POST"])
    def api_auth_login():
        data, error_response = _load_json_dict()
        if error_response is not None:
            return error_response
        password = data.get("password", "")
        if not isinstance(password, str):
            return jsonify({"ok": False, "error": "password 参数必须为字符串"}), 400
        
        success, token, expiry_duration = login_admin(password)
        if success:
            return jsonify({
                "ok": True, 
                "token": token, 
                "expiry_duration": expiry_duration
            })
        else:
            return jsonify({"ok": False, "error": "密码错误"}), 403

    @app.route("/api/admin/auth/logout", methods=["POST"])
    def api_auth_logout():
        logout_admin()
        return jsonify({"ok": True})
