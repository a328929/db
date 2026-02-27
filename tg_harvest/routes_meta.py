# -*- coding: utf-8 -*-
import sqlite3
from contextlib import closing

from flask import jsonify


def register_meta_routes(app, *, logger, get_conn_fn, build_meta_payload_fn) -> None:
    @app.get("/api/meta")
    def api_meta():
        try:
            with closing(get_conn_fn()) as conn:
                payload = build_meta_payload_fn(conn)
            return jsonify(payload)
        except sqlite3.Error:
            logger.exception("读取群列表失败")
            return jsonify({"ok": False, "error": "读取群列表失败"}), 500
        except Exception:
            logger.exception("系统异常")
            return jsonify({"ok": False, "error": "系统异常"}), 500
