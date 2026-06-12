import sqlite3
from contextlib import closing

from flask import jsonify

from tg_harvest.web.responses import logged_json_error


def register_meta_routes(app, *, logger, get_conn_fn, build_meta_payload_fn) -> None:
    @app.get("/api/meta")
    def api_meta():
        try:
            with closing(get_conn_fn()) as conn:
                payload = build_meta_payload_fn(conn)
            response = jsonify(payload)
            response.headers["Cache-Control"] = "no-store"
            return response
        except sqlite3.Error:
            return logged_json_error(logger, "读取群列表失败", "读取群列表失败")
        except Exception:
            return logged_json_error(logger, "系统异常", "系统异常")
