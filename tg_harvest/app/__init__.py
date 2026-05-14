from typing import Any

__all__ = ["create_app", "get_app", "run_web_server"]


def create_app(*args: Any, **kwargs: Any):
    from tg_harvest.app.factory import create_app as _create_app

    return _create_app(*args, **kwargs)


def get_app():
    from tg_harvest.app.factory import app as _app

    return _app


def run_web_server(*args: Any, **kwargs: Any):
    from tg_harvest.app.factory import run_web_server as _run_web_server

    return _run_web_server(*args, **kwargs)
