from typing import Any

__all__ = ["run_harvest"]


def run_harvest(*args: Any, **kwargs: Any):
    """
    延迟导入采集模块，避免 Web-only 安装在导入 tg_harvest 时因缺少 telethon 直接失败。
    """
    from .harvest import run_harvest as _run_harvest

    return _run_harvest(*args, **kwargs)
