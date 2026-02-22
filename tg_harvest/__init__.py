from typing import Any

__all__ = ["run_harvest"]


def run_harvest(*args: Any, **kwargs: Any):
    """
    延迟导入采集模块，避免 Web-only 安装在导入 tg_harvest 时因缺少 telethon 直接失败。

    主实现入口为拆分版 `harvest_runner.run_harvest`；
    `tg_harvest.harvest` 仅保留兼容导出。
    """
    from .harvest_runner import run_harvest as _run_harvest

    return _run_harvest(*args, **kwargs)
