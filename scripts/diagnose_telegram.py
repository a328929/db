import socket
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROJECT_ROOT_STR = str(PROJECT_ROOT)
if PROJECT_ROOT_STR not in sys.path:
    sys.path.insert(0, PROJECT_ROOT_STR)

PROBE_ENDPOINTS = [
    ("149.154.167.51", 443),
    ("149.154.167.91", 443),
]


def _print_header(title: str) -> None:
    print(f"\n=== {title} ===")


def _runtime_deps():
    from telethon.sync import TelegramClient

    from tg_harvest.config import CFG

    return CFG, TelegramClient


def _probe_socket() -> bool:
    _print_header("网络探测")
    for host, port in PROBE_ENDPOINTS:
        try:
            with socket.create_connection((host, port), timeout=5):
                print(f"[OK] 已连通 {host}:{port}")
                return True
        except PermissionError as exc:
            print(f"[FAIL] 网络被当前环境阻止：{exc}")
            return False
        except OSError as exc:
            print(f"[WARN] 无法连接 {host}:{port} -> {exc}")
    print("[FAIL] 所有 Telegram 探测地址均不可达")
    return False


def _check_config() -> bool:
    CFG, _TelegramClient = _runtime_deps()
    _print_header("配置检查")
    ok = True
    if not CFG.api_id:
        print("[FAIL] TG_API_ID 未配置")
        ok = False
    else:
        print(f"[OK] TG_API_ID={CFG.api_id}")

    if not CFG.api_hash:
        print("[FAIL] TG_API_HASH 未配置")
        ok = False
    else:
        print("[OK] TG_API_HASH 已配置")

    print(f"[INFO] Session 名称: {CFG.session_name}")
    print(f"[INFO] Session 文件: {Path(f'{CFG.session_name}.session').resolve()}")
    return ok


def _check_telethon() -> bool:
    from tg_harvest.runtime.paths import secure_session_artifacts

    CFG, TelegramClient = _runtime_deps()
    _print_header("Telegram 鉴权检查")
    try:
        with TelegramClient(
            CFG.session_name, CFG.api_id, CFG.api_hash, receive_updates=False
        ) as client:
            secure_session_artifacts(CFG.session_name)
            if not client.is_user_authorized():
                print("[FAIL] 已连接 Telegram，但当前 session 未授权或已失效")
                return False

            me = client.get_me()
            print("[OK] 已成功连接 Telegram")
            if me is not None:
                print(
                    f"[OK] 当前账号: id={getattr(me, 'id', '')} "
                    f"name={getattr(me, 'first_name', '')} "
                    f"username=@{getattr(me, 'username', '') or '无'}"
                )
            return True
    except PermissionError as exc:
        print(f"[FAIL] 当前环境禁止外连：{exc}")
        return False
    except ConnectionError as exc:
        print(f"[FAIL] 连接 Telegram 失败：{exc}")
        return False
    except Exception as exc:
        print(f"[FAIL] Telethon 初始化或鉴权失败：{type(exc).__name__}: {exc}")
        return False


def main() -> int:
    config_ok = _check_config()
    network_ok = _probe_socket()

    if not config_ok:
        return 2
    if not network_ok:
        return 3

    auth_ok = _check_telethon()
    return 0 if auth_ok else 4


if __name__ == "__main__":
    raise SystemExit(main())
