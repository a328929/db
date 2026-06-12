import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROJECT_ROOT_STR = str(PROJECT_ROOT)
if PROJECT_ROOT_STR not in sys.path:
    sys.path.insert(0, PROJECT_ROOT_STR)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="导出 Telegram 中已加入但数据库 chats 表里不存在的群聊或频道"
    )
    parser.add_argument(
        "--output",
        default="missing_chats.txt",
        help="输出 txt 文件路径，默认 missing_chats.txt",
    )
    return parser


def scan() -> int:
    from telethon.sync import TelegramClient

    from tg_harvest.config import CFG
    from tg_harvest.domain.chat_inventory import (
        find_missing_joined_chats,
        load_known_chat_ids,
        write_missing_chat_report,
    )
    from tg_harvest.storage.connection import ensure_configured_db

    args = _build_parser().parse_args()
    conn, _ = ensure_configured_db(cfg=CFG)
    try:
        db_chat_ids = load_known_chat_ids(conn)
        print(f"数据库路径: {CFG.db_name}")
        print(f"chats 表中已有 {len(db_chat_ids)} 个 chat_id。")

        print(f"正在连接 Telegram (session={CFG.session_name})...")
        with TelegramClient(
            CFG.session_name,
            CFG.api_id,
            CFG.api_hash,
            receive_updates=False,
        ) as client:
            if not client.is_user_authorized():
                print("当前 Telegram session 未授权，无法枚举已加入的群聊或频道。")
                return 2

            print("Telegram 连接成功，开始枚举已加入的群聊和频道...")
            rows = find_missing_joined_chats(client.iter_dialogs(), db_chat_ids)

        output_path = write_missing_chat_report(rows, args.output)
        print(f"扫描完成，发现 {len(rows)} 个未入库目标。")
        print(f"结果文件: {output_path}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(scan())
