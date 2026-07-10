import asyncio
import logging
import os
import re
import sys
from pathlib import Path

from telethon import TelegramClient, events

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# 尝试加载项目配置以复用 API 凭证
try:
    from tg_harvest.config import CFG
    API_ID = CFG.api_id
    API_HASH = CFG.api_hash
    SESSION_NAME = CFG.session_name
except ImportError:
    API_ID = int(os.getenv("TG_API_ID", "0") or "0")
    API_HASH = os.getenv("TG_API_HASH", "")
    SESSION_NAME = os.getenv("TG_SESSION_NAME", "my_session")

# Telegram 官方机器人 ID 始终是 777000
TELEGRAM_BOT_ID = 777000

logging.basicConfig(format='[%(levelname) 5s/%(asctime)s] %(name)s: %(message)s', level=logging.INFO)

async def main():
    from tg_harvest.runtime.paths import secure_session_artifacts

    print(f"正在启动客户端 (会话: {SESSION_NAME})...")
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    
    await client.start()
    secure_session_artifacts(SESSION_NAME)
    me = await client.get_me()
    print(f"成功连接！当前登录用户: {me.first_name} (@{me.username or '无'})")
    print("-" * 30)

    # 1. 尝试获取最近的一条验证码消息
    print("正在检查历史消息中的验证码...")
    async for message in client.iter_messages(TELEGRAM_BOT_ID, limit=1):
        if message.text:
            print(f"最近收到的消息内容:\n{message.text}")
            # 提取 5 位或 6 位数字验证码
            codes = re.findall(r'\b\d{5,6}\b', message.text)
            if codes:
                print(f">>> 检测到可能的验证码: {codes[0]}")
            else:
                print(">>> 未能在该消息中找到明确的数字验证码")
    
    print("-" * 30)
    print("正在实时监听新消息，请在此处观察验证码...")

    # 2. 实时监听新消息
    @client.on(events.NewMessage(from_users=TELEGRAM_BOT_ID))
    async def handler(event):
        msg_text = event.message.text
        print("\n[新消息收悉] 来自: Telegram 官方")
        print(f"内容:\n{msg_text}")
        
        # 尝试提取验证码
        codes = re.findall(r'\b\d{5,6}\b', msg_text)
        if codes:
            print("\n" + "*" * 20)
            print(f"*** 验证码: {codes[0]} ***")
            print("*" * 20 + "\n")
        else:
            print("未能在消息中识别出数字验证码。")

    print("已就绪。你可以现在尝试登录 Telegram，验证码将在此处显示。")
    print("按下 Ctrl+C 可停止运行。")
    await client.run_until_disconnected()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n脚本已停止。")
