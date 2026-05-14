# -*- coding: utf-8 -*-
import sys
import os
import sqlite3
import asyncio
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from telethon import TelegramClient
from tg_harvest.config import CFG
from tg_harvest.domain.chat_ids import candidate_chat_entity_ids
from tg_harvest.storage.connection import ensure_configured_db
from tg_harvest.ingest.parse import MessageParser
from tg_harvest.ingest.runner import _prepare_db_rows
from tg_harvest.ingest.store import _batch_upsert_messages, _batch_upsert_media

# 减少不必要的日志输出
logging.basicConfig(level=logging.WARNING)

def get_missing_message_ids(conn, chat_id, last_id):
    """
    通过集合差集，找出 1 到 last_id 之间，数据库中缺失的 message_id。
    返回倒序排序的 ID 列表。
    """
    cur = conn.cursor()
    cur.execute("SELECT message_id FROM messages WHERE chat_id = ? AND message_id <= ?", (chat_id, last_id))
    existing_ids = {row[0] for row in cur.fetchall()}
    
    # 假设消息是连续递增的
    all_possible_ids = set(range(1, last_id + 1))
    missing_ids = all_possible_ids - existing_ids
    
    return sorted(list(missing_ids), reverse=True)


async def main():
    print(f"====================================")
    print(f"  Telegram 缺失多媒体/消息极速找回工具")
    print(f"====================================\n")
    print(f"正在连接本地数据库: {CFG.db_name}")
    
    conn, _ = ensure_configured_db(cfg=CFG)
    cur = conn.cursor()
    
    # 找到所有群组和它们当前记录的最大 message_id
    cur.execute("SELECT chat_id, MAX(message_id) as max_id FROM messages GROUP BY chat_id")
    chat_info = cur.fetchall()
    
    if not chat_info:
        print("数据库为空，无需找回。")
        return
        
    print(f"正在连接 Telegram API (使用 Session: {CFG.session_name})...")
    client = TelegramClient(CFG.session_name, CFG.api_id, CFG.api_hash)
    await client.start()
    print("API 连接成功！\n")
    
    total_recovered = 0
    
    try:
        for row in chat_info:
            chat_id = row["chat_id"]
            max_id = row["max_id"]
            
            # 由于被清理过，我们需要先解析实体
            try:
                entity = await client.get_entity(chat_id)
            except Exception as e:
                # Telethon 群组 ID 补丁
                err_msg = str(e).lower()
                if "could not find the input entity" in err_msg:
                    entity = None
                    for candidate_id in candidate_chat_entity_ids(chat_id):
                        if candidate_id == int(chat_id):
                            continue
                        try:
                            entity = await client.get_entity(candidate_id)
                            break
                        except Exception:
                            pass
                    if entity is None:
                        print(f"[跳过] 无法获取群组 {chat_id} 的访问权限。")
                        continue
                else:
                    print(f"[跳过] 无法获取群组 {chat_id} 的访问权限。")
                    continue
            
            chat_title = getattr(entity, 'title', str(chat_id))
            print(f"正在分析群组/频道: {chat_title}")
            
            # 精确计算断层（被您清理掉的记录 ID）
            missing_ids = get_missing_message_ids(conn, chat_id, max_id)
            if not missing_ids:
                print(f"  -> 该群组历史消息完美连续，无需修复。\n")
                continue
                
            print(f"  -> 发现 {len(missing_ids)} 个历史空洞。开始使用靶向抓取 (Targeted Fetch)...")
            
            batch_msg_rows = []
            batch_media_rows = []
            recovered_for_chat = 0
            
            # Telethon 的 get_messages(ids=[...]) 是一次性获取，但单次最好不要太大
            chunk_size = 100
            
            for i in range(0, len(missing_ids), chunk_size):
                chunk_ids = missing_ids[i:i+chunk_size]
                print(f"    [网络请求] 正在向 Telegram 询问 {len(chunk_ids)} 个 ID (从 {chunk_ids[0]} 到 {chunk_ids[-1]})...")
                
                try:
                    # 极速靶向拉取
                    messages = await client.get_messages(entity, ids=chunk_ids)
                    
                    found_in_this_chunk = 0
                    for msg in messages:
                        if not msg or not msg.id:
                            continue # 说明该消息在 Telegram 服务器上确实被发件人撤回或删除了
                            
                        # 这条消息如果在服务器上存在，就被重新救活了！
                        p = MessageParser.parse(msg)
                        if not p:
                            continue
                            
                        # prepare_db_rows 里面内置了自动把文件名作为 title 的最新逻辑！
                        m_row, med_row = _prepare_db_rows(entity, chat_id, p)
                        if m_row:
                            batch_msg_rows.append(m_row)
                            found_in_this_chunk += 1
                        if med_row:
                            batch_media_rows.append(med_row)
                            recovered_for_chat += 1
                            total_recovered += 1
                            
                    print(f"      -> 这一批找到了 {found_in_this_chunk} 条存活的消息。")
                            
                    # 满 500 条写一次库，释放内存
                    if len(batch_msg_rows) >= 500:
                        cur.execute("BEGIN IMMEDIATE")
                        _batch_upsert_messages(cur, batch_msg_rows)
                        if batch_media_rows:
                            _batch_upsert_media(cur, batch_media_rows)
                        conn.commit()
                        batch_msg_rows = []
                        batch_media_rows = []
                        print(f"    [入库] 已将新救回的数据安全存入本地数据库...")
                        
                    await asyncio.sleep(1.0) # 防止被 Telegram 官方限流
                    
                except Exception as e:
                    if "A wait of" in str(e):
                        print(f"    [限流] 触发 Telegram 风控，需等待: {e}")
                        await asyncio.sleep(15)
                    else:
                        print(f"    [错误] 获取区块时出错: {e}")
                    
            # 循环结束，写入剩余尾部数据
            if batch_msg_rows:
                 cur.execute("BEGIN IMMEDIATE")
                 _batch_upsert_messages(cur, batch_msg_rows)
                 if batch_media_rows:
                     _batch_upsert_media(cur, batch_media_rows)
                 conn.commit()
                 
            print(f"  -> 修复完成！该群组成功救回 {recovered_for_chat} 条多媒体记录。\n")
                 
    finally:
        await client.disconnect()
        cur.close()
        conn.close()
        
    print(f"====================================")
    print(f"修复总计完成！全库共成功找回 {total_recovered} 条被误删的多媒体记录。")
    print(f"它们已经满血复活，并且自带提取好的文件名，随时可以被全文检索到！")
    print(f"====================================")

if __name__ == "__main__":
    asyncio.run(main())
