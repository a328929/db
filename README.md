# tg_search_web

本项目包含 Web 搜索界面与 Telegram 采集器。

## 安装与运行

```bash
# 1. 安装依赖
python3 -m pip install -r requirements.txt

# 2. 运行应用
python3 -m tg_harvest web

# 3. 运行采集器
python3 -m tg_harvest harvest
```

也可以直接一键启动：

```bash
bash start_web.sh
```

## 目录要求
默认运行态文件位于 `.runtime/`：

- `.runtime/db/tg_data.db`
- `.runtime/sessions/*.session`

也可以通过环境变量覆盖路径。

## 常用环境变量

- `TG_DB_NAME`: SQLite 数据库路径
- `TG_SESSION_NAME`: Telegram session 路径基名
- `TG_API_ID` / `TG_API_HASH`: Telegram API 凭据
- `TG_TARGET_GROUP`: 默认采集目标
- `TG_SQLITE_CACHE_MB`: SQLite cache 大小
- `TG_SQLITE_MMAP_MB`: SQLite mmap 大小
- `TG_ADMIN_PASSWORD`: 管理页密码

## 运行说明

- Web 服务默认监听 `8890` 端口。
- 应用首次请求或直接运行 `python3 -m tg_harvest web` 时会自动初始化数据库结构与 FTS 索引。
- 搜索结果支持全量分页返回，不会把总结果数硬裁到 5000/10000。

## 开发检查

```bash
# 语法检查
python3 -m compileall -q tg_harvest tests

# 路由注册与应用启动冒烟检查
python3 tools/smoke_check_app.py

# 网络 / session / 账号分级诊断
python3 scripts/diagnose_telegram.py
```
