# tg_search_web

本项目包含 Web 搜索界面与 Telegram 采集器。

## 安装与运行

```bash
# 1. 安装依赖
python3 -m pip install -r requirements.txt

# 2. 检查本地配置
# 编辑 .env，确认后台管理密码和电报接口信息

# 3. 运行应用
python3 -m tg_harvest web

# 4. 运行采集器
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
- `TG_SECONDARY_SESSION_NAME`: 可选第二账号 Telegram session 路径基名；配置后新增大群会尝试双账号区间拉取
- `TG_MULTI_ACCOUNT_MIN_MESSAGE_ID`: 新增目标最后一条消息 ID 达到该值时才尝试双账号区间拉取，默认 `5000`
- `TG_MULTI_ACCOUNT_RANGE_CHUNK_SIZE`: 双账号区间拉取的 message_id 范围跨度，默认 `1000`
- `TG_FLOOD_WAIT_SWITCH_THRESHOLD`: Telegram FloodWait 超过该秒数时不再等待，改由后台任务尝试切换另一个账号，默认 `30`
- `TG_ADMIN_UPDATE_CONCURRENCY`: 后台数据库管理页“批量更新所有群组”的总并发上限，默认 `4`；单账号模式会按该值并发，双账号模式默认收敛为“每个账号同一时刻只跑 1 个群组”，并在单个群组更新失败时自动切换另一账号重试
- `TG_ADMIN_UPDATE_MIN_CHAT_START_GAP_SECONDS`: 后台“批量更新所有群组”时，同一账号启动下一个群组前的最小间隔秒数；默认按账号数自动取值，单账号约 `0.25` 秒、双账号约 `1.0` 秒；如需更激进或更保守可显式设置
- `TG_ADMIN_UPDATE_SECONDARY_PUBLIC_RESOLVE_LIMIT`: 后台“批量更新所有群组”时，允许第二账号主动按公开 username 解析的群组数量上限；留空时会按第二账号当前缓存覆盖率自动给一个小的每日预热预算（冷启动默认约 `12`），设为 `0` 可彻底关闭主动预热
- `TG_ADMIN_UPDATE_MAX_COOLDOWN_WAIT_SECONDS`: 批量更新所有群组时，全部账号都处于 FloodWait 后最多短等秒数，默认 `45`；超过该值会提前收尾并保留未启动群组供下次继续，避免把剩余群组批量记为失败
- `TG_OPS_BOT_ENABLED`: 运维机器人通知开关，默认 `0`；开启后会把后台任务创建、终态和重要长等待日志发到 Telegram
- `TG_OPS_BOT_TOKEN`: 运维机器人 token，只从环境变量读取
- `TG_OPS_BOT_NOTIFY_CHAT_ID`: 运维机器人通知目标 chat_id
- `TG_OPS_BOT_TIMEOUT_SECONDS`: 运维机器人请求超时秒数，默认 `3`
- `TG_API_ID` / `TG_API_HASH`: Telegram API 凭据
- `TG_TARGET_GROUP`: 默认采集目标
- `TG_SQLITE_CACHE_MB`: SQLite cache 大小
- `TG_SQLITE_MMAP_MB`: SQLite mmap 大小
- `TG_ADMIN_PASSWORD`: 管理页密码，必须显式设置；未设置时后台登录会被拒绝
- `FLASK_SECRET_KEY`: Flask session 签名密钥，建议显式设置以避免重启后登录态失效
- `TG_REQUIRE_SECURE_CONFIG`: 设置为 `1` 时启用生产安全配置校验；缺少 `TG_ADMIN_PASSWORD` 或 `FLASK_SECRET_KEY` 会拒绝启动
- `TG_SESSION_COOKIE_SECURE`: 控制后台 session cookie 是否只在 HTTPS 发送；生产安全配置启用时默认开启，可在反代终止 TLS 的本地明文链路中显式设为 `0`
- `TG_SKIP_FTS_AUTO_HEAL`: 设置为 `1` 时跳过启动期 FTS 全量修复，仅恢复增量同步触发器；磁盘紧张的大库恢复场景可临时使用

## 运行说明

- Web 服务默认监听 `8890` 端口。
- 应用首次请求或直接运行 `python3 -m tg_harvest web` 时会自动初始化数据库结构与 FTS 索引。
- 搜索结果支持全量分页返回，不会把总结果数硬裁到 5000/10000。
- 后台管理页面需要先访问 `/admin/login` 完成登录；后台写操作使用登录态绑定的 CSRF token 防护。公网部署时建议放在 HTTPS、内网/VPN 或反向代理鉴权之后，并设置 `TG_REQUIRE_SECURE_CONFIG=1`。

## 开发检查

```bash
# 安装开发依赖
python3 -m pip install -r requirements-dev.txt

# 推荐：运行和 CI 一致的完整质量门禁
python3 tools/check_project_quality.py

# 查看当前补丁按主题归类后的变更清单
python3 tools/change_inventory.py

# 查看 SQLite 主库空间构成
python3 tools/db_space_report.py --db .runtime/db/tg_data.db

# 在有足够临时磁盘的维护窗口生成紧凑替换库，不会修改源库；
# 工具会为一致性快照和构建库预留空间，校验通过后才生成最终目标库
python3 tools/compact_sqlite_db.py --source .runtime/db/tg_data.db --target /path/to/tg_data.compact.db

# 手机或低内存设备可降低批大小；目标已存在时才需要 --force
python3 tools/compact_sqlite_db.py --source /path/to/tg_data.db --target /path/to/tg_data.compact.db --batch-size 10000

# 只检查变更中是否混入运行态/凭据/缓存文件
python3 tools/change_inventory.py --check

# 语法检查
python3 -m compileall -q tg_harvest tests scripts tools

# 单元测试
python3 -m pytest

# 路由注册与应用启动冒烟检查
python3 tools/smoke_check_app.py

# CI 强制：完整静态检查
python3 -m ruff check tg_harvest tests scripts tools

# CI 强制：前端 JavaScript 语法检查
python3 tools/check_static_js.py

# 网络 / session / 账号分级诊断
python3 scripts/diagnose_telegram.py
```

## 容量规划与维护窗口

`/admin/sync` 会展示数据库容量、WAL/SHM、空闲页、消息与媒体组摘要、CJK
短词索引队列、FTS 就绪状态，以及最近记录的维护活动。该状态只读取文件属性、
SQLite PRAGMA、少量元数据和已缓存或统计的计数；它不会在 Web 请求中运行
`dbstat`、`integrity_check` 或全库扫描。因此，页面中的“健康”只表示容量和维护
风险信号，不等同于完整性校验。

可通过以下环境变量调整告警边界；配置加载时会校正 warning/critical 的顺序：

| 环境变量 | 默认值 | 含义 |
| --- | ---: | --- |
| `TG_DB_HEALTH_SIZE_WARNING_BYTES` / `TG_DB_HEALTH_SIZE_CRITICAL_BYTES` | 20 GiB / 50 GiB | 主库大小预警与严重阈值 |
| `TG_DB_HEALTH_WAL_WARNING_BYTES` / `TG_DB_HEALTH_WAL_CRITICAL_BYTES` | 512 MiB / 2 GiB | WAL 文件大小预警与严重阈值 |
| `TG_DB_HEALTH_DISK_FREE_WARNING_BYTES` / `TG_DB_HEALTH_DISK_FREE_CRITICAL_BYTES` | 10 GiB / 3 GiB | 数据库所在磁盘剩余空间阈值 |
| `TG_DB_HEALTH_CJK_QUEUE_WARNING` / `TG_DB_HEALTH_CJK_QUEUE_CRITICAL` | 10,000 / 100,000 | 中文短词索引待维护队列阈值 |

容量预警不执行自动压缩、checkpoint 或索引重建。看到 WAL 持续增长时，先排查
长时间读取连接和后台大任务；看到 CJK 队列积压或 FTS 未就绪时，先确认维护线程
和磁盘空间。进行 `compact_sqlite_db.py` 前，应安排低峰维护窗口并预留至少两份
当前主库与 sidecar 文件的空间，再加 64 MiB 构建余量。紧凑构建工具会做其自身的
离线校验，完成后再按部署流程切换数据库文件。

较大改动的拆分、评审和提交顺序见 `docs/change_management.md`。
