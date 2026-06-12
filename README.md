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

较大改动的拆分、评审和提交顺序见 `docs/change_management.md`。
