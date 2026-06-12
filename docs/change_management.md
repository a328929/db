# Change Management

本项目已经从单点修补进入持续维护阶段。后续每次较大改动都必须先能回答三件事：

- 改动属于哪个主题。
- 质量门禁是否完整通过。
- 是否把运行态数据、凭据、缓存或临时产物带进补丁。

## 本地质量门禁

首选统一入口：

```bash
python3 tools/check_project_quality.py
```

该脚本与 CI 使用同一套检查：

- 变更卫生检查：拒绝运行态数据、凭据、缓存目录进入可见补丁。
- Python 编译检查：`compileall` 覆盖运行时代码、测试、脚本和工具。
- Python 静态检查：`ruff check tg_harvest tests scripts tools`。
- 前端静态 JavaScript 语法检查：`node --check static/*.js`。
- 单元测试：`pytest`。
- Flask 应用启动和关键路由冒烟检查。

快速开发时可以临时跳过测试：

```bash
python3 tools/check_project_quality.py --skip-tests
```

跳过测试只能用于本地快速迭代，提交前仍需跑完整门禁。

## 变更盘点

查看当前工作树按主题归类后的补丁清单：

```bash
python3 tools/change_inventory.py
```

只做变更卫生检查：

```bash
python3 tools/change_inventory.py --check
```

主题划分用于拆分提交和评审范围：

- CI / Dev Tooling：CI、ruff、pytest、统一质量脚本、静态 JS 检查。
- Security / Admin Auth：后台登录、CSRF、生产安全配置、后台写接口鉴权。
- Admin Jobs / Runtime Recovery：后台任务状态、心跳、恢复、互斥执行。
- Frontend Admin UX / JS Safety：后台页面交互、焦点管理、前端注入风险。
- Search / Storage / Ingest Correctness：搜索、存储、采集解析和数据一致性。
- Operator Tools / Telegram Scripts：需要连接 Telegram 或批量改库的运维工具。
- Documentation：README 和维护文档。
- Tests：不能更精确归属到单一主题的测试。

## 推荐提交顺序

较大补丁建议按下面顺序拆分，避免一次提交混合过多行为：

1. CI / Dev Tooling
2. Security / Admin Auth
3. Admin Jobs / Runtime Recovery
4. Frontend Admin UX / JS Safety
5. Search / Storage / Ingest Correctness
6. Operator Tools / Telegram Scripts
7. Documentation and test-only follow-ups

每个提交至少要能通过 `python3 tools/check_project_quality.py --skip-tests`。合并前必须通过完整质量门禁。

## 禁止进入补丁的内容

以下内容只能留在本机运行态，不应出现在可提交补丁中：

- `.env`
- `.runtime/`
- `.venv/` 或 `venv/`
- `__pycache__/`
- `.pytest_cache/`
- `.ruff_cache/`
- `media/`
- `downloads/`
- `*.db`, `*.sqlite*`, `*.session*`, `*.log`

这些规则由 `.gitignore` 和 `tools/change_inventory.py --check` 双层保护。
