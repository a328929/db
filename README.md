# tg_search_web

本项目包含两部分能力：

- **Web 搜索界面**（`app.py`）
- **Telegram 采集器**（`jb.py` / `tg_harvest`）

为避免依赖误解，依赖文件按用途拆分：

- `requirements-web.txt`：仅 Web 依赖
- `requirements-harvest.txt`：仅采集依赖（含 `telethon`）
- `requirements.txt`：完整依赖（Web + 采集）

## 安装方式

### 1) 只运行 Web

```bash
pip install -r requirements-web.txt
python app.py
```

打开：`http://127.0.0.1:8890`

### 2) 只运行采集

```bash
pip install -r requirements-harvest.txt
python jb.py
```

> 采集功能依赖 `telethon`，仅安装 Web 依赖无法运行采集。

### 3) 同时运行 Web + 采集（推荐新手）

```bash
pip install -r requirements.txt
```

## 目录要求

把 `tg_data.db` 放在和 `app.py` 同一目录（或通过环境变量/配置指定路径）。
