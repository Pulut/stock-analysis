# 线上数据库（Postgres）使用说明

本项目默认使用本地 `stock_data.db`（SQLite）。如果你希望把**所有数据（行情 + 交易）都放到线上**并避免每天提交 200MB 的数据库文件，可以改用线上 Postgres。

## 1) 选择一个免费 Postgres

常见的免费档选择：

- Neon（Postgres）
- Supabase（Postgres）

任选其一，创建数据库后拿到连接串（`postgresql://...`）。

## 2) 配置 `DATABASE_URL`

项目会优先读取 `DATABASE_URL`（环境变量或 Streamlit secrets）。一旦配置成功，就会自动切换到 Postgres。

### Streamlit Community Cloud

在应用的 Secrets 中新增：

```toml
DATABASE_URL = "postgresql://USER:PASSWORD@HOST:5432/DBNAME?sslmode=require"
```

### 本地/服务器环境

设置环境变量后运行：

- Windows PowerShell:
  - `$env:DATABASE_URL="postgresql://..."; python data_loader.py`
- Linux/macOS:
  - `DATABASE_URL="postgresql://..." python data_loader.py`

## 3) 初始化/增量更新数据

配置好 `DATABASE_URL` 后，直接执行一次数据更新：

```bash
python data_loader.py
```

首次运行会自动创建所需表；之后每天运行会按增量逻辑更新数据。

## 4) 交易数据持久化

`dashboard.py` 启动时会调用 `trader.init_trade_system()` 创建 `trade_account/trade_positions/trade_orders` 表（默认**不清表**）。

如果需要强制重置交易表（危险：会清空持仓/流水），在 Python 中执行：

```python
import trader
trader.init_trade_system(reset=True)
```

## 5) 把现有 `stock_data.db` 迁移到 Postgres（可选但推荐）

如果你已经有一份 200MB 的 `stock_data.db`，建议先迁移到 Postgres，避免重新全量跑一遍数据。

一个常用办法是使用 `pgloader`（可用 Docker 跑）：

### 方案：pgloader（最快）

```bash
pgloader stock_data.db "postgresql://USER:PASSWORD@HOST:5432/DBNAME?sslmode=require"
```

迁移完成后，再把 Streamlit 的 `DATABASE_URL` 指向这套 Postgres 即可。
