# 个人投资工作台

一个面向个人投资研究的本地 A 股工作台，当前重点是：

- 维护本地 MySQL 日线库
- 查看单只股票行情和 K 线
- 运行日线同步、质量修复、日终选股
- 保留一个轻量回测入口
- 通过 Web 页面接入外部 `re_agent` 研究助手

这个项目不是交易系统，也不是通用量化平台。它更像一套“自己可控的数据底座 + 一个够用的研究台面”。

## 当前状态

当前主链路已经比较明确：

- `MySQL` 是主数据源
- 日线同步、质量修复、股票查询、选股都围绕 MySQL 工作
- Web 工作台已经可用，支持后台同步进度轮询
- 历史本地日线 CSV 仓库已经移除，不再作为主数据链路
- `data/cache/` 仍然保留为第三方接口缓存层

当前已经落地的页面能力：

- 数据库概览
- 同步到最新
- 修复最近日线
- 单股行情查询和 K 线展示
- 日终选股
- 研究助手对话框（依赖独立 `re_agent` 服务）
- 最近任务和操作结果查看

## 技术栈

- Python 3.14
- FastAPI + Uvicorn
- SQLAlchemy 2.0 + PyMySQL
- pandas / numpy
- 数据源路由：`mootdx` + 腾讯财经 + `baostock` + AKShare/Sina/Eastmoney 兜底与补充

依赖见 [requirements.txt](/Users/lilinxing/Codes/quant/requirements.txt:1)。

## 数据链路

### 主数据源

- 股票基础信息：AKShare
- 日线行情：`DailyBarRouter` 路由多源抓取
- 停牌信息、交易日历、估值、分红等：通过 `MarketDataClient` 获取并缓存

### 当前日线口径

- `volume = 手`
- `amount = 元`
- `turnover = 百分数数值`

### 存储层

- 主库存放在 MySQL
- 第三方接口缓存存放在 `data/cache/`
- 同步任务与明细记录在 `sync_runs` / `sync_run_items`

## 快速开始

### 1. 安装依赖

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. 配置 MySQL

```bash
export QUANT_DATABASE_URL='mysql+pymysql://root:1234@127.0.0.1:3306/quant?charset=utf8mb4'
```

初始化 schema：

```bash
/usr/local/mysql/bin/mysql -uroot -p1234 --local-infile=1 < sql/schema_mysql.sql
```

如果库里还没有基础股票表、交易日历、分红汇总、停牌快照，可以用缓存摘要补一次：

```bash
python3 scripts/import_mysql_data.py --password 1234
```

### 3. 启动 Web 工作台

```bash
python3 main.py --mode serve --host 127.0.0.1 --port 8000
```

打开 [http://127.0.0.1:8000](http://127.0.0.1:8000)。

如果你本地已经装好了依赖，也可以直接运行：

```bash
./scripts/start_quant.command
```

## 常用命令

### 增量同步到最新

```bash
python3 main.py \
  --mode update-daily \
  --end 2026-05-18 \
  --workers 8
```

说明：

- 系统会按 MySQL 中每只股票的最新日期决定补数起点
- 已有历史数据的股票会从 `latest_db_date + 1` 开始补
- 空库或新股票才会回退到 `--start` 指定的起点

`download-daily` 目前仍然保留，但实现上和 `update-daily` 走的是同一条链路，主要是兼容旧用法。

### 首次大范围回填

```bash
python3 main.py \
  --mode update-daily \
  --start 2015-01-01 \
  --end 2026-05-18 \
  --workers 8
```

### 修复最近异常日线

```bash
python3 main.py \
  --mode repair-daily-quality \
  --start 2026-05-01 \
  --end 2026-05-18 \
  --workers 4
```

这条链路会扫描 MySQL 中 OHLC、成交量、成交额等异常记录，重新抓取并覆盖。

### 系统性修复历史单位问题

```bash
python3 scripts/repair_daily_units.py \
  --start 2010-01-01 \
  --end 2026-05-18 \
  --workers 1
```

这个脚本适合处理历史库里成交量、成交额、换手率单位不一致的问题。

### 日终选股

```bash
python3 main.py \
  --mode screen \
  --trade-date 2026-05-18 \
  --price-ma-ratio 0.9 \
  --min-dividend-yield 5
```

当前选股逻辑基于：

- `close < MA120 * ratio`
- 最低股息率过滤
- 非亏损过滤

结果里会附带：

- `dividend_yield`
- `pe_ttm`
- `market_cap`
- 数据来源说明

说明：

- 这里的股息率优先取直接股息率数据
- 拿不到时会退回到历史分红汇总，用“年均股息 / 最新收盘价”做估算
- 所以结果里可能同时存在“直连股息率”和“估算股息率”

### 回测

```bash
python3 main.py \
  --mode backtest \
  --start 2020-01-01 \
  --end 2024-12-31 \
  --min-dividend-yield 5 \
  --rebalance-days 20 \
  --max-stocks 100
```

注意：

- 回测当前主要通过 `MarketDataClient` 直接取数并使用缓存
- 它更像策略原型验证，不是严格的生产级回测框架

## Web 页面说明

当前页面包含这些区域：

- 数据库概览：总行数、覆盖股票数、最新交易日、最近任务
- 增量同步日线：后台启动，前端轮询进度
- 修复最近日线：面向近期异常记录
- 日终选股：执行当前策略并展示结果
- 股票行情：按代码或名称查看本地 MySQL K 线
- 研究助手：通过 `/api/re-agent/chat` 代理外部 `re_agent`

## `re_agent` 说明

Web 页面里的“研究助手”不是本项目内部实现的 LLM，而是代理一个独立服务。

默认配置：

- `RE_AGENT_BASE_URL=http://127.0.0.1:8010`
- `RE_AGENT_CHAT_PATH=/research/single-symbol`

如果本地没有启动 `re_agent`，不影响行情、同步、选股这些核心功能，只是助手面板不可用。

## 目录结构

```text
quant/
├── data/
│   ├── cache/
│   └── ...
├── docs/
│   └── WORKBENCH_ARCHITECTURE.md
├── main.py
├── requirements.txt
├── scripts/
│   ├── import_mysql_data.py
│   ├── repair_daily_units.py
│   └── start_quant.command
├── sql/
│   └── schema_mysql.sql
└── src/
    ├── quant_backtest/
    ├── quant_data/
    └── quant_web/
```

## 主要模块

- [main.py](/Users/lilinxing/Codes/quant/main.py:1)
  CLI 入口，支持 `serve / update-daily / download-daily / repair-daily-quality / screen / backtest`

- [src/quant_web/app.py](/Users/lilinxing/Codes/quant/src/quant_web/app.py:1)
  FastAPI 路由和内嵌单页前端

- [src/quant_web/service.py](/Users/lilinxing/Codes/quant/src/quant_web/service.py:1)
  同步、修复、股票查询、选股等服务层逻辑

- [src/quant_web/db.py](/Users/lilinxing/Codes/quant/src/quant_web/db.py:1)
  MySQL ORM 模型和数据库初始化

- [src/quant_data/router.py](/Users/lilinxing/Codes/quant/src/quant_data/router.py:1)
  多数据源路由

- [src/quant_backtest/data.py](/Users/lilinxing/Codes/quant/src/quant_backtest/data.py:1)
  缓存、分红、估值、交易日历等数据访问封装

## 当前边界

- 目前仍以日线为主，不是实时行情系统
- 第三方数据源会波动，偶尔会出现缺字段、缺成交量、局部断供
- 北交所股票的稳定性通常比沪深主板差一些
- `daily_bar_indicators` 表仍存在，但当前主同步链路不会自动刷新均线表
- 选股和回测仍属于研究原型，不应直接当作交易建议
- `re_agent` 是外部依赖，不保证在所有环境默认可用

## 下一步更值得做的事

- 在同步或修复后自动刷新 `daily_bar_indicators`
- 给股息率补一张独立因子表，而不是只在选股时临时计算
- 把选股和修复任务也统一成更完整的后台任务体系
- 增加自选股、笔记和股票观察流
