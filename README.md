# 个人投资工作台

这是一个面向个人投资研究和日常跟踪的本地工作台。它最初从 A 股日线数据维护、选股和回测脚本长出来，现在正在演进成一个统一入口：

- 维护本地 A 股行情数据库
- 查看单只股票行情和 K 线
- 运行数据同步、选股、回测等研究任务
- 沉淀未来投资助手 agent 可调用的数据、工具和任务记录

项目不定位为高频交易系统，也不直接替代专业行情终端。它更像一个可持续扩展的个人研究桌面：数据在自己手里，策略可以慢慢迭代，未来 agent 可以通过同一套 Web/API 出口参与分析和执行。

## 当前能力

### 1. 数据底座

- 使用 MySQL 作为主数据库，默认通过 `QUANT_DATABASE_URL` 配置连接
- 使用 `AKShare` 获取 A 股基础信息、日线行情、停复牌、估值和分红等数据
- 支持首次全量下载历史日线
- 支持按本地最新日期做增量更新
- 支持自动补齐中间缺口
- 能区分 `updated`、`up_to_date`、`no_new_data`、`suspended`、`failed` 等同步结果
- 会记录同步任务和明细，方便后续追踪和 agent 读取

### 2. Web 工作台

当前 Web 页面已经包含：

- 数据库概览：总行数、覆盖股票数、最新交易日、最近任务
- 数据维护：导入本地日线库、增量同步日线
- 股票行情：按代码或名称搜索股票，查看本地 MySQL 日 K 数据和 K 线图
- 日终选股：保留当前简单策略作为研究入口
- 选股结果：展示当次策略输出

### 3. 策略研究

现有策略还比较轻量，主要用于验证数据链路和策略框架：

- 支持按交易日选股
- 支持 `close < MA120 * ratio`
- 支持股息率过滤
- 支持排除亏损股票
- 支持输出股息率、`PE(TTM)`、总市值等字段
- 支持简单日频回测

后续策略模块可以继续扩展，但它不再是这个项目唯一的中心。行情、数据维护、研究任务和 agent 出口都会成为工作台的一部分。

## 推荐架构方向

详细设计草案见 [docs/WORKBENCH_ARCHITECTURE.md](/Users/lilinxing/Codes/quant/docs/WORKBENCH_ARCHITECTURE.md)。

核心思路：

- MySQL 是主数据源，Web/API 不再写 SQLite
- 数据同步、行情查询、策略研究、agent 调用都通过服务层访问数据
- 长耗时任务不要阻塞页面，后续应改成后台任务加状态轮询
- agent 不直接随意写库，而是通过明确的工具接口、任务记录和审批边界工作
- Web 是统一入口，但底层模块要保持边界清晰，避免所有逻辑堆在页面函数里

建议逐步拆成这些模块：

- `quant_data`：数据源、同步、清洗、入库
- `quant_market`：行情查询、K 线、指标、股票画像
- `quant_strategy`：选股规则、组合构建、回测
- `quant_tasks`：后台任务、任务状态、运行日志
- `quant_agent`：投资助手 agent 的工具注册、上下文构建和执行审计
- `quant_web`：Web 页面和 API 编排

当前代码还没有完全拆成这些包，可以按功能增长逐步演进，不需要一次性大重构。

## 安装

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## MySQL 配置

Web 服务需要 MySQL 连接串：

```bash
export QUANT_DATABASE_URL='mysql+pymysql://root:1234@127.0.0.1:3306/quant?charset=utf8mb4'
```

如果需要初始化 MySQL schema：

```bash
/usr/local/mysql/bin/mysql -uroot -p1234 --local-infile=1 < sql/schema_mysql.sql
```

如果需要从现有本地 CSV 导入 MySQL：

```bash
python3 scripts/import_mysql_data.py --password 1234
```

## 常用命令

### 启动 Web 工作台

```bash
export QUANT_DATABASE_URL='mysql+pymysql://root:1234@127.0.0.1:3306/quant?charset=utf8mb4'

python3 main.py \
  --mode serve \
  --host 127.0.0.1 \
  --port 8000
```

然后打开 [http://127.0.0.1:8000](http://127.0.0.1:8000)。

### 首次全量下载历史日线

```bash
python3 main.py \
  --mode download-daily \
  --start 2015-01-01 \
  --end 2026-04-09 \
  --workers 8 \
  --skip-existing-store
```

### 每日增量更新

```bash
python3 main.py \
  --mode update-daily \
  --end 2026-04-09 \
  --workers 8
```

日常增量同步由数据库状态驱动：系统会读取每只股票在 MySQL 中的最新日线日期，已有数据的股票只从 `latest_db_date + 1` 补到目标日期；只有空库或新股票才使用 `--start` 作为历史回填起点。Web 工作台的“同步到最新”不再要求填写起始日期。

日线同步现在通过 provider/router 层选择数据源，默认优先使用 `mootdx` 通达信接口获取前复权日线，第二优先级使用腾讯财经，第三优先级使用 `baostock`，新浪仅作为兜底源；东财行情源不进入默认同步链路。同步会把真实 `data_source` 和 `quality_flags` 写入 MySQL，便于排查上游缺成交量、缺成交额、字段变化或局部断供。

当前统一口径为：`volume=手`、`amount=元`、`turnover=百分数数值`。`mootdx` 负责主行情，`baostock` 负责历史换手率修复，腾讯直连 quote 负责最近交易日的实时换手率和成交额补充。

### 日线质量修复

```bash
python3 main.py \
  --mode repair-daily-quality \
  --start 2020-01-01 \
  --end 2026-04-09 \
  --workers 4
```

该模式会扫描 MySQL 中 OHLC 缺失/异常、成交量缺失、成交额缺失的日线记录，按股票生成修复队列，并通过同一套 provider 重新拉取后覆盖修复。建议在每日增量更新后或次日盘前运行一次。

如果历史库存在成交量单位混乱、成交额异常或历史换手率缺失，可以使用 `scripts/repair_daily_units.py` 做一次系统性校准。脚本会用 `mootdx` 重拉前复权 OHLCV/成交额，用 `baostock` 补历史换手率，并在最新交易日可用时用腾讯实时 quote 补最近一日换手率。

Web 工作台还提供“修复最近日线”，默认扫描最近 10 个交易日附近的异常记录，适合修补最近几天上游延迟、缺成交量或接口短暂失败造成的坏数据。

### 日终选股

```bash
python3 main.py \
  --mode screen \
  --trade-date 2026-04-09 \
  --price-ma-ratio 0.9 \
  --min-dividend-yield 5 \
  --max-stocks 0 \
  --output data/screen_2026-04-09.csv
```

### 简单回测

```bash
python3 main.py \
  --mode backtest \
  --start 2020-01-01 \
  --end 2024-12-31 \
  --min-dividend-yield 5 \
  --rebalance-days 20 \
  --max-stocks 100
```

## 当前边界

- 当前数据频率以日线为主，还不是实时行情系统
- 数据源主要依赖 `AKShare`，第三方接口偶尔会失败
- 北交所部分股票接口稳定性相对差一些
- 选股和回测仍是研究原型，不是实盘交易系统
- Web 任务目前还有同步请求，后续应改成后台任务状态模型
- agent 模块尚未接入，现阶段先保留清晰的 API 和任务记录边界

## 目录结构

```text
quant/
├── data/
│   ├── cache/
│   ├── daily_store/
│   └── reports/
├── docs/
│   └── WORKBENCH_ARCHITECTURE.md
├── main.py
├── requirements.txt
├── scripts/
│   └── import_mysql_data.py
├── sql/
│   └── schema_mysql.sql
└── src/
    ├── quant_backtest/
    │   ├── backtest.py
    │   ├── data.py
    │   └── strategy.py
    └── quant_web/
        ├── app.py
        ├── db.py
        └── service.py
```

## 下一步优先级

- 把 Web 的长耗时任务改成后台任务和状态轮询
- 增加自选股、关注列表和股票笔记
- 给行情页增加均线、成交量、复权切换和区间选择
- 把选股策略抽象成可配置规则
- 为投资助手 agent 设计工具调用协议、权限边界和运行日志
