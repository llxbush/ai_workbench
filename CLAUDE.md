# 个人投资工作台 (Quant Workbench)

本地 A 股投资研究桌面：行情数据维护、选股回测、Web 工作台，以及为后续投资助手 agent 预留的统一出口。

## 技术栈

- Python 3.14 + FastAPI + uvicorn
- MySQL (SQLAlchemy 2.0 ORM)，连接串通过 `QUANT_DATABASE_URL` 环境变量配置
- 数据源：mootdx (通达信，主) → 腾讯财经 → baostock → 新浪 (兜底)
- 日线统一口径：volume=手、amount=元、turnover=百分数数值

## 项目结构

```
main.py                  # CLI 入口，mode: serve / download-daily / update-daily / repair-daily-quality / backtest / screen
src/quant_web/           # Web 工作台 (FastAPI)
  app.py                 # API 路由 + 内嵌单页前端 (Canvas K 线图、选股、agent 对话)
  db.py                  # ORM 模型: Instrument, DailyBar, SyncRun, SyncRunItem
  service.py             # 业务逻辑：日线同步、质量修复、选股、股票搜索
src/quant_data/          # 数据管道（新模块，替代老的 akshare 直接调用）
  providers/base.py      # Provider 协议 + 熔断器 + 限速器
  providers/daily_bars.py  # mootdx 通达信 + 东财
  providers/baostock_daily.py
  providers/tencent_quote.py  # 腾讯实时行情
  router.py              # 多源路由，按优先级 + 熔断自动切换
  quality.py             # 日线质量评估和标准化
src/quant_backtest/      # 回测引擎 (backtest.py, data.py, strategy.py)
scripts/                 # 运维脚本
sql/schema_mysql.sql     # MySQL schema
```

## 常用命令

```bash
# 启动 Web
python3 main.py --mode serve --host 127.0.0.1 --port 8000

# 增量同步日线
python3 main.py --mode update-daily --end 2026-05-16 --workers 8

# 质量修复
python3 main.py --mode repair-daily-quality --start 2020-01-01 --end 2026-05-16 --workers 4

# 选股
python3 main.py --mode screen --trade-date 2026-05-16 --price-ma-ratio 0.9 --min-dividend-yield 5

# 回测
python3 main.py --mode backtest --start 2020-01-01 --end 2024-12-31 --min-dividend-yield 5
```

## 编码约定

- 所有模块用 `from __future__ import annotations`，类型注解用延迟求值
- ORM 模型定义在 `src/quant_web/db.py`，Mapped 风格
- 数据同步通过 `DailyBarRouter` 多源路由，不要直接调单个 provider
- 同步结果状态：created / updated / up_to_date / no_new_data / suspended / failed
- `run_daily_update_and_ingest` 由数据库状态驱动：已有数据的股票从 latest_db_date+1 补，空库才用 --start 回填
- 同步进度通过 `sync_runs` 表实时写入（processed_symbols/current_symbol/progress_percent 等），前端 1.5s 轮询读取
- 拉取数据的 worker 线程不共享 DB session，主线程负责写库和更新进度
- 每只股票处理完立刻 commit 进度，方便前端即时感知
- 页面刷新时会通过 `/api/tasks/update-daily/current` 恢复运行中任务的进度条
- Web 前端是内嵌在 app.py 里的单页 HTML，Canvas 手绘 K 线图
- 长期目标是把代码拆成 README 里描述的六个独立包，但按功能增长逐步演进，不做一次性大重构

## 当前边界

- 日线为主，不是实时行情系统
- 选股和回测是研究原型，不是实盘交易系统
- Web 日线同步已改成后台线程 + 前端轮询进度，进度通过 sync_runs 表实时读写。其他任务待改造
- agent 模块尚未接入，保留 API 和任务记录边界
- re_agent 是独立服务（默认 http://127.0.0.1:8010），quant 通过 `/api/re-agent/chat` 代理调用
