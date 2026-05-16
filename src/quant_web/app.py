from __future__ import annotations

import json
import os
import socket
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from os import getenv
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from .db import DEFAULT_DATABASE_URL, init_db, safe_database_url
from .service import (
    get_overview,
    get_stock_bars,
    run_daily_quality_repair,
    run_daily_update_and_ingest,
    run_screen,
    search_stocks,
)


class UpdateRequest(BaseModel):
    end_date: str = ""
    backfill_start_date: str = "2010-01-01"
    start_date: str | None = None
    workers: int = Field(default=8, ge=1, le=32)
    max_stocks: int = Field(default=0, ge=0)


class RepairRecentRequest(BaseModel):
    end_date: str = ""
    lookback_days: int = Field(default=10, ge=1, le=120)
    workers: int = Field(default=4, ge=1, le=32)
    max_stocks: int = Field(default=0, ge=0)


class ScreenRequest(BaseModel):
    trade_date: str
    min_dividend_yield: float = 5.0
    price_to_ma_ratio: float = 0.9
    max_stocks: int = 0


class ReAgentMessage(BaseModel):
    role: str
    content: str


class ReAgentChatRequest(BaseModel):
    query: str = Field(min_length=1)
    stock: str = ""
    conversation_id: str | None = None
    messages: list[ReAgentMessage] = Field(default_factory=list)


_RESTART_LOCK = threading.Lock()
_RESTART_SCHEDULED = False


def create_app(database_url: str = DEFAULT_DATABASE_URL) -> FastAPI:
    init_db(database_url=database_url)
    app = FastAPI(title="Quant Service", version="0.1.0")
    app.state.database_url = database_url
    app.state.re_agent_base_url = getenv("RE_AGENT_BASE_URL", "http://127.0.0.1:8010")
    app.state.re_agent_chat_path = getenv("RE_AGENT_CHAT_PATH", "/research/single-symbol")
    app.state.started_at = datetime.now(timezone.utc)
    app.state.started_monotonic = time.monotonic()

    @app.get("/api/health")
    def health() -> dict:
        return {"status": "ok", "database_url": safe_database_url(app.state.database_url)}

    @app.get("/api/overview")
    def overview() -> dict:
        return get_overview(database_url=app.state.database_url)

    @app.get("/api/services/status")
    def services_status() -> dict:
        return _get_services_status(app)

    @app.post("/api/services/quant/restart")
    def restart_quant() -> dict:
        _schedule_self_restart()
        return {
            "ok": True,
            "service": "quant",
            "status": "restarting",
            "message": "quant service restart has been scheduled.",
        }

    @app.get("/api/stocks/search")
    def stocks_search(
        q: str = Query(min_length=1),
        limit: int = Query(default=10, ge=1, le=50),
    ) -> dict:
        return search_stocks(query=q, limit=limit, database_url=app.state.database_url)

    @app.get("/api/stocks/bars")
    def stocks_bars(
        q: str = Query(min_length=1),
        adjust: str = Query(default="qfq", pattern="^(qfq|hfq|)$"),
        limit: int = Query(default=180, ge=1, le=1200),
    ) -> dict:
        return get_stock_bars(
            query=q,
            adjust=adjust,
            limit=limit,
            database_url=app.state.database_url,
        )

    @app.post("/api/tasks/update-daily")
    def update_daily(payload: UpdateRequest) -> dict:
        end_date = payload.end_date or datetime.now().date().isoformat()
        backfill_start_date = payload.start_date or payload.backfill_start_date or "2010-01-01"
        return run_daily_update_and_ingest(
            end_date=end_date,
            start_date=backfill_start_date,
            workers=payload.workers,
            max_stocks=payload.max_stocks,
            database_url=app.state.database_url,
            cache_dir=Path("data") / "cache",
        )

    @app.post("/api/tasks/repair-daily-recent")
    def repair_daily_recent(payload: RepairRecentRequest) -> dict:
        end_date = datetime.fromisoformat(payload.end_date).date() if payload.end_date else datetime.now().date()
        start_date = end_date - timedelta(days=max(payload.lookback_days * 2, payload.lookback_days))
        result = run_daily_quality_repair(
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            workers=payload.workers,
            max_stocks=payload.max_stocks,
            database_url=app.state.database_url,
            cache_dir=Path("data") / "cache",
        )
        result["repair_start_date"] = start_date.isoformat()
        result["repair_end_date"] = end_date.isoformat()
        result["lookback_days"] = payload.lookback_days
        return result

    @app.post("/api/tasks/screen")
    def screen(payload: ScreenRequest) -> dict:
        return run_screen(
            trade_date=payload.trade_date,
            min_dividend_yield=payload.min_dividend_yield,
            price_to_ma_ratio=payload.price_to_ma_ratio,
            max_stocks=payload.max_stocks,
            database_url=app.state.database_url,
        )

    @app.post("/api/re-agent/chat")
    def re_agent_chat(payload: ReAgentChatRequest) -> dict:
        return _call_re_agent_chat(
            payload=payload,
            base_url=app.state.re_agent_base_url,
            chat_path=app.state.re_agent_chat_path,
            database_url=app.state.database_url,
        )

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return """
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>个人投资工作台</title>
  <style>
    :root {
      --bg: #f4efe7;
      --panel: rgba(255,255,255,0.78);
      --line: #d7c8b6;
      --text: #231c16;
      --muted: #6a5b4d;
      --accent: #b04a2f;
      --accent-2: #1f6c5c;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Avenir Next", "PingFang SC", "Hiragino Sans GB", sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(176,74,47,0.18), transparent 28%),
        radial-gradient(circle at top right, rgba(31,108,92,0.16), transparent 30%),
        linear-gradient(135deg, #f7f2ea, #efe4d2 55%, #e7d7c4);
      min-height: 100vh;
    }
    .wrap {
      max-width: 1100px;
      margin: 0 auto;
      padding: 32px 20px 60px;
    }
    .hero { margin-bottom: 22px; }
    h1 { margin: 0 0 8px; font-size: 42px; letter-spacing: 0.02em; }
    .sub { color: var(--muted); max-width: 760px; line-height: 1.6; }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 16px;
      margin-top: 18px;
    }
    .card {
      background: var(--panel);
      backdrop-filter: blur(12px);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 18px;
      box-shadow: 0 12px 30px rgba(35, 28, 22, 0.08);
    }
    .card h2 { margin: 0 0 12px; font-size: 20px; }
    .stat { font-size: 28px; font-weight: 700; margin: 6px 0; }
    label { display: block; margin-top: 10px; font-size: 14px; color: var(--muted); }
    input, textarea {
      width: 100%;
      margin-top: 6px;
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 10px 12px;
      background: rgba(255,255,255,0.85);
      color: var(--text);
      font: inherit;
    }
    textarea {
      min-height: 92px;
      resize: vertical;
      line-height: 1.55;
    }
    button {
      margin-top: 14px;
      border: 0;
      border-radius: 999px;
      padding: 11px 16px;
      background: var(--accent);
      color: white;
      cursor: pointer;
      font-weight: 700;
    }
    button:disabled {
      opacity: 0.6;
      cursor: wait;
    }
    button.secondary { background: var(--accent-2); }
    pre {
      white-space: pre-wrap;
      word-break: break-word;
      background: rgba(35, 28, 22, 0.06);
      padding: 12px;
      border-radius: 12px;
      max-height: 360px;
      overflow: auto;
    }
    table { width: 100%; border-collapse: collapse; font-size: 14px; }
    th, td {
      text-align: left;
      padding: 8px 6px;
      border-bottom: 1px solid rgba(35, 28, 22, 0.08);
    }
    .wide { grid-column: 1 / -1; }
    .quote-toolbar {
      display: grid;
      grid-template-columns: minmax(220px, 1fr) 120px auto;
      gap: 10px;
      align-items: end;
    }
    .quote-toolbar button { width: 100%; }
    .quote-meta {
      display: flex;
      gap: 14px;
      flex-wrap: wrap;
      margin: 12px 0;
      color: var(--muted);
      font-size: 14px;
    }
    .quote-meta strong {
      color: var(--text);
      font-size: 18px;
    }
    .search-results {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-top: 12px;
    }
    .stock-chip {
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 7px 10px;
      margin-top: 0;
      background: rgba(255,255,255,0.68);
      color: var(--text);
      cursor: pointer;
      font-size: 13px;
    }
    .stock-chip:hover { border-color: var(--accent-2); color: var(--accent-2); }
    .chart-frame {
      position: relative;
      margin-top: 12px;
      border: 1px solid rgba(35, 28, 22, 0.1);
      border-radius: 14px;
      background: rgba(255,255,255,0.52);
      overflow: hidden;
    }
    #klineCanvas {
      display: block;
      width: 100%;
      height: 420px;
    }
    .chart-empty {
      position: absolute;
      inset: 0;
      display: grid;
      place-items: center;
      color: var(--muted);
      pointer-events: none;
    }
    .quote-table {
      max-height: 260px;
      overflow: auto;
      margin-top: 12px;
    }
    .agent-panel {
      display: grid;
      grid-template-columns: minmax(0, 1fr) 280px;
      gap: 14px;
      align-items: stretch;
    }
    .agent-thread {
      height: 420px;
      overflow: auto;
      border: 1px solid rgba(35, 28, 22, 0.1);
      border-radius: 14px;
      background: rgba(255,255,255,0.44);
      padding: 14px;
    }
    .agent-message {
      max-width: 82%;
      margin: 0 0 12px;
      padding: 11px 13px;
      border-radius: 14px;
      white-space: pre-wrap;
      line-height: 1.55;
      font-size: 14px;
    }
    .agent-user {
      margin-left: auto;
      background: var(--accent-2);
      color: white;
      border-bottom-right-radius: 5px;
    }
    .agent-assistant {
      margin-right: auto;
      background: rgba(35, 28, 22, 0.07);
      color: var(--text);
      border-bottom-left-radius: 5px;
    }
    .agent-controls {
      display: flex;
      flex-direction: column;
    }
    .agent-controls button { width: 100%; }
    .agent-meta {
      margin-top: 10px;
      min-height: 18px;
      color: var(--muted);
      font-size: 13px;
    }
    .service-list {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 12px;
    }
    .service-item {
      border: 1px solid rgba(35, 28, 22, 0.1);
      border-radius: 12px;
      background: rgba(255,255,255,0.48);
      padding: 12px;
    }
    .service-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 8px;
    }
    .service-name {
      font-weight: 700;
      font-size: 16px;
    }
    .status-pill {
      border-radius: 999px;
      padding: 4px 9px;
      font-size: 12px;
      font-weight: 700;
      background: rgba(106,91,77,0.14);
      color: var(--muted);
      white-space: nowrap;
    }
    .status-pill.ok {
      background: rgba(31,108,92,0.14);
      color: var(--accent-2);
    }
    .status-pill.bad {
      background: rgba(176,74,47,0.14);
      color: var(--accent);
    }
    .service-meta {
      color: var(--muted);
      font-size: 13px;
      line-height: 1.6;
      min-height: 42px;
    }
    .service-actions {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-top: 10px;
    }
    .service-actions button { margin-top: 0; }
    @media (max-width: 760px) {
      .quote-toolbar { grid-template-columns: 1fr; }
      .agent-panel { grid-template-columns: 1fr; }
      .agent-thread { height: 360px; }
      .agent-message { max-width: 92%; }
      #klineCanvas { height: 340px; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <h1>个人投资工作台</h1>
      <div class="sub">这是一个给自己用的本地投资研究入口：维护 MySQL 行情数据、查看单股 K 线、运行选股和回测任务，并为后续投资助手 agent 预留统一出口。</div>
    </div>

    <div class="grid">
      <div class="card wide">
        <h2>服务状态</h2>
        <div id="serviceStatus" class="service-list"></div>
        <div class="service-actions">
          <button id="serviceRefreshButton" class="secondary" onclick="loadServiceStatus()">刷新状态</button>
          <button id="quantRestartButton" onclick="restartQuantService()">重启 quant</button>
        </div>
      </div>
    </div>

    <div class="grid">
      <div class="card">
        <h2>数据库概览</h2>
        <div class="stat" id="totalRows">-</div>
        <div id="overviewMeta" class="sub">正在加载...</div>
        <button id="overviewButton" class="secondary" onclick="loadOverview()">刷新概览</button>
      </div>

      <div class="card">
        <h2>增量同步日线</h2>
        <label>线程数</label>
        <input id="updateWorkers" value="8" />
        <label>最大股票数</label>
        <input id="updateMaxStocks" value="0" />
        <button id="updateButton" onclick="runUpdate()">同步到最新</button>
        <label>修复最近 N 日</label>
        <input id="repairLookbackDays" value="10" />
        <button id="repairRecentButton" class="secondary" onclick="runRecentRepair()">修复最近日线</button>
      </div>

      <div class="card">
        <h2>日终选股</h2>
        <label>交易日</label>
        <input id="screenTradeDate" value="" />
        <label>最低股息率</label>
        <input id="screenDividend" value="5" />
        <label>价格 / MA120 阈值</label>
        <input id="screenPriceRatio" value="0.9" />
        <button id="screenButton" onclick="runScreen()">执行选股</button>
      </div>
    </div>

    <div class="grid">
      <div class="card wide">
        <h2>股票行情</h2>
        <div class="quote-toolbar">
          <div>
            <label>股票代码或名称</label>
            <input id="stockQuery" value="000001" placeholder="例如 000001 或 平安银行" onkeydown="handleStockKey(event)" />
          </div>
          <div>
            <label>K 线数量</label>
            <input id="stockLimit" value="180" />
          </div>
          <button id="stockButton" class="secondary" onclick="loadStockQuote()">查看行情</button>
        </div>
        <div id="stockSearchResults" class="search-results"></div>
        <div id="quoteMeta" class="quote-meta">输入股票代码或名称后查看本地 MySQL 行情。</div>
        <div class="chart-frame">
          <canvas id="klineCanvas"></canvas>
          <div id="chartEmpty" class="chart-empty">暂无 K 线数据</div>
        </div>
        <div id="quoteTable" class="quote-table sub">最近行情会显示在这里。</div>
      </div>
    </div>

    <div class="grid">
      <div class="card wide">
        <h2>研究助手</h2>
        <div class="agent-panel">
          <div id="agentThread" class="agent-thread"></div>
          <div class="agent-controls">
            <label>股票代码或名称</label>
            <input id="agentStock" value="000001" placeholder="例如 000001 或 平安银行" />
            <label>问题</label>
            <textarea id="agentInput" placeholder="研究一下这只股票的基本面、风险和近期走势"></textarea>
            <button id="agentButton" class="secondary" onclick="sendAgentMessage()">发送</button>
            <button class="stock-chip" onclick="resetAgentChat()">新对话</button>
            <div id="agentMeta" class="agent-meta">等待 re_agent 连接。</div>
          </div>
        </div>
      </div>
    </div>

    <div class="grid">
      <div class="card">
        <h2>最近任务</h2>
        <div id="runs">暂无数据</div>
      </div>
      <div class="card">
        <h2>操作结果</h2>
        <pre id="resultBox">等待操作...</pre>
      </div>
    </div>

    <div class="card" style="margin-top:16px;">
      <h2>选股结果</h2>
      <div id="screenTable" class="sub">执行一次选股后，这里会显示结果。</div>
    </div>
  </div>
  <script>
    function formatLocalDate(date = new Date()) {
      const year = date.getFullYear();
      const month = String(date.getMonth() + 1).padStart(2, "0");
      const day = String(date.getDate()).padStart(2, "0");
      return `${year}-${month}-${day}`;
    }

    const today = formatLocalDate();
    document.getElementById("screenTradeDate").value = today;
    let agentMessages = [];
    let agentConversationId = null;

    async function api(path, method = "GET", payload) {
      const res = await fetch(path, {
        method,
        headers: {"Content-Type": "application/json"},
        body: payload ? JSON.stringify(payload) : undefined
      });
      const text = await res.text();
      let data;
      try {
        data = text ? JSON.parse(text) : {};
      } catch {
        data = {raw: text};
      }
      if (!res.ok) {
        const message = data.detail || data.error || data.raw || `HTTP ${res.status}`;
        throw new Error(message);
      }
      return data;
    }

    function showResult(data) {
      document.getElementById("resultBox").textContent = JSON.stringify(data, null, 2);
    }

    function showError(action, error) {
      showResult({
        ok: false,
        action,
        error: error?.message || String(error)
      });
    }

    async function withButton(buttonId, label, action, task) {
      const button = document.getElementById(buttonId);
      const original = button.textContent;
      button.disabled = true;
      button.textContent = label;
      showResult({ok: true, action, status: "running"});
      try {
        return await task();
      } catch (error) {
        showError(action, error);
        throw error;
      } finally {
        button.disabled = false;
        button.textContent = original;
      }
    }

    function formatUptime(seconds) {
      if (!Number.isFinite(seconds)) return "-";
      const total = Math.max(0, Math.floor(seconds));
      const hours = Math.floor(total / 3600);
      const minutes = Math.floor((total % 3600) / 60);
      if (hours) return `${hours} 小时 ${minutes} 分钟`;
      if (minutes) return `${minutes} 分钟`;
      return `${total} 秒`;
    }

    function renderServiceStatus(data) {
      const root = document.getElementById("serviceStatus");
      const quant = data.quant || {};
      const reAgent = data.re_agent || {};
      const items = [
        {
          name: "quant",
          status: quant.status,
          pill: quant.status === "running" ? "运行中" : "异常",
          meta: [
            `PID ${quant.pid || "-"}`,
            `已运行 ${formatUptime(quant.uptime_seconds)}`,
            `页面服务 ${quant.start_supported ? "可启动" : "需外部启动"}`
          ]
        },
        {
          name: "re_agent",
          status: reAgent.status,
          pill: reAgent.status === "running" ? "运行中" : "未连接",
          meta: [
            reAgent.base_url || "-",
            reAgent.error || `端口 ${reAgent.port || "-"}`
          ]
        }
      ];
      root.innerHTML = items.map(item => {
        const cls = item.status === "running" ? "ok" : "bad";
        return `<div class="service-item">
          <div class="service-head">
            <div class="service-name">${escapeHtml(item.name)}</div>
            <div class="status-pill ${cls}">${escapeHtml(item.pill)}</div>
          </div>
          <div class="service-meta">${item.meta.map(line => `<div>${escapeHtml(line)}</div>`).join("")}</div>
        </div>`;
      }).join("");
    }

    async function loadServiceStatus() {
      await withButton("serviceRefreshButton", "刷新中...", "service_status", async () => {
        const data = await api("/api/services/status");
        renderServiceStatus(data);
        showResult(data);
      });
    }

    async function restartQuantService() {
      if (!confirm("确认重启 quant 服务？页面会短暂断开。")) {
        return;
      }
      await withButton("quantRestartButton", "重启中...", "quant_restart", async () => {
        const data = await api("/api/services/quant/restart", "POST", {});
        showResult(data);
        document.getElementById("serviceStatus").innerHTML = `<div class="service-item"><div class="service-head"><div class="service-name">quant</div><div class="status-pill">重启中</div></div><div class="service-meta">等待服务重新可用...</div></div>`;
        setTimeout(waitForQuantService, 1400);
      });
    }

    async function waitForQuantService(attempt = 1) {
      try {
        const data = await api("/api/services/status");
        renderServiceStatus(data);
        showResult({ok: true, action: "quant_restart", status: "ready", attempts: attempt});
        await loadOverview();
      } catch (error) {
        if (attempt < 20) {
          setTimeout(() => waitForQuantService(attempt + 1), 1000);
        } else {
          showError("quant_restart", error);
        }
      }
    }

    async function loadOverview() {
      await withButton("overviewButton", "刷新中...", "loadOverview", async () => {
        const data = await api("/api/overview");
        document.getElementById("totalRows").textContent = `${data.total_rows} rows`;
        document.getElementById("overviewMeta").textContent = `股票数 ${data.total_symbols} | 最新交易日 ${data.latest_trade_date || "-"}`;
        const runs = data.recent_runs.length
          ? `<table><thead><tr><th>任务</th><th>状态</th><th>开始</th><th>详情</th></tr></thead><tbody>${data.recent_runs.map(run => `<tr><td>${run.task_name}</td><td>${run.status}</td><td>${run.started_at || ""}</td><td>${run.detail || ""}</td></tr>`).join("")}</tbody></table>`
          : "暂无任务记录";
        document.getElementById("runs").innerHTML = runs;
        showResult(data);
      });
    }

    async function runUpdate() {
      await withButton("updateButton", "同步中...", "update_daily", async () => {
        const data = await api("/api/tasks/update-daily", "POST", {
          workers: Number(document.getElementById("updateWorkers").value || 8),
          max_stocks: Number(document.getElementById("updateMaxStocks").value || 0)
        });
        showResult(data);
        await loadOverview();
      });
    }

    async function runRecentRepair() {
      await withButton("repairRecentButton", "修复中...", "repair_daily_recent", async () => {
        const data = await api("/api/tasks/repair-daily-recent", "POST", {
          lookback_days: Number(document.getElementById("repairLookbackDays").value || 10),
          workers: Number(document.getElementById("updateWorkers").value || 4),
          max_stocks: Number(document.getElementById("updateMaxStocks").value || 0)
        });
        showResult(data);
        await loadOverview();
      });
    }

    async function runScreen() {
      await withButton("screenButton", "选股中...", "screen", async () => {
        const data = await api("/api/tasks/screen", "POST", {
          trade_date: document.getElementById("screenTradeDate").value,
          min_dividend_yield: Number(document.getElementById("screenDividend").value || 5),
          price_to_ma_ratio: Number(document.getElementById("screenPriceRatio").value || 0.9),
          max_stocks: 0
        });
        showResult(data);
        const rows = data.candidates || [];
        if (!rows.length) {
          document.getElementById("screenTable").innerHTML = "本次没有筛出股票。";
          return;
        }
        const head = ["symbol", "name", "trade_date", "close", "ma120", "dividend_yield", "pe_ttm"];
        const html = `<table><thead><tr>${head.map(col => `<th>${col}</th>`).join("")}</tr></thead><tbody>${rows.slice(0, 50).map(row => `<tr>${head.map(col => `<td>${row[col] ?? ""}</td>`).join("")}</tr>`).join("")}</tbody></table>`;
        document.getElementById("screenTable").innerHTML = html;
      });
    }

    function resetAgentChat() {
      agentMessages = [];
      agentConversationId = null;
      document.getElementById("agentInput").value = "";
      document.getElementById("agentMeta").textContent = "新对话已准备。";
      renderAgentThread();
    }

    async function sendAgentMessage() {
      const stockInput = document.getElementById("agentStock");
      const input = document.getElementById("agentInput");
      const stock = stockInput.value.trim();
      const query = input.value.trim();
      if (!query) {
        showResult({ok: false, action: "re_agent_chat", error: "请输入问题"});
        return;
      }
      if (stock) {
        stockInput.value = stock;
      }
      input.value = "";
      agentMessages.push({role: "user", content: query});
      const pendingIndex = agentMessages.push({role: "assistant", content: "思考中..."}) - 1;
      renderAgentThread();

      await withButton("agentButton", "发送中...", "re_agent_chat", async () => {
        try {
          const data = await api("/api/re-agent/chat", "POST", {
            query,
            stock,
            conversation_id: agentConversationId,
            messages: agentMessages.filter((_, index) => index !== pendingIndex)
          });
          agentConversationId = data.conversation_id || agentConversationId;
          agentMessages[pendingIndex] = {role: "assistant", content: data.answer || "没有返回内容。"};
          document.getElementById("agentMeta").textContent = agentConversationId ? `会话 ${agentConversationId}` : "已回复";
          showResult({answer: data.answer, conversation_id: agentConversationId});
        } catch (error) {
          agentMessages[pendingIndex] = {role: "assistant", content: error?.message || String(error)};
          document.getElementById("agentMeta").textContent = "re_agent 调用失败。";
          throw error;
        } finally {
          renderAgentThread();
        }
      });
    }

    function renderAgentThread() {
      const thread = document.getElementById("agentThread");
      if (!agentMessages.length) {
        thread.innerHTML = `<div class="agent-message agent-assistant">请输入股票和研究问题。</div>`;
        return;
      }
      thread.innerHTML = agentMessages.map(message => {
        const roleClass = message.role === "user" ? "agent-user" : "agent-assistant";
        return `<div class="agent-message ${roleClass}">${escapeHtml(message.content)}</div>`;
      }).join("");
      thread.scrollTop = thread.scrollHeight;
    }

    function handleStockKey(event) {
      if (event.key === "Enter") {
        loadStockQuote();
      }
    }

    async function loadStockQuote(queryOverride) {
      const queryInput = document.getElementById("stockQuery");
      const query = queryOverride || queryInput.value.trim();
      if (!query) {
        showResult({ok: false, action: "stock_quote", error: "请输入股票代码或名称"});
        return;
      }
      queryInput.value = query;
      await withButton("stockButton", "加载中...", "stock_quote", async () => {
        const limit = Number(document.getElementById("stockLimit").value || 180);
        const search = await api(`/api/stocks/search?q=${encodeURIComponent(query)}&limit=8`);
        renderStockSearchResults(search.results || []);
        const data = await api(`/api/stocks/bars?q=${encodeURIComponent(query)}&limit=${encodeURIComponent(limit)}&adjust=qfq`);
        showResult({
          instrument: data.instrument,
          count: data.count,
          latest: data.latest,
          change: data.change,
          change_pct: data.change_pct
        });
        renderQuote(data);
      });
    }

    function renderStockSearchResults(results) {
      const box = document.getElementById("stockSearchResults");
      if (!results.length) {
        box.innerHTML = "";
        return;
      }
      box.innerHTML = results.map(stock => (
        `<button class="stock-chip" onclick="loadStockQuote('${escapeAttr(stock.symbol)}')">${escapeHtml(stock.symbol)} ${escapeHtml(stock.name || "")}</button>`
      )).join("");
    }

    function renderQuote(data) {
      const meta = document.getElementById("quoteMeta");
      const table = document.getElementById("quoteTable");
      const empty = document.getElementById("chartEmpty");
      if (!data.instrument) {
        meta.textContent = data.message || "没有找到匹配的股票。";
        table.textContent = "暂无数据。";
        empty.style.display = "grid";
        drawKline([]);
        return;
      }
      const latest = data.latest;
      const changeText = data.change == null ? "-" : `${formatNumber(data.change, 2)} (${formatNumber(data.change_pct, 2)}%)`;
      meta.innerHTML = [
        `<span><strong>${escapeHtml(data.instrument.symbol)} ${escapeHtml(data.instrument.name)}</strong></span>`,
        `<span>市场 ${escapeHtml(data.instrument.exchange_code || data.instrument.market || "-")}</span>`,
        `<span>最新 ${latest ? escapeHtml(latest.date) : "-"}</span>`,
        `<span>收盘 ${latest ? formatNumber(latest.close, 2) : "-"}</span>`,
        `<span>涨跌 ${changeText}</span>`,
        `<span>K 线 ${data.count || 0} 条</span>`
      ].join("");
      empty.style.display = data.bars?.length ? "none" : "grid";
      drawKline(data.bars || []);
      renderQuoteTable(data.bars || []);
    }

    function renderQuoteTable(bars) {
      const table = document.getElementById("quoteTable");
      if (!bars.length) {
        table.textContent = "暂无数据。";
        return;
      }
      const rows = [...bars].slice(-30).reverse();
      table.innerHTML = `<table><thead><tr><th>日期</th><th>开</th><th>高</th><th>低</th><th>收</th><th>成交量</th><th>换手率</th></tr></thead><tbody>${rows.map(row => (
        `<tr><td>${escapeHtml(row.date)}</td><td>${formatNumber(row.open, 2)}</td><td>${formatNumber(row.high, 2)}</td><td>${formatNumber(row.low, 2)}</td><td>${formatNumber(row.close, 2)}</td><td>${formatNumber(row.volume, 0)}</td><td>${formatNumber(row.turnover, 2)}</td></tr>`
      )).join("")}</tbody></table>`;
    }

    function drawKline(bars) {
      window.lastQuoteBars = bars;
      const canvas = document.getElementById("klineCanvas");
      const ctx = canvas.getContext("2d");
      const rect = canvas.getBoundingClientRect();
      const ratio = window.devicePixelRatio || 1;
      canvas.width = Math.max(1, Math.floor(rect.width * ratio));
      canvas.height = Math.max(1, Math.floor(rect.height * ratio));
      ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
      ctx.clearRect(0, 0, rect.width, rect.height);
      ctx.fillStyle = "rgba(255,255,255,0.32)";
      ctx.fillRect(0, 0, rect.width, rect.height);
      if (!bars.length) {
        return;
      }

      const pad = {left: 56, right: 18, top: 18, bottom: 34};
      const volumeHeight = 76;
      const chartBottom = rect.height - pad.bottom - volumeHeight - 18;
      const volumeTop = chartBottom + 18;
      const chartWidth = rect.width - pad.left - pad.right;
      const highs = bars.map(row => Number(row.high)).filter(Number.isFinite);
      const lows = bars.map(row => Number(row.low)).filter(Number.isFinite);
      const volumes = bars.map(row => Number(row.volume)).filter(Number.isFinite);
      const maxPrice = Math.max(...highs);
      const minPrice = Math.min(...lows);
      const maxVolume = Math.max(...volumes, 1);
      const priceRange = Math.max(0.01, maxPrice - minPrice);
      const candleGap = chartWidth / bars.length;
      const candleWidth = Math.max(3, Math.min(12, candleGap * 0.58));
      const upColor = "#b23a2b";
      const downColor = "#207a66";
      const gridColor = "rgba(35, 28, 22, 0.1)";

      function priceY(price) {
        return pad.top + (maxPrice - price) / priceRange * (chartBottom - pad.top);
      }

      ctx.strokeStyle = gridColor;
      ctx.fillStyle = "rgba(35, 28, 22, 0.54)";
      ctx.font = "12px Avenir Next, sans-serif";
      ctx.lineWidth = 1;
      for (let i = 0; i <= 4; i += 1) {
        const y = pad.top + (chartBottom - pad.top) * i / 4;
        const price = maxPrice - priceRange * i / 4;
        ctx.beginPath();
        ctx.moveTo(pad.left, y);
        ctx.lineTo(rect.width - pad.right, y);
        ctx.stroke();
        ctx.fillText(formatNumber(price, 2), 8, y + 4);
      }

      bars.forEach((row, index) => {
        const open = Number(row.open);
        const close = Number(row.close);
        const high = Number(row.high);
        const low = Number(row.low);
        const volume = Number(row.volume || 0);
        if (![open, close, high, low].every(Number.isFinite)) return;
        const x = pad.left + candleGap * index + candleGap / 2;
        const color = close >= open ? upColor : downColor;
        const yOpen = priceY(open);
        const yClose = priceY(close);
        const yHigh = priceY(high);
        const yLow = priceY(low);
        const bodyTop = Math.min(yOpen, yClose);
        const bodyHeight = Math.max(1, Math.abs(yOpen - yClose));
        ctx.strokeStyle = color;
        ctx.fillStyle = color;
        ctx.beginPath();
        ctx.moveTo(x, yHigh);
        ctx.lineTo(x, yLow);
        ctx.stroke();
        ctx.fillRect(x - candleWidth / 2, bodyTop, candleWidth, bodyHeight);

        const volumeHeightValue = maxVolume ? volume / maxVolume * volumeHeight : 0;
        ctx.globalAlpha = 0.36;
        ctx.fillRect(x - candleWidth / 2, volumeTop + volumeHeight - volumeHeightValue, candleWidth, volumeHeightValue);
        ctx.globalAlpha = 1;
      });

      ctx.strokeStyle = gridColor;
      ctx.beginPath();
      ctx.moveTo(pad.left, volumeTop);
      ctx.lineTo(rect.width - pad.right, volumeTop);
      ctx.stroke();
      ctx.fillStyle = "rgba(35, 28, 22, 0.54)";
      const first = bars[0];
      const last = bars[bars.length - 1];
      ctx.fillText(first.date, pad.left, rect.height - 12);
      const lastText = last.date;
      const lastWidth = ctx.measureText(lastText).width;
      ctx.fillText(lastText, rect.width - pad.right - lastWidth, rect.height - 12);
    }

    function formatNumber(value, digits = 2) {
      const number = Number(value);
      if (!Number.isFinite(number)) return "-";
      return number.toLocaleString("zh-CN", {maximumFractionDigits: digits, minimumFractionDigits: digits});
    }

    function escapeHtml(value) {
      return String(value ?? "").replace(/[&<>"']/g, char => ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;"
      }[char]));
    }

    function escapeAttr(value) {
      return escapeHtml(value).replace(/`/g, "&#96;");
    }

    window.addEventListener("resize", () => {
      const query = document.getElementById("stockQuery").value.trim();
      if (query && window.lastQuoteBars) {
        drawKline(window.lastQuoteBars);
      }
    });

    document.getElementById("agentInput").addEventListener("keydown", event => {
      if (event.key === "Enter" && (event.metaKey || event.ctrlKey)) {
        sendAgentMessage();
      }
    });

    renderAgentThread();
    loadServiceStatus().catch(() => {});
    loadOverview();
    loadStockQuote("000001").catch(() => {});
  </script>
</body>
</html>
"""

    return app


def _get_services_status(app: FastAPI) -> dict:
    re_agent = _probe_tcp_service(app.state.re_agent_base_url)
    return {
        "quant": {
            "status": "running",
            "pid": os.getpid(),
            "started_at": app.state.started_at.isoformat(),
            "uptime_seconds": round(time.monotonic() - app.state.started_monotonic, 1),
            "restart_supported": True,
            "start_supported": False,
        },
        "re_agent": re_agent,
    }


def _probe_tcp_service(base_url: str) -> dict:
    parsed = urlparse(base_url)
    host = parsed.hostname
    port = parsed.port
    if not host:
        return {
            "status": "unknown",
            "base_url": base_url,
            "error": "base URL is not configured.",
        }
    if port is None:
        port = 443 if parsed.scheme == "https" else 80

    try:
        with socket.create_connection((host, port), timeout=0.8):
            pass
    except OSError as exc:
        return {
            "status": "down",
            "base_url": base_url,
            "host": host,
            "port": port,
            "error": str(exc),
        }
    return {
        "status": "running",
        "base_url": base_url,
        "host": host,
        "port": port,
    }


def _schedule_self_restart() -> None:
    global _RESTART_SCHEDULED
    with _RESTART_LOCK:
        if _RESTART_SCHEDULED:
            return
        _RESTART_SCHEDULED = True

    def restart() -> None:
        time.sleep(0.8)
        os.execv(sys.executable, [sys.executable, *sys.argv])

    thread = threading.Thread(target=restart, name="quant-self-restart", daemon=True)
    thread.start()


def _call_re_agent_chat(
    payload: ReAgentChatRequest,
    base_url: str,
    chat_path: str,
    database_url: str,
) -> dict:
    if not base_url:
        raise HTTPException(
            status_code=503,
            detail="RE_AGENT_BASE_URL is not configured for the research agent service.",
        )

    endpoint = urljoin(base_url.rstrip("/") + "/", chat_path.lstrip("/"))
    symbol = _resolve_re_agent_symbol(payload.stock, database_url=database_url)
    request_payload = {
        "symbol": symbol,
        "objective": payload.query,
    }
    body = json.dumps(request_payload, ensure_ascii=False).encode("utf-8")
    request = Request(
        endpoint,
        data=body,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )

    try:
        with urlopen(request, timeout=120) as response:
            response_text = response.read().decode("utf-8")
    except HTTPError as exc:
        error_text = exc.read().decode("utf-8", errors="replace")
        raise HTTPException(status_code=exc.code, detail=error_text or str(exc)) from exc
    except URLError as exc:
        raise HTTPException(status_code=502, detail=f"re_agent request failed: {exc.reason}") from exc
    except TimeoutError as exc:
        raise HTTPException(status_code=504, detail="re_agent request timed out") from exc

    try:
        data: Any = json.loads(response_text) if response_text else {}
    except json.JSONDecodeError:
        data = {"answer": response_text}

    answer = _extract_re_agent_answer(data)
    result = {
        "answer": answer,
        "conversation_id": _extract_re_agent_conversation_id(data, payload.conversation_id),
        "symbol": symbol,
    }
    if isinstance(data, dict):
        for key in ("rating", "confidence", "scope", "objective"):
            if key in data:
                result[key] = data[key]
    return result


def _extract_re_agent_answer(data: Any) -> str:
    if isinstance(data, str):
        return data
    if not isinstance(data, dict):
        return json.dumps(data, ensure_ascii=False)
    if any(key in data for key in ("rating", "thesis", "findings")):
        return _format_single_symbol_research(data)
    for key in ("answer", "content", "message", "response", "text", "result"):
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            return value
    message = data.get("message")
    if isinstance(message, dict):
        content = message.get("content")
        if isinstance(content, str) and content.strip():
            return content
    choices = data.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0]
        if isinstance(first, dict):
            choice_message = first.get("message")
            if isinstance(choice_message, dict) and isinstance(choice_message.get("content"), str):
                return choice_message["content"]
            if isinstance(first.get("text"), str):
                return first["text"]
    return json.dumps(data, ensure_ascii=False)


def _format_single_symbol_research(data: dict[str, Any]) -> str:
    parts: list[str] = []
    rating = data.get("rating")
    if rating:
        parts.append(f"评级：{rating}")
    confidence = data.get("confidence")
    if confidence is not None:
        parts.append(f"置信度：{confidence}")
    thesis = data.get("thesis")
    if thesis:
        parts.append(f"投资观点：\n{thesis}")
    findings = data.get("findings")
    if findings:
        parts.append(f"关键发现：\n{_format_findings(findings)}")
    risks = data.get("risks")
    if risks:
        parts.append(f"主要风险：\n{_format_findings(risks)}")
    open_questions = data.get("open_questions")
    if open_questions:
        parts.append(f"待确认问题：\n{_format_findings(open_questions)}")
    return "\n\n".join(parts) if parts else json.dumps(data, ensure_ascii=False)


def _format_findings(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        lines = []
        for item in value:
            if isinstance(item, str):
                lines.append(f"- {item}")
            elif isinstance(item, dict):
                title = item.get("title") or item.get("name") or item.get("metric") or ""
                detail = item.get("detail") or item.get("description") or item.get("value") or item
                lines.append(f"- {title}: {detail}" if title else f"- {detail}")
            else:
                lines.append(f"- {item}")
        return "\n".join(lines)
    return json.dumps(value, ensure_ascii=False)


def _resolve_re_agent_symbol(stock: str, database_url: str) -> str:
    stock_text = stock.strip()
    if stock_text.isdigit() and len(stock_text) <= 6:
        return stock_text.zfill(6)
    if stock_text:
        results = search_stocks(query=stock_text, limit=1, database_url=database_url).get("results", [])
        if results:
            return str(results[0]["symbol"])
    raise HTTPException(status_code=400, detail="请输入股票代码，或输入能匹配到唯一股票的名称。")


def _extract_re_agent_conversation_id(data: Any, fallback: str | None) -> str | None:
    if isinstance(data, dict):
        for key in ("conversation_id", "session_id", "thread_id", "id"):
            value = data.get(key)
            if isinstance(value, str) and value:
                return value
    return fallback
