CREATE DATABASE IF NOT EXISTS quant
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_ai_ci;

USE quant;

CREATE TABLE IF NOT EXISTS instruments (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    symbol VARCHAR(16) NOT NULL,
    name VARCHAR(64) NOT NULL,
    market VARCHAR(16) NULL,
    exchange_code VARCHAR(16) NULL,
    board VARCHAR(32) NULL,
    list_date DATE NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'listed',
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_instruments_symbol (symbol),
    KEY idx_instruments_market_board (market, board)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS trade_calendar (
    trade_date DATE NOT NULL,
    exchange_code VARCHAR(16) NOT NULL DEFAULT 'CN-A',
    is_open TINYINT(1) NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, exchange_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS daily_bars (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    instrument_id BIGINT UNSIGNED NOT NULL,
    trade_date DATE NOT NULL,
    adjust_type VARCHAR(8) NOT NULL DEFAULT 'qfq',
    open DECIMAL(18,4) NULL,
    high DECIMAL(18,4) NULL,
    low DECIMAL(18,4) NULL,
    close DECIMAL(18,4) NULL,
    volume BIGINT NULL,
    amount DECIMAL(20,2) NULL,
    turnover DECIMAL(12,4) NULL,
    data_source VARCHAR(32) NOT NULL DEFAULT 'akshare_csv',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_daily_bars_instrument_date_adjust (instrument_id, trade_date, adjust_type),
    KEY idx_daily_bars_trade_date (trade_date),
    KEY idx_daily_bars_instrument_date (instrument_id, trade_date),
    CONSTRAINT fk_daily_bars_instrument
        FOREIGN KEY (instrument_id) REFERENCES instruments (id)
        ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS daily_bar_indicators (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    instrument_id BIGINT UNSIGNED NOT NULL,
    trade_date DATE NOT NULL,
    adjust_type VARCHAR(8) NOT NULL DEFAULT 'qfq',
    ma5 DECIMAL(18,4) NULL,
    ma10 DECIMAL(18,4) NULL,
    ma20 DECIMAL(18,4) NULL,
    ma30 DECIMAL(18,4) NULL,
    ma60 DECIMAL(18,4) NULL,
    ma120 DECIMAL(18,4) NULL,
    ma250 DECIMAL(18,4) NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_daily_bar_indicators_instrument_date_adjust (instrument_id, trade_date, adjust_type),
    KEY idx_daily_bar_indicators_trade_date (trade_date),
    CONSTRAINT fk_daily_bar_indicators_instrument
        FOREIGN KEY (instrument_id) REFERENCES instruments (id)
        ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS sync_runs (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    run_type VARCHAR(32) NOT NULL,
    source_file VARCHAR(255) NULL,
    target_date DATE NULL,
    start_date DATE NULL,
    end_date DATE NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'completed',
    triggered_by VARCHAR(32) NOT NULL DEFAULT 'migration',
    total_symbols INT NOT NULL DEFAULT 0,
    success_symbols INT NOT NULL DEFAULT 0,
    failed_symbols INT NOT NULL DEFAULT 0,
    skipped_symbols INT NOT NULL DEFAULT 0,
    params_json JSON NULL,
    message TEXT NULL,
    started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at DATETIME NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_sync_runs_type_started (run_type, started_at),
    KEY idx_sync_runs_status_started (status, started_at),
    KEY idx_sync_runs_target_date (target_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS sync_run_items (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    run_id BIGINT UNSIGNED NOT NULL,
    instrument_id BIGINT UNSIGNED NULL,
    symbol VARCHAR(16) NOT NULL,
    name VARCHAR(64) NULL,
    status VARCHAR(16) NOT NULL,
    planned_start_date DATE NULL,
    latest_date DATE NULL,
    before_latest_date DATE NULL,
    rows_added INT NOT NULL DEFAULT 0,
    total_rows INT NOT NULL DEFAULT 0,
    download_reason VARCHAR(32) NULL,
    error_message TEXT NULL,
    suspension_reason VARCHAR(255) NULL,
    expected_resume_date DATE NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_sync_run_items_run_status (run_id, status),
    KEY idx_sync_run_items_symbol (symbol),
    KEY idx_sync_run_items_status_created (status, created_at),
    CONSTRAINT fk_sync_run_items_run
        FOREIGN KEY (run_id) REFERENCES sync_runs (id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_sync_run_items_instrument
        FOREIGN KEY (instrument_id) REFERENCES instruments (id)
        ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS suspension_events (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    instrument_id BIGINT UNSIGNED NULL,
    symbol VARCHAR(16) NOT NULL,
    name VARCHAR(64) NULL,
    snapshot_date DATE NOT NULL,
    suspend_start_date DATE NULL,
    suspend_end_date DATE NULL,
    suspend_reason VARCHAR(255) NULL,
    market_name VARCHAR(64) NULL,
    expected_resume_date DATE NULL,
    source VARCHAR(32) NOT NULL DEFAULT 'akshare',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_suspension_events_symbol_snapshot (symbol, snapshot_date),
    KEY idx_suspension_events_resume (expected_resume_date),
    CONSTRAINT fk_suspension_events_instrument
        FOREIGN KEY (instrument_id) REFERENCES instruments (id)
        ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS dividend_summary (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    instrument_id BIGINT UNSIGNED NULL,
    symbol VARCHAR(16) NOT NULL,
    name VARCHAR(64) NULL,
    list_date DATE NULL,
    total_dividend DECIMAL(18,4) NULL,
    avg_annual_dividend DECIMAL(18,4) NULL,
    dividend_count INT NULL,
    total_financing DECIMAL(18,4) NULL,
    financing_count INT NULL,
    source VARCHAR(32) NOT NULL DEFAULT 'akshare',
    snapshot_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_dividend_summary_symbol (symbol),
    CONSTRAINT fk_dividend_summary_instrument
        FOREIGN KEY (instrument_id) REFERENCES instruments (id)
        ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS screen_runs (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    trade_date DATE NOT NULL,
    strategy_name VARCHAR(64) NOT NULL DEFAULT 'ma_dividend_screen',
    status VARCHAR(16) NOT NULL DEFAULT 'completed',
    result_count INT NOT NULL DEFAULT 0,
    params_json JSON NULL,
    message TEXT NULL,
    started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at DATETIME NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_screen_runs_trade_date (trade_date),
    KEY idx_screen_runs_status_started (status, started_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS screen_results (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    screen_run_id BIGINT UNSIGNED NOT NULL,
    instrument_id BIGINT UNSIGNED NULL,
    symbol VARCHAR(16) NOT NULL,
    name VARCHAR(64) NULL,
    trade_date DATE NOT NULL,
    close DECIMAL(18,4) NULL,
    ma120 DECIMAL(18,4) NULL,
    ma_threshold DECIMAL(18,4) NULL,
    discount_vs_ma120 DECIMAL(12,6) NULL,
    dividend_yield DECIMAL(12,6) NULL,
    pe_ttm DECIMAL(18,6) NULL,
    market_cap DECIMAL(20,2) NULL,
    is_profitable TINYINT(1) NULL,
    dividend_source VARCHAR(64) NULL,
    valuation_source VARCHAR(64) NULL,
    profitability_source VARCHAR(64) NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_screen_results_run (screen_run_id),
    KEY idx_screen_results_symbol_date (symbol, trade_date),
    CONSTRAINT fk_screen_results_run
        FOREIGN KEY (screen_run_id) REFERENCES screen_runs (id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_screen_results_instrument
        FOREIGN KEY (instrument_id) REFERENCES instruments (id)
        ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
