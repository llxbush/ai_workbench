from __future__ import annotations

from contextlib import contextmanager
from os import getenv
from typing import Iterator

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    func,
    inspect,
    text,
)
from sqlalchemy.engine import make_url
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker


DEFAULT_DATABASE_URL = getenv("QUANT_DATABASE_URL", "")


class Base(DeclarativeBase):
    pass


class Instrument(Base):
    __tablename__ = "instruments"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(16), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(64))
    market: Mapped[str | None] = mapped_column(String(16), nullable=True)
    exchange_code: Mapped[str | None] = mapped_column(String(16), nullable=True)
    board: Mapped[str | None] = mapped_column(String(32), nullable=True)
    list_date: Mapped[object | None] = mapped_column(Date, nullable=True)
    status: Mapped[str] = mapped_column(String(16), default="listed")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[object] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[object] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())


class DailyBar(Base):
    __tablename__ = "daily_bars"
    __table_args__ = (
        UniqueConstraint("instrument_id", "trade_date", "adjust_type", name="uq_daily_bars_instrument_date_adjust"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    instrument_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("instruments.id"), index=True)
    trade_date: Mapped[object] = mapped_column(Date, index=True)
    adjust_type: Mapped[str] = mapped_column(String(8), default="qfq", index=True)
    open: Mapped[float | None] = mapped_column(Numeric(18, 4), nullable=True)
    high: Mapped[float | None] = mapped_column(Numeric(18, 4), nullable=True)
    low: Mapped[float | None] = mapped_column(Numeric(18, 4), nullable=True)
    close: Mapped[float | None] = mapped_column(Numeric(18, 4), nullable=True)
    volume: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    amount: Mapped[float | None] = mapped_column(Numeric(20, 2), nullable=True)
    turnover: Mapped[float | None] = mapped_column(Numeric(12, 4), nullable=True)
    data_source: Mapped[str] = mapped_column(String(32), default="akshare_web")
    quality_flags: Mapped[list | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[object] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[object] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())


class SyncRun(Base):
    __tablename__ = "sync_runs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_type: Mapped[str] = mapped_column(String(32), index=True)
    source_file: Mapped[str | None] = mapped_column(String(255), nullable=True)
    target_date: Mapped[object | None] = mapped_column(Date, nullable=True, index=True)
    start_date: Mapped[object | None] = mapped_column(Date, nullable=True)
    end_date: Mapped[object | None] = mapped_column(Date, nullable=True)
    status: Mapped[str] = mapped_column(String(16), default="completed", index=True)
    triggered_by: Mapped[str] = mapped_column(String(32), default="web")
    total_symbols: Mapped[int] = mapped_column(Integer, default=0)
    success_symbols: Mapped[int] = mapped_column(Integer, default=0)
    failed_symbols: Mapped[int] = mapped_column(Integer, default=0)
    skipped_symbols: Mapped[int] = mapped_column(Integer, default=0)
    params_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[object] = mapped_column(DateTime, server_default=func.now(), index=True)
    finished_at: Mapped[object] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[object] = mapped_column(DateTime, server_default=func.now())


class SyncRunItem(Base):
    __tablename__ = "sync_run_items"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("sync_runs.id"), index=True)
    instrument_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("instruments.id"), nullable=True)
    symbol: Mapped[str] = mapped_column(String(16), index=True)
    name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    status: Mapped[str] = mapped_column(String(16), index=True)
    planned_start_date: Mapped[object | None] = mapped_column(Date, nullable=True)
    latest_date: Mapped[object | None] = mapped_column(Date, nullable=True)
    before_latest_date: Mapped[object | None] = mapped_column(Date, nullable=True)
    rows_added: Mapped[int] = mapped_column(Integer, default=0)
    total_rows: Mapped[int] = mapped_column(Integer, default=0)
    download_reason: Mapped[str | None] = mapped_column(String(32), nullable=True)
    data_source: Mapped[str | None] = mapped_column(String(32), nullable=True)
    quality_flags: Mapped[list | None] = mapped_column(JSON, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    suspension_reason: Mapped[str | None] = mapped_column(String(255), nullable=True)
    expected_resume_date: Mapped[object | None] = mapped_column(Date, nullable=True)
    created_at: Mapped[object] = mapped_column(DateTime, server_default=func.now())


def ensure_mysql_url(database_url: str) -> None:
    if not database_url:
        raise ValueError(
            "Quant Web requires QUANT_DATABASE_URL or --database-url, "
            "e.g. mysql+pymysql://user:password@127.0.0.1:3306/quant?charset=utf8mb4"
        )
    driver = make_url(database_url).drivername
    if not driver.startswith("mysql"):
        raise ValueError(f"Quant Web now requires a MySQL database URL, got: {driver}")


def safe_database_url(database_url: str) -> str:
    if not database_url:
        return ""
    return make_url(database_url).render_as_string(hide_password=True)


def build_engine(database_url: str = DEFAULT_DATABASE_URL):
    ensure_mysql_url(database_url)
    return create_engine(database_url, future=True, pool_pre_ping=True)


def build_session_factory(database_url: str = DEFAULT_DATABASE_URL):
    engine = build_engine(database_url=database_url)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False, future=True)


def init_db(database_url: str = DEFAULT_DATABASE_URL) -> None:
    engine = build_engine(database_url=database_url)
    Base.metadata.create_all(engine)
    _ensure_runtime_columns(engine)


def _ensure_runtime_columns(engine) -> None:
    inspector = inspect(engine)
    table_columns = {
        table_name: {column["name"] for column in inspector.get_columns(table_name)}
        for table_name in ("daily_bars", "sync_run_items")
        if inspector.has_table(table_name)
    }
    statements: list[str] = []
    if "daily_bars" in table_columns and "quality_flags" not in table_columns["daily_bars"]:
        statements.append("ALTER TABLE daily_bars ADD COLUMN quality_flags JSON NULL AFTER data_source")
    if "sync_run_items" in table_columns:
        if "data_source" not in table_columns["sync_run_items"]:
            statements.append("ALTER TABLE sync_run_items ADD COLUMN data_source VARCHAR(32) NULL AFTER download_reason")
        if "quality_flags" not in table_columns["sync_run_items"]:
            statements.append("ALTER TABLE sync_run_items ADD COLUMN quality_flags JSON NULL AFTER data_source")
    if not statements:
        return
    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))


@contextmanager
def session_scope(database_url: str = DEFAULT_DATABASE_URL) -> Iterator[Session]:
    session_factory = build_session_factory(database_url=database_url)
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
