from __future__ import annotations

import asyncio
import json
import hmac
import logging
import random
import ssl
import time
import uuid
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from typing import Any, Literal

import httpx
import websockets
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from pydantic import AliasChoices, BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import DateTime, Integer, JSON, Numeric, String, func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.pool import NullPool

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("arbdesk")
BITOPRO_PAIRS = ("BTC_TWD", "USDT_TWD", "BTC_USDT")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    database_url: str = Field(default="", validation_alias=AliasChoices(
        "DATABASE_URL_UNPOOLED", "DATABASE_URL", "POSTGRES_URL_NON_POOLING", "POSTGRES_URL",
    ))
    backend_proxy_secret: str = ""
    market_data_mode: Literal["demo", "live", "history"] = "demo"
    bitopro_taker_fee_bps: Decimal = Decimal("10")
    okx_taker_fee_bps: Decimal = Decimal("10")
    max_book_age_ms: int = 5000
    max_triangle_skew_ms: int = 2500
    max_cross_skew_ms: int = 2500
    auto_simulation_size_btc: Decimal = Field(default=Decimal("0.001"), gt=0, decimal_places=12)
    auto_simulation_size_twd: Decimal = Field(default=Decimal("10000"), gt=0, decimal_places=12)


settings = Settings()


def make_engine(database_url: str, *, on_demand: bool = False) -> AsyncEngine:
    url = make_url(database_url).set(drivername="postgresql+asyncpg")
    # Marketplace URLs use libpq query parameters. Pass verified TLS directly to asyncpg.
    sslmode = url.query.get("sslmode")
    url = url.difference_update_query(["sslmode", "channel_binding"])
    connect_args: dict[str, Any] = {"timeout": 5, "command_timeout": 8}
    if on_demand or sslmode in ("require", "verify-ca", "verify-full"):
        connect_args["ssl"] = ssl.create_default_context()
    if on_demand:
        return create_async_engine(url, connect_args=connect_args, poolclass=NullPool)
    return create_async_engine(url, connect_args=connect_args, pool_pre_ping=True)


engine = make_engine(settings.database_url or "postgresql+asyncpg://arb:arb@localhost:5432/arb")
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class AccountBalance(Base):
    __tablename__ = "account_balances"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    venue: Mapped[str] = mapped_column(String(20), index=True)
    asset: Mapped[str] = mapped_column(String(12), index=True)
    amount: Mapped[Decimal] = mapped_column(Numeric(30, 12), default=Decimal("0"))


class PaperTrade(Base):
    __tablename__ = "paper_trades"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    strategy_id: Mapped[str] = mapped_column(String(64), index=True)
    strategy_name: Mapped[str] = mapped_column(String(120))
    mode: Mapped[str] = mapped_column(String(16))
    trigger_reason: Mapped[str] = mapped_column(String(200))
    start_asset: Mapped[str] = mapped_column(String(12))
    start_amount: Mapped[Decimal] = mapped_column(Numeric(30, 12))
    end_asset: Mapped[str] = mapped_column(String(12))
    end_amount: Mapped[Decimal] = mapped_column(Numeric(30, 12))
    pnl: Mapped[Decimal] = mapped_column(Numeric(30, 12))
    pnl_percent: Mapped[Decimal] = mapped_column(Numeric(18, 8))
    legs: Mapped[list[dict[str, Any]]] = mapped_column(JSON)
    market_snapshot: Mapped[dict[str, Any]] = mapped_column(JSON)


class SignalRecord(Base):
    __tablename__ = "signal_records"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    strategy_id: Mapped[str] = mapped_column(String(64), index=True)
    strategy_name: Mapped[str] = mapped_column(String(120))
    mode: Mapped[str] = mapped_column(String(16))
    status: Mapped[str] = mapped_column(String(40))
    net_pct: Mapped[Decimal] = mapped_column(Numeric(18, 8))
    reason: Mapped[str] = mapped_column(String(200))
    snapshot: Mapped[dict[str, Any]] = mapped_column(JSON)


class AppSetting(Base):
    __tablename__ = "app_settings"
    key: Mapped[str] = mapped_column(String(80), primary_key=True)
    value: Mapped[dict[str, Any]] = mapped_column(JSON)


class Level(BaseModel):
    price: Decimal
    amount: Decimal


class Book(BaseModel):
    venue: str
    pair: str
    source: Literal["websocket", "rest", "demo"]
    bids: list[Level]
    asks: list[Level]
    exchange_ts: int | None
    received_ts: int
    sequence: int | None = None
    valid: bool = True

    @property
    def age_ms(self) -> int:
        return max(0, int(time.time() * 1000) - min(self.received_ts, self.observed_ts))

    @property
    def observed_ts(self) -> int:
        return self.exchange_ts if self.exchange_ts is not None else self.received_ts


class MarketData:
    def __init__(self, mode: Literal["demo", "live", "history"] | None = None) -> None:
        self.books: dict[str, Book] = {}
        self.connections = {
            "bitopro": {"connected": False, "last_message_ts": None, "error": None},
            "okx": {"connected": False, "last_message_ts": None, "error": None},
        }
        self.tasks: list[asyncio.Task[Any]] = []
        self._okx_levels: dict[str, tuple[dict[str, Decimal], dict[str, Decimal], int | None]] = {}
        self._demo_rng = random.Random(20261121)
        self.mode = mode or settings.market_data_mode

    async def start(self) -> None:
        self.mode = settings.market_data_mode
        if self.mode == "live":
            self.tasks = [
                asyncio.create_task(self._reconnect_loop("bitopro", self._bitopro_stream)),
                asyncio.create_task(self._reconnect_loop("okx", self._okx_stream)),
                asyncio.create_task(self._bitopro_rest_recovery()),
            ]
        elif self.mode == "demo":
            self.tasks = [asyncio.create_task(self._demo_stream())]

    async def stop(self) -> None:
        for task in self.tasks:
            task.cancel()
        for task in self.tasks:
            with suppress(asyncio.CancelledError):
                await task
        self.tasks = []

    async def set_mode(self, mode: Literal["demo", "live", "history"]) -> None:
        await self.stop()
        self.books.clear()
        self._okx_levels.clear()
        for status in self.connections.values():
            status.update(connected=False, last_message_ts=None, error=None)
        settings.market_data_mode = mode
        self.mode = mode
        await self.start()

    async def _reconnect_loop(self, venue: str, worker: Any) -> None:
        delay = 1
        while True:
            try:
                await worker()
                delay = 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("%s stream disconnected: %s", venue, exc)
                self.connections[venue].update(connected=False, error=str(exc)[:180])
                for book in self.books.values():
                    if book.venue == venue:
                        book.valid = False
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

    def _set_book(self, venue: str, pair: str, bids: list[dict[str, Any]], asks: list[dict[str, Any]], ts: Any, seq: int | None = None, source: Literal["websocket", "rest", "demo"] = "websocket") -> None:
        now = int(time.time() * 1000)
        def levels(rows: list[dict[str, Any]], *, reverse: bool = False) -> list[Level]:
            result = []
            for row in rows:
                price, amount = Decimal(str(row["price"])), Decimal(str(row["amount"]))
                if price.is_finite() and price > 0 and amount.is_finite() and amount > 0:
                    result.append(Level(price=price, amount=amount))
            return sorted(result, key=lambda level: level.price, reverse=reverse)[:50]

        levels_b, levels_a = levels(bids, reverse=True), levels(asks)
        exchange_ts = int(ts) if ts is not None else None
        self.books[f"{venue}:{pair}"] = Book(
            venue=venue, pair=pair, source=source, bids=levels_b, asks=levels_a,
            exchange_ts=exchange_ts, received_ts=now, sequence=seq,
            valid=bool(levels_b and levels_a and levels_b[0].price < levels_a[0].price)
            and (exchange_ts is None or 0 < exchange_ts <= now + 1000),
        )
        if source != "rest":
            self.connections[venue].update(connected=True, last_message_ts=now, error=None)

    async def _bitopro_stream(self, *, snapshot_only: bool = False) -> None:
        pairs = ",".join(f"{pair}:50" for pair in BITOPRO_PAIRS)
        url = f"wss://stream.bitopro.com:443/ws/v1/pub/order-books?pairs={pairs}"
        async with websockets.connect(url, open_timeout=3, ping_interval=15, ping_timeout=10, close_timeout=1, max_size=2**22) as ws:
            async for raw in ws:
                payload = json.loads(raw)
                events = payload if isinstance(payload, list) else [payload]
                for item in events:
                    if item.get("event") != "ORDER_BOOK" or item.get("pair") not in BITOPRO_PAIRS:
                        continue
                    self._set_book(
                        "bitopro", item["pair"], item.get("bids", []), item.get("asks", []),
                        item.get("timestamp"), source="websocket",
                    )
                if snapshot_only and all(f"bitopro:{pair}" in self.books for pair in BITOPRO_PAIRS):
                    return

    async def _okx_stream(self) -> None:
        url = "wss://ws.okx.com/ws/v5/public"
        async with websockets.connect(url, ping_interval=20, ping_timeout=10, close_timeout=3, max_size=2**22) as ws:
            await ws.send(json.dumps({"op": "subscribe", "args": [{"channel": "books", "instId": "BTC-USDT"}]}))
            async for raw in ws:
                payload = json.loads(raw)
                if "data" not in payload:
                    continue
                action = payload.get("action", "snapshot")
                for item in payload["data"]:
                    pair = item.get("instId", "BTC-USDT")
                    bids, asks, last_seq = self._okx_levels.get(pair, ({}, {}, None))
                    seq = int(item.get("seqId", 0))
                    prev = int(item.get("prevSeqId", -1))
                    if action == "snapshot":
                        bids, asks = {}, {}
                    elif last_seq is None or prev != last_seq:
                        raise RuntimeError(f"OKX sequence gap for {pair}: prev={prev}, last={last_seq}; requesting a fresh snapshot")
                    for side, levels in ((bids, item.get("bids", [])), (asks, item.get("asks", []))):
                        for row in levels:
                            price, amount = str(row[0]), Decimal(str(row[1]))
                            if amount == 0:
                                side.pop(price, None)
                            else:
                                side[price] = amount
                    self._okx_levels[pair] = (bids, asks, seq)
                    b = [{"price": p, "amount": a} for p, a in bids.items()]
                    a = [{"price": p, "amount": q} for p, q in asks.items()]
                    self._set_book("okx", pair, b, a, item.get("ts"), seq, source="websocket")

    async def _bitopro_rest_recovery(self) -> None:
        async with httpx.AsyncClient(timeout=5) as client:
            while True:
                await asyncio.sleep(10)
                for pair in BITOPRO_PAIRS:
                    current = self.books.get(f"bitopro:{pair}")
                    if current and current.valid and current.age_ms <= active_trading_settings.max_book_age_ms:
                        continue
                    try:
                        response = await client.get(f"https://api.bitopro.com/v3/order-book/{pair}", params={"limit": 50})
                        response.raise_for_status()
                        payload = response.json()
                        self._set_book("bitopro", pair, payload.get("bids", []), payload.get("asks", []), None, source="rest")
                    except Exception as exc:
                        logger.info("BitoPro REST snapshot unavailable for %s: %s", pair, exc)
                        self.connections["bitopro"].update(error=f"REST snapshot unavailable for {pair}")

    def publish_demo(self, anchors: dict[str, Decimal]) -> None:
        now = int(time.time() * 1000)
        for key, anchor in anchors.items():
            drift = Decimal(str(self._demo_rng.uniform(-0.00018, 0.00018)))
            mid = anchors[key] = anchor * (Decimal("1") + drift)
            venue, pair = key.split(":", 1)
            spread = mid * Decimal("0.00012")
            step = max(mid * Decimal("0.00008"), Decimal("0.0001"))
            first_size, size_step = Decimal("0.04"), Decimal("0.003")
            if pair == "BTC_TWD":
                step = max(step, Decimal("0.01"))
                first_size, size_step = Decimal("0.03"), Decimal("0.002")
            elif pair == "USDT_TWD":
                step = Decimal("0.002")
                first_size, size_step = Decimal("5000"), Decimal("300")
            bids = [{"price": mid - spread - i * step, "amount": first_size + i * size_step} for i in range(50)]
            asks = [{"price": mid + spread + i * step, "amount": first_size + i * size_step} for i in range(50)]
            self._set_book(venue, pair, bids, asks, now, source="demo")

    async def _demo_stream(self) -> None:
        anchors = DEMO_ANCHORS.copy()
        while True:
            self.publish_demo(anchors)
            await asyncio.sleep(1)

    def snapshot(self) -> dict[str, Any]:
        books = {k: b.model_dump(mode="json") | {"age_ms": b.age_ms} for k, b in self.books.items()}
        return {
            "mode": self.mode,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "books": books,
            "connections": self.connections,
        }


DEMO_ANCHORS = {
    "bitopro:BTC_USDT": Decimal("68120"),
    "okx:BTC-USDT": Decimal("67860"),
    "bitopro:BTC_TWD": Decimal("2180500"),
    "bitopro:USDT_TWD": Decimal("32.02"),
}
market = MarketData()


class TradingSettings(BaseModel):
    bitopro_fee_bps: Decimal = Field(default=Decimal("10"), ge=0, le=200)
    okx_fee_bps: Decimal = Field(default=Decimal("10"), ge=0, le=200)
    net_threshold_bps: Decimal = Field(default=Decimal("30"), ge=0, le=10000)
    cooldown_seconds: int = Field(default=60, ge=1, le=86400)
    max_book_age_ms: int = Field(default=5000, ge=250, le=60000)
    auto_simulation_enabled: bool = False


default_trading_settings = TradingSettings(
    bitopro_fee_bps=settings.bitopro_taker_fee_bps,
    okx_fee_bps=settings.okx_taker_fee_bps,
    max_book_age_ms=settings.max_book_age_ms,
)
active_trading_settings = default_trading_settings


def fee_rate(venue: str, trading: TradingSettings) -> Decimal:
    bps = trading.bitopro_fee_bps if venue == "bitopro" else trading.okx_fee_bps
    return bps / Decimal("10000")


def _consume(book: Book, side: Literal["buy", "sell"], quantity: Decimal) -> tuple[Decimal, Decimal, list[dict[str, str]]]:
    levels = book.asks if side == "buy" else book.bids
    remaining = quantity
    total = Decimal("0")
    filled = Decimal("0")
    fills: list[dict[str, str]] = []
    for level in levels:
        amount = min(remaining, level.amount)
        if amount <= 0:
            continue
        notional = amount * level.price
        total += notional
        filled += amount
        remaining -= amount
        fills.append({"price": str(level.price), "amount": str(amount), "notional": str(notional)})
        if remaining <= 0:
            break
    if remaining > 0:
        raise ValueError("可見委託簿深度不足，已拒絕整筆模擬")
    return filled, total, fills


def _buy_with_quote(book: Book, quote_amount: Decimal, rate: Decimal) -> tuple[Decimal, Decimal, Decimal, list[dict[str, str]]]:
    remaining = quote_amount
    bought = Decimal("0")
    spent = Decimal("0")
    fees = Decimal("0")
    fills: list[dict[str, str]] = []
    for level in book.asks:
        max_qty_from_budget = remaining / (level.price * (Decimal("1") + rate))
        qty = min(level.amount, max_qty_from_budget)
        if qty <= 0:
            continue
        chunk_quote = qty * level.price
        fee = chunk_quote * rate
        bought += qty
        spent += chunk_quote + fee
        fees += fee
        remaining -= chunk_quote + fee
        fills.append({"price": str(level.price), "amount": str(qty), "notional": str(chunk_quote), "fee": str(fee)})
        if remaining <= Decimal("0.000000000001"):
            break
    if remaining > Decimal("0.000000000001"):
        raise ValueError("可見委託簿深度不足，已拒絕整筆模擬")
    return bought, spent, fees, fills


def _sell_base(book: Book, quantity: Decimal, rate: Decimal) -> tuple[Decimal, Decimal, list[dict[str, str]]]:
    _, gross, fills = _consume(book, "sell", quantity)
    fee = gross * rate
    return gross - fee, fee, fills


def _executable(book: Book | None, max_age_ms: int) -> bool:
    return bool(book and book.valid and book.bids and book.asks and book.age_ms <= max_age_ms)


def opportunities(market_data: MarketData, trading: TradingSettings) -> list[dict[str, Any]]:
    max_age = trading.max_book_age_ms
    rows: list[dict[str, Any]] = []
    cross_paths = [
        ("cross_bito_buy_okx_sell", "幣託買入 → OKX 賣出", "bitopro", "okx", "BTC_USDT", "BTC-USDT"),
        ("cross_okx_buy_bito_sell", "OKX 買入 → 幣託賣出", "okx", "bitopro", "BTC-USDT", "BTC_USDT"),
    ]
    for route_id, title, buy_venue, sell_venue, buy_pair, sell_pair in cross_paths:
        buy = market_data.books.get(f"{buy_venue}:{buy_pair}")
        sell = market_data.books.get(f"{sell_venue}:{sell_pair}")
        feed_ready = _executable(buy, max_age) and _executable(sell, max_age)
        if feed_ready and buy and sell:
            feed_ready = abs(buy.observed_ts - sell.observed_ts) <= settings.max_cross_skew_ms
        raw_pct = Decimal("0")
        net_pct = Decimal("0")
        sample_size = settings.auto_simulation_size_btc
        depth_capacity = Decimal("0")
        estimate_pnl = Decimal("0")
        depth_ready = False
        if feed_ready and buy and sell:
            depth_capacity = min(sum((x.amount for x in buy.asks), Decimal("0")), sum((x.amount for x in sell.bids), Decimal("0")))
            sample_size = min(sample_size, depth_capacity)
            if sample_size > 0:
                try:
                    _, buy_gross, _ = _consume(buy, "buy", sample_size)
                    _, sell_gross, _ = _consume(sell, "sell", sample_size)
                    buy_fee = buy_gross * fee_rate(buy_venue, trading)
                    sell_fee = sell_gross * fee_rate(sell_venue, trading)
                    buy_total = buy_gross + buy_fee
                    sell_net = sell_gross - sell_fee
                    raw_pct = (sell_gross - buy_gross) / buy_gross * Decimal("100")
                    estimate_pnl = sell_net - buy_total
                    net_pct = estimate_pnl / buy_total * Decimal("100")
                    depth_ready = depth_capacity >= settings.auto_simulation_size_btc
                except ValueError:
                    pass
        rows.append({
            "id": route_id, "title": title, "kind": "cross", "ready": feed_ready, "depth_ready": depth_ready,
            "raw_pct": str(raw_pct), "net_pct": str(net_pct), "size_unit": "BTC",
            "reference_size": str(sample_size), "estimate_pnl": str(estimate_pnl), "estimate_asset": "USDT", "depth_capacity": str(depth_capacity),
            "buy_venue": buy_venue, "sell_venue": sell_venue,
            "books": [f"{buy_venue}:{buy_pair}", f"{sell_venue}:{sell_pair}"],
            "reason": None if feed_ready else "行情未就緒或已過期",
        })
    triangle_paths = [
        ("triangle_twd_btc_usdt_twd", "TWD → BTC → USDT → TWD", [("BTC_TWD", "buy"), ("BTC_USDT", "sell"), ("USDT_TWD", "sell")]),
        ("triangle_twd_usdt_btc_twd", "TWD → USDT → BTC → TWD", [("USDT_TWD", "buy"), ("BTC_USDT", "buy"), ("BTC_TWD", "sell")]),
    ]
    for route_id, title, legs in triangle_paths:
        books = [market_data.books.get(f"bitopro:{pair}") for pair, _ in legs]
        ready = all(_executable(b, max_age) for b in books)
        timestamps = [b.observed_ts for b in books if b]
        ready = ready and len(timestamps) == len(legs) and max(timestamps) - min(timestamps) <= settings.max_triangle_skew_ms
        raw_pct = Decimal("0")
        net_pct = Decimal("0")
        start = settings.auto_simulation_size_twd
        raw_output = Decimal("0")
        net_output = Decimal("0")
        depth_ready = False
        if ready:
            try:
                def walk_triangle(with_fees: bool) -> Decimal:
                    amount = start
                    for (_, side), book in zip(legs, books):
                        assert book is not None
                        rate = fee_rate("bitopro", trading) if with_fees else Decimal("0")
                        if side == "buy":
                            amount, _, _, _ = _buy_with_quote(book, amount, rate)
                        else:
                            amount, _, _ = _sell_base(book, amount, rate)
                    return amount
                raw_output = walk_triangle(False)
                net_output = walk_triangle(True)
                raw_pct = (raw_output - start) / start * Decimal("100")
                net_pct = (net_output - start) / start * Decimal("100")
                depth_ready = True
            except (ValueError, ArithmeticError):
                depth_ready = False
        rows.append({
            "id": route_id, "title": title, "kind": "triangle", "ready": ready, "depth_ready": depth_ready,
            "raw_pct": str(raw_pct), "net_pct": str(net_pct), "size_unit": "TWD",
            "reference_size": str(start), "estimate_pnl": str(net_output - start if depth_ready else Decimal("0")), "estimate_asset": "TWD", "depth_capacity": None,
            "buy_venue": "bitopro", "sell_venue": "bitopro",
            "books": [f"bitopro:{pair}" for pair, _ in legs],
            "reason": None if ready else "行情不同步或已過期",
        })
    return rows


def _public_book(book: Book | None) -> dict[str, Any] | None:
    if not book:
        return None
    return book.model_dump(mode="json") | {"age_ms": book.age_ms}


async def _balances(session: AsyncSession) -> list[dict[str, str]]:
    result = await session.execute(select(AccountBalance).order_by(AccountBalance.venue, AccountBalance.asset))
    return [{"venue": x.venue, "asset": x.asset, "amount": str(x.amount)} for x in result.scalars()]


async def _trades(session: AsyncSession, limit: int = 20, mode: str | None = None) -> list[dict[str, Any]]:
    query = select(PaperTrade).order_by(PaperTrade.created_at.desc()).limit(limit)
    if mode is not None:
        query = query.where(PaperTrade.mode == mode)
    result = await session.execute(query)
    return [{
        "id": x.id, "created_at": x.created_at.isoformat(), "strategy_id": x.strategy_id,
        "strategy_name": x.strategy_name, "mode": x.mode, "trigger_reason": x.trigger_reason,
        "start_asset": x.start_asset, "start_amount": str(x.start_amount),
        "end_asset": x.end_asset, "end_amount": str(x.end_amount),
        "pnl": str(x.pnl), "pnl_percent": str(x.pnl_percent), "legs": x.legs,
    } for x in result.scalars()]


async def _signals(session: AsyncSession, limit: int = 20, mode: str | None = None) -> list[dict[str, Any]]:
    query = select(SignalRecord).order_by(SignalRecord.observed_at.desc()).limit(limit)
    if mode is not None:
        query = query.where(SignalRecord.mode == mode)
    result = await session.execute(query)
    return [{
        "id": x.id, "observed_at": x.observed_at.isoformat(), "strategy_id": x.strategy_id,
        "strategy_name": x.strategy_name, "mode": x.mode, "status": x.status,
        "net_pct": str(x.net_pct), "reason": x.reason,
    } for x in result.scalars()]


async def _get_trading_settings(session: AsyncSession) -> TradingSettings:
    row = await session.get(AppSetting, "trading")
    return TradingSettings.model_validate(row.value) if row else default_trading_settings


async def _state(
    session: AsyncSession | None,
    market_data: MarketData = market,
    *,
    ledger_mode: str | None = None,
) -> dict[str, Any]:
    trading = default_trading_settings
    balances: list[dict[str, str]] = []
    trades: list[dict[str, Any]] = []
    signals: list[dict[str, Any]] = []
    summary = {"trade_count": 0, "realized_pnl_by_asset": {}}
    if session is not None:
        ledger_mode = ledger_mode or (None if market_data.mode == "history" else market_data.mode)
        trading = await _get_trading_settings(session)
        query = select(PaperTrade.end_asset, func.count(PaperTrade.id), func.sum(PaperTrade.pnl)).group_by(PaperTrade.end_asset)
        if ledger_mode is not None:
            query = query.where(PaperTrade.mode == ledger_mode)
        totals = await session.execute(query)
        for asset, count, pnl in totals.all():
            summary["trade_count"] += int(count)
            summary["realized_pnl_by_asset"][asset] = str(pnl or Decimal("0"))
        balances = await _balances(session)
        trades = await _trades(session, mode=ledger_mode)
        signals = await _signals(session, mode=ledger_mode)
    return {
        "market": market_data.snapshot(),
        "opportunities": opportunities(market_data, trading),
        "balances": balances,
        "trades": trades,
        "signals": signals,
        "settings": trading.model_dump(mode="json"),
        "summary": summary,
    }


async def initialize_database(db: AsyncEngine) -> None:
    async with db.begin() as connection:
        # Vercel instances can start concurrently against an empty database.
        await connection.execute(text("SELECT pg_advisory_xact_lock(9284051661)"))
        await connection.run_sync(Base.metadata.create_all)
        async with AsyncSession(bind=connection) as session:
            if await session.scalar(select(AccountBalance.id).limit(1)) is None:
                session.add_all([
                    AccountBalance(venue="bitopro", asset="TWD", amount=Decimal("500000")),
                    AccountBalance(venue="bitopro", asset="BTC", amount=Decimal("0.05")),
                    AccountBalance(venue="bitopro", asset="USDT", amount=Decimal("30000")),
                    AccountBalance(venue="okx", asset="BTC", amount=Decimal("0.05")),
                    AccountBalance(venue="okx", asset="USDT", amount=Decimal("30000")),
                ])
            if await session.get(AppSetting, "trading") is None:
                session.add(AppSetting(key="trading", value=default_trading_settings.model_dump(mode="json")))
            await session.flush()


@asynccontextmanager
async def lifespan(_: FastAPI):
    global active_trading_settings
    await initialize_database(engine)
    async with SessionLocal() as session:
        active_trading_settings = await _get_trading_settings(session)
    await market.start()
    auto_task = asyncio.create_task(_auto_simulation_loop())
    try:
        yield
    finally:
        auto_task.cancel()
        with suppress(asyncio.CancelledError):
            await auto_task
        await market.stop()
        await engine.dispose()


app = FastAPI(title="Arbitrage Desk API", version="0.1.0", lifespan=lifespan)


@app.middleware("http")
async def require_vercel_proxy_secret(request: Request, call_next: Any) -> Any:
    path = request.url.path
    if path.startswith("/api/") and path != "/api/health":
        expected = settings.backend_proxy_secret
        supplied = request.headers.get("x-backend-proxy-secret", "")
        if not expected:
            return JSONResponse(status_code=503, content={"detail": "Backend proxy secret is not configured"})
        if not hmac.compare_digest(supplied, expected):
            return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    return await call_next(request)


@app.get("/api/health")
async def health() -> dict[str, Any]:
    try:
        async with SessionLocal() as session:
            await session.execute(select(1))
    except Exception as exc:
        logger.warning("Health check database query failed: %s", exc)
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    return {
        "ok": True,
        "mode": market.mode,
        "database_connected": True,
        "live_books": sum(1 for book in market.books.values() if book.source == "websocket" and book.valid),
    }


@app.get("/api/state")
async def api_state() -> dict[str, Any]:
    async with SessionLocal() as session:
        return await _state(session)


@app.get("/api/trades")
async def list_trades() -> dict[str, Any]:
    async with SessionLocal() as session:
        return {"items": await _trades(session, 200)}


@app.get("/api/signals")
async def list_signals() -> dict[str, Any]:
    async with SessionLocal() as session:
        return {"items": await _signals(session, 200)}


class SimulateRequest(BaseModel):
    strategy_id: str
    size: Decimal = Field(gt=0)
    trigger_reason: str = "manual"


class BalanceInput(BaseModel):
    venue: Literal["bitopro", "okx"]
    asset: Literal["BTC", "USDT", "TWD"]
    amount: Decimal = Field(ge=0, le=Decimal("1000000000000"))


class BalanceUpdate(BaseModel):
    balances: list[BalanceInput]


@app.put("/api/balances")
async def update_balances(body: BalanceUpdate) -> dict[str, Any]:
    async with SessionLocal() as session:
        return await save_balances(session, body)


async def save_balances(session: AsyncSession, body: BalanceUpdate) -> dict[str, Any]:
    expected = {("bitopro", "TWD"), ("bitopro", "BTC"), ("bitopro", "USDT"), ("okx", "BTC"), ("okx", "USDT")}
    received = {(item.venue, item.asset) for item in body.balances}
    if len(received) != len(body.balances) or received != expected:
        raise HTTPException(status_code=422, detail="請提供每個虛擬資產各一次；OKX 不支援此版本中的 TWD 路徑")
    result = await session.execute(select(AccountBalance).order_by(AccountBalance.id).with_for_update())
    rows = {(row.venue, row.asset): row for row in result.scalars()}
    for item in body.balances:
        amount = item.amount.quantize(Decimal("0.000000000001"), rounding=ROUND_DOWN)
        row = rows.get((item.venue, item.asset))
        if row is None:
            session.add(AccountBalance(venue=item.venue, asset=item.asset, amount=amount))
        else:
            row.amount = amount
    await session.flush()
    balances = await _balances(session)
    await session.commit()
    return {"balances": balances}


def _simulate_cross(
    route: dict[str, Any], size: Decimal, balances: dict[tuple[str, str], Decimal],
    market_data: MarketData, trading: TradingSettings,
) -> tuple[str, Decimal, str, Decimal, list[dict[str, Any]]]:
    buy_venue, sell_venue = route["buy_venue"], route["sell_venue"]
    buy_pair = "BTC_USDT" if buy_venue == "bitopro" else "BTC-USDT"
    sell_pair = "BTC_USDT" if sell_venue == "bitopro" else "BTC-USDT"
    buy_book = market_data.books.get(f"{buy_venue}:{buy_pair}")
    sell_book = market_data.books.get(f"{sell_venue}:{sell_pair}")
    if not _executable(buy_book, trading.max_book_age_ms) or not _executable(sell_book, trading.max_book_age_ms):
        raise ValueError("跨所行情未就緒或已過期")
    assert buy_book and sell_book
    if abs(buy_book.observed_ts - sell_book.observed_ts) > settings.max_cross_skew_ms:
        raise ValueError("跨所行情時間差過大")
    _, buy_gross, buy_fills = _consume(buy_book, "buy", size)
    sale, sell_fee, sell_fills = _sell_base(sell_book, size, fee_rate(sell_venue, trading))
    buy_fee = buy_gross * fee_rate(buy_venue, trading)
    required_usdt = buy_gross + buy_fee
    if balances.get((buy_venue, "USDT"), Decimal("0")) < required_usdt:
        raise ValueError(f"{buy_venue} 的 USDT 餘額不足")
    if balances.get((sell_venue, "BTC"), Decimal("0")) < size:
        raise ValueError(f"{sell_venue} 的 BTC 餘額不足")
    balances[(buy_venue, "USDT")] -= required_usdt
    balances[(buy_venue, "BTC")] = balances.get((buy_venue, "BTC"), Decimal("0")) + size
    balances[(sell_venue, "BTC")] -= size
    balances[(sell_venue, "USDT")] = balances.get((sell_venue, "USDT"), Decimal("0")) + sale
    pnl = sale - required_usdt
    legs = [
        {"venue": buy_venue, "pair": buy_pair, "side": "buy", "base_amount": str(size), "gross_quote": str(buy_gross), "fee_quote": str(buy_fee), "fills": buy_fills},
        {"venue": sell_venue, "pair": sell_pair, "side": "sell", "base_amount": str(size), "net_quote": str(sale), "fee_quote": str(sell_fee), "fills": sell_fills},
    ]
    return "USDT", required_usdt, "USDT", sale, legs


def _simulate_triangle(
    route_id: str, start_amount: Decimal, balances: dict[tuple[str, str], Decimal],
    market_data: MarketData, trading: TradingSettings,
) -> tuple[str, Decimal, str, Decimal, Decimal, list[dict[str, Any]]]:
    paths = {
        "triangle_twd_btc_usdt_twd": [("BTC_TWD", "buy", "TWD", "BTC"), ("BTC_USDT", "sell", "BTC", "USDT"), ("USDT_TWD", "sell", "USDT", "TWD")],
        "triangle_twd_usdt_btc_twd": [("USDT_TWD", "buy", "TWD", "USDT"), ("BTC_USDT", "buy", "USDT", "BTC"), ("BTC_TWD", "sell", "BTC", "TWD")],
    }
    path = paths.get(route_id)
    if not path:
        raise ValueError("未知的三角套利路徑")
    if balances.get(("bitopro", "TWD"), Decimal("0")) < start_amount:
        raise ValueError("幣託的 TWD 餘額不足")
    books = [market_data.books.get(f"bitopro:{pair}") for pair, _, _, _ in path]
    if not all(_executable(b, trading.max_book_age_ms) for b in books):
        raise ValueError("三角路徑行情未就緒或已過期")
    ts = [b.observed_ts for b in books if b]
    if len(ts) != 3 or max(ts) - min(ts) > settings.max_triangle_skew_ms:
        raise ValueError("三角路徑行情時間差過大")
    amount = start_amount
    rate = fee_rate("bitopro", trading)
    legs: list[dict[str, Any]] = []
    for (pair, side, input_asset, output_asset), book in zip(path, books):
        assert book
        if side == "buy":
            output, spent, fee, fills = _buy_with_quote(book, amount, rate)
            if balances.get(("bitopro", input_asset), Decimal("0")) < amount:
                raise ValueError(f"幣託的 {input_asset} 餘額不足")
            balances[("bitopro", input_asset)] -= spent
            balances[("bitopro", output_asset)] = balances.get(("bitopro", output_asset), Decimal("0")) + output
            amount = output
            legs.append({"venue": "bitopro", "pair": pair, "side": side, "input_asset": input_asset, "input_amount": str(spent), "output_asset": output_asset, "output_amount": str(output), "fee_quote": str(fee), "fills": fills})
        else:
            quantity = amount
            output, fee, fills = _sell_base(book, quantity, rate)
            if balances.get(("bitopro", input_asset), Decimal("0")) < quantity:
                raise ValueError(f"幣託的 {input_asset} 餘額不足")
            balances[("bitopro", input_asset)] -= quantity
            balances[("bitopro", output_asset)] = balances.get(("bitopro", output_asset), Decimal("0")) + output
            amount = output
            legs.append({"venue": "bitopro", "pair": pair, "side": side, "input_asset": input_asset, "input_amount": str(quantity), "output_asset": output_asset, "output_amount": str(output), "fee_quote": str(fee), "fills": fills})
    pnl = amount - start_amount
    return "TWD", start_amount, "TWD", amount, pnl, legs


async def execute_simulation(
    session: AsyncSession, request: SimulateRequest, market_data: MarketData = market,
    *, commit: bool = True, trading: TradingSettings | None = None,
) -> dict[str, Any]:
    precision = Decimal("0.000000000001")
    if request.size != request.size.quantize(precision, rounding=ROUND_DOWN):
        raise ValueError("投入數量最多支援 12 位小數")
    if market_data.mode == "history":
        raise ValueError("歷史模式不可新增模擬成交")
    trading = trading or await _get_trading_settings(session)
    result = await session.execute(select(AccountBalance).order_by(AccountBalance.id).with_for_update())
    routes = {r["id"]: r for r in opportunities(market_data, trading)}
    route = routes.get(request.strategy_id)
    if not route or not route["ready"]:
        raise ValueError("這條路徑目前沒有有效、同步且未過期的行情")
    balance_rows = {(x.venue, x.asset): x for x in result.scalars()}
    before = {f"{venue}:{asset}": str(row.amount) for (venue, asset), row in balance_rows.items()}
    balances = {(venue, asset): row.amount for (venue, asset), row in balance_rows.items()}
    if route["kind"] == "cross":
        start_asset, start_amount, end_asset, end_amount, legs = _simulate_cross(route, request.size, balances, market_data, trading)
        pnl = end_amount - start_amount
        pnl_pct = pnl / start_amount * Decimal("100") if start_amount else Decimal("0")
    else:
        start_asset, start_amount, end_asset, end_amount, pnl, legs = _simulate_triangle(request.strategy_id, request.size, balances, market_data, trading)
        pnl_pct = pnl / start_amount * Decimal("100") if start_amount else Decimal("0")
    # Verify every venue book used by the route still belongs to one recent snapshot before writing.
    used_books = {key: market_data.books.get(key) for key in route["books"]}
    if not all(_executable(b, trading.max_book_age_ms) for b in used_books.values()):
        raise ValueError("成交前行情已過期，沒有建立模擬紀錄")
    if any(amount < 0 for amount in balances.values()):
        raise ValueError("模擬後餘額不得為負數")
    for (venue, asset), amount in balances.items():
        amount = amount.quantize(Decimal("0.000000000001"), rounding=ROUND_DOWN)
        balances[(venue, asset)] = amount
        row = balance_rows.get((venue, asset))
        if row is None:
            row = AccountBalance(venue=venue, asset=asset, amount=amount)
            session.add(row)
        else:
            row.amount = amount
    after = {f"{venue}:{asset}": str(amount) for (venue, asset), amount in balances.items()}
    trade = PaperTrade(
        id=str(uuid.uuid4()), created_at=datetime.now(timezone.utc),
        strategy_id=request.strategy_id, strategy_name=route["title"],
        mode=market_data.mode, trigger_reason=request.trigger_reason,
        start_asset=start_asset, start_amount=start_amount,
        end_asset=end_asset, end_amount=end_amount, pnl=pnl, pnl_percent=pnl_pct,
        legs=legs, market_snapshot={"before": before, "after": after, "books": {key: _public_book(book) for key, book in used_books.items()}},
    )
    session.add(trade)
    if commit:
        await session.commit()
    else:
        await session.flush()
    return {"ok": True, "trade_id": trade.id, "strategy": route["title"], "pnl": str(pnl), "pnl_percent": str(pnl_pct), "start_amount": str(start_amount), "start_asset": start_asset, "end_amount": str(end_amount), "end_asset": end_asset, "legs": legs}


@app.post("/api/simulations")
async def create_simulation(request: SimulateRequest) -> dict[str, Any]:
    async with SessionLocal() as session:
        try:
            return await execute_simulation(session, request)
        except (ValueError, ArithmeticError) as exc:
            await session.rollback()
            raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.put("/api/settings")
async def update_settings(body: TradingSettings) -> dict[str, Any]:
    global active_trading_settings
    async with SessionLocal() as session:
        result = await save_trading_settings(session, body)
        active_trading_settings = body
        return result


async def save_trading_settings(session: AsyncSession, body: TradingSettings) -> dict[str, Any]:
    value = body.model_dump(mode="json")
    row = await session.get(AppSetting, "trading", with_for_update=True)
    if row is None:
        session.add(AppSetting(key="trading", value=value))
    else:
        row.value = value
    await session.commit()
    return {"settings": value}


class ModeRequest(BaseModel):
    mode: Literal["demo", "live", "history"]


@app.put("/api/mode")
async def update_mode(body: ModeRequest) -> dict[str, str]:
    await market.set_mode(body.mode)
    return {"mode": body.mode}


async def _auto_simulation_loop() -> None:
    last_trigger: dict[str, float] = {}
    last_signal_status: dict[str, tuple[str, float]] = {}
    while True:
        try:
            async with SessionLocal() as session:
                trading = await _get_trading_settings(session)
                if market.mode != "history":
                    signal_rows: list[SignalRecord] = []
                    for route in opportunities(market, trading):
                        status = signal_status(route)
                        previous_status, last_recorded = last_signal_status.get(route["id"], ("", 0.0))
                        should_record = status != previous_status or (status == "淨利為正" and time.monotonic() - last_recorded >= 60)
                        if should_record:
                            route_books = {key: _public_book(market.books.get(key)) for key in route["books"]}
                            signal_rows.append(SignalRecord(
                                id=str(uuid.uuid4()), observed_at=datetime.now(timezone.utc),
                                strategy_id=route["id"], strategy_name=route["title"],
                                mode=market.mode, status=status,
                                net_pct=Decimal(route["net_pct"]), reason=route.get("reason") or status,
                                snapshot={"opportunity": route, "books": route_books},
                            ))
                            last_signal_status[route["id"]] = (status, time.monotonic())
                    if signal_rows:
                        session.add_all(signal_rows)
                        await session.commit()
                if trading.auto_simulation_enabled:
                    current = time.monotonic()
                    for route in opportunities(market, trading):
                        if not route["ready"] or not route["depth_ready"] or Decimal(route["net_pct"]) * Decimal("100") < trading.net_threshold_bps:
                            continue
                        if current - last_trigger.get(route["id"], 0) < trading.cooldown_seconds:
                            continue
                        amount = settings.auto_simulation_size_btc if route["kind"] == "cross" else settings.auto_simulation_size_twd
                        try:
                            await execute_simulation(session, SimulateRequest(strategy_id=route["id"], size=amount, trigger_reason=f"auto: net threshold {trading.net_threshold_bps} bps"))
                            last_trigger[route["id"]] = current
                        except (ValueError, ArithmeticError):
                            await session.rollback()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Auto-simulation loop failed")
        await asyncio.sleep(1)


def signal_status(route: dict[str, Any]) -> str:
    if not route["ready"]:
        return "行情失效"
    if not route["depth_ready"]:
        return "參考投入深度不足"
    if Decimal(route["net_pct"]) > 0:
        return "淨利為正"
    return "淨利未達正值"


@app.websocket("/ws")
async def websocket_feed(socket: WebSocket) -> None:
    expected = settings.backend_proxy_secret
    supplied = socket.headers.get("x-backend-proxy-secret", "")
    if not expected or not hmac.compare_digest(supplied, expected):
        await socket.close(code=1008)
        return
    await socket.accept()
    try:
        while True:
            async with SessionLocal() as session:
                await socket.send_json(await _state(session))
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        return

