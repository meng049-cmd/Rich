from __future__ import annotations

import asyncio
import hmac
import logging
import os
import time
import uuid
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Literal

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from pydantic import BaseModel, Field
from sqlalchemy import func, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from . import core as desk


logger = logging.getLogger("rich.vercel")
Mode = Literal["live", "demo", "history"]
PAIRS = ("BTC_TWD", "USDT_TWD", "BTC_USDT")
POLL_INTERVAL_MS = 5000
SESSION_COOKIE = "__Host-rich-ledger"
SESSION_SECONDS = 12 * 60 * 60
access_key = os.environ.get("RICH_LEDGER_ACCESS_KEY", "")
db = desk.make_engine(desk.settings.database_url, on_demand=True) if desk.settings.database_url else None
sessions = async_sessionmaker(db, expire_on_commit=False)
schema_ready = False
schema_lock = asyncio.Lock()
market_lock = asyncio.Lock()
cached_snapshot: desk.MarketData | None = None
snapshot_fetched_at = 0.0

app = FastAPI(title="Rich Market and Paper Ledger", docs_url=None, redoc_url=None, openapi_url=None)


@app.middleware("http")
async def no_cache(request: Request, call_next: Any) -> Response:
    response = await call_next(request)
    response.headers["Cache-Control"] = "private, no-store"
    return response


def _session_signature(expires: str) -> str:
    return hmac.new(access_key.encode(), f"rich-ledger-session:{expires}".encode(), "sha256").hexdigest()


def _signed_in(request: Request) -> bool:
    if len(access_key) < 32:
        return False
    expires, _, signature = request.cookies.get(SESSION_COOKIE, "").partition(".")
    return bool(
        len(expires) == 10 and expires.isascii() and expires.isdecimal()
        and int(expires) > time.time()
        and hmac.compare_digest(signature.encode(), _session_signature(expires).encode())
    )


def _require_same_origin(request: Request) -> None:
    if request.headers.get("sec-fetch-site") in ("cross-site", "same-site"):
        raise HTTPException(403, "請從 Rich 網頁操作")
    origin = request.headers.get("origin")
    if origin and origin != f"https://{request.headers.get('host', '')}":
        raise HTTPException(403, "請從 Rich 網頁操作")


async def _prepare_database() -> None:
    global schema_ready
    if db is None:
        raise HTTPException(503, "Rich 尚未連接 PostgreSQL")
    if not schema_ready:
        async with schema_lock:
            if not schema_ready:
                await desk.initialize_database(db)
                schema_ready = True


async def _ledger_session(request: Request) -> AsyncIterator[AsyncSession]:
    if request.method != "GET":
        _require_same_origin(request)
    if not _signed_in(request):
        raise HTTPException(401, "請先登入 Rich 模擬帳本")
    try:
        await _prepare_database()
        async with sessions() as session:
            yield session
    except (SQLAlchemyError, OSError, TimeoutError) as exc:
        logger.warning("Paper ledger unavailable (%s)", type(exc).__name__)
        raise HTTPException(503, "PostgreSQL 暫時無法連線，請稍後再試") from exc


class SessionRequest(BaseModel):
    key: str = Field(min_length=1, max_length=128)


@app.post("/api/session")
async def sign_in(body: SessionRequest, request: Request, response: Response) -> dict[str, bool]:
    _require_same_origin(request)
    if db is None or len(access_key) < 32:
        raise HTTPException(503, "Rich 模擬帳本尚未設定完成")
    if not hmac.compare_digest(body.key.encode(), access_key.encode()):
        raise HTTPException(401, "存取金鑰不正確")
    expires = str(int(time.time()) + SESSION_SECONDS)
    response.set_cookie(
        SESSION_COOKIE, f"{expires}.{_session_signature(expires)}",
        max_age=SESSION_SECONDS, secure=True, httponly=True, samesite="strict",
    )
    return {"ok": True}


@app.delete("/api/session")
async def sign_out(request: Request, response: Response) -> dict[str, bool]:
    _require_same_origin(request)
    response.delete_cookie(SESSION_COOKIE, secure=True, httponly=True, samesite="strict")
    return {"ok": True}


async def _load_book(client: httpx.AsyncClient, snapshot: desk.MarketData, venue: str, pair: str) -> None:
    try:
        if venue == "bitopro":
            response = await client.get(f"https://api.bitopro.com/v3/order-book/{pair.lower()}", params={"limit": 50})
            response.raise_for_status()
            data = response.json(parse_float=Decimal)
            snapshot._set_book(venue, pair, data["bids"], data["asks"], data.get("timestamp"), source="rest")
        else:
            response = await client.get("https://www.okx.com/api/v5/market/books", params={"instId": pair, "sz": "50"})
            response.raise_for_status()
            payload = response.json(parse_float=Decimal)
            if payload.get("code") != "0":
                raise ValueError("OKX rejected the market data query")
            data = payload["data"][0]
            bids = [{"price": row[0], "amount": row[1]} for row in data["bids"]]
            asks = [{"price": row[0], "amount": row[1]} for row in data["asks"]]
            sequence = data.get("seqId")
            snapshot._set_book(venue, pair, bids, asks, int(data["ts"]), int(sequence) if sequence is not None else None, source="rest")
        book = snapshot.books[f"{venue}:{pair}"]
        if book.valid:
            snapshot.connections[venue]["last_message_ts"] = book.received_ts
        else:
            snapshot.connections[venue]["error"] = f"{pair} 委託簿無效"
    except (httpx.HTTPError, ArithmeticError, KeyError, IndexError, TypeError, ValueError) as exc:
        status = exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else type(exc).__name__
        logger.info("%s %s market data unavailable (%s)", venue, pair, status)
        snapshot.connections[venue]["error"] = f"{pair} 公開委託簿暫不可用"


async def _snapshot(mode: Mode, *, fresh: bool = False) -> desk.MarketData:
    global cached_snapshot, snapshot_fetched_at
    if mode != "live":
        market = desk.MarketData(mode)
        if mode == "demo":
            market.publish_demo(desk.DEMO_ANCHORS.copy())
        return market
    started_at = time.monotonic()
    async with market_lock:
        if cached_snapshot is not None:
            shared_refresh = snapshot_fetched_at >= started_at
            recently_fetched = time.monotonic() - snapshot_fetched_at < 2
            if shared_refresh or (recently_fetched and not fresh):
                return cached_snapshot
        market = desk.MarketData("live")
        async with httpx.AsyncClient(timeout=httpx.Timeout(3, connect=2)) as client:
            await asyncio.gather(
                *(_load_book(client, market, "bitopro", pair) for pair in PAIRS),
                _load_book(client, market, "okx", "BTC-USDT"),
            )
        cached_snapshot = market
        snapshot_fetched_at = time.monotonic()
        return market


def _with_host_status(state: dict[str, Any], ledger_status: str) -> dict[str, Any]:
    return state | {
        "hosting": {"market_transport": "rest", "poll_interval_ms": POLL_INTERVAL_MS, "ledger_status": ledger_status},
    }


@app.get("/api/health")
async def health() -> dict[str, Any]:
    return {"ok": True, "mode": "live", "market_transport": "rest", "database_configured": db is not None}


@app.get("/api/state")
async def dashboard_state(request: Request, mode: Mode = "live") -> dict[str, Any]:
    if db is None or len(access_key) < 32:
        ledger_status = "setup"
    elif not _signed_in(request):
        ledger_status = "locked"
    else:
        ledger_status = "ready"
    if ledger_status == "ready" and mode != "demo":
        try:
            await _prepare_database()
            async with sessions() as session:
                return _with_host_status(await desk._state(session, await _snapshot(mode), ledger_mode="live"), "ready")
        except (SQLAlchemyError, OSError, TimeoutError) as exc:
            logger.warning("Paper ledger unavailable (%s)", type(exc).__name__)
            ledger_status = "unavailable"
    return _with_host_status(await desk._state(None, await _snapshot(mode)), ledger_status)


@app.get("/api/trades")
async def trades(session: AsyncSession = Depends(_ledger_session)) -> dict[str, Any]:
    return {"items": await desk._trades(session, limit=200, mode="live")}


@app.get("/api/signals")
async def signals(session: AsyncSession = Depends(_ledger_session)) -> dict[str, Any]:
    return {"items": await desk._signals(session, limit=200, mode="live")}


@app.put("/api/settings")
async def save_settings(body: desk.TradingSettings, session: AsyncSession = Depends(_ledger_session)) -> dict[str, Any]:
    return await desk.save_trading_settings(session, body)


@app.put("/api/balances")
async def save_balances(body: desk.BalanceUpdate, session: AsyncSession = Depends(_ledger_session)) -> dict[str, Any]:
    return await desk.save_balances(session, body)


@app.post("/api/simulations")
async def simulate(body: desk.SimulateRequest, session: AsyncSession = Depends(_ledger_session)) -> dict[str, Any]:
    market = await _snapshot("live", fresh=True)
    try:
        return await desk.execute_simulation(session, body.model_copy(update={"trigger_reason": "manual"}), market)
    except (ValueError, ArithmeticError) as exc:
        await session.rollback()
        raise HTTPException(409, str(exc)) from exc


async def _monitor(session: AsyncSession, market: desk.MarketData) -> None:
    # One monitor transaction at a time across tabs and function instances.
    if not await session.scalar(text("SELECT pg_try_advisory_xact_lock(9284051662)")):
        return
    settings_row = await session.get(desk.AppSetting, "trading", with_for_update=True)
    trading = desk.TradingSettings.model_validate(settings_row.value) if settings_row else desk.default_trading_settings
    now = datetime.now(timezone.utc)
    recent_signals = await session.execute(
        select(desk.SignalRecord.strategy_id, func.max(desk.SignalRecord.observed_at))
        .where(desk.SignalRecord.mode == "live", desk.SignalRecord.observed_at > now - timedelta(minutes=5))
        .group_by(desk.SignalRecord.strategy_id)
    )
    last_signal = dict(recent_signals.all())
    last_auto: dict[str, datetime] = {}
    if trading.auto_simulation_enabled:
        recent_trades = await session.execute(
            select(desk.PaperTrade.strategy_id, func.max(desk.PaperTrade.created_at))
            .where(desk.PaperTrade.mode == "live", desk.PaperTrade.trigger_reason.like("auto:%"),
                   desk.PaperTrade.created_at > now - timedelta(seconds=trading.cooldown_seconds))
            .group_by(desk.PaperTrade.strategy_id)
        )
        last_auto = dict(recent_trades.all())
    for route in desk.opportunities(market, trading):
        route_id = route["id"]
        net_pct = Decimal(route["net_pct"])
        if route_id not in last_signal:
            status = desk.signal_status(route)
            books = {}
            for key in route["books"]:
                book = market.books.get(key)
                if book is not None:
                    books[key] = book.model_dump(mode="json", exclude={"bids", "asks"}) | {
                        "best_bid": book.bids[0].model_dump(mode="json") if book.bids else None,
                        "best_ask": book.asks[0].model_dump(mode="json") if book.asks else None,
                    }
            session.add(desk.SignalRecord(
                id=str(uuid.uuid4()), observed_at=now, strategy_id=route_id, strategy_name=route["title"],
                mode="live", status=status, net_pct=net_pct, reason=route["reason"] or status,
                snapshot={"opportunity": route, "books": books},
            ))
        if (not trading.auto_simulation_enabled or route_id in last_auto or not route["ready"]
                or not route["depth_ready"] or net_pct * 100 < trading.net_threshold_bps):
            continue
        size = desk.settings.auto_simulation_size_btc if route["kind"] == "cross" else desk.settings.auto_simulation_size_twd
        try:
            await desk.execute_simulation(
                session, desk.SimulateRequest(strategy_id=route_id, size=size, trigger_reason="auto: net threshold"),
                market, commit=False, trading=trading,
            )
        except (ValueError, ArithmeticError):
            continue
    await session.commit()


@app.post("/api/monitor")
async def monitor(session: AsyncSession = Depends(_ledger_session)) -> dict[str, Any]:
    market = await _snapshot("live")
    await _monitor(session, market)
    return _with_host_status(await desk._state(session, market, ledger_mode="live"), "ready")
