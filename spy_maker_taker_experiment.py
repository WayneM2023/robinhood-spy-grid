#!/usr/bin/env python3
"""SPY Maker -> Taker Hedge experiment.

This module is deliberately independent from ``spy_grid_bot.py``.  The
exchange adapter is small and injectable so the safety/state machine can be
tested without credentials or a live exchange.  The default points adapter is
unavailable (it never guesses an endpoint).
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import signal
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import inspect
import itertools
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Optional, Protocol
from enum import Enum

try:
    import fcntl
except ImportError:  # pragma: no cover - the LIVE target is Linux
    fcntl = None

LOG = logging.getLogger("spy-experiment")
ZERO = Decimal("0")
# These values mean the exchange can no longer execute the order.  ``filled``
# still requires fill/position reconciliation before its accounting is final;
# the cancel path must nevertheless stop waiting for another cancel response.
NON_EXECUTABLE_ORDER_STATUSES = {
    "filled", "cancelled", "canceled", "canceled-post-only", "rejected", "expired",
}


@dataclass(frozen=True)
class CycleExecutionMetrics:
    """Execution-derived economics for one completed maker/taker cycle.

    These values intentionally do not use account PnL fields.  Those fields
    are optional in Lighter trade responses and therefore cannot be a source
    of truth for experiment economics.
    """
    maker_vwap: Decimal
    taker_vwap: Decimal
    matched_qty: Decimal
    maker_fee: Optional[Decimal]
    taker_fee: Optional[Decimal]
    gross_price_pnl: Decimal
    net_pnl: Optional[Decimal]
    slippage_cost: Decimal


def _execution_metrics(maker: Any, taker: Any) -> CycleExecutionMetrics:
    """Compute matched PnL from actual execution records, never account PnL."""
    maker_fills = list(getattr(maker, "execution_fills", None) or [maker])
    taker_fills = list(getattr(taker, "execution_fills", None) or [taker])

    def leg(fills: list[Any]) -> tuple[Decimal, Decimal, Decimal, Optional[Decimal]]:
        qty = notional = fees = ZERO
        fee_known = True
        for fill in fills:
            fill_qty = abs(dec(getattr(fill, "qty", "0") or "0"))
            fill_price = dec(getattr(fill, "price", "0") or "0")
            qty += fill_qty
            reported_notional = abs(dec(getattr(fill, "notional", None) or "0"))
            notional += reported_notional if reported_notional > ZERO else abs(fill_price * fill_qty)
            raw_fee = getattr(fill, "fee", None)
            if raw_fee is None or getattr(fill, "fee_known", True) is False:
                fee_known = False
            else:
                fees += abs(dec(raw_fee))
        return qty, notional, (notional / qty if qty else ZERO), (fees if fee_known else None)

    maker_qty, _, maker_vwap, maker_fee = leg(maker_fills)
    taker_qty, _, taker_vwap, taker_fee = leg(taker_fills)
    matched_qty = min(maker_qty, taker_qty)
    maker_side = str(getattr(maker, "side", "buy")).lower()
    gross = ((taker_vwap - maker_vwap) if maker_side == "buy" else (maker_vwap - taker_vwap)) * matched_qty
    total_fee = None if maker_fee is None or taker_fee is None else maker_fee + taker_fee
    return CycleExecutionMetrics(
        maker_vwap=maker_vwap, taker_vwap=taker_vwap, matched_qty=matched_qty,
        maker_fee=maker_fee, taker_fee=taker_fee, gross_price_pnl=gross,
        net_pnl=None if total_fee is None else gross - total_fee,
        slippage_cost=max(ZERO, -gross),
    )


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


def _proc_cmdline(pid: int) -> str:
    try:
        return Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace").strip()
    except OSError:
        return ""


def _proc_environ(pid: int) -> str:
    try:
        return Path(f"/proc/{pid}/environ").read_bytes().replace(b"\0", b" ").decode(errors="replace")
    except OSError:
        return ""


def find_live_experiment_processes(exclude_pid: Optional[int] = None) -> list[int]:
    """Find non-self LIVE experiment processes before taking the account lock."""
    found: list[int] = []
    for proc in Path("/proc").glob("[0-9]*"):
        try:
            pid = int(proc.name)
        except ValueError:
            continue
        if pid == os.getpid() or pid == exclude_pid:
            continue
        command = _proc_cmdline(pid)
        if "spy_maker_taker_experiment.py" not in command:
            continue
        env = _proc_environ(pid)
        if "DRY_RUN=false" in env or "LIVE_CONFIRM=I_UNDERSTAND" in env:
            found.append(pid)
    return found


def _arm_parent_death_signal() -> None:
    """Make an SSH/session parent loss deliver SIGTERM to the LIVE child."""
    if not sys.platform.startswith("linux"):
        return
    try:
        import ctypes
        ctypes.CDLL(None).prctl(1, signal.SIGTERM, 0, 0, 0)  # PR_SET_PDEATHSIG
    except Exception as exc:  # pragma: no cover - platform/runtime dependent
        LOG.warning("parent-death signal unavailable: %s", exc)


class ExperimentInstanceLock:
    """Exclusive account+market lock that survives SSH session loss."""

    def __init__(self, account_index: int, market_id: int = 26, lock_dir: Path | str = "/var/run") -> None:
        if fcntl is None:
            raise RuntimeError("single-instance lock requires fcntl")
        self.account_index = int(account_index)
        self.market_id = int(market_id)
        self.path = Path(lock_dir) / f"robinhood-spy-experiment-{self.account_index}-{self.market_id}.lock"
        self._file: Any = None
        self.record: dict[str, Any] = {}
        self.acquired = False

    def acquire(self, experiment_id: str) -> dict[str, Any]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self.path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(self._file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self._file.seek(0)
            try:
                existing = json.load(self._file)
            except Exception:
                existing = {}
            pid = int(existing.get("pid", 0) or 0)
            if _pid_alive(pid):
                cmd = _proc_cmdline(pid)
                belongs = "spy_maker_taker_experiment.py" in cmd and (
                    str(existing.get("experiment_id", "")) in cmd
                    or f"EXPERIMENT_ID={existing.get('experiment_id', '')}" in _proc_environ(pid)
                )
                raise RuntimeError(
                    f"active experiment lock: pid={pid} experiment={existing.get('experiment_id')} "
                    f"command_matches={belongs}"
                ) from exc
            raise RuntimeError(f"lock is held by an uninspectable process: {self.path}") from exc
        self.record = {
            "pid": os.getpid(),
            "experiment_id": experiment_id,
            "account_index": self.account_index,
            "market_id": self.market_id,
            "command_line": " ".join(sys.argv),
            "acquired_at": iso(),
        }
        self._file.seek(0)
        self._file.truncate()
        json.dump(self.record, self._file)
        self._file.flush()
        os.fsync(self._file.fileno())
        self.acquired = True
        return self.record

    def release(self) -> None:
        if self._file is None:
            return
        if not self.acquired:
            self._file.close()
            self._file = None
            return
        try:
            # Keep one stable inode for the lifetime of this account/market
            # lock.  unlink-after-unlock permits a third process to acquire a
            # new inode while another process still owns the old one.
            self.record["released_at"] = iso()
            self.record["status"] = "released"
            self._file.seek(0)
            self._file.truncate()
            json.dump(self.record, self._file)
            self._file.flush()
            os.fsync(self._file.fileno())
            fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
        finally:
            self._file.close()
            self._file = None
            self.acquired = False


class EventKind(str, Enum):
    CONNECTION = "connection"
    SUBSCRIPTION = "subscription"
    ORDER = "order"
    FILL = "fill"
    SHUTDOWN = "shutdown"
    TIMER = "timer"


class ExperimentState(str, Enum):
    IDLE = "IDLE"
    QUOTING = "QUOTING"
    MAKER_RESTING = "MAKER_RESTING"
    CANCELING = "CANCELING"
    HEDGING = "HEDGING"
    RECONCILING = "RECONCILING"
    FLAT = "FLAT"
    HALTING = "HALTING"
    STOPPED = "STOPPED"


@dataclass(frozen=True)
class NormalizedEvent:
    kind: EventKind
    payload: Any = None
    source: str = ""
    received_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class RateLimitError(RuntimeError):
    """REST remained rate-limited after bounded backoff."""


class RESTRateLimiter:
    """Priority REST scheduler with non-blocking backoff.

    A failed low-priority request is re-queued after its backoff rather than
    holding the dispatcher lock.  All priorities share the same minimum
    request interval, including P0.
    """
    def __init__(self, min_interval: float = 1.0) -> None:
        self.min_interval = min_interval
        self._queue: asyncio.PriorityQueue[Any] = asyncio.PriorityQueue()
        self._sequence = itertools.count()
        self._last_request = 0.0
        self._worker: asyncio.Task[Any] | None = None
        self._closed = False

    @staticmethod
    def is_429(exc: BaseException) -> bool:
        text = str(exc).lower()
        return "429" in text or "too many requests" in text or getattr(exc, "status", None) == 429

    @staticmethod
    def _priority(value: int | bool) -> int:
        # Backward-compatible bool API: priority=True means P0.
        return 0 if value is True else (2 if value is False else int(value))

    async def call(self, operation: Any, *, priority: int | bool = 2) -> Any:
        if self._closed:
            raise RuntimeError("REST scheduler is closed")
        if self._worker is None:
            self._worker = asyncio.create_task(self._run())
        future = asyncio.get_running_loop().create_future()
        await self._queue.put((self._priority(priority), next(self._sequence), operation, future, 1.0))
        return await future

    async def _requeue_after(self, item: tuple[Any, ...], delay: float) -> None:
        await asyncio.sleep(delay)
        if not self._closed:
            await self._queue.put(item)

    async def _run(self) -> None:
        while not self._closed:
            priority, sequence, operation, future, backoff = await self._queue.get()
            if future.cancelled():
                continue
            wait = self.min_interval - (time.monotonic() - self._last_request)
            if wait > 0:
                await asyncio.sleep(wait)
            try:
                result = operation()
                if inspect.isawaitable(result):
                    result = await result
                self._last_request = time.monotonic()
                if not future.done():
                    future.set_result(result)
            except Exception as exc:
                self._last_request = time.monotonic()
                if not self.is_429(exc):
                    if not future.done():
                        future.set_exception(exc)
                elif backoff > 8.0:
                    if not future.done():
                        future.set_exception(RateLimitError("REST 429 persisted after exponential backoff"))
                else:
                    LOG.warning("REST 429; re-queueing P%s after %.1fs", priority, backoff)
                    asyncio.create_task(self._requeue_after(
                        (priority, next(self._sequence), operation, future, min(backoff * 2.0, 15.0)),
                        min(backoff, 15.0),
                    ))

    async def close(self) -> None:
        self._closed = True
        if self._worker is not None:
            self._worker.cancel()
            await asyncio.gather(self._worker, return_exceptions=True)


def dec(value: Any) -> Decimal:
    return Decimal(str(value))


def iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def timestamp_ms(value: Any) -> Optional[int]:
    try:
        raw = int(str(value))
        return raw if raw > 10_000_000_000 else raw * 1000
    except (TypeError, ValueError):
        return None


def attribution_timestamp_ms(value: Any) -> Optional[int]:
    """Parse either exchange epoch time or coordinator ISO time for attribution."""
    parsed = timestamp_ms(value)
    if parsed is not None:
        return parsed
    try:
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp() * 1000)
    except (TypeError, ValueError):
        return None


def hedge_protection_price(side: str, best_bid: Decimal, best_ask: Decimal, buffer_bps: Decimal) -> Decimal:
    buffer = buffer_bps / Decimal("10000")
    return best_bid * (Decimal("1") - buffer) if side == "sell" else best_ask * (Decimal("1") + buffer)


@dataclass(frozen=True)
class ExperimentConfig:
    experiment_id: str = "SPY_MT_001"
    strategy_version: str = "spy-maker-taker-v1"
    order_notional: Decimal = Decimal("500")
    quote_refresh_interval: float = 2.0
    max_net_position: Decimal = Decimal("500")
    target_taker_turnover: Decimal = Decimal("10000")
    poll_seconds: float = 1.0
    max_runtime_minutes: float = 60.0
    summary_seconds: float = 600.0
    hedge_latency_warning_ms: int = 5000
    hedge_latency_limit_ms: int = 5000
    min_position_tolerance: Decimal = Decimal("0.0001")
    hedge_slippage_buffer_bps: Decimal = Decimal("5")
    realized_pnl_stop: Decimal = Decimal("-10")
    failed_hedges_stop: int = 2
    cancel_failures_stop: int = 3
    dry_run: bool = True
    data_dir: Path = Path("/var/log/robinhood-spy-experiment")

    @classmethod
    def from_env(cls) -> "ExperimentConfig":
        def b(n: str, default: bool) -> bool:
            return os.getenv(n, str(default)).lower() in {"1", "true", "yes", "on"}
        cfg = cls(
            experiment_id=os.getenv("EXPERIMENT_ID", "SPY_MT_001"),
            strategy_version=os.getenv("STRATEGY_VERSION", "spy-maker-taker-v1"),
            order_notional=dec(os.getenv("ORDER_NOTIONAL", "500")),
            quote_refresh_interval=float(os.getenv("QUOTE_REFRESH_INTERVAL", "2")),
            max_net_position=dec(os.getenv("MAX_NET_POSITION", "500")),
            target_taker_turnover=dec(os.getenv("TARGET_TAKER_TURNOVER", "10000")),
            poll_seconds=float(os.getenv("EXPERIMENT_POLL_SECONDS", "1")),
            max_runtime_minutes=float(os.getenv("MAX_RUNTIME_MINUTES", "60")),
            dry_run=b("DRY_RUN", True),
            data_dir=Path(os.getenv("EXPERIMENT_DATA_DIR", "/var/log/robinhood-spy-experiment")),
            min_position_tolerance=dec(os.getenv("MIN_POSITION_TOLERANCE", "0.0001")),
            hedge_slippage_buffer_bps=dec(os.getenv("HEDGE_SLIPPAGE_BUFFER_BPS", "5")),
            hedge_latency_warning_ms=int(os.getenv("HEDGE_LATENCY_WARNING_MS", "5000")),
            hedge_latency_limit_ms=int(os.getenv("HEDGE_LATENCY_LIMIT_MS", "5000")),
        )
        if cfg.order_notional <= 0 or cfg.max_net_position <= 0 or cfg.target_taker_turnover <= 0:
            raise ValueError("order notional, max position and target turnover must be positive")
        if cfg.max_net_position < cfg.order_notional * Decimal("0.9"):
            raise ValueError("MAX_NET_POSITION is too small for the configured order")
        if cfg.hedge_slippage_buffer_bps < 0:
            raise ValueError("HEDGE_SLIPPAGE_BUFFER_BPS must be non-negative")
        if cfg.hedge_latency_warning_ms <= 0 or cfg.hedge_latency_limit_ms < cfg.hedge_latency_warning_ms:
            raise ValueError("hedge latency limit must be >= positive warning threshold")
        return cfg


@dataclass
class ExperimentStats:
    maker_volume: Decimal = ZERO
    taker_volume: Decimal = ZERO
    total_turnover: Decimal = ZERO
    cycles: int = 0
    fills: int = 0
    realized_pnl: Decimal = ZERO
    fees: Decimal = ZERO
    estimated_spread_slippage: Decimal = ZERO
    gross_price_pnl: Decimal = ZERO
    net_pnl_known: bool = True
    fees_known: bool = True
    unknown_fee_cycles: int = 0
    failed_hedges: int = 0
    cancel_failures: int = 0
    max_net_position: Decimal = ZERO
    max_net_position_qty: Decimal = ZERO
    max_net_position_usd: Decimal = ZERO
    position_abs_sum: Decimal = ZERO
    position_abs_sum_usd: Decimal = ZERO
    position_samples: int = 0
    nonflat_seconds: float = 0
    nonflat_started: Optional[float] = None
    hedge_latencies_ms: list[int] = field(default_factory=list)
    execution_failures: int = 0
    latency_violations: int = 0
    latency_warnings: int = 0
    reconciliation_failures: int = 0
    data_quality_failures: int = 0

    def json(self) -> dict[str, Any]:
        d = asdict(self)
        d.pop("nonflat_started", None)
        d["maker_volume"] = str(self.maker_volume)
        d["taker_volume"] = str(self.taker_volume)
        d["total_turnover"] = str(self.total_turnover)
        for k in ("realized_pnl", "fees", "estimated_spread_slippage", "gross_price_pnl", "max_net_position"):
            d[k] = str(getattr(self, k))
        return d


@dataclass
class PendingHedgeAttribution:
    """A position-confirmed id-less hedge awaiting its own execution record.

    Exposure is already flat when this is created, so it must never trigger a
    new hedge.  It exists solely to attach a later account trade to the
    completed cycle without accepting an unrelated account taker trade.
    """
    cycle_id: int
    maker: Any
    position_before: Decimal
    maker_timestamp: str
    hedge_submit_timestamp: str
    client_order_ids: set[str]
    side: str
    expected_qty: Decimal
    position_flat_timestamp: str
    filled_qty: Decimal = ZERO
    filled_notional: Decimal = ZERO
    first_fill: Any = None
    last_fill: Any = None
    execution_fills: list[Any] = field(default_factory=list)


class ExperimentBroker(Protocol):
    async def best_bid_ask(self) -> tuple[Decimal, Decimal]: ...
    async def position(self, priority: bool = False) -> Decimal: ...
    async def resting_orders(self) -> list[Any]: ...
    async def post_only(self, side: str, price: Decimal, qty: Decimal, client_id: str) -> Any: ...
    async def cancel(self, order: Any) -> None: ...
    async def cancel_order(self, order_id: int) -> dict[str, Any]: ...
    async def confirm_cancel_order(self, order_id: int) -> dict[str, Any]: ...
    async def market_reduce_only(self, side: str, qty: Decimal, client_id: str) -> Any: ...
    async def fills(self, since: Optional[str]) -> list[Any]: ...
    async def cancel_order_confirmed(self, order_id: int) -> dict[str, Any]: ...
    async def order_status(self, order_id: int) -> dict[str, Any]: ...

    async def start_fill_stream(self, seen_fill_ids: set[str]) -> bool: ...
    async def next_stream_fill(self, timeout: float) -> Any: ...
    async def stop_fill_stream(self) -> None: ...
    async def start_order_stream(self) -> bool: ...
    async def next_order_update(self, timeout: float) -> Any: ...
    async def stop_order_stream(self) -> None: ...
    def ws_ready(self) -> bool: ...


class PointsProvider(Protocol):
    async def snapshot(self) -> tuple[Optional[Decimal], Optional[int]]: ...


class UnavailablePoints:
    async def snapshot(self) -> tuple[None, None]:
        return None, None


class LighterExperimentBroker:
    """Adapter over the already-reviewed LighterBroker primitives.

    It intentionally refuses to classify a fill unless the API exposes an
    explicit maker flag.  A missing flag must not silently turn into taker
    turnover data.
    """
    def __init__(self, classic_cfg: Any) -> None:
        from spy_grid_bot import LighterBroker, Market, PlannedOrder, ROBINHOOD_API_URL, SPY_MARKET_ID, scaled_int
        self._broker = LighterBroker(classic_cfg)
        self._Market = Market
        self._PlannedOrder = PlannedOrder
        self._market_id = SPY_MARKET_ID
        self._api_url = ROBINHOOD_API_URL
        self._scaled_int = scaled_int
        self._rest_limiter = RESTRateLimiter(1.0)
        self.hedge_slippage_buffer_bps = dec(os.getenv("HEDGE_SLIPPAGE_BUFFER_BPS", "5"))
        self._market_cache: tuple[Any, float] | None = None
        self._book_cache: tuple[tuple[Decimal, Decimal], float] | None = None
        self._fill_stream_seen: set[str] = set()
        self._vendor_ws: Any = None
        self._ws_loop: asyncio.AbstractEventLoop | None = None
        self.fill_stream_status = "not_started"
        self.events: asyncio.Queue[NormalizedEvent] = asyncio.Queue()
        self.private_subscriptions: set[str] = set()
        self.connection_ready = False
        self.order_stream_status = "not_started"

    async def _rest(self, operation: Any, *, priority: int | bool = 2) -> Any:
        return await self._rest_limiter.call(operation, priority=priority)

    async def best_bid_ask(self) -> tuple[Decimal, Decimal]:
        if self._book_cache is not None and time.monotonic() - self._book_cache[1] <= 1.0:
            return self._book_cache[0]
        url = "https://api.rh.lighter.xyz/api/v1/orderBookOrders?" + urllib.parse.urlencode({"market_id": self._market_id, "limit": 250})
        def get() -> dict[str, Any]:
            req = urllib.request.Request(url, headers={"User-Agent": "spy-maker-taker-experiment/1.0"})
            with urllib.request.urlopen(req, timeout=10) as response:
                return json.loads(response.read().decode())
        data = await self._rest(lambda: asyncio.to_thread(get), priority=3)
        bids, asks = data.get("bids"), data.get("asks")
        if bids is None or asks is None:
            book = data.get("order_book") or data.get("order_book_orders") or {}
            bids, asks = book.get("bids"), book.get("asks")
        if not bids or not asks:
            raise RuntimeError("SPY best bid/ask unavailable")
        book = (max(dec(x["price"]) for x in bids), min(dec(x["price"]) for x in asks))
        self._book_cache = (book, time.monotonic())
        return book

    async def position(self, priority: bool = False) -> Decimal:
        pos, _, _ = await self._rest(self._broker.account_snapshot, priority=0 if priority else 2)
        return pos.size

    async def resting_orders(self) -> list[Any]:
        return await self._rest(self._broker.active_orders, priority=3)

    async def cancel_order(self, order_id: int) -> dict[str, Any]:
        """Submit an order-id-specific cancel without waiting for terminal state."""
        started = time.monotonic()
        # Lighter's signer cancel primitive only needs ``order_index``.  Do
        # not spend a P1 active-orders lookup before the P0 cancel submit;
        # a maker fill has already put us in the hedge critical path.
        await self._rest(lambda: self._broker.cancel(SimpleNamespace(order_index=int(order_id))), priority=0)
        return {"order_id": int(order_id), "cancel_requested_at": started}

    async def confirm_cancel_order(self, order_id: int) -> dict[str, Any]:
        """Boundedly reconcile a previously submitted cancel by exact id."""
        started = time.monotonic()
        # Bounded reconciliation.  The coordinator feeds private order events
        # into ``_terminal_order_status``; REST is only the fallback.
        for _ in range(3):
            self._broker.ensure_auth()
            response = await self._rest(lambda: self._broker.order_api.account_inactive_orders(
                authorization=self._broker.auth,
                account_index=self._broker.cfg.account_index,
                limit=100,
                market_id=self._market_id,
                market_type="perp",
            ), priority=1)
            inactive = next((o for o in (response.orders or []) if int(getattr(o, "order_index", -1)) == int(order_id)), None)
            status = str(getattr(inactive, "status", "")).lower() if inactive is not None else ""
            # Lighter uses ``canceled-post-only`` when a post-only maker is
            # removed by the matching engine.  It is terminal just like a
            # regular cancel and must not turn into a false cancel timeout.
            if status in NON_EXECUTABLE_ORDER_STATUSES:
                return {"order_id": int(order_id), "status": status, "cancel_requested_at": started, "confirmed_at": time.monotonic()}
            # Only query active orders if inactive did not prove a terminal
            # state.  This avoids an unnecessary request in the normal path.
            active = await self._rest(self._broker.active_orders, priority=1)
            active_item = next((o for o in active if int(getattr(o, "order_index", -1)) == int(order_id)), None)
            if active_item is None and inactive is None:
                return {"order_id": int(order_id), "status": "not_active", "cancel_requested_at": started, "confirmed_at": time.monotonic()}
            await asyncio.sleep(0.25)
        raise RuntimeError(f"order {order_id} cancel not confirmed within 3 seconds")

    async def cancel_order_confirmed(self, order_id: int) -> dict[str, Any]:
        """Compatibility wrapper for callers that need submit + terminal confirmation."""
        try:
            requested = await self.cancel_order(order_id)
        except Exception:
            # A fill/cancel race may reject the signer request after the order
            # already became terminal; exact-id reconciliation decides safely.
            requested = {"order_id": int(order_id), "cancel_requested_at": time.monotonic()}
        result = await self.confirm_cancel_order(order_id)
        result.setdefault("cancel_requested_at", requested["cancel_requested_at"])
        return result

    async def order_status(self, order_id: int) -> dict[str, Any]:
        """Read one order by id from active/inactive REST state."""
        def fields(item: Any, source: str) -> dict[str, Any]:
            return {
                "order_id": int(order_id),
                "status": str(getattr(item, "status", "active")).lower(),
                "filled_qty": str(getattr(item, "filled_base_amount", getattr(item, "filled_qty", "0")) or "0"),
                "remaining_qty": str(getattr(item, "remaining_base_amount", getattr(item, "remaining_qty", "0")) or "0"),
                "price": str(getattr(item, "price", "0") or "0"),
                "side": "sell" if bool(getattr(item, "is_ask", False)) else str(getattr(item, "side", "buy")),
                "fill_timestamp": getattr(item, "filled_at", getattr(item, "timestamp", None)),
                "source": source,
            }
        active = await self._rest(self._broker.active_orders, priority=1)
        item = next((o for o in active if int(getattr(o, "order_index", -1)) == int(order_id)), None)
        if item is not None:
            return fields(item, "rest_active")
        self._broker.ensure_auth()
        response = await self._rest(lambda: self._broker.order_api.account_inactive_orders(
            authorization=self._broker.auth, account_index=self._broker.cfg.account_index,
            limit=100, market_id=self._market_id, market_type="perp"), priority=1)
        item = next((o for o in (response.orders or []) if int(getattr(o, "order_index", -1)) == int(order_id)), None)
        if item is None:
            return {"order_id": int(order_id), "status": "unknown", "source": "rest_inactive"}
        return fields(item, "rest_inactive")

    async def account_state(self) -> dict[str, Any]:
        response = await self._rest(lambda: self._broker.account_api.account(
            by="index", value=str(self._broker.cfg.account_index), active_only=True))
        if not response.accounts:
            raise RuntimeError("Lighter account was not found")
        account = response.accounts[0]
        state: dict[str, Any] = {"position": "0", "entry_price": None, "unrealized_pnl": None}
        for item in account.positions or []:
            if int(item.market_id) != self._market_id:
                continue
            qty = dec(item.position)
            signed = qty if int(item.sign) > 0 else -qty
            state["position"] = str(signed)
            state["entry_price"] = str(getattr(item, "avg_entry_price", None))
            pnl = getattr(item, "unrealized_pnl", None)
            state["unrealized_pnl"] = None if pnl is None else str(pnl)
        return state

    async def raw_trades(self) -> list[Any]:
        return await self._rest(self._broker.recent_trades, priority=1)

    def cleanup_metrics(self, before_ids: set[str], trades: list[Any]) -> dict[str, Any]:
        account_id = int(self._broker.cfg.account_index)
        taker_volume = ZERO
        realized_pnl = ZERO
        fees = ZERO
        for trade in trades:
            tid = str(getattr(trade, "trade_id_str", getattr(trade, "trade_id", "")))
            if tid in before_ids:
                continue
            own_ask = int(getattr(trade, "ask_account_id", -1)) == account_id
            own_bid = int(getattr(trade, "bid_account_id", -1)) == account_id
            maker_ask = bool(getattr(trade, "is_maker_ask", False))
            own_maker = (own_ask and maker_ask) or (own_bid and not maker_ask)
            if not own_ask and not own_bid:
                continue
            if not own_maker:
                taker_volume += abs(dec(getattr(trade, "usd_amount", "0")))
                pnl = getattr(trade, "ask_account_pnl", None) if own_ask else getattr(trade, "bid_account_pnl", None)
                if pnl is not None:
                    realized_pnl += dec(pnl)
                fee = getattr(trade, "taker_fee", None)
                if fee is not None:
                    fees += abs(dec(fee))
        return {"taker_volume": taker_volume, "realized_pnl": realized_pnl, "fees": fees}

    async def post_only(self, side: str, price: Decimal, qty: Decimal, client_id: str) -> Any:
        market = await self._broker_market()
        order = self._PlannedOrder(side, price, qty, False, client_id, abs(hash(client_id)) % (2**48 - 1))
        await self._rest(lambda: self._broker.create(order, market), priority=3)
        return order

    async def cancel(self, order: Any) -> None:
        await self._rest(lambda: self._broker.cancel(order), priority=0)

    async def market_reduce_only(self, side: str, qty: Decimal, client_id: str) -> Any:
        market = await self._broker_market()
        bid, ask = await self.best_bid_ask()
        # Refresh the executable side immediately before every hedge.  The
        # protection buffer is part of the order price, while IOC/reduce_only
        # still guarantee this order cannot rest or add exposure.
        price = hedge_protection_price(side, bid, ask, self.hedge_slippage_buffer_bps)
        self._broker.ensure_auth()
        client_order_index = abs(hash(client_id)) % (2**48 - 1)
        _, response, error = await self._rest(lambda: self._broker.signer.create_order(
            market_index=self._market_id,
            client_order_index=client_order_index,
            base_amount=self._scaled_int(qty, market.size_decimals),
            price=self._scaled_int(price, market.price_decimals),
            is_ask=side == "sell",
            order_type=self._broker.signer.ORDER_TYPE_MARKET,
            time_in_force=self._broker.signer.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
            reduce_only=True,
            order_expiry=self._broker.signer.DEFAULT_IOC_EXPIRY,
            api_key_index=self._broker.cfg.api_key_index,
        ), priority=0)
        if error is not None or getattr(response, "code", 200) != 200:
            raise RuntimeError(f"IOC reduce-only hedge rejected: {error or response}")
        order_id = None
        for name in ("order_index", "order_id", "index"):
            value = getattr(response, name, None)
            if value is not None:
                order_id = int(value)
                break
        return SimpleNamespace(response=response, error=error, order_id=order_id,
                               client_order_id=client_order_index, requested_qty=qty,
                               best_bid=bid, best_ask=ask, protection_price=price,
                               slippage_buffer_bps=self.hedge_slippage_buffer_bps)

    async def fills(self, since: Optional[str]) -> list[Any]:
        fills = await self._rest(self._broker.recent_trades, priority=1)
        account_id = int(self._broker.cfg.account_index)
        normalized: list[Any] = []
        for fill in fills:
            ask_own = int(getattr(fill, "ask_account_id", -1)) == account_id
            bid_own = int(getattr(fill, "bid_account_id", -1)) == account_id
            maker = getattr(fill, "is_maker", None)
            if maker is None:
                maker = getattr(fill, "maker", None)
            if maker is None:
                if not ask_own and not bid_own:
                    continue
                maker_ask = getattr(fill, "is_maker_ask", None)
                if maker_ask is None:
                    raise RuntimeError("trade API did not expose explicit maker/taker classification")
                maker = (ask_own and bool(maker_ask)) or (bid_own and not bool(maker_ask))
            notional_raw = getattr(fill, "usd_amount", None)
            fee_raw = getattr(fill, "maker_fee", None) if bool(maker) else getattr(fill, "taker_fee", None)
            pnl_raw = (
                getattr(fill, "ask_account_pnl", None)
                if ask_own
                else getattr(fill, "bid_account_pnl", None)
            )
            normalized.append(SimpleNamespace(
                maker=bool(maker),
                id=getattr(fill, "trade_id_str", getattr(fill, "trade_id", "")),
                qty=getattr(fill, "size", "0"),
                notional=abs(dec(notional_raw or "0")),
                price=getattr(fill, "price", "0"),
                timestamp=getattr(fill, "timestamp", iso()),
                order_id=int(getattr(fill, "ask_id", 0) if ask_own else getattr(fill, "bid_id", 0)),
                client_order_id=str(getattr(fill, "ask_client_id", "") if ask_own else getattr(fill, "bid_client_id", "") or ""),
                side="sell" if ask_own else "buy",
                fee=None if fee_raw is None else abs(dec(fee_raw)),
                # Retained only for raw diagnostics.  Execution economics are
                # calculated from the matched maker/taker fills below.
                pnl=None if pnl_raw is None else dec(pnl_raw),
            ))
        return normalized

    def _normalize_stream_trade(self, trade: Any) -> Any | None:
        """Convert an account_all WS Trade into the experiment Fill shape."""
        if not isinstance(trade, dict):
            return None
        account_id = int(self._broker.cfg.account_index)
        ask_own = int(trade.get("ask_account_id", -1)) == account_id
        bid_own = int(trade.get("bid_account_id", -1)) == account_id
        if not ask_own and not bid_own:
            return None
        maker_ask = trade.get("is_maker_ask")
        if maker_ask is None:
            return None
        fill_id = str(trade.get("trade_id", trade.get("trade_id_str", "")))
        if not fill_id:
            return None
        own_maker = (ask_own and bool(maker_ask)) or (bid_own and not bool(maker_ask))
        fee_raw = trade.get("maker_fee") if own_maker else trade.get("taker_fee")
        return SimpleNamespace(
            maker=own_maker,
            id=fill_id,
            qty=trade.get("size", "0"),
            notional=abs(dec(trade.get("usd_amount", "0"))),
            price=trade.get("price", "0"),
            timestamp=trade.get("timestamp", iso()),
            order_id=int(trade.get("ask_id", 0) if ask_own else trade.get("bid_id", 0)),
            client_order_id=str(trade.get("ask_client_id", "") if ask_own else trade.get("bid_client_id", "") or ""),
            side="sell" if ask_own else "buy",
            fee=(None if fee_raw is None else abs(dec(fee_raw))),
            pnl=(None if (trade.get("ask_account_pnl") if ask_own else trade.get("bid_account_pnl")) is None
                 else dec(trade.get("ask_account_pnl") if ask_own else trade.get("bid_account_pnl"))),
        )

    @staticmethod
    def _ws_normalize_order_record(raw: dict[str, Any], _symbol: str) -> dict[str, Any]:
        item = dict(raw)
        item["order_id"] = item.get("order_id", item.get("order_index", item.get("id")))
        item["client_order_id"] = item.get("client_order_id", item.get("client_order_index", item.get("client_id", "")))
        item["side"] = item.get("side", "sell" if item.get("is_ask", item.get("isAsk", False)) else "buy")
        item["price"] = item.get("price", item.get("limit_price", "0"))
        item["quantity"] = item.get("quantity", item.get("original_quantity", item.get("size", "0")))
        item["filled_quantity"] = item.get("filled_quantity", item.get("filled_base_amount", item.get("filled_qty", item.get("executed_qty", "0"))))
        item["remaining_quantity"] = item.get("remaining_quantity", item.get("remaining_base_amount", item.get("remaining_qty", "0")))
        return item

    @staticmethod
    def _ws_normalize_trade_record(raw: dict[str, Any]) -> dict[str, Any]:
        item = dict(raw)
        item["trade_id"] = item.get("trade_id", item.get("tradeId", item.get("id")))
        item["size"] = item.get("size", item.get("quantity", item.get("qty", "0")))
        item["price"] = item.get("price", item.get("fill_price", "0"))
        item["order_id"] = item.get("order_id", item.get("order_index", item.get("id")))
        item["side"] = item.get("side", "sell" if item.get("is_ask", item.get("isAsk", False)) else "buy")
        item["is_maker"] = item.get("is_maker", item.get("maker", item.get("isMaker", True)))
        return item

    async def start_fill_stream(self, seen_fill_ids: set[str]) -> bool:
        """Start one private WS connection; readiness waits for both ACKs."""
        try:
            from ws_client import RobinhoodLighterWebSocket
            self._fill_stream_seen = set(seen_fill_ids)
            self._ws_loop = asyncio.get_running_loop()

            def emit(kind: EventKind, payload: Any = None, source: str = "ws") -> None:
                if self._ws_loop is not None:
                    self._ws_loop.call_soon_threadsafe(
                        self.events.put_nowait, NormalizedEvent(kind, payload, source))

            def on_message(stream: str, payload: Any) -> None:
                if self._vendor_ws is None or self._ws_loop is None:
                    return
                if stream in {"connection", "connected"}:
                    emit(EventKind.CONNECTION, payload)
                elif stream == "subscribed":
                    emit(EventKind.SUBSCRIPTION, payload)
                elif stream.startswith("private.orders"):
                    for update in self._vendor_ws.parse_order_updates(payload):
                        emit(EventKind.ORDER, update, "account_orders_ws")
                elif stream.startswith("private.fills"):
                    # account_all_trades carries ask/bid account identities.
                    # Normalize against our account before emitting, rather than
                    # applying the generic public-trade parser.
                    records = ((payload.get("trades") or payload.get("fills") or payload.get("data") or [])
                               if isinstance(payload, dict) else [])
                    if isinstance(records, dict):
                        records = [r for value in records.values() for r in (value if isinstance(value, list) else [value]) if isinstance(r, dict)]
                    for record in records if isinstance(records, list) else []:
                        fill = self._normalize_stream_trade(record)
                        if fill is not None and str(fill.id) not in self._fill_stream_seen:
                            self._fill_stream_seen.add(str(fill.id))
                            emit(EventKind.FILL, fill, "account_all_trade_ws")

            adapter = SimpleNamespace(
                account_index=int(self._broker.cfg.account_index),
                _get_auth_token=lambda: (self._broker.ensure_auth() or self._broker.auth),
                _lookup_market=lambda _symbol: {"market_id": self._market_id},
                _normalize_order_record=self._ws_normalize_order_record,
                _normalize_trade_record=self._ws_normalize_trade_record,
            )
            self._vendor_ws = RobinhoodLighterWebSocket(
                symbol="SPY", enable_private=True, auto_reconnect=True,
                on_message_callback=on_message,
                ws_url=self._api_url.replace("https://", "wss://").rstrip("/") + "/stream",
                rest_config={"_adapter": adapter, "private_only": True, "disable_api_fallback": True,
                             "ws_url": self._api_url.replace("https://", "wss://").rstrip("/") + "/stream"},
            )
            self._vendor_ws.connect()
            self.fill_stream_status = "connecting"
            return True
        except Exception as exc:
            self.fill_stream_status = f"unavailable:{type(exc).__name__}"
            LOG.warning("Lighter account fill stream unavailable; using REST fallback: %s", exc)
            return False

    async def start_order_stream(self) -> bool:
        return self._vendor_ws is not None

    def ws_ready(self) -> bool:
        return self.connection_ready and {"account_orders", "account_all_trades"}.issubset(self.private_subscriptions)

    async def next_order_update(self, timeout: float) -> Any:
        return await asyncio.wait_for(self.events.get(), timeout=max(0.0, timeout))

    async def stop_order_stream(self) -> None:
        if self._vendor_ws is not None:
            self._vendor_ws.close()
            self._vendor_ws = None
        self.order_stream_status = "stopped"

    async def next_stream_fill(self, timeout: float) -> Any:
        return await asyncio.wait_for(self.events.get(), timeout=max(0.0, timeout))

    async def stop_fill_stream(self) -> None:
        self.fill_stream_status = "stopped"

    async def _broker_market(self) -> Any:
        if self._market_cache is not None and time.monotonic() - self._market_cache[1] <= 60.0:
            return self._market_cache[0]
        market = await self._rest(self._broker_public_market, priority=3)
        self._market_cache = (market, time.monotonic())
        return market

    async def _broker_public_market(self) -> Any:
        from spy_grid_bot import PublicMarketClient
        return await PublicMarketClient().get_spy_market()

    async def close(self) -> None:
        await self.stop_order_stream()
        await self.stop_fill_stream()
        await self._broker.close()
        await self._rest_limiter.close()


@dataclass
class Experiment:
    cfg: ExperimentConfig
    broker: ExperimentBroker
    points: PointsProvider = field(default_factory=UnavailablePoints)
    stats: ExperimentStats = field(default_factory=ExperimentStats)
    started_at: str = field(default_factory=iso)
    last_fill_cursor: Optional[str] = None
    cycle_id: int = 0
    halted: bool = False
    halt_reason: Optional[str] = None
    points_before: Optional[Decimal] = None
    rank: Optional[int] = None
    points_after: Optional[Decimal] = None
    processed_fill_ids: set[str] = field(default_factory=set)
    current_order_ids: dict[str, int] = field(default_factory=dict)
    # Intent is registered before either maker request is submitted.  Exchange
    # IDs bind later from submit/order/trade events; they are not proof of
    # ownership.
    current_client_order_ids: dict[str, str] = field(default_factory=dict)
    maker_side_by_client_id: dict[str, str] = field(default_factory=dict)
    maker_qty_by_client_id: dict[str, Decimal] = field(default_factory=dict)
    terminal_client_status: dict[str, str] = field(default_factory=dict)
    maker_client_reconciled: dict[str, bool] = field(default_factory=dict)
    quote_active: bool = False
    event_log: Path = field(init=False)
    _last_summary: float = field(default_factory=time.monotonic)
    last_position: Optional[Decimal] = None
    last_open_orders: Optional[int] = None
    last_quote_order_check: float = 0.0
    quote_started_at: float = 0.0
    quote_prices: dict[str, Decimal] = field(default_factory=dict)
    quote_refresh_count: int = 0
    _pending_quote_bbo: Optional[tuple[Decimal, Decimal]] = None
    fill_stream_active: bool = False
    order_stream_active: bool = False
    maker_order_filled_qty: dict[int, Decimal] = field(default_factory=dict)
    maker_lifecycle_reconcile_attempts: dict[str, int] = field(default_factory=dict)
    process_pid: Optional[int] = None
    lock_status: str = "not_acquired"
    cleanup_completed: bool = False
    state: ExperimentState = ExperimentState.IDLE
    critical_section: asyncio.Lock = field(default_factory=asyncio.Lock)
    shutdown_requested: bool = False
    maker_handled_cumulative: dict[int, Decimal] = field(default_factory=dict)
    maker_trade_cumulative: dict[int, Decimal] = field(default_factory=dict)
    terminal_order_status: dict[int, str] = field(default_factory=dict)
    maker_order_reconciled: dict[int, bool] = field(default_factory=dict)
    final_report_written: bool = False
    last_position_sanity_at: float = 0.0
    fill_detection_misses: int = 0
    pending_hedge_attributions: list[PendingHedgeAttribution] = field(default_factory=list)
    deferred_halt_reason: Optional[str] = None

    def __post_init__(self) -> None:
        self.cfg.data_dir.mkdir(parents=True, exist_ok=True)
        self.event_log = self.cfg.data_dir / "events.jsonl"
        self._ensure_history()

    def _event(self, event: str, **fields: Any) -> None:
        with self.event_log.open("a", encoding="utf-8") as f:
            f.write(json.dumps({"timestamp": iso(), "experiment_id": self.cfg.experiment_id, "state": self.state.value, "event": event, **fields}, default=str) + "\n")

    @property
    def history(self) -> Path:
        return self.cfg.data_dir / "history.csv"

    @property
    def summary_path(self) -> Path:
        return self.cfg.data_dir / "latest_summary.json"

    def _ensure_history(self) -> None:
        fields = ["experiment_id", "strategy_version", "git_commit", "order_notional", "quote_refresh_interval", "max_net_position", "target_taker_turnover", "start_time", "end_time", "timestamp", "cycle_id", "maker_side", "maker_price", "maker_qty", "maker_notional", "taker_side", "taker_price", "taker_qty", "taker_notional", "maker_fill_timestamp", "taker_fill_timestamp", "maker_order_id", "taker_order_id", "hedge_latency_ms", "position_before", "position_after", "maker_vwap", "taker_vwap", "matched_qty", "maker_fee", "taker_fee", "gross_price_pnl", "cycle_pnl", "fees", "estimated_spread_slippage"]
        if not self.history.exists():
            with self.history.open("w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=fields).writeheader()
        else:
            with self.history.open(newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                existing = reader.fieldnames or []
                rows = list(reader)
            missing = [field for field in fields if field not in existing]
            if missing:
                merged = existing + missing
                temp = self.history.with_suffix(".migrate.tmp")
                with temp.open("w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=merged)
                    writer.writeheader()
                    writer.writerows(rows)
                temp.replace(self.history)

    def _git_commit(self) -> str:
        try:
            return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL).strip()
        except Exception:
            return "UNKNOWN"

    async def start(self) -> None:
        self.points_before, self.rank = await self.points.snapshot()
        baseline = await self.broker.fills(None)
        self.processed_fill_ids = {str(getattr(fill, "id", "")) for fill in baseline}
        start_stream = getattr(self.broker, "start_fill_stream", None)
        if start_stream is not None:
            try:
                self.fill_stream_active = bool(await start_stream(self.processed_fill_ids))
            except Exception as exc:
                LOG.warning("fill stream start failed; using REST fallback: %s", exc)
                self.fill_stream_active = False
        self._event("experiment_initialized", baseline_fill_count=len(self.processed_fill_ids))
        self._event("fill_stream_status", enabled=self.fill_stream_active,
                    status=getattr(self.broker, "fill_stream_status", "unknown"))
        start_order_stream = getattr(self.broker, "start_order_stream", None)
        if start_order_stream is not None:
            try:
                self.order_stream_active = bool(await start_order_stream())
            except Exception as exc:
                LOG.warning("order update stream start failed; account_all remains fallback: %s", exc)
                self.order_stream_active = False
        self._event("order_update_stream_status", enabled=self.order_stream_active,
                    status=getattr(self.broker, "order_stream_status", "unavailable"),
                    channel="account_orders")

    def _set_state(self, state: ExperimentState, reason: str = "") -> None:
        self.state = state
        self._event("state_transition", to_state=state.value, reason=reason)

    def _quote_pair_finalized(self) -> bool:
        """Both maker identities must be terminal and their known fills reconciled."""
        for side, order_id in self.current_order_ids.items():
            if side not in self.current_client_order_ids:
                client = f"legacy:{side}:{order_id}"
                self.current_client_order_ids[side] = client
                self.maker_side_by_client_id.setdefault(client, side)
                self.maker_client_reconciled.setdefault(client, False)
        clients = set(self.current_client_order_ids.values())
        terminal = NON_EXECUTABLE_ORDER_STATUSES
        return bool(clients) and all(
            self.terminal_client_status.get(client) in terminal
            and self.maker_client_reconciled.get(client, False)
            for client in clients
        )

    def _client_for_order(self, order_id: int, client_order_id: Any = None, side: Any = None) -> str | None:
        """Resolve a current quote identity without consulting active orders.

        A fast first fill may arrive before REST ever exposes the resting
        order.  The pre-registered client intent is therefore authoritative
        for ownership; an exchange id is merely bound to it here.
        """
        client = str(client_order_id or "")
        if client in self.maker_side_by_client_id:
            resolved = client
        else:
            side_key = next((s for s, oid in self.current_order_ids.items() if oid == order_id), None)
            resolved = self.current_client_order_ids.get(side_key) if side_key is not None else None
            if resolved is None and side_key is not None:
                # Backward-compatible state for recovery/tests created before
                # client intent registration existed.
                resolved = f"legacy:{side_key}:{order_id}"
                self.current_client_order_ids[side_key] = resolved
                self.maker_side_by_client_id[resolved] = side_key
                self.maker_client_reconciled.setdefault(resolved, False)
            if resolved is None and side is not None:
                normalized_side = "sell" if str(side).lower() in {"sell", "ask"} else "buy"
                candidates = [c for c, s in self.maker_side_by_client_id.items()
                              if s == normalized_side and c not in self.terminal_client_status]
                resolved = candidates[0] if len(candidates) == 1 else None
        if resolved is not None and order_id >= 0:
            side_key = self.maker_side_by_client_id[resolved]
            self.current_order_ids[side_key] = order_id
            self.maker_order_filled_qty.setdefault(order_id, ZERO)
            self.maker_handled_cumulative.setdefault(order_id, ZERO)
            self.maker_trade_cumulative.setdefault(order_id, ZERO)
            self.maker_order_reconciled.setdefault(order_id, False)
        return resolved

    def _mark_terminal(self, client: str | None, order_id: int, status: str, reconciled: bool = False) -> None:
        if order_id >= 0:
            self.terminal_order_status[order_id] = status
            if reconciled:
                self.maker_order_reconciled[order_id] = True
        if client is not None:
            self.terminal_client_status[client] = status
            if reconciled:
                self.maker_client_reconciled[client] = True

    async def _owned_side(self, client: str, inbound_side: Any, order_id: int, source: str,
                          is_ask: Any = None) -> str:
        """Owned submit intent is authoritative; Lighter order side is diagnostic."""
        owned = self.maker_side_by_client_id[client]
        # account_orders.side is diagnostic only.  Lighter's is_ask is the
        # protocol-side check; local client/order identity remains execution
        # authority when either field conflicts.
        inbound = ("sell" if bool(is_ask) else "buy") if is_ask is not None else (
            "sell" if str(inbound_side).lower() in {"sell", "ask"} else "buy")
        if inbound != owned:
            # Lighter account_orders has returned the opposite side for a
            # confirmed owned BUY (the signed position proved the local intent
            # correct).  Never let this display field choose cancel/hedge side.
            self._event("ORDER_SIDE_IGNORED", order_id=order_id, client_order_id=client,
                        expected_side=owned, inbound_side=inbound, source=source)
        return owned

    async def _return_to_idle(self) -> None:
        # A flat net position does not prove that the maker quote pair is
        # complete: the other side may have filled concurrently and its WS
        # event can arrive after the first hedge/reconciliation path.
        if not self._quote_pair_finalized():
            self._set_state(ExperimentState.FLAT, "awaiting_quote_pair_reconciliation")
            return
        self.quote_active = False
        self.current_order_ids.clear()
        self.current_client_order_ids.clear()
        self.maker_side_by_client_id.clear()
        self.maker_qty_by_client_id.clear()
        self.terminal_client_status.clear()
        self.maker_client_reconciled.clear()
        self.maker_order_filled_qty.clear()
        self.maker_order_reconciled.clear()
        self._set_state(ExperimentState.FLAT, "position_flat")
        self._set_state(ExperimentState.IDLE, "cycle_complete")

    async def handle_event(self, event: NormalizedEvent) -> None:
        """The only coordinator entry point that may mutate execution state."""
        async with self.critical_section:
            if self.state in {ExperimentState.HALTING, ExperimentState.STOPPED}:
                return
            if event.kind is EventKind.SHUTDOWN:
                self.shutdown_requested = True
                await self.halt(str(event.payload or "shutdown requested"))
                return
            if event.kind is EventKind.CONNECTION:
                connected = bool(isinstance(event.payload, dict) and (
                    event.payload.get("state") == "connected" or event.payload.get("type") == "connected"))
                if hasattr(self.broker, "connection_ready"):
                    self.broker.connection_ready = connected
                if not connected:
                    if hasattr(self.broker, "private_subscriptions"):
                        self.broker.private_subscriptions.clear()
                    self._event("ws_disconnected")
                    if self.quote_active:
                        await self.halt("private websocket disconnected with active maker")
                return
            if event.kind is EventKind.SUBSCRIPTION:
                channel = str((event.payload or {}).get("channel", ""))
                base = channel.split("/", 1)[0].split(":", 1)[0]
                if base in {"account_orders", "account_all_trades"} and hasattr(self.broker, "private_subscriptions"):
                    self.broker.private_subscriptions.add(base)
                    self._event("private_subscription_ack", channel=base)
                return
            if event.kind is EventKind.ORDER:
                await self._handle_order_event(event.payload, event.source)
                return
            if event.kind is EventKind.FILL:
                await self._handle_fill_event(event.payload, event.source)
                return
            if event.kind is EventKind.TIMER:
                if self.state is ExperimentState.IDLE or self.quote_active:
                    await self.tick()
                elif self.stats.taker_volume >= self.cfg.target_taker_turnover:
                    await self.halt("target taker turnover reached")

    async def _handle_order_event(self, update: Any, source: str) -> None:
        order_id = int(getattr(update, "order_id", -1) or -1)
        status = str(getattr(update, "status", "")).lower()
        client = self._client_for_order(order_id, getattr(update, "client_order_id", None), getattr(update, "side", None))
        if client is None or self.state not in {ExperimentState.MAKER_RESTING, ExperimentState.FLAT}:
            return
        owned_side = await self._owned_side(client, getattr(update, "side", None), order_id, source,
                                            getattr(update, "is_ask", None))
        if self.halted:
            return
        cumulative = abs(dec(getattr(update, "filled_quantity", "0") or "0"))
        previous = self.maker_handled_cumulative.get(order_id, ZERO)
        if status in {"filled", "cancelled", "canceled", "canceled-post-only", "rejected", "expired"}:
            # FILLED is an order-lifecycle fact, not execution accounting by
            # itself.  It is reconciled only once its cumulative fill was
            # recorded below (or an execution event arrived).
            self._mark_terminal(client, order_id, status, reconciled=status != "filled" or cumulative > ZERO)
        if status not in {"partially_filled", "filled"} or cumulative <= previous:
            if self._quote_pair_finalized():
                await self._return_to_idle()
            return
        delta = cumulative - previous
        self.maker_handled_cumulative[order_id] = cumulative
        fill = SimpleNamespace(maker=True, id=f"order-cumulative:{order_id}:{cumulative}", qty=str(delta),
                               notional=abs(dec(getattr(update, "price", "0") or "0") * delta),
                               price=str(getattr(update, "price", "0") or "0"),
                               timestamp=getattr(update, "timestamp", iso()), order_id=order_id,
                               side=owned_side)
        recorded = self.maker_order_filled_qty.get(order_id, ZERO)
        if cumulative > recorded:
            maker_record = SimpleNamespace(**{**fill.__dict__, "qty": str(cumulative - recorded),
                                               "notional": abs(dec(getattr(update, "price", "0") or "0") * (cumulative - recorded))})
            await self._record_maker_fill(maker_record)
            self.maker_order_filled_qty[order_id] = cumulative
            if status == "filled":
                self._mark_terminal(client, order_id, status, reconciled=True)
        await self._consume_new_fills([fill], ZERO, source, already_deduped=True, record_maker=False)
        if self._quote_pair_finalized():
            await self._return_to_idle()

    async def _handle_fill_event(self, fill: Any, source: str) -> None:
        fill_id = str(getattr(fill, "fill_id", getattr(fill, "id", "")))
        if not fill_id or fill_id in self.processed_fill_ids:
            return
        normalized = SimpleNamespace(maker=bool(getattr(fill, "is_maker", getattr(fill, "maker", False))), id=fill_id,
            qty=str(getattr(fill, "quantity", getattr(fill, "qty", "0"))),
            price=str(getattr(fill, "price", "0")), timestamp=getattr(fill, "timestamp", iso()),
            order_id=int(getattr(fill, "order_id", 0) or 0),
            client_order_id=str(getattr(fill, "client_order_id", "") or ""),
            notional=abs(dec(getattr(fill, "price", "0")) * dec(getattr(fill, "quantity", getattr(fill, "qty", "0")))),
            side="sell" if str(getattr(fill, "side", "BUY")).lower() in {"sell", "ask"} else "buy")
        if not normalized.maker:
            self.processed_fill_ids.add(fill_id)
            # account_all_trades includes every account taker execution.  An
            # id-less hedge may only claim a record with its generated client
            # index, side and submit-time window; never count a coincidental
            # account trade as experiment turnover.
            if await self._attribute_pending_hedge_fill(normalized):
                await self.process_fill(normalized)
                # Attribution may have completed a late fill that violates
                # the hard exchange-latency limit.  Fill accounting must land
                # before halt() writes the one-and-only final summary.
                if self.deferred_halt_reason is not None:
                    reason, self.deferred_halt_reason = self.deferred_halt_reason, None
                    await self.halt(reason)
            else:
                self._event("unattributed_taker_fill_ignored", fill_id=fill_id,
                            order_id=normalized.order_id, client_order_id=normalized.client_order_id,
                            side=normalized.side, source=source)
            return
        order_id = normalized.order_id
        client = self._client_for_order(order_id, normalized.client_order_id, normalized.side)
        if client is None:
            return
        normalized.side = await self._owned_side(client, normalized.side, order_id, source)
        if self.halted:
            return
        self.processed_fill_ids.add(fill_id)
        qty = abs(dec(normalized.qty)); trade_total = self.maker_trade_cumulative.get(order_id, ZERO) + qty
        self.maker_trade_cumulative[order_id] = trade_total
        recorded = self.maker_order_filled_qty.get(order_id, ZERO)
        if trade_total > recorded:
            record_qty = trade_total - recorded
            record = SimpleNamespace(**{**normalized.__dict__, "qty": str(record_qty),
                                        "notional": abs(dec(normalized.price) * record_qty)})
            await self._record_maker_fill(record)
            self.maker_order_filled_qty[order_id] = trade_total
        if self.terminal_order_status.get(order_id) == "filled":
            self.maker_order_reconciled[order_id] = True
            self.maker_client_reconciled[client] = True
        previous = self.maker_handled_cumulative.get(order_id, ZERO)
        if self.state is ExperimentState.MAKER_RESTING and trade_total > previous:
            delta = trade_total - previous
            self.maker_handled_cumulative[order_id] = trade_total
            trigger = SimpleNamespace(**{**normalized.__dict__, "id": f"trade-trigger:{order_id}:{trade_total}", "qty": str(delta), "notional": abs(dec(normalized.price) * delta)})
            await self._consume_new_fills([trigger], ZERO, source, already_deduped=True, record_maker=False)
        if self._quote_pair_finalized():
            await self._return_to_idle()

    def _queue_pending_hedge_attribution(self, maker: Any, position_before: Decimal,
                                         maker_ts: str, hedge_submit_ts: str,
                                         hedge_client_id: Any, hedge_client_order_id: Any,
                                         side: str, expected_qty: Decimal,
                                         position_flat_ts: str) -> None:
        """Keep immutable cycle context until the id-less hedge fill arrives."""
        client_ids = {str(value) for value in (hedge_client_id, hedge_client_order_id) if value is not None}
        if not client_ids:
            # There is no safe identity on which to attribute a later fill.
            self._event("pending_hedge_attribution_unavailable", cycle_id=self.cycle_id,
                        reason="missing_hedge_client_order_id")
            return
        pending = PendingHedgeAttribution(
            cycle_id=self.cycle_id, maker=maker, position_before=position_before,
            maker_timestamp=maker_ts, hedge_submit_timestamp=hedge_submit_ts,
            client_order_ids=client_ids, side=side, expected_qty=abs(expected_qty),
            position_flat_timestamp=position_flat_ts,
        )
        self.pending_hedge_attributions.append(pending)
        self._event("pending_hedge_attribution", cycle_id=pending.cycle_id,
                    client_order_ids=sorted(client_ids), side=side,
                    expected_qty=str(pending.expected_qty), position_flat_ts=position_flat_ts)

    async def _attribute_pending_hedge_fill(self, fill: Any) -> bool:
        """Attach one late taker execution to exactly one id-less hedge.

        Matching remains deliberately strict: generated client index, hedge
        side, post-submit exchange timestamp and remaining signed quantity.
        This closes reporting only; position truth has already completed the
        safety path.
        """
        candidate_client = str(getattr(fill, "client_order_id", "") or "")
        candidate_side = str(getattr(fill, "side", "")).lower()
        candidate_ts = attribution_timestamp_ms(getattr(fill, "timestamp", None))
        qty = abs(dec(getattr(fill, "qty", "0") or "0"))
        if not candidate_client or candidate_ts is None or qty <= ZERO:
            return False
        for pending in list(self.pending_hedge_attributions):
            submit_ms = attribution_timestamp_ms(pending.hedge_submit_timestamp)
            if (candidate_client not in pending.client_order_ids
                    or candidate_side != pending.side
                    or submit_ms is None or candidate_ts < submit_ms):
                continue
            remaining = pending.expected_qty - pending.filled_qty
            if qty > remaining + self.cfg.min_position_tolerance:
                continue
            pending.filled_qty += qty
            pending.filled_notional += abs(dec(getattr(fill, "notional", "0") or dec(getattr(fill, "price", "0")) * qty))
            pending.first_fill = pending.first_fill or fill
            pending.last_fill = fill
            pending.execution_fills.append(fill)
            self._event("late_hedge_fill_attributed", cycle_id=pending.cycle_id,
                        fill_id=getattr(fill, "id", None), client_order_id=candidate_client,
                        hedge_fill_qty=str(qty), accumulated_qty=str(pending.filled_qty),
                        expected_qty=str(pending.expected_qty))
            if pending.filled_qty + self.cfg.min_position_tolerance >= pending.expected_qty:
                first_fill = pending.first_fill
                maker_ms = attribution_timestamp_ms(pending.maker_timestamp)
                taker_ms = attribution_timestamp_ms(getattr(first_fill, "timestamp", None))
                if maker_ms is None or taker_ms is None:
                    self.stats.data_quality_failures += 1
                    self._event("late_hedge_attribution_incomplete", cycle_id=pending.cycle_id,
                                reason="exchange_timestamps_unavailable")
                    self.pending_hedge_attributions.remove(pending)
                    return True
                latency = max(0, taker_ms - maker_ms)
                self.stats.hedge_latencies_ms.append(latency)
                aggregate = SimpleNamespace(**{**getattr(first_fill, "__dict__", {}),
                                                "qty": str(pending.filled_qty),
                                                "notional": str(pending.filled_notional),
                                                "execution_fills": list(pending.execution_fills)})
                self._append_cycle(pending.maker, pending.position_before, ZERO, latency,
                                   pending.maker_timestamp, str(getattr(first_fill, "timestamp", "")),
                                   aggregate, cycle_id=pending.cycle_id)
                self._event("late_hedge_attribution_completed", cycle_id=pending.cycle_id,
                            maker_to_taker_latency_ms=latency,
                            position_flat_ts=pending.position_flat_timestamp,
                            hedge_fill_qty=str(pending.filled_qty))
                self.pending_hedge_attributions.remove(pending)
                # The trade arrived after position already proved flat, but
                # exchange timestamps are the safety clock.  Preserve the
                # completed accounting and pause quoting only for this
                # recovery boundary; latency is not an execution failure.
                if latency > self.cfg.hedge_latency_warning_ms:
                    self.stats.latency_warnings += 1
                    self._event("hedge_latency_warning", cycle_id=pending.cycle_id,
                                latency_ms=latency,
                                warning_ms=self.cfg.hedge_latency_warning_ms,
                                limit_ms=self.cfg.hedge_latency_limit_ms,
                                position_flat_ts=pending.position_flat_timestamp)
                if latency > self.cfg.hedge_latency_limit_ms:
                    self.stats.latency_violations += 1
                    self._event("hedge_latency_violation", cycle_id=pending.cycle_id,
                                latency_ms=latency,
                                limit_ms=self.cfg.hedge_latency_limit_ms,
                                position_flat_ts=pending.position_flat_timestamp)
                    self._event("quote_pause_after_latency_violation", cycle_id=pending.cycle_id,
                                action="pause_until_flat_then_resume")
            return True
        return False

    async def _cancel_all_confirmed(self) -> None:
        active = await self.broker.resting_orders()
        own = set(self.current_order_ids.values())
        own_clients = set(self.current_client_order_ids.values())
        # Never widen a cleanup to all account SPY orders.  The experiment can
        # only cancel an exchange id or client id that it registered itself.
        active = [order for order in active if (
            int(getattr(order, "order_index", -1)) in own or
            str(getattr(order, "client_order_index", getattr(order, "client_id", ""))) in own_clients
        )]
        self.last_open_orders = len(active)
        for order in active:
            try:
                await self.broker.cancel(order)
            except Exception:
                self.stats.cancel_failures += 1
                raise
        for _ in range(3):
            remaining = await self.broker.resting_orders()
            remaining = [order for order in remaining if (
                int(getattr(order, "order_index", -1)) in own or
                str(getattr(order, "client_order_index", getattr(order, "client_id", ""))) in own_clients
            )]
            if not remaining:
                self.last_open_orders = 0
                return
            await asyncio.sleep(0.1)
        self.stats.cancel_failures += 1
        raise RuntimeError("resting orders could not be confirmed cancelled")

    async def _reconcile_maker_resting_lifecycle(self) -> str:
        """Bounded ownership check: MAKER_RESTING may mean only two live makers.

        This is intentionally order-specific (at most the current pair), never
        a fill/open-order watchdog for the account.  A missing or filled order
        is unsafe without an execution record, so it fails closed.
        """
        active_states = {"active", "open", "new", "partially_filled", "partial"}
        terminal_states = NON_EXECUTABLE_ORDER_STATUSES
        observed: dict[str, str] = {}
        for side, client in self.current_client_order_ids.items():
            order_id = self.current_order_ids.get(side)
            if order_id is None:
                self._event("maker_lifecycle_unknown", side=side, client_order_id=client, reason="exchange_order_id_unbound")
                return "unknown"
            status = await self.broker.order_status(order_id)
            state = str(status.get("status", "unknown")).lower()
            observed[side] = state
            self._event("maker_lifecycle_check", side=side, order_id=order_id, client_order_id=client,
                        order_status=state, source=status.get("source"))
            if state not in active_states | terminal_states:
                return "unknown"
            if state in terminal_states:
                if state == "filled":
                    # REST can observe terminal FILLED before the private WS
                    # event reaches the coordinator.  Reconcile this exact
                    # owned identity once before fail-closed; do not treat a
                    # transport-ordering delay as an execution failure.
                    attempt_key = str(order_id)
                    attempt = self.maker_lifecycle_reconcile_attempts.get(attempt_key, 0) + 1
                    self.maker_lifecycle_reconcile_attempts[attempt_key] = attempt
                    self._event("maker_lifecycle_reconcile_attempt", order_id=order_id,
                                client_order_id=client, attempt=attempt, max_attempts=3)
                    fills = await self.broker.fills(None)
                    owned_fills = [fill for fill in fills
                                   if bool(getattr(fill, "maker", False))
                                   and (int(getattr(fill, "order_id", -1) or -1) == order_id
                                        or str(getattr(fill, "client_order_id", "")) == str(client))]
                    if owned_fills:
                        for fill in owned_fills:
                            await self._consume_new_fills(
                                [fill], ZERO,
                                detection_source="lifecycle_reconciliation",
                            )
                        return "filled_reconciled"
                    position = dec(await self.broker.position())
                    if abs(position) > self.cfg.min_position_tolerance:
                        price = dec(status.get("price", self.quote_prices.get(side, ZERO)) or ZERO)
                        missed = SimpleNamespace(
                            maker=True, id=f"lifecycle-position:{order_id}:{position}",
                            order_id=order_id, client_order_id=client, side=side,
                            qty=str(abs(position)), price=str(price),
                            notional=abs(position) * price, timestamp=iso(),
                        )
                        await self._consume_new_fills(
                            [missed], ZERO,
                            detection_source="lifecycle_position_reconciliation",
                            already_deduped=True,
                        )
                        return "filled_reconciled"
                    if attempt < 3:
                        # A terminal REST state can precede the fill stream and
                        # fills REST index by a short interval.  Retry only
                        # this exact owned identity, with bounded backoff; do
                        # not turn this into an account-wide watchdog.
                        await asyncio.sleep(0.25 * attempt)
                        return "filled_pending_reconcile"
                    return "filled_unreconciled"
                self._mark_terminal(client, order_id, state, reconciled=True)
        return "terminal_no_fill" if any(state in terminal_states for state in observed.values()) else "active"

    async def _cancel_specific_request(self, order_id: Optional[int]) -> None:
        """Send P0 cancel now; terminal reconciliation may happen after hedge submit."""
        if order_id is None:
            self._event("cancel_not_required", reason="order_not_active_or_already_filled")
            return
        self._event("cancel_request", order_id=order_id)
        try:
            request = getattr(self.broker, "cancel_order", None)
            if request is not None:
                result = await request(order_id)
            else:  # Compatibility for test/dry adapters.
                result = await self.broker.cancel_order_confirmed(order_id)
            self._event("cancel_submitted", **(result if isinstance(result, dict) else {"order_id": order_id}))
        except Exception:
            self.stats.cancel_failures += 1
            self._event("cancel_failed", order_id=order_id)
            raise

    async def _cancel_specific_confirmed(self, order_id: Optional[int], *, already_requested: bool = False) -> None:
        if order_id is None:
            if self.cfg.dry_run:
                await self._cancel_all_confirmed()
                return
            self._event("cancel_not_required", reason="order_not_active_or_already_filled")
            return
        self._event("cancel_confirmation_start" if already_requested else "cancel_request", order_id=order_id)
        try:
            confirm = getattr(self.broker, "confirm_cancel_order", None) if already_requested else None
            result = await confirm(order_id) if confirm is not None else await self.broker.cancel_order_confirmed(order_id)
            status = str(result.get("status", "")).lower() if isinstance(result, dict) else ""
            if status in NON_EXECUTABLE_ORDER_STATUSES:
                # Do not overwrite an already-observed FILLED order with a
                # stale cancel acknowledgement. A cancel result of FILLED
                # still awaits its account_orders cumulative fill update.
                if self.terminal_order_status.get(order_id) != "filled":
                    self.terminal_order_status[order_id] = status
                if status != "filled":
                    self.maker_order_reconciled[order_id] = True
            self._event("cancel_confirmed", **result)
        except Exception:
            self.stats.cancel_failures += 1
            self._event("cancel_failed", order_id=order_id)
            raise

    async def _flat_confirmed(self) -> Decimal:
        self._event("cleanup_start", purpose="cleanup")
        pos = dec(await self.broker.position())
        self.last_position = pos
        self._event("position_snapshot", purpose="cleanup", position=str(pos))
        if abs(pos) <= self.cfg.min_position_tolerance:
            self._event("position_flat", purpose="cleanup", flat_confirmation_source="position")
            return pos
        side = "sell" if pos > ZERO else "buy"
        result = await self.broker.market_reduce_only(side, abs(pos), f"{self.cfg.experiment_id}-flatten")
        self._event("cleanup_hedge_submitted", purpose="cleanup", side=side, quantity=str(abs(pos)),
                    order_id=getattr(result, "order_id", None), client_order_id=getattr(result, "client_order_id", None))
        for _ in range(5):
            await asyncio.sleep(0.1)
            pos = dec(await self.broker.position())
            self.last_position = pos
            self._event("position_snapshot", purpose="cleanup", position=str(pos))
            if abs(pos) <= self.cfg.min_position_tolerance:
                self._event("position_flat", purpose="cleanup", flat_confirmation_source="position")
                return pos
        raise RuntimeError("position could not be confirmed flat")

    def _maker_residual_requires_cancel(self, maker: Any) -> bool:
        """Return true only when the filled maker can still add exposure."""
        order_id = getattr(maker, "order_id", None)
        if order_id is None:
            return False
        order_id = int(order_id)
        if self.terminal_order_status.get(order_id) in NON_EXECUTABLE_ORDER_STATUSES:
            return False
        side = next((key for key, value in self.current_order_ids.items() if value == order_id), None)
        client = self.current_client_order_ids.get(side) if side is not None else None
        intended = self.maker_qty_by_client_id.get(client) if client is not None else None
        if intended is None:
            # Identity was not fully bound (for example position-sanity
            # recovery).  Preserve the conservative residual cancel.
            return True
        cumulative = max(
            self.maker_handled_cumulative.get(order_id, ZERO),
            self.maker_trade_cumulative.get(order_id, ZERO),
            self.maker_order_filled_qty.get(order_id, ZERO),
        )
        return cumulative + self.cfg.min_position_tolerance < intended

    async def halt(self, reason: str) -> None:
        if self.halted:
            return
        self._set_state(ExperimentState.HALTING, reason)
        self.halted, self.halt_reason = True, reason
        LOG.error("Experiment halted: %s", reason)
        cleanup_errors: list[str] = []
        try:
            try:
                await self._cancel_all_confirmed()
            except Exception as exc:
                cleanup_errors.append(f"cancel cleanup: {exc}")
            try:
                await self._flat_confirmed()
            except Exception as exc:
                cleanup_errors.append(f"flat cleanup: {exc}")
        finally:
            self.cleanup_completed = not cleanup_errors
            if cleanup_errors:
                self.halt_reason = f"{reason}; " + "; ".join(cleanup_errors)
            if not self.final_report_written:
                await self.write_summary(final=True)
                self.write_evaluation()
                self.write_recommendation()
                self.final_report_written = True
            self._set_state(ExperimentState.STOPPED, reason)

    async def _hedge(self, maker: Any, position_before: Decimal, maker_ts: str,
                     allow_over_cap_recovery: bool = False) -> None:
        self._set_state(ExperimentState.CANCELING, "maker_fill")
        maker_order_id = getattr(maker, "order_id", None)
        opposite_order_id = next((oid for oid in self.current_order_ids.values()
                                  if oid != maker_order_id), None)
        # P0 signer cancel must leave immediately.  Its terminal state is
        # reconciled only after the reduce-only hedge has been submitted; no
        # new quote can start while this hedge state is active.
        await self._cancel_specific_request(opposite_order_id)
        # A maker can be partially filled.  Cancel its residual as well so no
        # additional exposure can appear while the hedge is in flight.
        if self._maker_residual_requires_cancel(maker):
            await self._cancel_specific_confirmed(int(maker_order_id))
        else:
            self._event("maker_residual_cancel_skipped", maker_order_id=maker_order_id,
                        reason="maker_order_terminal_or_fully_filled")
        self._event("opposite_cancel_requested", maker_order_id=maker_order_id, opposite_order_id=opposite_order_id)
        active: list[Any] = []
        if opposite_order_id is None:
            active = await self.broker.resting_orders()
            own_clients = set(self.current_client_order_ids.values())
            for order in active:
                order_id = int(getattr(order, "order_index", -1))
                client_id = str(getattr(order, "client_order_index", getattr(order, "client_id", "")))
                if client_id in own_clients:
                    self._client_for_order(order_id, client_id, getattr(order, "side", None))
                if (client_id in own_clients and order_id >= 0
                        and order_id != int(maker_order_id or -1)):
                    await self._cancel_specific_confirmed(order_id)
            active = await self.broker.resting_orders()
        own_ids = set(self.current_order_ids.values())
        if any(int(getattr(order, "order_index", -1)) in own_ids and int(getattr(order, "order_index", -1)) != int(maker_order_id or -1) for order in active):
            raise RuntimeError("own resting opposite order remains before hedge")
        try:
            actual = dec(await self.broker.position(priority=True))
        except TypeError:
            actual = dec(await self.broker.position())
        mark_for_metrics = dec(getattr(maker, "price", "0") or "0")
        self.stats.max_net_position_qty = max(self.stats.max_net_position_qty, abs(actual))
        self.stats.max_net_position_usd = max(self.stats.max_net_position_usd, abs(actual) * mark_for_metrics)
        if actual == ZERO:
            await self._cancel_specific_confirmed(opposite_order_id, already_requested=True)
            self._event("opposite_reconciled", maker_order_id=maker_order_id, opposite_order_id=opposite_order_id)
            self._event("no_hedge_required", reason="opposite_maker_fill_flattened_net_position")
            await self._return_to_idle()
            return
        mid = dec(getattr(maker, "price", "0") or "0")
        position_notional = abs(actual) * mid
        self.last_position = actual
        self._event("position_risk", position_qty=str(actual), position_notional_usd=str(position_notional))
        if position_notional > self.cfg.max_net_position:
            self._event("position_cap_recovery", position_qty=str(actual),
                        position_notional_usd=str(position_notional),
                        configured_max_net_position=str(self.cfg.max_net_position),
                        action="stop_quotes_flatten_before_resume")
        side = "sell" if actual > ZERO else "buy"
        if self.stats.nonflat_started is None:
            self.stats.nonflat_started = time.monotonic()
        position_snapshots: list[dict[str, Any]] = []
        hedge_fill = None
        hedge_fill_qty = ZERO
        hedge_fill_notional = ZERO
        hedge_execution_fills: list[Any] = []
        hedge_attempts = 1
        attempt_fill_seen = False
        self._set_state(ExperimentState.HEDGING, "cancel_requested")
        hedge_submit_ts = iso()
        hedge_client_id = f"{self.cfg.experiment_id}-hedge-{self.cycle_id}"
        self._event("hedge_request", side=side, quantity=str(abs(actual)), position=str(actual),
                    hedge_submit_ts=hedge_submit_ts, position_before_hedge=str(actual), purpose="hedge")
        result = await self.broker.market_reduce_only(side, abs(actual), hedge_client_id)
        hedge_order_id = getattr(result, "order_id", None)
        hedge_client_order_id = getattr(result, "client_order_id", None)
        last_hedge_result = result
        response = getattr(result, "response", result)
        submit_status = str(getattr(response, "status", getattr(response, "code", "submitted")))
        self._event("hedge_submit_response", purpose="hedge", hedge_submit_ts=hedge_submit_ts,
                    hedge_order_id=hedge_order_id, client_order_id=hedge_client_order_id,
                    hedge_submit_status=submit_status, response=repr(response), hedge_attempt=1,
                    best_bid_at_hedge=getattr(result, "best_bid", None), best_ask_at_hedge=getattr(result, "best_ask", None),
                    protection_price=getattr(result, "protection_price", None),
                    slippage_buffer_bps=getattr(result, "slippage_buffer_bps", None))
        # Hedge is now safely submitted.  Finish the exact-id sibling cancel
        # reconciliation before any later FLAT/IDLE transition can quote again.
        await self._cancel_specific_confirmed(opposite_order_id, already_requested=True)
        self._event("opposite_reconciled", maker_order_id=maker_order_id, opposite_order_id=opposite_order_id)
        # Reconciliation is deliberately three-way: order status, fills, and
        # the signed position.  A fill can be visible before position updates,
        # and position can flatten before the fills endpoint catches up.
        self._set_state(ExperimentState.RECONCILING, "hedge_submitted")
        for attempt in range(12):
            await asyncio.sleep(0.25)
            after = dec(await self.broker.position())
            snap = {"timestamp": iso(), "position": str(after), "attempt": attempt}
            position_snapshots.append(snap)
            self._event("position_snapshot", purpose="hedge", hedge_order_id=hedge_order_id, **snap)
            order_state = {}
            if hedge_order_id is not None and hasattr(self.broker, "order_status"):
                order_state = await self.broker.order_status(int(hedge_order_id))
                self._event("hedge_order_status", purpose="hedge", hedge_order_id=hedge_order_id, **order_state)
            candidates = await self.broker.fills(None)
            for candidate in candidates:
                cid = str(getattr(candidate, "id", ""))
                if cid in self.processed_fill_ids or bool(getattr(candidate, "maker", False)):
                    continue
                candidate_order = getattr(candidate, "order_id", None)
                if hedge_order_id is not None and candidate_order not in (None, int(hedge_order_id)):
                    continue
                if hedge_order_id is None:
                    # A successful HTTP submit is not an execution identity.
                    # Without exchange order_id only a bounded match on our
                    # client index, time window, side and requested residual
                    # can be attributed to this hedge.
                    candidate_client = str(getattr(candidate, "client_order_id", "") or "")
                    candidate_side = str(getattr(candidate, "side", "")).lower()
                    candidate_ts = timestamp_ms(getattr(candidate, "timestamp", None))
                    submit_ms = timestamp_ms(hedge_submit_ts)
                    if (candidate_client != str(hedge_client_order_id or hedge_client_id)
                            or candidate_side != side
                            or candidate_ts is None or submit_ms is None or candidate_ts < submit_ms
                            or abs(dec(getattr(candidate, "qty", "0"))) > abs(actual)):
                        continue
                self.processed_fill_ids.add(cid)
                await self.process_fill(candidate)
                hedge_fill = candidate if hedge_fill is None else hedge_fill
                attempt_fill_seen = True
                hedge_fill_qty += abs(dec(getattr(candidate, "qty", "0")))
                hedge_fill_notional += abs(dec(getattr(candidate, "notional", "0") or dec(getattr(candidate, "price", "0")) * dec(getattr(candidate, "qty", "0"))))
                hedge_execution_fills.append(candidate)
                ref = getattr(last_hedge_result, "best_ask", None) if side == "buy" else getattr(last_hedge_result, "best_bid", None)
                fill_price = dec(getattr(candidate, "price", "0"))
                actual_slippage = None if not ref or not fill_price else abs((fill_price / dec(ref) - Decimal("1")) * Decimal("10000"))
                self._event("hedge_fill_observed", purpose="hedge", hedge_order_id=hedge_order_id,
                            hedge_fill_ts=getattr(candidate, "timestamp", None), hedge_fill_qty=str(getattr(candidate, "qty", "0")),
                            hedge_fill_price=str(getattr(candidate, "price", "")), actual_slippage_bps=actual_slippage,
                            maker=False, fill_id=cid)
            terminal = order_state.get("status") in {"filled", "canceled", "cancelled", "rejected", "expired", "canceled-too-much-slippage"}
            # Signer responses often omit an order id.  A non-flat position
            # after one bounded reconciliation window is sufficient evidence
            # to retry by the real residual, never by maker quantity.
            if hedge_order_id is None and (attempt >= 1 or attempt_fill_seen) and abs(after) > self.cfg.min_position_tolerance:
                terminal = True
            if abs(after) > self.cfg.min_position_tolerance and order_state.get("status") == "rejected":
                raise RuntimeError(f"hedge order rejected: {order_state}")
            if abs(after) > self.cfg.min_position_tolerance and hedge_attempts < 3 and (attempt_fill_seen or terminal):
                # IOC partials or slippage cancellations are reconciled against
                # the live signed position, never against the original maker qty.
                remaining = abs(after)
                hedge_attempts += 1
                self._event("hedge_reconcile_remaining", purpose="hedge", hedge_order_id=hedge_order_id,
                            actual_position=str(after), remaining_qty=str(remaining), order_status=order_state.get("status"),
                            hedge_attempt=hedge_attempts)
                side = "sell" if after > ZERO else "buy"
                hedge_client_id = f"{self.cfg.experiment_id}-hedge-{self.cycle_id}-retry-{hedge_attempts}"
                result = await self.broker.market_reduce_only(side, remaining, hedge_client_id)
                hedge_order_id = getattr(result, "order_id", None)
                hedge_client_order_id = getattr(result, "client_order_id", None)
                last_hedge_result = result
                retry_response = getattr(result, "response", result)
                self._event("hedge_submit_response", purpose="hedge", hedge_submit_ts=iso(),
                            hedge_order_id=hedge_order_id, client_order_id=hedge_client_order_id,
                            hedge_submit_status=str(getattr(retry_response, "status", getattr(retry_response, "code", "submitted"))),
                            response=repr(retry_response), hedge_attempt=hedge_attempts,
                            best_bid_at_hedge=getattr(result, "best_bid", None), best_ask_at_hedge=getattr(result, "best_ask", None),
                            protection_price=getattr(result, "protection_price", None),
                            slippage_buffer_bps=getattr(result, "slippage_buffer_bps", None))
                attempt_fill_seen = False
                continue
            if abs(after) <= self.cfg.min_position_tolerance:
                if self.stats.nonflat_started is not None:
                    self.stats.nonflat_seconds += time.monotonic() - self.stats.nonflat_started
                    self.stats.nonflat_started = None
                if hedge_fill is None and not self.cfg.dry_run:
                    # Signed position is the exposure truth.  A signed hedge
                    # request with no exchange order id can flatten before the
                    # fills feed becomes attributable; never resubmit merely
                    # to satisfy reporting visibility.
                    position_flat_ts = iso()
                    self._queue_pending_hedge_attribution(
                        maker, position_before, maker_ts, hedge_submit_ts,
                        hedge_client_id, hedge_client_order_id, side, abs(actual), position_flat_ts,
                    )
                    self._event("flat_by_position_pending_fill_attribution", purpose="hedge",
                                hedge_submit_ts=hedge_submit_ts, client_order_id=hedge_client_order_id,
                                position_before_hedge=str(actual), position_flat_ts=position_flat_ts,
                                flat_confirmation_source="position")
                    await self._return_to_idle()
                    return
                maker_ms = timestamp_ms(maker_ts)
                taker_ms = timestamp_ms(getattr(hedge_fill, "timestamp", None)) if hedge_fill is not None else None
                latency = None if maker_ms is None or taker_ms is None else max(0, taker_ms - maker_ms)
                if latency is None and not self.cfg.dry_run:
                    raise RuntimeError("maker/taker exchange timestamps unavailable")
                latency = latency or 0
                self.stats.hedge_latencies_ms.append(latency)
                self._event("hedge_confirmed", purpose="hedge", hedge_submit_ts=hedge_submit_ts,
                            hedge_order_id=hedge_order_id, client_order_id=hedge_client_order_id,
                            hedge_fill_ts=getattr(hedge_fill, "timestamp", None), hedge_fill_qty=str(hedge_fill_qty),
                            hedge_fill_price=str(getattr(hedge_fill, "price", "")), position_before_hedge=str(actual),
                            position_snapshots=position_snapshots, position_flat_ts=iso(),
                            flat_confirmation_source="order_status+fills+position", latency_ms=latency,
                            order_final_status=order_state.get("status", "unknown"),
                            slippage_buffer_bps=getattr(last_hedge_result, "slippage_buffer_bps", None))
                if latency > self.cfg.hedge_latency_warning_ms:
                    self.stats.latency_warnings += 1
                    self._event("hedge_latency_warning", purpose="hedge", latency_ms=latency,
                                warning_ms=self.cfg.hedge_latency_warning_ms,
                                limit_ms=self.cfg.hedge_latency_limit_ms)
                if latency > self.cfg.hedge_latency_limit_ms:
                    self.stats.latency_violations += 1
                    aggregate_fill = SimpleNamespace(**{**getattr(hedge_fill, "__dict__", {}), "qty": hedge_fill_qty, "notional": hedge_fill_notional, "execution_fills": list(hedge_execution_fills)})
                    self._event("quote_pause_after_latency_violation", cycle_id=self.cycle_id,
                                action="pause_until_flat_then_resume")
                    self._append_cycle(maker, position_before, after, latency, maker_ts, str(getattr(hedge_fill, "timestamp", iso())), aggregate_fill)
                    await self._return_to_idle()
                    return
                aggregate_fill = SimpleNamespace(**{**getattr(hedge_fill, "__dict__", {}), "qty": hedge_fill_qty, "notional": hedge_fill_notional, "execution_fills": list(hedge_execution_fills)})
                self._append_cycle(maker, position_before, after, latency, maker_ts, str(getattr(hedge_fill, "timestamp", iso())), aggregate_fill)
                await self._return_to_idle()
                return
        self.stats.reconciliation_failures += 1
        raise RuntimeError("hedge reconciliation did not prove flat")

    def _append_cycle(self, maker: Any, before: Decimal, after: Decimal, latency: int, maker_ts: str,
                      taker_ts: str, taker_fill: Any = None, cycle_id: Optional[int] = None) -> None:
        maker_side = str(getattr(maker, "side", "unknown"))
        metrics = _execution_metrics(maker, taker_fill)
        maker_price, maker_qty = metrics.maker_vwap, metrics.matched_qty
        taker_side = "sell" if maker_side == "buy" else "buy"
        maker_fee = "unknown" if metrics.maker_fee is None else str(metrics.maker_fee)
        taker_fee = "unknown" if metrics.taker_fee is None else str(metrics.taker_fee)
        total_fees = None if metrics.maker_fee is None or metrics.taker_fee is None else metrics.maker_fee + metrics.taker_fee
        row = {"experiment_id": self.cfg.experiment_id, "strategy_version": self.cfg.strategy_version, "git_commit": self._git_commit(), "order_notional": str(self.cfg.order_notional), "quote_refresh_interval": self.cfg.quote_refresh_interval, "max_net_position": str(self.cfg.max_net_position), "target_taker_turnover": str(self.cfg.target_taker_turnover), "start_time": self.started_at, "end_time": "", "timestamp": iso(), "cycle_id": self.cycle_id if cycle_id is None else cycle_id, "maker_side": maker_side, "maker_price": str(metrics.maker_vwap), "maker_qty": str(metrics.matched_qty), "maker_notional": str(metrics.maker_vwap * metrics.matched_qty), "taker_side": taker_side, "taker_price": str(metrics.taker_vwap), "taker_qty": str(metrics.matched_qty), "taker_notional": str(metrics.taker_vwap * metrics.matched_qty), "maker_fill_timestamp": maker_ts, "taker_fill_timestamp": str(getattr(taker_fill, "timestamp", taker_ts)), "maker_order_id": str(getattr(maker, "order_id", "")), "taker_order_id": str(getattr(taker_fill, "order_id", "")), "hedge_latency_ms": latency, "position_before": str(before), "position_after": str(after), "maker_vwap": str(metrics.maker_vwap), "taker_vwap": str(metrics.taker_vwap), "matched_qty": str(metrics.matched_qty), "maker_fee": maker_fee, "taker_fee": taker_fee, "gross_price_pnl": str(metrics.gross_price_pnl), "cycle_pnl": "unknown" if metrics.net_pnl is None else str(metrics.net_pnl), "fees": "unknown" if total_fees is None else str(total_fees), "estimated_spread_slippage": str(metrics.slippage_cost)}
        with self.history.open("a", newline="", encoding="utf-8") as f:
            fieldnames = next(csv.reader([self.history.read_text(encoding="utf-8").splitlines()[0]]))
            csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore").writerow(row)
        self.stats.cycles += 1
        self.stats.gross_price_pnl += metrics.gross_price_pnl
        self.stats.estimated_spread_slippage += metrics.slippage_cost
        if metrics.net_pnl is None:
            self.stats.net_pnl_known = False
            self.stats.fees_known = False
            self.stats.unknown_fee_cycles += 1
        else:
            self.stats.realized_pnl += metrics.net_pnl
            self.stats.fees += total_fees or ZERO

    async def process_fill(self, fill: Any) -> None:
        maker = bool(getattr(fill, "maker", False))
        volume = abs(dec(getattr(fill, "notional", 0) or dec(getattr(fill, "price", 0)) * dec(getattr(fill, "qty", 0))))
        if maker:
            self.stats.maker_volume += volume
        else:
            self.stats.taker_volume += volume
        self.stats.total_turnover += volume
        self.stats.fills += 1

    async def _consume_new_fills(self, fills: list[Any], before: Decimal, detection_source: Optional[str] = None,
                                 already_deduped: bool = False, record_maker: bool = True) -> bool:
        handled = False
        for fill in fills:
            fill_id = str(getattr(fill, "id", ""))
            if fill_id in self.processed_fill_ids:
                continue
            if bool(getattr(fill, "maker", False)):
                order_id = int(getattr(fill, "order_id", -1) or -1)
                qty = abs(dec(getattr(fill, "qty", "0") or "0"))
                previous_qty = self.maker_order_filled_qty.get(order_id, ZERO)
                if not already_deduped and order_id >= 0 and qty and qty <= previous_qty:
                    self.processed_fill_ids.add(fill_id)
                    continue
                if order_id >= 0 and not already_deduped:
                    self.maker_order_filled_qty[order_id] = previous_qty + qty
            self.processed_fill_ids.add(fill_id)
            self.last_fill_cursor = fill_id
            handled = True
            if bool(getattr(fill, "maker", False)):
                received_ts = iso()
                exchange_ms = timestamp_ms(getattr(fill, "timestamp", None))
                received_ms = timestamp_ms(received_ts)
                detection_latency = None if exchange_ms is None or received_ms is None else max(0, received_ms - exchange_ms)
                self._event("maker_fill_detected", maker_order_id=getattr(fill, "order_id", None),
                            maker_fill_exchange_ts=getattr(fill, "timestamp", None),
                            maker_fill_received_ts=received_ts,
                            maker_fill_detection_latency_ms=detection_latency,
                            detection_source=detection_source or ("websocket" if self.fill_stream_active else "rest_fallback"))
                try:
                    if record_maker:
                        await self._record_maker_fill(fill)
                    await self._hedge(
                        fill, before, str(getattr(fill, "timestamp", iso())),
                        allow_over_cap_recovery=(detection_source == "position_sanity"),
                    )
                except RateLimitError as exc:
                    await self.halt(f"REST rate limit: {exc}")
                except Exception as exc:
                    self.stats.failed_hedges += 1
                    self.stats.execution_failures += 1
                    await self.halt(f"hedge/cancel failure: {exc}")
                break
            await self.process_fill(fill)
        return handled

    async def _record_maker_fill(self, fill: Any) -> None:
        """Record the maker leg before entering the hedge critical section."""
        maker_volume = abs(dec(getattr(fill, "notional", 0) or dec(getattr(fill, "price", 0)) * dec(getattr(fill, "qty", 0))))
        self.stats.maker_volume += maker_volume
        self.stats.total_turnover += maker_volume
        self.stats.fills += 1

    async def tick(self) -> None:
        if self.halted:
            return
        ws_ready = getattr(self.broker, "ws_ready", None)
        if self.fill_stream_active and ws_ready is not None and not ws_ready():
            self._event("quote_blocked", reason="private_ws_disconnected")
            return
        if self.stats.taker_volume >= self.cfg.target_taker_turnover:
            await self.halt("target taker turnover reached")
            return
        # While a pair is resting this is the only periodic REST safety read:
        # one signed-position check roughly every two seconds.  It deliberately
        # does not fan out into fills/open-orders/order-status polling.
        if self.quote_active:
            now = time.monotonic()
            before = self.last_position if self.last_position is not None else ZERO
            if now - self.last_position_sanity_at >= 2.0:
                self.last_position_sanity_at = now
                before = dec(await self.broker.position())
                self.last_position = before
                if abs(before) > self.cfg.min_position_tolerance:
                    self.fill_detection_misses += 1
                    side = "buy" if before > ZERO else "sell"
                    order_id = self.current_order_ids.get(side, -1)
                    price = self.quote_prices.get(side, ZERO)
                    missed = SimpleNamespace(
                        maker=True, id=f"position-sanity:{self.cycle_id}:{side}:{before}",
                        order_id=order_id, side=side, qty=str(abs(before)), price=str(price),
                        notional=abs(before) * price, timestamp=iso(),
                    )
                    self._event("fill_detection_miss", position_qty=str(before), fill_detection_miss=self.fill_detection_misses,
                                detection_source="position_sanity", inferred_maker_side=side, maker_order_id=order_id)
                    # Exposure truth wins: treat the signed delta as a missed
                    # maker fill and enter the existing serialized hedge path.
                    await self._consume_new_fills([missed], ZERO, detection_source="position_sanity", already_deduped=True)
                    return
            # A stale, unfilled flat quote may move only once it is no longer
            # at BBO.  This makes exactly one book request for a refresh
            # decision; fills always take precedence over this maintenance.
            age = now - self.quote_started_at
            no_maker_fill = not any(self.maker_order_filled_qty.values()) and not any(self.maker_handled_cumulative.values()) and not any(self.maker_trade_cumulative.values())
            if before == ZERO and no_maker_fill and age >= 20:
                lifecycle = await self._reconcile_maker_resting_lifecycle()
                if lifecycle == "filled_pending_reconcile":
                    return
                if lifecycle in {"unknown", "filled_unreconciled"}:
                    self.stats.reconciliation_failures += 1
                    await self.halt(f"maker lifecycle {lifecycle}")
                    return
                if lifecycle == "filled_reconciled":
                    return
                bid, ask = await self.broker.best_bid_ask()
                buy_distance = abs(self.quote_prices.get("buy", bid) - bid)
                sell_distance = abs(self.quote_prices.get("sell", ask) - ask)
                tick = Decimal("0.01")
                self._event("quote_refresh_check", quote_age_seconds=round(age, 3),
                            buy_distance_from_bbo=str(buy_distance), sell_distance_from_bbo=str(sell_distance),
                            refresh_count=self.quote_refresh_count)
                if lifecycle == "terminal_no_fill" or buy_distance >= tick or sell_distance >= tick:
                    self._set_state(ExperimentState.QUOTING, "stale_quote_refresh")
                    await self._cancel_all_confirmed()
                    for client in self.current_client_order_ids.values():
                        self.terminal_client_status[client] = "cancelled"
                        self.maker_client_reconciled[client] = True
                    self.quote_active = False
                    self.current_order_ids.clear()
                    self.quote_prices.clear()
                    self._pending_quote_bbo = (bid, ask)
                    self.quote_refresh_count += 1
                    self._set_state(ExperimentState.IDLE, "stale_quote_cancelled")
            return
        before = dec(await self.broker.position())
        self.last_position = before
        self.stats.position_abs_sum += abs(before)
        self.stats.position_samples += 1
        self.stats.max_net_position = max(self.stats.max_net_position, abs(before))
        self.stats.max_net_position_qty = max(self.stats.max_net_position_qty, abs(before))
        if before != ZERO:
            bid, ask = await self.broker.best_bid_ask()
            notional = abs(before) * ((bid + ask) / 2)
            self.stats.max_net_position_usd = max(self.stats.max_net_position_usd, notional)
            if notional > self.cfg.max_net_position:
                await self.halt("max net position exceeded")
                return
        bid, ask = self._pending_quote_bbo or await self.broker.best_bid_ask()
        self._pending_quote_bbo = None
        if bid <= 0 or ask <= bid:
            await self.halt("invalid or crossed best bid/ask")
            return
        mid = (bid + ask) / 2
        self.stats.position_abs_sum_usd += abs(before) * mid
        self.stats.max_net_position_usd = max(self.stats.max_net_position_usd, abs(before) * mid)
        # Post both orders atomically from the strategy's perspective.  Any
        # fill transitions immediately into cancel-confirm-hedge; no new quote
        # is permitted while this is in progress.
        qty = self.cfg.order_notional / ((bid + ask) / 2)
        self._set_state(ExperimentState.QUOTING, "submit_maker_pair")
        self.cycle_id += 1
        cycle = self.cycle_id
        buy_label, sell_label = f"{self.cfg.experiment_id}-b-{cycle}", f"{self.cfg.experiment_id}-s-{cycle}"
        buy_client = str(abs(hash(buy_label)) % (2**48 - 1))
        sell_client = str(abs(hash(sell_label)) % (2**48 - 1))
        # Register intent before the first network call.  A fill that races a
        # submit/REST-active-order lookup is still unambiguously ours.
        self.current_client_order_ids = {"buy": buy_client, "sell": sell_client}
        self.maker_side_by_client_id = {buy_client: "buy", sell_client: "sell"}
        self.maker_qty_by_client_id = {buy_client: qty, sell_client: qty}
        self.maker_client_reconciled = {buy_client: False, sell_client: False}
        self.current_order_ids.clear()
        await self.broker.post_only("buy", bid, qty, buy_label)
        await self.broker.post_only("sell", ask, qty, sell_label)
        self.quote_active = True
        self.last_quote_order_check = time.monotonic()
        self.quote_started_at = self.last_quote_order_check
        self.quote_prices = {"buy": bid, "sell": ask}
        active_after_quote = await self.broker.resting_orders()
        for order in active_after_quote:
            client = str(getattr(order, "client_order_index", getattr(order, "client_id", "")))
            self._client_for_order(int(getattr(order, "order_index", -1)), client, getattr(order, "side", None))
        self.maker_order_filled_qty = {order_id: ZERO for order_id in self.current_order_ids.values()}
        self.maker_lifecycle_reconcile_attempts.clear()
        self.maker_handled_cumulative = {order_id: ZERO for order_id in self.current_order_ids.values()}
        self.maker_trade_cumulative = {order_id: ZERO for order_id in self.current_order_ids.values()}
        self.maker_order_reconciled = {order_id: False for order_id in self.current_order_ids.values()}
        self._set_state(ExperimentState.MAKER_RESTING, "maker_pair_active")
        if not self.fill_stream_active:
            await asyncio.sleep(self.cfg.quote_refresh_interval)
            await self._consume_new_fills(await self.broker.fills(self.last_fill_cursor), before)
        if time.monotonic() - self._last_summary >= self.cfg.summary_seconds:
            await self.write_summary()

    def summary(self, final: bool = False) -> dict[str, Any]:
        points_after, rank = self.points_after, self.rank
        delta = None if self.points_before is None or points_after is None else points_after - self.points_before
        taker = self.stats.taker_volume
        realized_pnl = self.stats.realized_pnl if self.stats.net_pnl_known else None
        fees = self.stats.fees if self.stats.fees_known else None
        runtime = max(0.001, (time.time() - datetime.fromisoformat(self.started_at).timestamp()))
        return {"experiment_id": self.cfg.experiment_id, "timestamp": iso(), "runtime_minutes": round(runtime / 60, 2), "state": self.state.value, "points_total": points_after, "points_delta": delta, "rank": rank, "maker_volume": self.stats.maker_volume, "taker_volume": taker, "total_turnover": self.stats.total_turnover, "cycles": self.stats.cycles, "fills": self.stats.fills, "realized_pnl": realized_pnl, "fees": fees, "gross_price_pnl": self.stats.gross_price_pnl, "pnl_status": "OK" if self.stats.net_pnl_known else "UNKNOWN_FEE", "fees_status": "OK" if self.stats.fees_known else "UNKNOWN", "unknown_fee_cycles": self.stats.unknown_fee_cycles, "estimated_spread_slippage": self.stats.estimated_spread_slippage, "avg_hedge_latency_ms": round(sum(self.stats.hedge_latencies_ms) / len(self.stats.hedge_latencies_ms), 2) if self.stats.hedge_latencies_ms else 0, "max_hedge_latency_ms": max(self.stats.hedge_latencies_ms, default=0), "failed_hedges": self.stats.failed_hedges, "execution_failure": self.stats.execution_failures, "latency_warning": self.stats.latency_warnings, "latency_violation": self.stats.latency_violations, "reconciliation_failure": self.stats.reconciliation_failures, "data_quality_failure": self.stats.data_quality_failures, "cancel_failure": self.stats.cancel_failures, "cancel_failures": self.stats.cancel_failures, "avg_net_position": self.stats.position_abs_sum / max(1, self.stats.position_samples), "avg_net_position_qty": self.stats.position_abs_sum / max(1, self.stats.position_samples), "avg_net_position_usd": self.stats.position_abs_sum_usd / max(1, self.stats.position_samples), "max_net_position": self.stats.max_net_position_usd, "max_net_position_qty": self.stats.max_net_position_qty, "max_net_position_usd": self.stats.max_net_position_usd, "configured_max_net_position": self.cfg.max_net_position, "position_qty": self.last_position, "position_notional_usd": self.stats.max_net_position_usd, "position": self.last_position, "open_orders": self.last_open_orders, "stop_reason": self.halt_reason, "shutdown_reason": self.halt_reason, "process_pid": self.process_pid, "lock_status": self.lock_status, "cleanup_completed": self.cleanup_completed, "hedge_slippage_buffer_bps": self.cfg.hedge_slippage_buffer_bps, "time_nonflat_pct": min(100, self.stats.nonflat_seconds / runtime * 100), "points_per_1m_taker": None if delta is None or taker == 0 else delta / taker * 1000000, "cost_per_point": None if realized_pnl is None or delta in (None, ZERO) else max(ZERO, -realized_pnl) / delta, "pnl_per_1m_taker": None if realized_pnl is None or taker == 0 else realized_pnl / taker * 1000000, "order_notional": self.cfg.order_notional, "quote_refresh_interval": self.cfg.quote_refresh_interval, "target_taker_turnover": self.cfg.target_taker_turnover, "max_runtime_minutes": self.cfg.max_runtime_minutes, "status": "FINAL" if final else "RUNNING", "points_status": "POINTS_DATA_UNAVAILABLE" if points_after is None else "OK"}

    async def write_summary(self, final: bool = False) -> None:
        points_after, rank = await self.points.snapshot()
        # Keep the fetched point values in the object for the report lifecycle.
        self.points_after, self.rank = points_after, rank
        data = self.summary(final)
        data["points_total"] = points_after
        data["points_delta"] = None if self.points_before is None or points_after is None else points_after - self.points_before
        for k, v in list(data.items()):
            if isinstance(v, Decimal):
                data[k] = float(v)
        self.summary_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        self._last_summary = time.monotonic()

    def write_evaluation(self) -> None:
        s = self.summary(True)
        rate = self.stats.failed_hedges / max(1, self.stats.cycles)
        ppm = s.get("points_per_1m_taker")
        if ppm is not None and self.stats.net_pnl_known and ppm >= 2 and rate < .01 and self.stats.max_net_position <= self.cfg.max_net_position and self.stats.realized_pnl / max(self.stats.taker_volume, Decimal("1")) > Decimal("-0.0001"):
            result = "GOOD"
        elif ppm is not None and ppm >= .5 and rate < .03:
            result = "NEUTRAL"
        else:
            result = "BAD" if ppm is None or ppm < .5 or rate >= .03 else "NEUTRAL"
        (self.cfg.data_dir / "evaluation.json").write_text(json.dumps({"experiment_id": self.cfg.experiment_id, "evaluation": result, "hedge_failure_rate": rate, "summary": s}, indent=2, default=str), encoding="utf-8")

    def write_recommendation(self) -> None:
        evaluation = json.loads((self.cfg.data_dir / "evaluation.json").read_text())
        if evaluation["evaluation"] == "GOOD":
            next_notional = min(self.cfg.order_notional * Decimal("1.25"), self.cfg.order_notional + self.cfg.order_notional * Decimal(".25"))
        elif evaluation["evaluation"] == "NEUTRAL":
            next_notional = self.cfg.order_notional
        else:
            next_notional = self.cfg.order_notional * Decimal(".5")
        (self.cfg.data_dir / "next_experiment_recommendation.json").write_text(json.dumps({"from_experiment_id": self.cfg.experiment_id, "evaluation": evaluation["evaluation"], "recommended": {"order_notional": float(next_notional), "quote_refresh_interval": self.cfg.quote_refresh_interval, "target_taker_turnover": float(self.cfg.target_taker_turnover)}, "only_approved_fields": ["order_notional", "quote_refresh_interval", "target_taker_turnover"]}, indent=2), encoding="utf-8")


async def run(cfg: ExperimentConfig, broker: ExperimentBroker, experiment: Optional[Experiment] = None) -> Experiment:
    experiment = experiment or Experiment(cfg, broker)
    await experiment.start()
    deadline = time.monotonic() + cfg.max_runtime_minutes * 60
    while not experiment.halted:
        try:
            if time.monotonic() >= deadline:
                await experiment.handle_event(NormalizedEvent(EventKind.SHUTDOWN, "maximum runtime reached"))
                break
            queue = getattr(broker, "events", None)
            if queue is None:
                await experiment.handle_event(NormalizedEvent(EventKind.TIMER))
                await asyncio.sleep(cfg.poll_seconds)
                continue
            try:
                event = await asyncio.wait_for(queue.get(), timeout=cfg.poll_seconds)
            except asyncio.TimeoutError:
                event = NormalizedEvent(EventKind.TIMER)
            await experiment.handle_event(event)
        except Exception as exc:
            experiment.stats.data_quality_failures += 1
            await experiment.handle_event(NormalizedEvent(EventKind.SHUTDOWN, f"uncertain state: {exc}"))
    return experiment


async def run_live(cfg: ExperimentConfig, once: bool) -> None:
    # Reuse the existing credential validation and signer construction.  No
    # credential value is logged or included in experiment files.
    from spy_grid_bot import Config as ClassicConfig
    classic_cfg = ClassicConfig.from_env(ignore_grid_limits=True)
    broker = LighterExperimentBroker(classic_cfg)
    from grid_guard import pause_classic_grid
    lock = ExperimentInstanceLock(
        classic_cfg.account_index,
        market_id=26,
        lock_dir=os.getenv("EXPERIMENT_LOCK_DIR", "/var/run"),
    )
    experiment: Optional[Experiment] = None
    loop = asyncio.get_running_loop()

    def on_signal(signum: int) -> None:
        # Signals only enqueue intent.  The coordinator owns all cancel/hedge
        # state transitions and therefore cannot race a live hedge task.
        if hasattr(broker, "events"):
            broker.events.put_nowait(NormalizedEvent(EventKind.SHUTDOWN, f"signal {signal.Signals(signum).name}"))

    installed_signals: list[int] = []
    try:
        _arm_parent_death_signal()
        orphans = find_live_experiment_processes()
        if orphans:
            raise RuntimeError(f"live experiment process already exists: {orphans}")
        lock.acquire(cfg.experiment_id)
        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(signum, on_signal, signum)
            installed_signals.append(signum)
        from grid_guard import discover_classic_grid
        running = discover_classic_grid()
        if running:
            paused = pause_classic_grid(cfg.data_dir)
            LOG.warning("Paused classic grid processes: %s", ", ".join(f"{x.source}:{x.pid}" for x in paused))
        if discover_classic_grid():
            raise RuntimeError("classic grid process remains running; refusing experiment start")
        # Create the coordinator before any account-mutating preflight step so
        # every exception takes the same fail-closed cleanup path.
        experiment = Experiment(cfg, broker)
        experiment.process_pid = os.getpid()
        experiment.lock_status = "held"
        before_trades = await broker.raw_trades()
        before_ids = {str(getattr(t, "trade_id_str", getattr(t, "trade_id", ""))) for t in before_trades}
        state_before = await broker.account_state()
        bid, ask = await broker.best_bid_ask()
        orders_before = await broker.resting_orders()
        snapshot = {"timestamp": iso(), "position": state_before, "best_bid": str(bid), "best_ask": str(ask), "open_orders": [{"order_index": getattr(o, "order_index", None), "client_order_index": getattr(o, "client_order_index", None), "side": "sell" if bool(getattr(o, "is_ask", False)) else "buy", "price": str(getattr(o, "price", "")), "remaining_base_amount": str(getattr(o, "remaining_base_amount", "")), "reduce_only": bool(getattr(o, "reduce_only", False))} for o in orders_before]}
        (cfg.data_dir / "pre_smoke_cleanup_snapshot.json").write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
        for order in orders_before:
            await broker.cancel(order)
        for _ in range(5):
            if not await broker.resting_orders():
                break
            await asyncio.sleep(0.2)
        if await broker.resting_orders():
            raise RuntimeError("cleanup could not confirm SPY open_orders=0")
        position = dec(state_before["position"])
        if position != ZERO:
            side = "sell" if position > ZERO else "buy"
            await broker.market_reduce_only(side, abs(position), "SPY_MT_CLEANUP_FLATTEN")
            for _ in range(10):
                await asyncio.sleep(0.2)
                if dec((await broker.account_state())["position"]) == ZERO:
                    break
        state_after = await broker.account_state()
        if dec(state_after["position"]) != ZERO or await broker.resting_orders():
            raise RuntimeError("cleanup did not confirm flat position and zero open orders")
        cleanup = broker.cleanup_metrics(before_ids, await broker.raw_trades())
        cleanup.update({"experiment_id": "PRE_EXPERIMENT_CLEANUP", "timestamp": iso(), "final_position": state_after["position"], "final_open_orders": 0})
        (cfg.data_dir / "pre_smoke_cleanup_summary.json").write_text(json.dumps(cleanup, indent=2, default=str), encoding="utf-8")
        if once:
            await experiment.start()
            await experiment.tick()
            await experiment.halt("one-cycle live run completed")
        else:
            await run(cfg, broker, experiment)
    except BaseException as exc:
        if experiment is not None and not experiment.halted:
            async with experiment.critical_section:
                await experiment.halt(f"exception {type(exc).__name__}")
        raise
    finally:
        for signum in installed_signals:
            try:
                loop.remove_signal_handler(signum)
            except Exception:
                pass
        if experiment is not None and not experiment.halted:
            async with experiment.critical_section:
                await experiment.halt("process shutdown")
        await broker.close()
        lock.release()
        if experiment is not None and not experiment.final_report_written:
            experiment.lock_status = "released"
            await experiment.write_summary(final=True)
            experiment.write_evaluation()
            experiment.write_recommendation()
            experiment.final_report_written = True


async def run_dry_preflight(cfg: ExperimentConfig) -> None:
    """Read-only exchange preflight; never calls post_only or hedge."""
    from spy_grid_bot import Config as ClassicConfig
    classic_cfg = ClassicConfig.from_env(ignore_grid_limits=True)
    broker = LighterExperimentBroker(classic_cfg)
    try:
        bid, ask = await broker.best_bid_ask()
        position = await broker.position()
        orders = await broker.resting_orders()
        print(json.dumps({
            "dry_run": True,
            "market_id": 26,
            "best_bid": str(bid),
            "best_ask": str(ask),
            "position": str(position),
            "open_orders": len(orders),
            "orders_submitted": False,
        }, indent=2))
    finally:
        await broker.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="validate configuration only; never starts live trading")
    args = parser.parse_args()
    cfg = ExperimentConfig.from_env()
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if not cfg.dry_run:
        asyncio.run(run_live(cfg, args.once))
        return
    if args.once:
        asyncio.run(run_dry_preflight(cfg))
        return
    print(json.dumps({"mode": "SPY Maker -> Taker Hedge Test", "experiment_id": cfg.experiment_id, "dry_run": cfg.dry_run, "order_notional": float(cfg.order_notional), "target_taker_turnover": float(cfg.target_taker_turnover)}, indent=2))


if __name__ == "__main__":
    main()
