#!/usr/bin/env python3
"""Risk-capped SPY perpetual grid bot for Lighter on Robinhood Chain.

The bot is dry-run by default. Live trading requires all of:
  DRY_RUN=false
  LIVE_CONFIRM=I_UNDERSTAND
  ACK_DEDICATED_SUBACCOUNT=true

Use a dedicated Lighter sub-account: the bot treats every active SPY order on
that account as its own and may cancel it during reconciliation.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import time
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN, ROUND_FLOOR
from pathlib import Path
from typing import Any, Iterable, Optional


LOG = logging.getLogger("spy-grid")
ZERO = Decimal("0")
ONE = Decimal("1")
ROBINHOOD_API_URL = "https://api.rh.lighter.xyz"
ROBINHOOD_CHAIN_ID = 466324
SPY_MARKET_ID = 26
MAX_CLIENT_ORDER_INDEX = 2**48 - 1


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_decimal(name: str, default: str) -> Decimal:
    try:
        return Decimal(os.getenv(name, default))
    except Exception as exc:
        raise ValueError(f"{name} must be numeric") from exc


def utc_day_start_ms() -> int:
    now = datetime.now(timezone.utc)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(start.timestamp() * 1000)


def round_price(value: Decimal, decimals: int, side: str) -> Decimal:
    quantum = Decimal(1).scaleb(-decimals)
    rounding = ROUND_FLOOR if side == "buy" else ROUND_CEILING
    return value.quantize(quantum, rounding=rounding)


def round_size(value: Decimal, decimals: int) -> Decimal:
    quantum = Decimal(1).scaleb(-decimals)
    return value.quantize(quantum, rounding=ROUND_DOWN)


def scaled_int(value: Decimal, decimals: int) -> int:
    return int((value * (Decimal(10) ** decimals)).to_integral_exact())


@dataclass(frozen=True)
class Market:
    market_id: int
    symbol: str
    mark_price: Decimal
    size_decimals: int
    price_decimals: int
    min_base_amount: Decimal
    min_quote_amount: Decimal


@dataclass(frozen=True)
class Position:
    size: Decimal  # positive = long, negative = short
    average_entry: Decimal

    @property
    def is_flat(self) -> bool:
        return self.size == ZERO


@dataclass(frozen=True)
class PlannedOrder:
    side: str
    price: Decimal
    base_size: Decimal
    reduce_only: bool
    label: str
    client_order_index: Optional[int] = None

    @property
    def notional(self) -> Decimal:
        return self.price * self.base_size


@dataclass
class Lot:
    lot_id: str
    slot_label: str
    side: str
    entry_price: str
    base_size: str


@dataclass
class BotState:
    anchor_price: Optional[str] = None
    updated_at: Optional[str] = None
    lots: list[Lot] = field(default_factory=list)
    order_client_indexes: dict[str, int] = field(default_factory=dict)
    processed_trade_ids: list[str] = field(default_factory=list)
    trade_cursor_initialized: bool = False
    next_client_order_index: Optional[int] = None


@dataclass(frozen=True)
class Config:
    dry_run: bool
    account_index: Optional[int]
    api_key_index: Optional[int]
    api_private_key: Optional[str]
    grid_spacing: Decimal
    levels_each_side: int
    notional_per_level: Decimal
    max_net_position: Decimal
    max_account_gross_notional: Decimal
    min_collateral: Decimal
    max_daily_turnover: Decimal
    recenter_threshold: Decimal
    out_of_range_extra: Decimal
    emergency_exit_offset: Decimal
    poll_seconds: int
    state_file: Path

    @classmethod
    def from_env(cls, ignore_grid_limits: bool = False) -> "Config":
        # The experiment runner may share the process environment variable
        # MAX_NET_POSITION with its own safety cap.  This opt-in path keeps
        # classic grid defaults isolated; the default behavior is unchanged.
        grid_env = {} if ignore_grid_limits else os.environ
        cfg = cls(
            dry_run=env_bool("DRY_RUN", True),
            account_index=int(os.environ["LIGHTER_ACCOUNT_INDEX"])
            if os.getenv("LIGHTER_ACCOUNT_INDEX")
            else None,
            api_key_index=int(os.environ["LIGHTER_API_KEY_INDEX"])
            if os.getenv("LIGHTER_API_KEY_INDEX")
            else None,
            api_private_key=os.getenv("LIGHTER_API_PRIVATE_KEY"),
            grid_spacing=Decimal(grid_env.get("GRID_SPACING", "0.0020")),
            levels_each_side=int(grid_env.get("LEVELS_EACH_SIDE", "5")),
            notional_per_level=Decimal(grid_env.get("NOTIONAL_PER_LEVEL", "350")),
            max_net_position=Decimal(grid_env.get("MAX_NET_POSITION", "1750")),
            max_account_gross_notional=Decimal(grid_env.get("MAX_ACCOUNT_GROSS_NOTIONAL", "2000")),
            min_collateral=env_decimal("MIN_COLLATERAL", "1000"),
            max_daily_turnover=env_decimal("MAX_DAILY_TURNOVER", "10000"),
            recenter_threshold=env_decimal("RECENTER_THRESHOLD", "0.0040"),
            out_of_range_extra=env_decimal("OUT_OF_RANGE_EXTRA", "0.0030"),
            emergency_exit_offset=env_decimal(
                "EMERGENCY_EXIT_OFFSET", "0.0005"
            ),
            poll_seconds=int(os.getenv("POLL_SECONDS", "10")),
            state_file=Path(
                os.getenv("STATE_FILE", "state/spy_grid_state.json")
            ),
        )
        cfg.validate()
        return cfg

    @property
    def has_credentials(self) -> bool:
        return (
            self.account_index is not None
            and self.api_key_index is not None
            and bool(self.api_private_key)
        )

    @property
    def half_width(self) -> Decimal:
        return self.grid_spacing * self.levels_each_side

    def validate(self) -> None:
        if self.levels_each_side < 1 or self.levels_each_side > 20:
            raise ValueError("LEVELS_EACH_SIDE must be between 1 and 20")
        if not (ZERO < self.grid_spacing < Decimal("0.05")):
            raise ValueError("GRID_SPACING must be between 0 and 0.05")
        if self.notional_per_level <= ZERO:
            raise ValueError("NOTIONAL_PER_LEVEL must be positive")
        if self.max_net_position < self.notional_per_level:
            raise ValueError("MAX_NET_POSITION must cover at least one grid level")
        if self.max_account_gross_notional < self.max_net_position:
            raise ValueError(
                "MAX_ACCOUNT_GROSS_NOTIONAL cannot be below MAX_NET_POSITION"
            )
        if self.poll_seconds < 5:
            raise ValueError("POLL_SECONDS must be at least 5")
        supplied = [
            self.account_index is not None,
            self.api_key_index is not None,
            bool(self.api_private_key),
        ]
        if any(supplied) and not all(supplied):
            raise ValueError(
                "Set LIGHTER_ACCOUNT_INDEX, LIGHTER_API_KEY_INDEX and "
                "LIGHTER_API_PRIVATE_KEY together"
            )
        if self.api_key_index is not None and self.api_key_index < 4:
            raise ValueError(
                "API key indexes 0-3 are reserved for Lighter desktop/mobile; "
                "use an API key index >= 4"
            )
        if not self.dry_run:
            if not self.has_credentials:
                raise ValueError("Live mode requires Lighter API credentials")
            if os.getenv("LIVE_CONFIRM") != "I_UNDERSTAND":
                raise ValueError("Set LIVE_CONFIRM=I_UNDERSTAND for live mode")
            if not env_bool("ACK_DEDICATED_SUBACCOUNT", False):
                raise ValueError(
                    "Live mode requires ACK_DEDICATED_SUBACCOUNT=true"
                )


def build_plan(
    cfg: Config,
    market: Market,
    anchor: Decimal,
    position: Position,
    allow_opening: bool,
    emergency_reduce: bool,
    lots: Iterable[Lot] = (),
) -> list[PlannedOrder]:
    """Build an inventory-aware, risk-capped grid.

    Flat inventory quotes both sides. Once inventory exists, the bot only adds
    in that inventory direction and places one reduce-only mean-reversion exit.
    This prevents an exit from accidentally flipping the position.
    """
    orders: list[PlannedOrder] = []
    mark = market.mark_price
    lots = [lot for lot in lots if Decimal(lot.base_size) > ZERO]
    # Capacity is based on filled lot cost, not a mark that can oscillate by a
    # few cents and repeatedly add/cancel the last grid level.
    current_notional = (
        sum(Decimal(lot.base_size) * Decimal(lot.entry_price) for lot in lots)
        if lots
        else abs(position.size) * mark
    )
    remaining_capacity = max(cfg.max_net_position - current_notional, ZERO)
    opening_budget = min(
        cfg.levels_each_side,
        int(remaining_capacity // cfg.notional_per_level),
    )
    inactive_slots = {lot.slot_label for lot in lots}

    def add_opening(side: str, level: int) -> None:
        direction = -ONE if side == "buy" else ONE
        raw_price = anchor * (ONE + direction * cfg.grid_spacing * level)
        price = round_price(raw_price, market.price_decimals, side)
        size = round_size(
            cfg.notional_per_level / price, market.size_decimals
        )
        if size < market.min_base_amount or size * price < market.min_quote_amount:
            raise ValueError(
                f"Grid order {side} level {level} is below market minimum"
            )
        orders.append(
            PlannedOrder(side, price, size, False, f"open-{side}-L{level}")
        )

    if allow_opening:
        if position.is_flat:
            for side in ("buy", "sell"):
                added = 0
                for level in range(1, cfg.levels_each_side + 1):
                    if added >= opening_budget:
                        break
                    if f"open-{side}-L{level}" in inactive_slots:
                        continue
                    add_opening(side, level)
                    added += 1
        elif position.size > ZERO:
            added = 0
            for level in range(1, cfg.levels_each_side + 1):
                if added >= opening_budget:
                    break
                if f"open-buy-L{level}" in inactive_slots:
                    continue
                add_opening("buy", level)
                added += 1
        else:
            added = 0
            for level in range(1, cfg.levels_each_side + 1):
                if added >= opening_budget:
                    break
                if f"open-sell-L{level}" in inactive_slots:
                    continue
                add_opening("sell", level)
                added += 1

    for lot in lots:
        size = round_size(Decimal(lot.base_size), market.size_decimals)
        entry = Decimal(lot.entry_price)
        if lot.side == "long":
            raw_exit = mark * (ONE + cfg.emergency_exit_offset) if emergency_reduce else entry * (ONE + cfg.grid_spacing)
            side = "sell"
        else:
            raw_exit = mark * (ONE - cfg.emergency_exit_offset) if emergency_reduce else entry * (ONE - cfg.grid_spacing)
            side = "buy"
        orders.append(
            PlannedOrder(
                side,
                round_price(raw_exit, market.price_decimals, side),
                size,
                True,
                f"exit-{lot.lot_id}",
            )
        )

    return [order for order in orders if order.base_size > ZERO]


class PublicMarketClient:
    def __init__(self, api_url: str = ROBINHOOD_API_URL) -> None:
        self.api_url = api_url.rstrip("/")

    @staticmethod
    def _get_json(url: str) -> dict[str, Any]:
        req = urllib.request.Request(url, headers={"User-Agent": "spy-grid-bot/1.0"})
        with urllib.request.urlopen(req, timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))

    async def get_spy_market(self) -> Market:
        query = urllib.parse.urlencode({"market_id": SPY_MARKET_ID})
        data = await asyncio.to_thread(
            self._get_json,
            f"{self.api_url}/api/v1/orderBookDetails?{query}",
        )
        details = data.get("order_book_details") or []
        if not details:
            raise RuntimeError("SPY perpetual market details are unavailable")
        detail = details[0]
        if detail.get("symbol") != "SPY" or detail.get("market_type") != "perp":
            raise RuntimeError(f"Unexpected market response: {detail.get('symbol')}")
        if detail.get("status") != "active":
            raise RuntimeError("SPY perpetual market is not active")
        mark_raw = detail.get("mark_price") or detail.get("last_trade_price")
        mark = Decimal(str(mark_raw))
        if mark <= ZERO:
            raise RuntimeError("Invalid SPY mark price")
        return Market(
            market_id=int(detail["market_id"]),
            symbol=detail["symbol"],
            mark_price=mark,
            size_decimals=int(detail["size_decimals"]),
            price_decimals=int(detail["price_decimals"]),
            min_base_amount=Decimal(detail["min_base_amount"]),
            min_quote_amount=Decimal(detail["min_quote_amount"]),
        )


class LighterBroker:
    def __init__(self, cfg: Config) -> None:
        if not cfg.has_credentials:
            raise ValueError("Broker requires credentials")
        try:
            import lighter
        except ImportError as exc:
            raise RuntimeError(
                "lighter-sdk is not installed; run pip install -r requirements.txt"
            ) from exc

        self.lighter = lighter
        self.cfg = cfg
        self.api_client = lighter.ApiClient(
            configuration=lighter.Configuration(host=ROBINHOOD_API_URL)
        )
        self.account_api = lighter.AccountApi(self.api_client)
        self.order_api = lighter.OrderApi(self.api_client)
        self.signer = lighter.SignerClient(
            url=ROBINHOOD_API_URL,
            account_index=cfg.account_index,
            api_private_keys={cfg.api_key_index: cfg.api_private_key},
            chain_id=ROBINHOOD_CHAIN_ID,
        )
        error = self.signer.check_client()
        if error is not None:
            raise RuntimeError(f"Lighter API key check failed: {error}")
        self.auth, error = self.signer.create_auth_token_with_expiry(
            8 * 3600, api_key_index=cfg.api_key_index
        )
        if error is not None:
            raise RuntimeError(f"Unable to create auth token: {error}")
        self.auth_refresh_at = time.time() + 27_000

    def ensure_auth(self) -> None:
        if time.time() < self.auth_refresh_at:
            return
        self.auth, error = self.signer.create_auth_token_with_expiry(
            8 * 3600, api_key_index=self.cfg.api_key_index
        )
        if error is not None:
            raise RuntimeError(f"Unable to refresh auth token: {error}")
        self.auth_refresh_at = time.time() + 27_000

    async def close(self) -> None:
        await self.signer.close()
        await self.api_client.close()

    async def account_snapshot(self) -> tuple[Position, Decimal, Decimal]:
        response = await self.account_api.account(
            by="index", value=str(self.cfg.account_index), active_only=True
        )
        if not response.accounts:
            raise RuntimeError("Lighter account was not found")
        account = response.accounts[0]
        collateral = Decimal(account.collateral)
        gross = ZERO
        spy = Position(ZERO, ZERO)
        for item in account.positions or []:
            value = abs(Decimal(item.position_value))
            gross += value
            if int(item.market_id) == SPY_MARKET_ID:
                quantity = abs(Decimal(item.position))
                sign_value = 1 if int(item.sign) > 0 else -1
                signed_quantity = quantity * sign_value if quantity else ZERO
                spy = Position(signed_quantity, Decimal(item.avg_entry_price))
        return spy, collateral, gross

    async def active_orders(self) -> list[Any]:
        self.ensure_auth()
        response = await self.order_api.account_active_orders(
            authorization=self.auth,
            account_index=self.cfg.account_index,
            market_id=SPY_MARKET_ID,
            market_type="perp",
        )
        return list(response.orders or [])

    async def recent_trades(self) -> list[Any]:
        self.ensure_auth()
        response = await self.order_api.trades(
            sort_by="timestamp",
            sort_dir="desc",
            limit=100,
            authorization=self.auth,
            market_id=SPY_MARKET_ID,
            market_type="perp",
            account_index=self.cfg.account_index,
        )
        return list(response.trades or [])

    def daily_turnover(self, trades: Iterable[Any]) -> Decimal:
        cutoff = utc_day_start_ms()
        turnover = ZERO
        for trade in trades:
            ts = int(trade.timestamp)
            if ts >= cutoff:
                turnover += abs(Decimal(trade.usd_amount))
        return turnover

    async def cancel(self, order: Any) -> None:
        if self.cfg.dry_run:
            LOG.info(
                "DRY-RUN cancel id=%s side=%s price=%s reduce=%s",
                order.order_index,
                "sell" if order.is_ask else "buy",
                order.price,
                order.reduce_only,
            )
            return
        _, _, error = await self.signer.cancel_order(
            market_index=SPY_MARKET_ID,
            order_index=int(order.order_index),
            api_key_index=self.cfg.api_key_index,
        )
        if error is not None:
            raise RuntimeError(f"Cancel order {order.order_index} failed: {error}")

    async def create(self, order: PlannedOrder, market: Market) -> None:
        if self.cfg.dry_run:
            LOG.info(
                "DRY-RUN create %-4s price=%s size=%s notional=%.2f "
                "reduce=%s label=%s",
                order.side,
                order.price,
                order.base_size,
                order.notional,
                order.reduce_only,
                order.label,
            )
            return

        if order.client_order_index is None:
            raise RuntimeError(f"Missing client order index for {order.label}")
        time_in_force = (
            self.signer.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME
            if order.reduce_only
            else self.signer.ORDER_TIME_IN_FORCE_POST_ONLY
        )
        _, response, error = await self.signer.create_order(
            market_index=SPY_MARKET_ID,
            client_order_index=order.client_order_index,
            base_amount=scaled_int(order.base_size, market.size_decimals),
            price=scaled_int(order.price, market.price_decimals),
            is_ask=order.side == "sell",
            order_type=self.signer.ORDER_TYPE_LIMIT,
            time_in_force=time_in_force,
            reduce_only=order.reduce_only,
            api_key_index=self.cfg.api_key_index,
        )
        if error is not None:
            raise RuntimeError(f"Create {order.label} failed: {error}")
        code = getattr(response, "code", 200)
        if code != 200:
            raise RuntimeError(
                f"Create {order.label} rejected: code={code} response={response}"
            )
        LOG.info(
            "Created %-4s price=%s size=%s reduce=%s label=%s",
            order.side,
            order.price,
            order.base_size,
            order.reduce_only,
            order.label,
        )

def active_order_key(order: Any, market: Market) -> tuple[str, bool, Decimal, Decimal]:
    side = "sell" if bool(order.is_ask) else "buy"
    return (
        side,
        bool(order.reduce_only),
        round_price(Decimal(order.price), market.price_decimals, side),
        round_size(Decimal(order.remaining_base_amount), market.size_decimals),
    )


def planned_order_key(
    order: PlannedOrder, market: Market
) -> tuple[str, bool, Decimal, Decimal]:
    return (
        order.side,
        order.reduce_only,
        round_price(order.price, market.price_decimals, order.side),
        round_size(order.base_size, market.size_decimals),
    )


def reconciliation_diff(
    current: Iterable[Any],
    desired: Iterable[PlannedOrder],
    market: Market,
) -> tuple[list[Any], list[PlannedOrder], int]:
    unmatched_desired = list(desired)
    to_cancel: list[Any] = []
    kept = 0
    for active in current:
        client_id = getattr(active, "client_order_index", None)
        key = active_order_key(active, market)
        match_index = next(
            (
                idx
                for idx, candidate in enumerate(unmatched_desired)
                if client_id is not None
                and candidate.client_order_index == int(client_id)
                and planned_order_key(candidate, market) == key
            ),
            None,
        )
        if match_index is None:
            to_cancel.append(active)
        else:
            unmatched_desired.pop(match_index)
            kept += 1
    return to_cancel, unmatched_desired, kept


def cancellation_reason(
    active: Any,
    desired: Iterable[PlannedOrder],
    market: Market,
) -> str:
    client_id = getattr(active, "client_order_index", None)
    candidate = next(
        (
            order
            for order in desired
            if client_id is not None
            and order.client_order_index == int(client_id)
        ),
        None,
    )
    if candidate is None:
        return "client_id_not_in_desired_plan"
    active_key = active_order_key(active, market)
    desired_key = planned_order_key(candidate, market)
    fields = ("side", "reduce_only", "price", "remaining_size")
    changed = [name for name, old, new in zip(fields, active_key, desired_key) if old != new]
    return "desired_changed:" + ",".join(changed)


def trade_id(trade: Any) -> str:
    return str(getattr(trade, "trade_id_str", None) or trade.trade_id)


def trade_side_and_label(trade: Any, state: BotState) -> tuple[Optional[str], Optional[str]]:
    labels = {int(value): label for label, value in state.order_client_indexes.items()}
    ask_id = int(getattr(trade, "ask_client_id", 0) or 0)
    bid_id = int(getattr(trade, "bid_client_id", 0) or 0)
    if ask_id in labels:
        return "sell", labels[ask_id]
    if bid_id in labels:
        return "buy", labels[bid_id]
    return None, None


def apply_trades(state: BotState, trades: Iterable[Any]) -> None:
    ordered = sorted(trades, key=lambda item: (int(item.timestamp), trade_id(item)))
    if not state.trade_cursor_initialized:
        state.processed_trade_ids = [trade_id(item) for item in ordered][-500:]
        state.trade_cursor_initialized = True
        return
    processed = set(state.processed_trade_ids)
    new_trades = [item for item in ordered if trade_id(item) not in processed]
    if len(new_trades) >= 100:
        raise RuntimeError("Trade sync reached 100 unprocessed fills; opening halted")
    for trade in new_trades:
        tid = trade_id(trade)
        side, label = trade_side_and_label(trade, state)
        if side is None or label is None:
            processed.add(tid)
            continue
        remaining = Decimal(str(trade.size))
        if label.startswith("exit-"):
            lot_id = label.removeprefix("exit-")
            for lot in list(state.lots):
                if lot.lot_id != lot_id:
                    continue
                left = Decimal(lot.base_size) - remaining
                if left <= ZERO:
                    state.lots.remove(lot)
                    state.order_client_indexes.pop(label, None)
                else:
                    lot.base_size = str(left)
                remaining = ZERO
                break
        else:
            opposite = "short" if side == "buy" else "long"
            for lot in list(state.lots):
                if remaining <= ZERO or lot.side != opposite:
                    continue
                lot_size = Decimal(lot.base_size)
                closed = min(remaining, lot_size)
                remaining -= closed
                lot_size -= closed
                if lot_size <= ZERO:
                    state.lots.remove(lot)
                    state.order_client_indexes.pop(f"exit-{lot.lot_id}", None)
                else:
                    lot.base_size = str(lot_size)
            if remaining > ZERO:
                state.lots.append(
                    Lot(
                        lot_id=tid,
                        slot_label=label,
                        side="long" if side == "buy" else "short",
                        entry_price=str(trade.price),
                        base_size=str(remaining),
                    )
                )
        processed.add(tid)
    state.processed_trade_ids = list(processed)[-500:]


def allocate_client_order_index(state: BotState) -> int:
    if state.next_client_order_index is None:
        state.next_client_order_index = (int(time.time() * 1000) * 100) % MAX_CLIENT_ORDER_INDEX
    state.next_client_order_index = (state.next_client_order_index + 1) % MAX_CLIENT_ORDER_INDEX
    return state.next_client_order_index


def assign_client_order_indexes(
    state: BotState,
    desired: Iterable[PlannedOrder],
    current: Iterable[Any],
    market: Market,
) -> list[PlannedOrder]:
    desired = list(desired)
    current_by_client = {
        int(order.client_order_index): order
        for order in current
        if getattr(order, "client_order_index", None) is not None
    }
    desired_labels = {order.label for order in desired}
    active_ids = set(current_by_client)
    for label, client_id in list(state.order_client_indexes.items()):
        if label not in desired_labels and client_id not in active_ids:
            state.order_client_indexes.pop(label, None)
    assigned = []
    for order in desired:
        client_id = state.order_client_indexes.get(order.label)
        active = current_by_client.get(client_id) if client_id is not None else None
        if active is not None and active_order_key(active, market) != planned_order_key(order, market):
            client_id = None
        if client_id is None:
            client_id = allocate_client_order_index(state)
            state.order_client_indexes[order.label] = client_id
        assigned.append(replace(order, client_order_index=client_id))
    return assigned


async def reconcile_orders(
    broker: LighterBroker,
    market: Market,
    current: Iterable[Any],
    desired: Iterable[PlannedOrder],
) -> None:
    current = list(current)
    desired = list(desired)
    to_cancel, unmatched_desired, kept = reconciliation_diff(
        current, desired, market
    )

    # Protect a newly filled position before changing unrelated opening orders.
    # If the exchange does not confirm the exit, preserve the existing book.
    missing_protection = [order for order in unmatched_desired if order.reduce_only]
    for order in missing_protection:
        await broker.create(order, market)

    created_protection = len(missing_protection)
    if missing_protection and not broker.cfg.dry_run:
        confirmed_current: list[Any] = []
        missing_labels = [order.label for order in missing_protection]
        for attempt in range(3):
            confirmed_current = await broker.active_orders()
            _, still_missing, _ = reconciliation_diff(
                confirmed_current, missing_protection, market
            )
            if not still_missing:
                LOG.info(
                    "Confirmed reduce-only protection count=%d labels=%s",
                    len(missing_protection),
                    ",".join(missing_labels),
                )
                break
            if attempt < 2:
                await asyncio.sleep(0.25)
        else:
            raise RuntimeError(
                "Reduce-only protection was not confirmed; existing orders preserved: "
                + ",".join(missing_labels)
            )
        current = confirmed_current
        to_cancel, unmatched_desired, kept = reconciliation_diff(
            current, desired, market
        )
    elif missing_protection:
        missing_ids = {id(order) for order in missing_protection}
        unmatched_desired = [
            order for order in unmatched_desired if id(order) not in missing_ids
        ]

    for order in to_cancel:
        reason = cancellation_reason(order, desired, market)
        side = "sell" if bool(order.is_ask) else "buy"
        LOG.info(
            "Canceling order_id=%s client_id=%s side=%s price=%s remaining=%s "
            "reduce=%s reason=%s",
            order.order_index,
            getattr(order, "client_order_index", None),
            side,
            order.price,
            order.remaining_base_amount,
            order.reduce_only,
            reason,
        )
        await broker.cancel(order)
        LOG.info(
            "Canceled order_id=%s client_id=%s reason=%s",
            order.order_index,
            getattr(order, "client_order_index", None),
            reason,
        )
    for order in unmatched_desired:
        await broker.create(order, market)

    LOG.info(
        "Reconcile complete: kept=%d canceled=%d created=%d protection_first=%d",
        kept,
        len(to_cancel),
        len(unmatched_desired) + created_protection,
        created_protection,
    )


def load_state(path: Path) -> BotState:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        data["lots"] = [Lot(**item) for item in data.get("lots", [])]
        return BotState(**data)
    except FileNotFoundError:
        return BotState()
    except Exception as exc:
        raise RuntimeError(f"Invalid state file {path}: {exc}") from exc


def save_state(path: Path, state: BotState) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(asdict(state), indent=2), encoding="utf-8")
    temporary.replace(path)


def choose_anchor(
    cfg: Config,
    market: Market,
    position: Position,
    state: BotState,
) -> Decimal:
    if state.anchor_price is None:
        anchor = position.average_entry if not position.is_flat else market.mark_price
    else:
        anchor = Decimal(state.anchor_price)
    if position.is_flat and abs(market.mark_price / anchor - ONE) >= cfg.recenter_threshold:
        anchor = market.mark_price
    state.anchor_price = str(round_price(anchor, market.price_decimals, "buy"))
    state.updated_at = datetime.now(timezone.utc).isoformat()
    return Decimal(state.anchor_price)


def print_plan(
    cfg: Config,
    market: Market,
    anchor: Decimal,
    position: Position,
    collateral: Decimal,
    gross: Decimal,
    turnover: Decimal,
    desired: Iterable[PlannedOrder],
    reasons: list[str],
) -> None:
    LOG.info(
        "SPY mark=%s anchor=%s position=%s entry=%s collateral=%s "
        "account_gross=%s daily_turnover=%s dry_run=%s",
        market.mark_price,
        anchor,
        position.size,
        position.average_entry,
        collateral,
        gross,
        turnover,
        cfg.dry_run,
    )
    if reasons:
        LOG.warning("Opening orders disabled: %s", "; ".join(reasons))
    for order in desired:
        LOG.info(
            "PLAN %-4s price=%s size=%s notional=%.2f reduce=%s %s",
            order.side,
            order.price,
            order.base_size,
            order.notional,
            order.reduce_only,
            order.label,
        )


async def run_cycle(
    cfg: Config,
    public: PublicMarketClient,
    broker: Optional[LighterBroker],
) -> None:
    market = await public.get_spy_market()
    if broker is None:
        position = Position(ZERO, ZERO)
        collateral = Decimal("2000")
        gross = ZERO
        turnover = ZERO
        trades: list[Any] = []
        active: list[Any] = []
    else:
        position, collateral, gross = await broker.account_snapshot()
        trades = await broker.recent_trades()
        turnover = broker.daily_turnover(trades)
        active = await broker.active_orders()

    state = load_state(cfg.state_file)
    if broker is not None:
        apply_trades(state, trades)
        tracked_size = sum(
            Decimal(lot.base_size) * (ONE if lot.side == "long" else -ONE)
            for lot in state.lots
        )
        if not position.is_flat and tracked_size != position.size:
            # Account positions can become visible before the trade endpoint.
            # Retry briefly so the fill can be mapped to a lot and protected in
            # this cycle instead of waiting for the next 10-second poll.
            for attempt in range(6):
                await asyncio.sleep(0.5)
                position, collateral, gross = await broker.account_snapshot()
                trades = await broker.recent_trades()
                apply_trades(state, trades)
                tracked_size = sum(
                    Decimal(lot.base_size)
                    * (ONE if lot.side == "long" else -ONE)
                    for lot in state.lots
                )
                if tracked_size == position.size:
                    LOG.info(
                        "Fill synchronization confirmed position=%s attempt=%d",
                        position.size,
                        attempt + 1,
                    )
                    active = await broker.active_orders()
                    turnover = broker.daily_turnover(trades)
                    break
    anchor = choose_anchor(cfg, market, position, state)

    distance = abs(market.mark_price / anchor - ONE)
    out_of_range = distance > cfg.half_width + cfg.out_of_range_extra
    reasons: list[str] = []
    if collateral < cfg.min_collateral:
        reasons.append(f"collateral {collateral} < {cfg.min_collateral}")
    if gross >= cfg.max_account_gross_notional:
        reasons.append(
            f"account gross {gross} >= {cfg.max_account_gross_notional}"
        )
    tracked_size = sum(
        Decimal(lot.base_size) * (ONE if lot.side == "long" else -ONE)
        for lot in state.lots
    )
    if tracked_size != position.size:
        reasons.append(
            f"SPY position {position.size} != tracked lots {tracked_size}; "
            "fill synchronization incomplete"
        )
    if out_of_range:
        reasons.append(
            f"mark is {distance:.4%} from anchor; grid halt threshold exceeded"
        )
    allow_opening = not reasons
    current_spy_notional = abs(position.size) * market.mark_price
    non_spy_gross = max(gross - current_spy_notional, ZERO)
    effective_spy_cap = min(
        cfg.max_net_position,
        max(cfg.max_account_gross_notional - non_spy_gross, ZERO),
    )
    plan_cfg = replace(cfg, max_net_position=effective_spy_cap)
    desired = build_plan(
        plan_cfg,
        market,
        anchor,
        position,
        allow_opening=allow_opening,
        emergency_reduce=out_of_range,
        lots=state.lots,
    )
    desired = assign_client_order_indexes(state, desired, active, market)
    save_state(cfg.state_file, state)
    print_plan(
        cfg,
        market,
        anchor,
        position,
        collateral,
        gross,
        turnover,
        desired,
        reasons,
    )
    if broker is not None:
        if not cfg.dry_run and tracked_size != position.size:
            raise RuntimeError(
                "Refusing live reconciliation until the full SPY position is tracked"
            )
        await reconcile_orders(broker, market, active, desired)


async def main_async(args: argparse.Namespace) -> None:
    cfg = Config.from_env()
    public = PublicMarketClient()
    broker: Optional[LighterBroker] = None
    if cfg.has_credentials:
        broker = LighterBroker(cfg)
    elif not cfg.dry_run:
        raise RuntimeError("Live mode cannot run without credentials")

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    try:
        while not stop.is_set():
            try:
                await run_cycle(cfg, public, broker)
            except Exception:
                LOG.exception("Cycle failed; no further actions taken this cycle")
                if args.once:
                    raise
            if args.once:
                break
            try:
                await asyncio.wait_for(stop.wait(), timeout=cfg.poll_seconds)
            except asyncio.TimeoutError:
                pass
    finally:
        if broker is not None:
            await broker.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--once", action="store_true", help="Run one reconciliation cycle and exit"
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
