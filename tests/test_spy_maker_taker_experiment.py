import asyncio
import csv
import json
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch, AsyncMock
from dataclasses import dataclass, replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from datetime import datetime, timedelta

from spy_maker_taker_experiment import Experiment, ExperimentConfig, UnavailablePoints, timestamp_ms, RESTRateLimiter, RateLimitError, hedge_protection_price, LighterExperimentBroker, ExperimentInstanceLock, EventKind, ExperimentState, NormalizedEvent, run_live


@dataclass
class Fill:
    id: int
    maker: bool
    side: str = "buy"
    price: str = "100"
    qty: str = "5"
    timestamp: str = "2026-01-01T00:00:00Z"


class FakeBroker:
    def __init__(self, position=Decimal("0")):
        self.pos = position
        self.orders = []
        self.calls = []
        self.next_fills = []

    async def best_bid_ask(self): return Decimal("99"), Decimal("101")
    async def position(self, priority=False): return self.pos
    async def resting_orders(self): return list(self.orders)
    async def post_only(self, side, price, qty, client_id):
        self.calls.append(("post", side)); self.orders.append(SimpleNamespace(side=side, client_id=client_id))
    async def cancel(self, order): self.calls.append(("cancel", order.side)); self.orders.remove(order)
    async def market_reduce_only(self, side, qty, client_id):
        self.calls.append(("hedge", side, qty)); self.pos = Decimal("0")
    async def fills(self, since): return list(self.next_fills)
    async def order_status(self, order_id): return {"order_id": order_id, "status": "active", "source": "fake"}


class ReconcileBroker:
    """Deterministic order/fill/position timelines; no network or sleeps."""
    def __init__(self, positions, fills_by_poll, statuses=None, include_order_id=True):
        self.positions = list(positions)
        self.fills_by_poll = list(fills_by_poll)
        self.statuses = list(statuses or ["filled"] * max(1, len(fills_by_poll)))
        self.poll = 0
        self.orders = []
        self.hedges = []
        self.include_order_id = include_order_id

    async def best_bid_ask(self): return Decimal("99"), Decimal("101")
    async def position(self, priority=False):
        value = self.positions[min(self.poll, len(self.positions) - 1)]
        self.poll += 1
        return value
    async def resting_orders(self): return list(self.orders)
    async def post_only(self, side, price, qty, client_id): return None
    async def cancel(self, order): return None
    async def cancel_order_confirmed(self, order_id): return {"order_id": order_id, "status": "canceled"}
    async def market_reduce_only(self, side, qty, client_id):
        order_id = 100 + len(self.hedges)
        self.hedges.append((side, qty, client_id, order_id))
        return SimpleNamespace(order_id=order_id if self.include_order_id else None, client_order_id=200 + len(self.hedges), response=SimpleNamespace(code=200))
    async def order_status(self, order_id):
        idx = order_id - 100
        status = self.statuses[min(idx, len(self.statuses) - 1)]
        return {"order_id": order_id, "status": status, "source": "fake"}
    async def fills(self, since):
        idx = max(0, self.poll - 2)
        return list(self.fills_by_poll[min(idx, len(self.fills_by_poll) - 1)])


def config(path: Path) -> ExperimentConfig:
    return ExperimentConfig(data_dir=path, quote_refresh_interval=0, poll_seconds=0, summary_seconds=999999)


class MakerTakerTests(unittest.IsolatedAsyncioTestCase):
    async def test_position_sanity_recovers_missed_maker_fill_via_existing_hedge_path(self):
        with tempfile.TemporaryDirectory() as td:
            broker = FakeBroker(Decimal("0.125"))
            exp = Experiment(config(Path(td)), broker)
            exp.quote_active = True; exp.state = ExperimentState.MAKER_RESTING
            exp.current_order_ids = {"buy": 7, "sell": 8}; exp.quote_prices = {"buy": Decimal("100"), "sell": Decimal("101")}
            exp.last_position_sanity_at = 0
            exp._consume_new_fills = AsyncMock()
            await exp.tick()
            fill = exp._consume_new_fills.await_args.args[0][0]
            self.assertEqual(fill.side, "buy")
            self.assertEqual(Decimal(fill.qty), Decimal("0.125"))
            self.assertEqual(exp.fill_detection_misses, 1)
            self.assertEqual(exp._consume_new_fills.await_args.kwargs["detection_source"], "position_sanity")

    async def test_owned_side_overrides_matching_ws_side_for_buy_and_sell(self):
        with tempfile.TemporaryDirectory() as td:
            exp = Experiment(config(Path(td)), FakeBroker())
            exp.state = ExperimentState.MAKER_RESTING
            exp.current_order_ids = {"buy": 7, "sell": 8}
            exp.current_client_order_ids = {"buy": "101", "sell": "102"}
            exp.maker_side_by_client_id = {"101": "buy", "102": "sell"}
            exp.maker_handled_cumulative = {7: Decimal("0"), 8: Decimal("0")}
            exp._consume_new_fills = AsyncMock()
            for oid, client, side in ((7, "101", "BUY"), (8, "102", "SELL")):
                await exp._handle_order_event(SimpleNamespace(order_id=oid, client_order_id=client, status="PARTIALLY_FILLED", filled_quantity=Decimal("1"), price=Decimal("100"), side=side, timestamp=1), "orders")
            sides = [call.args[0][0].side for call in exp._consume_new_fills.await_args_list]
            self.assertEqual(sides, ["buy", "sell"])

    async def test_is_ask_conflict_keeps_local_order_direction(self):
        with tempfile.TemporaryDirectory() as td:
            exp = Experiment(config(Path(td)), FakeBroker())
            exp.state = ExperimentState.MAKER_RESTING
            exp.current_order_ids = {"buy": 7}
            exp.current_client_order_ids = {"buy": "101"}
            exp.maker_side_by_client_id = {"101": "buy"}
            exp._consume_new_fills = AsyncMock()
            update = SimpleNamespace(order_id=7, client_order_id="101", status="PARTIALLY_FILLED",
                                     filled_quantity=Decimal("1"), price=Decimal("100"),
                                     side="SELL", is_ask=True, timestamp=1)
            await exp._handle_order_event(update, "orders")
            self.assertEqual(exp._consume_new_fills.await_args.args[0][0].side, "buy")
            self.assertIn("ORDER_SIDE_IGNORED", (Path(td) / "events.jsonl").read_text())

    async def test_over_cap_hedge_flattens_before_failure(self):
        with tempfile.TemporaryDirectory() as td:
            broker = ReconcileBroker([Decimal("6"), Decimal("0")], [[]], ["unknown"])
            exp = Experiment(config(Path(td)), broker)
            exp.current_order_ids = {"buy": 7, "sell": 8}
            exp.current_client_order_ids = {"buy": "101", "sell": "102"}
            exp.maker_side_by_client_id = {"101": "buy", "102": "sell"}
            exp._cancel_specific_request = AsyncMock()
            exp._cancel_specific_confirmed = AsyncMock()
            await exp._hedge(SimpleNamespace(order_id=7, side="buy", price="100", qty="6",
                                             timestamp="2026-01-01T00:00:00Z"), Decimal("0"),
                             "2026-01-01T00:00:00Z")
            self.assertEqual(broker.hedges[0][0], "sell")
            self.assertIn("position_cap_recovery", (Path(td) / "events.jsonl").read_text())
            self.assertEqual(broker.positions[-1], Decimal("0"))

    async def test_unreliable_ws_side_is_ignored_for_owned_order(self):
        with tempfile.TemporaryDirectory() as td:
            exp = Experiment(config(Path(td)), FakeBroker())
            exp.state = ExperimentState.MAKER_RESTING
            exp.current_order_ids = {"buy": 7}; exp.current_client_order_ids = {"buy": "101"}; exp.maker_side_by_client_id = {"101": "buy"}
            exp._consume_new_fills = AsyncMock()
            await exp._handle_order_event(SimpleNamespace(order_id=7, client_order_id="101", status="PARTIALLY_FILLED", filled_quantity=Decimal("1"), price=Decimal("100"), side="SELL", timestamp=1), "orders")
            self.assertFalse(exp.halted)
            self.assertIn("ORDER_SIDE_IGNORED", (Path(td) / "events.jsonl").read_text())

    async def test_stale_flat_quote_refreshes_only_when_off_bbo(self):
        with tempfile.TemporaryDirectory() as td:
            class MovingBook(FakeBroker):
                async def best_bid_ask(self): return Decimal("100"), Decimal("102")
            broker = MovingBook()
            broker.orders = [SimpleNamespace(side="buy", order_index=7, client_order_index=101), SimpleNamespace(side="sell", order_index=8, client_order_index=102)]
            cfg = replace(config(Path(td)), dry_run=False)
            exp = Experiment(cfg, broker)
            exp.quote_active = True; exp.state = ExperimentState.MAKER_RESTING
            exp.current_order_ids = {"buy": 7, "sell": 8}
            exp.current_client_order_ids = {"buy": "101", "sell": "102"}
            exp.maker_side_by_client_id = {"101": "buy", "102": "sell"}
            exp.quote_prices = {"buy": Decimal("99"), "sell": Decimal("101")}
            exp.quote_started_at = __import__("time").monotonic() - 21
            exp.last_position_sanity_at = __import__("time").monotonic()
            await exp.tick()
            self.assertFalse(exp.quote_active)
            self.assertEqual(exp.quote_refresh_count, 1)
            self.assertEqual(exp._pending_quote_bbo, (Decimal("100"), Decimal("102")))

    async def test_quote_refresh_is_forbidden_after_any_maker_fill(self):
        with tempfile.TemporaryDirectory() as td:
            broker = FakeBroker()
            exp = Experiment(config(Path(td)), broker)
            exp.quote_active = True; exp.state = ExperimentState.MAKER_RESTING
            exp.quote_prices = {"buy": Decimal("90"), "sell": Decimal("110")}
            exp.quote_started_at = __import__("time").monotonic() - 21
            exp.maker_order_filled_qty = {7: Decimal("0.1")}
            exp.last_position_sanity_at = __import__("time").monotonic()
            await exp.tick()
            self.assertTrue(exp.quote_active)
            self.assertEqual(exp.quote_refresh_count, 0)

    async def test_silent_terminal_maker_order_exits_resting_and_refreshes_pair(self):
        with tempfile.TemporaryDirectory() as td:
            class SilentCancel(FakeBroker):
                async def order_status(self, order_id):
                    return {"order_id": order_id, "status": "canceled-post-only" if order_id == 7 else "active", "source": "rest_inactive"}
            broker = SilentCancel()
            broker.orders = [SimpleNamespace(side="buy", order_index=7, client_order_index=101), SimpleNamespace(side="sell", order_index=8, client_order_index=102)]
            exp = Experiment(config(Path(td)), broker)
            exp.quote_active = True; exp.state = ExperimentState.MAKER_RESTING
            exp.current_order_ids = {"buy": 7, "sell": 8}; exp.current_client_order_ids = {"buy": "101", "sell": "102"}
            exp.maker_side_by_client_id = {"101": "buy", "102": "sell"}; exp.quote_started_at = __import__("time").monotonic() - 21
            exp.last_position_sanity_at = __import__("time").monotonic()
            await exp.tick()
            self.assertFalse(exp.quote_active)
            self.assertEqual(exp.quote_refresh_count, 1)

    async def test_fast_maker_fill_before_active_order_lookup_is_owned_and_hedged(self):
        """A fill racing submit cannot be discarded merely because REST never saw it open."""
        with tempfile.TemporaryDirectory() as td, patch("spy_maker_taker_experiment.asyncio.sleep", new=async_noop):
            broker = ReconcileBroker([Decimal("1"), Decimal("0")], [[Fill(910, False, "sell", "100", "1", "1000000000100")]])
            exp = Experiment(config(Path(td)), broker)
            exp.quote_active = True; exp.state = ExperimentState.MAKER_RESTING
            exp.current_client_order_ids = {"buy": "101", "sell": "102"}
            exp.maker_side_by_client_id = {"101": "buy", "102": "sell"}
            exp.maker_qty_by_client_id = {"101": Decimal("1"), "102": Decimal("1")}
            exp.maker_client_reconciled = {"101": False, "102": False}
            update = SimpleNamespace(order_id=77, client_order_id="101", status="FILLED", filled_quantity=Decimal("1"), price=Decimal("100"), side="BUY", timestamp="1000000000000")
            await exp._handle_order_event(update, "account_orders_ws")
            self.assertEqual(broker.hedges[0][0:2], ("sell", Decimal("1")))
            self.assertEqual(exp.stats.maker_volume, Decimal("100"))

    async def test_position_sanity_divergence_cancels_owned_pair_and_flattens(self):
        with tempfile.TemporaryDirectory() as td:
            broker = FakeBroker(Decimal("1"))
            broker.orders = [SimpleNamespace(side="buy", order_index=7, client_order_index=101)]
            exp = Experiment(config(Path(td)), broker)
            exp.quote_active = True; exp.state = ExperimentState.MAKER_RESTING
            exp.current_order_ids = {"buy": 7}
            exp.current_client_order_ids = {"buy": "101", "sell": "102"}
            exp.maker_side_by_client_id = {"101": "buy", "102": "sell"}
            exp.last_position_sanity_at = 0
            await exp.tick()
            self.assertTrue(exp.halted)
            self.assertEqual(exp.fill_detection_misses, 1)
            self.assertIn(("hedge", "sell", Decimal("1")), broker.calls)

    async def test_cleanup_never_cancels_unowned_orders_when_identity_missing(self):
        with tempfile.TemporaryDirectory() as td:
            broker = FakeBroker()
            broker.orders = [SimpleNamespace(side="sell", order_index=77, client_order_index=999)]
            exp = Experiment(config(Path(td)), broker)
            await exp._cancel_all_confirmed()
            self.assertEqual([c for c in broker.calls if c[0] == "cancel"], [])

    async def test_simultaneous_two_sided_maker_fills_reconciles_once_without_naked_reversal(self):
        """A near-simultaneous pair must be fully accounted before recovery."""
        with tempfile.TemporaryDirectory() as td:
            broker = FakeBroker(Decimal("0"))
            async def cancel_status(order_id):
                # The exchange reports the opposite order as filled during the
                # cancel race; its cumulative account_orders update follows.
                return {"order_id": order_id, "status": "filled" if order_id == 8 else "canceled"}
            broker.cancel_order_confirmed = cancel_status
            exp = Experiment(config(Path(td)), broker)
            exp.state = ExperimentState.MAKER_RESTING
            exp.quote_active = True
            exp.current_order_ids = {"buy": 7, "sell": 8}
            exp.maker_handled_cumulative = {7: Decimal("0"), 8: Decimal("0")}
            exp.maker_trade_cumulative = {7: Decimal("0"), 8: Decimal("0")}

            # The exchange has filled both equal makers; signed position is
            # already flat, so recovery must not create a reverse position.
            for update in (
                SimpleNamespace(order_id=7, status="FILLED", filled_quantity=Decimal("1"), price=Decimal("100"), side="BUY", timestamp=1),
                SimpleNamespace(order_id=8, status="FILLED", filled_quantity=Decimal("1"), price=Decimal("100"), side="SELL", timestamp=1),
            ):
                await exp.handle_event(NormalizedEvent(EventKind.ORDER, update, "account_orders_ws"))

            self.assertEqual([call for call in broker.calls if call[0] == "hedge"], [])
            self.assertEqual(broker.pos, Decimal("0"))
            self.assertEqual(exp.stats.maker_volume, Decimal("200"))
            self.assertEqual(exp.stats.cycles, 0)
            self.assertEqual(exp.stats.fills, 2)
            self.assertEqual(exp.current_order_ids, {})
            await exp.halt("test completion")
            self.assertTrue(exp.cleanup_completed)

    async def test_sigterm_during_hedge_is_serialized_and_cleans_up_once(self):
        """A shutdown event waits behind the active hedge coordinator section."""
        class BlockingBroker(FakeBroker):
            def __init__(self):
                super().__init__(Decimal("1"))
                self.cancel_entered = asyncio.Event()
                self.release_cancel = asyncio.Event()
                self.cancel_calls = 0
                self.hedge_calls = 0

            async def cancel_order_confirmed(self, _order_id):
                self.cancel_calls += 1
                if self.cancel_calls == 1:
                    self.cancel_entered.set()
                    await self.release_cancel.wait()
                return {"status": "canceled"}

            async def market_reduce_only(self, side, qty, client_id):
                self.hedge_calls += 1
                self.calls.append(("hedge", side, qty, client_id))
                self.pos = Decimal("0")
                return SimpleNamespace(order_id=101, client_order_id=202, response=SimpleNamespace(code=200))

        with tempfile.TemporaryDirectory() as td:
            broker = BlockingBroker()
            exp = Experiment(config(Path(td)), broker)
            exp.state = ExperimentState.MAKER_RESTING
            exp.quote_active = True
            exp.current_order_ids = {"buy": 7, "sell": 8}
            exp.maker_handled_cumulative = {7: Decimal("0"), 8: Decimal("0")}
            exp.maker_trade_cumulative = {7: Decimal("0"), 8: Decimal("0")}
            flat = AsyncMock(wraps=exp._flat_confirmed)
            exp._flat_confirmed = flat
            fill = SimpleNamespace(fill_id="maker-fill", order_id=7, is_maker=True, quantity=Decimal("1"), price=Decimal("100"), side="BUY", timestamp=1)
            hedge_task = asyncio.create_task(exp.handle_event(NormalizedEvent(EventKind.FILL, fill, "account_orders_ws")))
            await asyncio.wait_for(broker.cancel_entered.wait(), timeout=1)
            shutdown_task = asyncio.create_task(exp.handle_event(NormalizedEvent(EventKind.SHUTDOWN, "signal SIGTERM")))
            await asyncio.sleep(0)
            self.assertEqual(broker.hedge_calls, 0)
            self.assertFalse(exp.halted)
            broker.release_cancel.set()
            await asyncio.gather(hedge_task, shutdown_task)
            self.assertEqual(broker.hedge_calls, 1)
            self.assertTrue(exp.cleanup_completed)
            self.assertEqual(flat.await_count, 1)
            self.assertEqual(broker.pos, Decimal("0"))
            self.assertEqual(await broker.resting_orders(), [])

    async def test_orphan_live_process_blocks_new_instance_before_any_order(self):
        """The preflight must fail before lock acquisition or broker order calls."""
        with tempfile.TemporaryDirectory() as td:
            fake_broker = SimpleNamespace(close=AsyncMock(), events=asyncio.Queue())
            fake_classic_cfg = SimpleNamespace(account_index=5745)
            cfg = ExperimentConfig(experiment_id="ORPHAN_TEST", dry_run=False, data_dir=Path(td))
            with patch("spy_grid_bot.Config.from_env", return_value=fake_classic_cfg), \
                 patch("spy_maker_taker_experiment.LighterExperimentBroker", return_value=fake_broker), \
                 patch("spy_maker_taker_experiment.find_live_experiment_processes", return_value=[424242]), \
                 patch("spy_maker_taker_experiment.ExperimentInstanceLock") as lock:
                with self.assertRaisesRegex(RuntimeError, "live experiment process already exists: \\[424242\\]"):
                    await run_live(cfg, once=False)
            lock.return_value.acquire.assert_not_called()
            fake_broker.close.assert_awaited_once()
    async def test_subscription_ack_is_required_for_quotes(self):
        with tempfile.TemporaryDirectory() as td:
            broker = FakeBroker()
            broker.ws_ready = lambda: False
            exp = Experiment(config(Path(td)), broker)
            exp.fill_stream_active = True
            await exp.tick()
            self.assertEqual(broker.calls, [])

    async def test_actual_subscription_snapshots_only_mark_ready_never_trigger_hedge(self):
        with tempfile.TemporaryDirectory() as td:
            broker = FakeBroker(); broker.connection_ready = False; broker.private_subscriptions = set()
            exp = Experiment(config(Path(td)), broker); exp.state = ExperimentState.MAKER_RESTING
            exp._consume_new_fills = AsyncMock()
            await exp.handle_event(NormalizedEvent(EventKind.CONNECTION, {"type": "connected"}))
            await exp.handle_event(NormalizedEvent(EventKind.SUBSCRIPTION, {"type": "subscribed/account_orders", "channel": "account_orders:26", "orders": {"26": []}}))
            await exp.handle_event(NormalizedEvent(EventKind.SUBSCRIPTION, {"type": "subscribed/account_all_trades", "channel": "account_all_trades:7", "trades": {}}))
            self.assertTrue(broker.connection_ready)
            self.assertEqual(broker.private_subscriptions, {"account_orders", "account_all_trades"})
            exp._consume_new_fills.assert_not_awaited()

    async def test_disconnect_resets_private_readiness(self):
        with tempfile.TemporaryDirectory() as td:
            broker = FakeBroker(); broker.connection_ready = True; broker.private_subscriptions = {"account_orders", "account_all_trades"}
            exp = Experiment(config(Path(td)), broker)
            await exp.handle_event(NormalizedEvent(EventKind.CONNECTION, {"state": "disconnected"}))
            self.assertFalse(broker.connection_ready)
            self.assertEqual(broker.private_subscriptions, set())

    async def test_reconnect_requires_fresh_acks_before_private_ready(self):
        with tempfile.TemporaryDirectory() as td:
            broker = FakeBroker(); broker.connection_ready = False; broker.private_subscriptions = set()
            broker.ws_ready = lambda: broker.connection_ready and broker.private_subscriptions == {"account_orders", "account_all_trades"}
            exp = Experiment(config(Path(td)), broker)
            for payload in (
                {"type": "connected"},
                {"type": "subscribed/account_orders", "channel": "account_orders:26", "orders": {"26": []}},
                {"type": "subscribed/account_all_trades", "channel": "account_all_trades:7", "trades": {}},
            ):
                await exp.handle_event(NormalizedEvent(EventKind.CONNECTION if payload["type"] == "connected" else EventKind.SUBSCRIPTION, payload))
            self.assertTrue(broker.ws_ready())
            await exp.handle_event(NormalizedEvent(EventKind.CONNECTION, {"state": "disconnected"}))
            self.assertFalse(broker.ws_ready())
            await exp.handle_event(NormalizedEvent(EventKind.CONNECTION, {"type": "connected"}))
            self.assertFalse(broker.ws_ready())
            await exp.handle_event(NormalizedEvent(EventKind.SUBSCRIPTION, {"type": "subscribed/account_orders", "channel": "account_orders:26", "orders": {"26": []}}))
            self.assertFalse(broker.ws_ready())
            await exp.handle_event(NormalizedEvent(EventKind.SUBSCRIPTION, {"type": "subscribed/account_all_trades", "channel": "account_all_trades:7", "trades": {}}))
            self.assertTrue(broker.ws_ready())

    async def test_account_orders_partial_then_filled_triggers_each_cumulative_delta_once(self):
        with tempfile.TemporaryDirectory() as td:
            exp = Experiment(config(Path(td)), FakeBroker())
            exp.state = ExperimentState.MAKER_RESTING
            exp.current_order_ids = {"buy": 7, "sell": 8}
            exp.maker_handled_cumulative = {7: Decimal("0"), 8: Decimal("0")}
            exp._consume_new_fills = AsyncMock()
            partial = SimpleNamespace(order_id="7", status="PARTIALLY_FILLED", filled_quantity=Decimal("1"), price=Decimal("100"), side="BUY", timestamp=1)
            filled = SimpleNamespace(order_id="7", status="FILLED", filled_quantity=Decimal("3"), price=Decimal("100"), side="BUY", timestamp=2)
            await exp._handle_order_event(partial, "account_orders_ws")
            await exp._handle_order_event(partial, "account_orders_ws")
            await exp._handle_order_event(filled, "account_orders_ws")
            self.assertEqual([Decimal(str(call.args[0][0].qty)) for call in exp._consume_new_fills.await_args_list], [Decimal("1"), Decimal("2")])

    async def test_order_then_trade_duplicate_does_not_trigger_second_hedge(self):
        with tempfile.TemporaryDirectory() as td:
            exp = Experiment(config(Path(td)), FakeBroker())
            exp.state = ExperimentState.MAKER_RESTING; exp.current_order_ids = {"buy": 7}
            exp.maker_handled_cumulative = {7: Decimal("0")}
            exp._consume_new_fills = AsyncMock()
            await exp._handle_order_event(SimpleNamespace(order_id="7", status="FILLED", filled_quantity=Decimal("1"), price=Decimal("100"), side="BUY", timestamp=1), "orders")
            await exp._handle_fill_event(SimpleNamespace(fill_id="trade-1", order_id="7", is_maker=True, quantity=Decimal("1"), price=Decimal("100"), side="BUY", timestamp=1), "trades")
            self.assertEqual(exp._consume_new_fills.await_count, 1)
            self.assertEqual(exp.stats.maker_volume, Decimal("100"))

    async def test_disconnect_with_resting_maker_enters_single_cleanup(self):
        with tempfile.TemporaryDirectory() as td:
            exp = Experiment(config(Path(td)), FakeBroker())
            exp.quote_active = True; exp.state = ExperimentState.MAKER_RESTING
            await exp.handle_event(NormalizedEvent(EventKind.CONNECTION, {"state": "disconnected"}))
            self.assertTrue(exp.halted)
            self.assertEqual(exp.state, ExperimentState.STOPPED)

    async def test_usd_position_cap_allows_configured_rounding_headroom(self):
        """$100 order may briefly mark inside the explicit $105 hard cap."""
        with tempfile.TemporaryDirectory() as td:
            broker = FakeBroker(Decimal("1.04"))  # $104 at the fake $100 mid
            cfg = ExperimentConfig(order_notional=Decimal("100"), max_net_position=Decimal("105"), data_dir=Path(td), quote_refresh_interval=0)
            exp = Experiment(cfg, broker)
            await exp.tick()
            self.assertFalse(exp.halted)
            self.assertLessEqual(exp.stats.max_net_position_usd, Decimal("105"))

    async def test_usd_position_cap_not_qty_cap(self):
        with tempfile.TemporaryDirectory() as td:
            broker = FakeBroker(Decimal("1.06"))  # $106 at the fake $100 mid
            cfg = ExperimentConfig(order_notional=Decimal("100"), max_net_position=Decimal("105"), data_dir=Path(td), quote_refresh_interval=0)
            exp = Experiment(cfg, broker)
            await exp.tick()
            self.assertTrue(exp.halted)
            self.assertIn("max net position", exp.halt_reason)

    async def test_scheduler_allows_p0_while_p3_backoff_is_pending(self):
        limiter = RESTRateLimiter(min_interval=0)
        low_calls = 0
        async def low():
            nonlocal low_calls
            low_calls += 1
            if low_calls == 1: raise RuntimeError("429")
            return "low"
        low_task = asyncio.create_task(limiter.call(low, priority=3))
        await asyncio.sleep(0)
        self.assertEqual(await limiter.call(lambda: "p0", priority=0), "p0")
        low_task.cancel(); await asyncio.gather(low_task, return_exceptions=True)
        await limiter.close()
    def test_stale_lock_is_replaced_safely(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "robinhood-spy-experiment-7-26.lock"
            path.write_text(json.dumps({"pid": 999999999, "experiment_id": "OLD"}))
            lock = ExperimentInstanceLock(7, 26, td)
            record = lock.acquire("NEW")
            self.assertEqual(record["pid"], __import__("os").getpid())
            lock.release()
            self.assertTrue(path.exists())
            self.assertIn("released", path.read_text())

    def test_active_lock_fails_closed_without_unlinking(self):
        with tempfile.TemporaryDirectory() as td:
            first = ExperimentInstanceLock(7, 26, td)
            first.acquire("FIRST")
            second = ExperimentInstanceLock(7, 26, td)
            with self.assertRaisesRegex(RuntimeError, "active experiment lock"):
                second.acquire("SECOND")
            second.release()
            self.assertTrue(first.path.exists())
            first.release()

    async def test_normal_exit_cleanup_and_report_fields(self):
        with tempfile.TemporaryDirectory() as td:
            exp = Experiment(config(Path(td)), FakeBroker())
            exp.process_pid = 123
            exp.lock_status = "held"
            await exp.halt("normal completion")
            self.assertTrue(exp.cleanup_completed)
            data = exp.summary(final=True)
            self.assertEqual(data["shutdown_reason"], "normal completion")
            self.assertTrue(data["cleanup_completed"])

    async def test_exception_cleanup_and_report_fields(self):
        with tempfile.TemporaryDirectory() as td:
            exp = Experiment(config(Path(td)), FakeBroker())
            await exp.halt("exception RuntimeError")
            self.assertTrue(exp.cleanup_completed)
            self.assertEqual(exp.summary(final=True)["shutdown_reason"], "exception RuntimeError")

    def test_account_all_trade_is_normalized_for_maker_order_detection(self):
        broker = object.__new__(LighterExperimentBroker)
        broker._market_id = 26
        broker._broker = SimpleNamespace(cfg=SimpleNamespace(account_index=123))
        fill = broker._normalize_stream_trade({
            "trade_id": 456,
            "size": "0.1301",
            "price": "768.79",
            "usd_amount": "100.019579",
            "ask_id": 9001,
            "bid_id": 9002,
            "ask_account_id": 123,
            "bid_account_id": 999,
            "is_maker_ask": True,
            "timestamp": 1787214390000,
        })
        self.assertIsNotNone(fill)
        self.assertTrue(fill.maker)
        self.assertEqual(fill.order_id, 9001)
        self.assertEqual(fill.id, "456")

    def test_account_all_trade_ignores_other_accounts(self):
        broker = object.__new__(LighterExperimentBroker)
        broker._market_id = 26
        broker._broker = SimpleNamespace(cfg=SimpleNamespace(account_index=123))
        self.assertIsNone(broker._normalize_stream_trade({
            "trade_id": 456, "ask_account_id": 999, "bid_account_id": 998,
            "is_maker_ask": True,
        }))

    async def test_fills_normalizes_none_decimal_fields(self):
        broker = object.__new__(LighterExperimentBroker)
        broker._broker = SimpleNamespace(
            cfg=SimpleNamespace(account_index=123),
            recent_trades=lambda: [SimpleNamespace(
                ask_account_id=123, bid_account_id=999, is_maker=False,
                trade_id_str="null-decimal-fields", size="1", price="100",
                usd_amount=None, taker_fee=None, ask_account_pnl=None,
                ask_id=1, bid_id=2,
            )],
        )
        async def rest(operation, priority=1):
            return operation()
        broker._rest = rest
        fills = await broker.fills(None)
        self.assertEqual(fills[0].notional, Decimal("0"))
        self.assertIsNone(fills[0].fee)
        self.assertIsNone(fills[0].pnl)

    async def test_completed_cycle_uses_actual_execution_vwaps_fees_and_price_pnl(self):
        with tempfile.TemporaryDirectory() as td:
            exp = Experiment(config(Path(td)), FakeBroker())
            maker = SimpleNamespace(side="buy", price="100", qty="2", fee=Decimal("0.10"), order_id=1)
            taker = SimpleNamespace(
                side="sell", price="101", qty="2", notional="203", fee=Decimal("0.20"), order_id=2,
                execution_fills=[
                    SimpleNamespace(price="101", qty="1", notional="101", fee=Decimal("0.10")),
                    SimpleNamespace(price="102", qty="1", notional="102", fee=Decimal("0.10")),
                ],
            )
            exp._append_cycle(maker, Decimal("2"), Decimal("0"), 10, "1", "2", taker)
            with exp.history.open(newline="", encoding="utf-8") as f:
                row = next(csv.DictReader(f))
            self.assertEqual(row["maker_vwap"], "100")
            self.assertEqual(row["taker_vwap"], "101.5")
            self.assertEqual(row["matched_qty"], "2")
            self.assertEqual(row["maker_fee"], "0.10")
            self.assertEqual(row["taker_fee"], "0.20")
            self.assertEqual(row["gross_price_pnl"], "3.0")
            self.assertEqual(row["cycle_pnl"], "2.70")
            self.assertEqual(row["fees"], "0.30")
            self.assertEqual(row["estimated_spread_slippage"], "0")
            self.assertEqual(exp.stats.realized_pnl, Decimal("2.70"))
            self.assertEqual(exp.stats.fees, Decimal("0.30"))

    async def test_unknown_fee_is_explicit_and_never_reported_as_zero(self):
        with tempfile.TemporaryDirectory() as td:
            exp = Experiment(config(Path(td)), FakeBroker())
            maker = SimpleNamespace(side="sell", price="102", qty="1", fee=None, order_id=1)
            taker = SimpleNamespace(side="buy", price="101", qty="1", fee=None, order_id=2)
            exp._append_cycle(maker, Decimal("-1"), Decimal("0"), 10, "1", "2", taker)
            with exp.history.open(newline="", encoding="utf-8") as f:
                row = next(csv.DictReader(f))
            self.assertEqual(row["cycle_pnl"], "unknown")
            self.assertEqual(row["fees"], "unknown")
            self.assertEqual(row["maker_fee"], "unknown")
            self.assertEqual(row["taker_fee"], "unknown")
            self.assertEqual(row["gross_price_pnl"], "1")
            summary = exp.summary(final=True)
            self.assertIsNone(summary["realized_pnl"])
            self.assertIsNone(summary["fees"])
            self.assertEqual(summary["pnl_status"], "UNKNOWN_FEE")

    def test_account_orders_update_is_normalized_without_rest_watchdog(self):
        self.assertTrue(hasattr(LighterExperimentBroker, "start_order_stream"))
        self.assertFalse(hasattr(LighterExperimentBroker, "poll_active_maker_orders"))
        source = Path("spy_maker_taker_experiment.py").read_text()
        self.assertIn("account_orders", source)
        self.assertNotIn("_order_stream_loop", source)

    def test_hedge_protection_price_uses_fresh_executable_side(self):
        bid, ask = Decimal("100"), Decimal("101")
        self.assertEqual(hedge_protection_price("buy", bid, ask, Decimal("5")), Decimal("101.0505"))
        self.assertEqual(hedge_protection_price("sell", bid, ask, Decimal("5")), Decimal("99.9500"))

    async def test_shared_rest_limiter_retries_429_with_bounded_backoff(self):
        limiter = RESTRateLimiter(min_interval=0)
        calls = 0
        async def operation():
            nonlocal calls
            calls += 1
            if calls < 3:
                raise RuntimeError("HTTP 429 Too Many Requests")
            return "ok"
        with patch("spy_maker_taker_experiment.asyncio.sleep", new=async_noop):
            self.assertEqual(await limiter.call(operation), "ok")
        self.assertEqual(calls, 3)

    async def test_persistent_429_is_distinct_from_hedge_failure(self):
        limiter = RESTRateLimiter(min_interval=0)
        async def operation():
            raise RuntimeError("HTTP 429 Too Many Requests")
        with patch("spy_maker_taker_experiment.asyncio.sleep", new=async_noop):
            with self.assertRaises(RateLimitError):
                await limiter.call(operation)

    def test_final_summary_contains_stop_state(self):
        with tempfile.TemporaryDirectory() as td:
            exp = Experiment(config(Path(td)), FakeBroker())
            exp.halt_reason = "REST rate limit"
            exp.last_position = Decimal("0")
            exp.last_open_orders = 0
            summary = exp.summary(final=True)
            self.assertEqual(summary["experiment_id"], exp.cfg.experiment_id)
            self.assertEqual(summary["stop_reason"], "REST rate limit")
            self.assertEqual(summary["position"], Decimal("0"))
            self.assertEqual(summary["open_orders"], 0)

    def test_exchange_timestamp_conversion(self):
        self.assertEqual(timestamp_ms("1787202914171"), 1787202914171)
        self.assertEqual(timestamp_ms("1000"), 1000000)
    async def test_buy_maker_cancels_sell_before_sell_hedge(self):
        with tempfile.TemporaryDirectory() as td:
            broker = FakeBroker()
            exp = Experiment(config(Path(td)), broker)
            broker.pos = Decimal("5")
            await exp._hedge(Fill(1, True, "buy", "100", "5"), Decimal("0"), "t0")
            self.assertEqual(broker.calls[0:2], [("hedge", "sell", Decimal("5"))])
            self.assertEqual(broker.pos, Decimal("0"))

    async def test_sell_maker_hedges_buy(self):
        with tempfile.TemporaryDirectory() as td:
            broker = FakeBroker(Decimal("-5"))
            exp = Experiment(config(Path(td)), broker)
            await exp._hedge(Fill(1, True, "sell", "100", "5"), Decimal("0"), "t0")
            self.assertEqual(broker.calls[0], ("hedge", "buy", Decimal("5")))

    async def test_points_unavailable_is_explicit(self):
        with tempfile.TemporaryDirectory() as td:
            exp = Experiment(config(Path(td)), FakeBroker(), UnavailablePoints())
            await exp.start(); await exp.write_summary()
            data = (Path(td) / "latest_summary.json").read_text()
            self.assertIn("POINTS_DATA_UNAVAILABLE", data)

    async def test_turnover_target_halts(self):
        with tempfile.TemporaryDirectory() as td:
            cfg = ExperimentConfig(data_dir=Path(td), target_taker_turnover=Decimal("1"))
            exp = Experiment(cfg, FakeBroker())
            exp.stats.taker_volume = Decimal("1")
            await exp.tick()
            self.assertTrue(exp.halted)
            self.assertIn("target taker turnover", exp.halt_reason)

    async def test_opposite_order_is_cancelled_before_hedge(self):
        with tempfile.TemporaryDirectory() as td:
            broker = FakeBroker(Decimal("5"))
            exp = Experiment(config(Path(td)), broker)
            broker.orders = [SimpleNamespace(side="sell", order_index=42, client_order_index=99)]
            exp.current_order_ids = {"sell": 42}
            async def cancel_status(_order_id):
                broker.calls.append(("cancel", "sell"))
                broker.orders.clear()
                return {"status": "canceled"}
            broker.cancel_order_confirmed = cancel_status
            await exp._hedge(Fill(1, True), Decimal("0"), "t0")
            self.assertEqual([x[0] for x in broker.calls][:2], ["cancel", "hedge"])

    async def test_full_maker_hedges_before_sibling_terminal_confirmation(self):
        with tempfile.TemporaryDirectory() as td:
            class OrderedBroker(FakeBroker):
                async def cancel_order(self, order_id):
                    self.calls.append(("cancel_request", order_id))
                    return {"order_id": order_id}
                async def confirm_cancel_order(self, order_id):
                    self.calls.append(("cancel_confirm", order_id))
                    return {"order_id": order_id, "status": "canceled"}
            broker = OrderedBroker(Decimal("1"))
            exp = Experiment(config(Path(td)), broker)
            exp.current_order_ids = {"buy": 7, "sell": 8}
            exp.current_client_order_ids = {"buy": "101", "sell": "102"}
            exp.maker_side_by_client_id = {"101": "buy", "102": "sell"}
            exp.terminal_order_status[7] = "filled"
            maker = SimpleNamespace(order_id=7, side="buy", qty="1", price="100", timestamp="1000000000000", maker=True)
            await exp._hedge(maker, Decimal("0"), "1000000000000")
            names = [call[0] for call in broker.calls]
            self.assertLess(names.index("cancel_request"), names.index("hedge"))
            self.assertLess(names.index("hedge"), names.index("cancel_confirm"))
            self.assertNotIn(("cancel_request", 7), broker.calls)

    async def test_partial_maker_residual_is_confirmed_before_hedge(self):
        with tempfile.TemporaryDirectory() as td:
            class OrderedBroker(FakeBroker):
                async def cancel_order(self, order_id):
                    self.calls.append(("cancel_request", order_id))
                    return {"order_id": order_id}
                async def confirm_cancel_order(self, order_id):
                    self.calls.append(("cancel_confirm", order_id))
                    return {"order_id": order_id, "status": "canceled"}
                async def cancel_order_confirmed(self, order_id):
                    self.calls.append(("cancel_confirm", order_id))
                    return {"order_id": order_id, "status": "canceled"}
            broker = OrderedBroker(Decimal("0.4"))
            exp = Experiment(config(Path(td)), broker)
            exp.current_order_ids = {"buy": 7, "sell": 8}
            exp.current_client_order_ids = {"buy": "101", "sell": "102"}
            exp.maker_side_by_client_id = {"101": "buy", "102": "sell"}
            exp.maker_qty_by_client_id = {"101": Decimal("1"), "102": Decimal("1")}
            exp.maker_handled_cumulative[7] = Decimal("0.4")
            maker = SimpleNamespace(order_id=7, side="buy", qty="0.4", price="100", timestamp="1000000000000", maker=True)
            await exp._hedge(maker, Decimal("0"), "1000000000000")
            names = [call[0] for call in broker.calls]
            # Own partially-filled maker must be terminal before hedge, while
            # sibling terminal confirmation remains post-submit.
            own_confirm = broker.calls.index(("cancel_confirm", 7))
            hedge = names.index("hedge")
            self.assertLess(own_confirm, hedge)

    async def test_order_specific_cancel_accepts_canceled_post_only_terminal(self):
        """A Lighter post-only cancel terminal must not be treated as timeout."""
        class RawBroker:
            def __init__(self):
                self.cfg = SimpleNamespace(account_index=4)
                self.auth = "test"
                self.cancelled = False
                self.order_api = SimpleNamespace(account_inactive_orders=self.inactive)

            async def active_orders(self):
                return [] if self.cancelled else [SimpleNamespace(order_index=42)]

            async def cancel(self, _order):
                self.cancelled = True

            async def inactive(self, **_kwargs):
                return SimpleNamespace(orders=[SimpleNamespace(order_index=42, status="canceled-post-only")])

            def ensure_auth(self):
                return None

        adapter = object.__new__(LighterExperimentBroker)
        adapter._broker = RawBroker()
        adapter._market_id = 26

        async def immediate(operation, *, priority=2):
            return await operation()

        adapter._rest = immediate
        result = await adapter.cancel_order_confirmed(42)
        self.assertEqual(result["status"], "canceled-post-only")

    async def test_order_specific_cancel_not_active_is_nonblocking(self):
        """A missing order is not executable; position reconciliation decides exposure."""
        class RawBroker:
            def __init__(self):
                self.cfg = SimpleNamespace(account_index=4)
                self.auth = "test"
                self.order_api = SimpleNamespace(account_inactive_orders=self.inactive)

            async def active_orders(self):
                return []

            async def inactive(self, **_kwargs):
                return SimpleNamespace(orders=[])

            def ensure_auth(self):
                return None

        adapter = object.__new__(LighterExperimentBroker)
        adapter._broker = RawBroker()
        adapter._market_id = 26

        async def immediate(operation, *, priority=2):
            return await operation()

        adapter._rest = immediate
        result = await adapter.cancel_order_confirmed(42)
        self.assertEqual(result["status"], "not_active")

    async def test_manual_points_are_persisted_and_calculated(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "latest_summary.json").write_text(json.dumps({
                "experiment_id": "SPY_MT_001", "taker_volume": 10000,
                "maker_volume": 10000, "realized_pnl": -0.42,
                "failed_hedges": 0, "cycles": 21, "max_net_position": 500,
                "configured_max_net_position": 500, "order_notional": 500,
                "target_taker_turnover": 10000, "avg_hedge_latency_ms": 840,
            }))
            (root / "history.csv").write_text("experiment_id,timestamp\nSPY_MT_001,old\n")
            subprocess.run([sys.executable, "tools/experiment_report.py", "--data-dir", td,
                            "--points-before", "0.0036", "--points-after", "0.0436",
                            "--rank-before", "x", "--rank-after", "y"], check=True,
                            stdout=subprocess.DEVNULL)
            summary = json.loads((root / "latest_summary.json").read_text())
            evaluation = json.loads((root / "evaluation.json").read_text())
            self.assertEqual(summary["points_source"], "manual")
            self.assertAlmostEqual(summary["points_per_1m_taker"], 4.0)
            self.assertAlmostEqual(summary["cost_per_point"], 10.5)
            self.assertEqual(evaluation["points_source"], "manual")

    async def test_hedge_latency_uses_exchange_fill_timestamps(self):
        with tempfile.TemporaryDirectory() as td, patch("spy_maker_taker_experiment.asyncio.sleep", new=async_noop):
            fill = Fill(900, False, "sell", "101", "5", "1000000000840")
            broker = ReconcileBroker([Decimal("5"), Decimal("0")], [[fill]])
            exp = Experiment(config(Path(td)), broker)
            await exp._hedge(Fill(1, True, "buy", "100", "5", "1000000000000"), Decimal("0"), "1000000000000")
            self.assertEqual(exp.stats.hedge_latencies_ms, [840])
            self.assertEqual(exp.stats.cycles, 1)
            event_text = (Path(td) / "events.jsonl").read_text()
            self.assertIn("hedge_submit_response", event_text)
            self.assertIn("position_snapshots", event_text)

    async def test_delayed_fill_timelines_are_reconciled_without_sleeping(self):
        for delay in (1, 3, 5):
            with self.subTest(delay=delay), tempfile.TemporaryDirectory() as td, patch("spy_maker_taker_experiment.asyncio.sleep", new=async_noop):
                fill = Fill(1000 + delay, False, "sell", "101", "5", str(1000000000000 + delay * 1000))
                visible = [[] for _ in range(delay)] + [[fill]]
                broker = ReconcileBroker([Decimal("5")] + [Decimal("5")] * delay + [Decimal("0")], visible)
                exp = Experiment(config(Path(td)), broker)
                await exp._hedge(Fill(2, True, "buy", "100", "5", "1000000000000"), Decimal("0"), "1000000000000")
                self.assertEqual(exp.stats.hedge_latencies_ms, [delay * 1000])

    async def test_ten_second_fill_hits_latency_stop_after_reconciliation(self):
        with tempfile.TemporaryDirectory() as td, patch("spy_maker_taker_experiment.asyncio.sleep", new=async_noop):
            fill = Fill(1010, False, "sell", "101", "5", "1000010000000")
            visible = [[] for _ in range(10)] + [[fill]]
            broker = ReconcileBroker([Decimal("5")] + [Decimal("5")] * 10 + [Decimal("0")], visible)
            exp = Experiment(config(Path(td)), broker)
            await exp._hedge(Fill(3, True, "buy", "100", "5", "1000000000000"), Decimal("0"), "1000000000000")
            self.assertEqual(broker.hedges[0][1], Decimal("5"))
            self.assertEqual(exp.stats.latency_violations, 1)

    async def test_partial_fill_rehedges_actual_remaining_position(self):
        with tempfile.TemporaryDirectory() as td, patch("spy_maker_taker_experiment.asyncio.sleep", new=async_noop):
            first = Fill(1101, False, "sell", "101", "2", "1000000000100")
            second = Fill(1102, False, "sell", "101", "3", "1000000000200")
            broker = ReconcileBroker([Decimal("5"), Decimal("3"), Decimal("0")], [[first], [first, second]], ["filled", "filled"])
            exp = Experiment(config(Path(td)), broker)
            await exp._hedge(Fill(4, True, "buy", "100", "5", "1000000000000"), Decimal("0"), "1000000000000")
            self.assertEqual([x[1] for x in broker.hedges], [Decimal("5"), Decimal("3")])
            self.assertEqual(exp.stats.cycles, 1)

    async def test_partial_fill_retries_even_without_order_id(self):
        with tempfile.TemporaryDirectory() as td, patch("spy_maker_taker_experiment.asyncio.sleep", new=async_noop):
            first = SimpleNamespace(**Fill(1201, False, "sell", "101", "2", "4102444800100").__dict__, client_order_id="201")
            second = SimpleNamespace(**Fill(1202, False, "sell", "101", "3", "4102444800200").__dict__, client_order_id="202")
            broker = ReconcileBroker([Decimal("5"), Decimal("3"), Decimal("3"), Decimal("0")], [[first], [first], [first, second]], ["unknown", "unknown"], include_order_id=False)
            exp = Experiment(config(Path(td)), broker)
            await exp._hedge(Fill(6, True, "buy", "100", "5", "1000000000000"), Decimal("0"), "1000000000000")
            self.assertEqual([x[1] for x in broker.hedges], [Decimal("5"), Decimal("3")])
            self.assertEqual(exp.stats.cycles, 1)

    async def test_flat_position_without_visible_idless_hedge_fill_does_not_retry_or_fail(self):
        with tempfile.TemporaryDirectory() as td, patch("spy_maker_taker_experiment.asyncio.sleep", new=async_noop):
            broker = ReconcileBroker([Decimal("5"), Decimal("0")], [[]], ["unknown"], include_order_id=False)
            cfg = replace(config(Path(td)), dry_run=False)
            exp = Experiment(cfg, broker)
            await exp._hedge(Fill(1203, True, "buy", "100", "5", "1000000000000"), Decimal("0"), "1000000000000")
            self.assertEqual(len(broker.hedges), 1)
            self.assertEqual(exp.stats.reconciliation_failures, 0)
            self.assertIn("flat_by_position_pending_fill_attribution", (Path(td) / "events.jsonl").read_text())

    async def test_late_idless_hedge_fill_closes_the_original_cycle_once(self):
        """Position-flat id-less hedges complete stats only on their own late trade."""
        with tempfile.TemporaryDirectory() as td, patch("spy_maker_taker_experiment.asyncio.sleep", new=async_noop):
            broker = ReconcileBroker([Decimal("5"), Decimal("0")], [[]], ["unknown"], include_order_id=False)
            cfg = replace(config(Path(td)), dry_run=False)
            exp = Experiment(cfg, broker)
            maker_ts = "2026-08-20T13:00:00+00:00"
            await exp._hedge(Fill(1204, True, "buy", "100", "5", maker_ts), Decimal("0"), maker_ts)
            self.assertEqual(exp.stats.cycles, 0)
            self.assertEqual(len(exp.pending_hedge_attributions), 1)
            pending = exp.pending_hedge_attributions[0]
            fill_ts = (datetime.fromisoformat(pending.hedge_submit_timestamp) + timedelta(seconds=1)).isoformat()
            foreign = SimpleNamespace(fill_id="foreign", order_id=999, is_maker=False, quantity=Decimal("5"),
                                      price=Decimal("101"), side="SELL", client_order_id="other", timestamp=fill_ts)
            await exp._handle_fill_event(foreign, "account_all_trade_ws")
            self.assertEqual(exp.stats.taker_volume, Decimal("0"))
            self.assertEqual(exp.stats.cycles, 0)
            own = SimpleNamespace(fill_id="own", order_id=0, is_maker=False, quantity=Decimal("5"),
                                  price=Decimal("101"), side="SELL",
                                  client_order_id=next(iter(pending.client_order_ids)), timestamp=fill_ts)
            await exp._handle_fill_event(own, "account_all_trade_ws")
            self.assertEqual(exp.stats.taker_volume, Decimal("505"))
            self.assertEqual(exp.stats.cycles, 1)
            self.assertEqual(len(exp.pending_hedge_attributions), 0)
            self.assertEqual(len(exp.stats.hedge_latencies_ms), 1)
            self.assertEqual(exp.stats.latency_violations, 1)
            self.assertEqual(exp.stats.execution_failures, 0)
            self.assertFalse(exp.halted)
            self.assertIn("quote_pause_after_latency_violation", (Path(td) / "events.jsonl").read_text())
            self.assertFalse(exp.cleanup_completed)

    async def test_latency_warning_below_hard_limit_keeps_experiment_running(self):
        with tempfile.TemporaryDirectory() as td:
            cfg = replace(config(Path(td)), dry_run=False,
                          hedge_latency_warning_ms=1, hedge_latency_limit_ms=999999999)
            exp = Experiment(cfg, FakeBroker())
            maker_ts = "2026-08-20T13:00:00+00:00"
            submit_ts = "2026-08-20T13:00:01+00:00"
            exp._queue_pending_hedge_attribution(
                SimpleNamespace(order_id=7, side="buy", qty="1", price="100"), Decimal("1"),
                maker_ts, submit_ts, "hedge-label", "123", "sell", Decimal("1"), submit_ts,
            )
            fill = SimpleNamespace(fill_id="warning-only", order_id=8, is_maker=False, quantity=Decimal("1"),
                                   price=Decimal("101"), side="SELL", client_order_id="123",
                                   timestamp="2026-08-20T13:00:02+00:00")
            await exp._handle_fill_event(fill, "account_all_trade_ws")
            self.assertEqual(exp.stats.latency_warnings, 1)
            self.assertEqual(exp.stats.latency_violations, 0)
            self.assertFalse(exp.halted)
            self.assertEqual(exp.stats.cycles, 1)

    async def test_rejected_hedge_is_not_misclassified_as_cleanup(self):
        with tempfile.TemporaryDirectory() as td, patch("spy_maker_taker_experiment.asyncio.sleep", new=async_noop):
            broker = ReconcileBroker([Decimal("5"), Decimal("5")], [[]], ["rejected"])
            exp = Experiment(config(Path(td)), broker)
            with self.assertRaisesRegex(RuntimeError, "rejected"):
                await exp._hedge(Fill(5, True, "buy", "100", "5", "1000000000000"), Decimal("0"), "1000000000000")
            text = (Path(td) / "events.jsonl").read_text()
            self.assertNotIn('"event": "hedge_confirmed"', text)
            self.assertNotIn('"purpose": "cleanup"', text)


async def async_noop(_delay):
    return None


if __name__ == "__main__":
    unittest.main()
