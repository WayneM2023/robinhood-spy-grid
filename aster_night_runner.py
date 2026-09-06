#!/usr/bin/env python3
"""Bounded overnight runner for the Aster maker->taker experiment."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from aster_maker_taker import AsterV3, Config, dec, live_once


def emit(log_path: Path, event: dict) -> None:
    event = {"ts": datetime.now(timezone.utc).isoformat(), **event}
    line = json.dumps(event, separators=(",", ":"))
    print(line, flush=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(line + "\n")


def available_balance(client: AsterV3) -> Decimal:
    rows = client.get("/fapi/v3/balance")
    row = next(item for item in rows if item.get("asset") == "USD1")
    return dec(row["availableBalance"])


def spread_bps(client: AsterV3, symbol: str) -> Decimal:
    book = client.public("/fapi/v3/depth", {"symbol": symbol, "limit": 5})
    bid, ask = dec(book["bids"][0][0]), dec(book["asks"][0][0])
    return (ask - bid) / ((ask + bid) / 2) * Decimal("10000")


def main() -> None:
    cfg = Config.from_env()
    cfg.validate()
    client = AsterV3(cfg)
    max_cycles = int(os.getenv("NIGHT_MAX_CYCLES", "3"))
    interval = int(os.getenv("NIGHT_INTERVAL_SECONDS", "1800"))
    max_spread = dec(os.getenv("NIGHT_MAX_SPREAD_BPS", "0.20"))
    max_loss = dec(os.getenv("NIGHT_MAX_LOSS_USD", "0.10"))
    stop_epoch = int(os.getenv("NIGHT_STOP_EPOCH", "0"))
    log_path = Path(os.getenv("NIGHT_LOG", "/var/log/aster-maker-taker/night.jsonl"))
    start_balance = available_balance(client)
    emit(log_path, {"event": "start", "symbol": cfg.symbol, "notional": str(cfg.notional),
                    "start_balance": str(start_balance), "max_cycles": max_cycles})
    completed = 0
    while completed < max_cycles:
        if stop_epoch and time.time() >= stop_epoch:
            emit(log_path, {"event": "stop", "reason": "time_limit"})
            break
        current_balance = available_balance(client)
        loss = max(start_balance - current_balance, Decimal("0"))
        if loss >= max_loss:
            emit(log_path, {"event": "stop", "reason": "loss_limit", "loss": str(loss)})
            break
        spread = spread_bps(client, cfg.symbol)
        if spread > max_spread:
            emit(log_path, {"event": "skip", "reason": "spread", "spread_bps": str(spread)})
        else:
            result = live_once(cfg, client)
            after = available_balance(client)
            completed += 1
            emit(log_path, {"event": "cycle", "number": completed, "spread_bps": str(spread),
                            "balance": str(after), "loss_from_start": str(max(start_balance-after, Decimal("0"))),
                            "result": result})
        if completed < max_cycles:
            time.sleep(interval)
    emit(log_path, {"event": "finish", "completed": completed,
                    "final_balance": str(available_balance(client))})


if __name__ == "__main__":
    main()
