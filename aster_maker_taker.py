#!/usr/bin/env python3
"""Safety-capped Aster V3 maker-fill -> taker-flatten experiment.

Dry-run is mandatory by default. Live mode only runs one bounded cycle and
requires an explicit confirmation string. Credentials are read from the
environment and are never logged.
"""

from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any

from eth_account import Account
from eth_account.messages import encode_typed_data


ZERO = Decimal("0")
DOMAIN = {
    "name": "AsterSignTransaction",
    "version": "1",
    "chainId": 1666,
    "verifyingContract": "0x0000000000000000000000000000000000000000",
}
TYPES = {
    "EIP712Domain": [
        {"name": "name", "type": "string"},
        {"name": "version", "type": "string"},
        {"name": "chainId", "type": "uint256"},
        {"name": "verifyingContract", "type": "address"},
    ],
    "Message": [{"name": "msg", "type": "string"}],
}


def dec(value: Any) -> Decimal:
    return Decimal(str(value))


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    return default if raw is None else raw.strip().lower() in {"1", "true", "yes"}


@dataclass(frozen=True)
class Config:
    base_url: str
    user: str
    signer: str
    private_key: str
    symbol: str
    notional: Decimal
    max_net: Decimal
    max_slippage_bps: Decimal
    max_taker_fee_bps: Decimal
    maker_timeout_seconds: int
    poll_seconds: float
    dry_run: bool
    live_confirm: str
    maker_side: str
    data_dir: Path

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            base_url=os.getenv("ASTER_BASE_URL", "https://fapi.asterdex.com").rstrip("/"),
            user=os.getenv("ASTER_ACCOUNT_ADDRESS", "").strip(),
            signer=(os.getenv("ASTER_AGENT_ADDRESS") or os.getenv("ASTER_API_KEY", "")).strip(),
            private_key=(os.getenv("ASTER_AGENT_PRIVATE_KEY") or os.getenv("ASTER_API_SECRET", "")).strip(),
            symbol=os.getenv("ASTER_SYMBOL", "XAUUSD1").upper(),
            notional=dec(os.getenv("ORDER_NOTIONAL_USD", "100")),
            max_net=dec(os.getenv("MAX_NET_POSITION_USD", "150")),
            max_slippage_bps=dec(os.getenv("MAX_SLIPPAGE_BPS", "10")),
            max_taker_fee_bps=dec(os.getenv("MAX_TAKER_FEE_BPS", "1")),
            maker_timeout_seconds=int(os.getenv("MAKER_TIMEOUT_SECONDS", "120")),
            poll_seconds=float(os.getenv("POLL_SECONDS", "0.5")),
            dry_run=env_bool("DRY_RUN", True),
            live_confirm=os.getenv("LIVE_CONFIRM", ""),
            maker_side=os.getenv("MAKER_SIDE", "BUY").upper(),
            data_dir=Path(os.getenv("ASTER_DATA_DIR", "/var/log/aster-maker-taker")),
        )

    def validate(self) -> None:
        if not (self.user.startswith("0x") and len(self.user) == 42):
            raise ValueError("ASTER_ACCOUNT_ADDRESS must be a 0x wallet address")
        if not (self.signer.startswith("0x") and len(self.signer) == 42):
            raise ValueError("API Wallet address must be a 0x address")
        key = self.private_key[2:] if self.private_key.startswith("0x") else self.private_key
        if len(key) != 64:
            raise ValueError("API Wallet private key must be 32 bytes")
        derived = Account.from_key(self.private_key).address
        if derived.lower() != self.signer.lower():
            raise ValueError("API Wallet private key does not match API Wallet address")
        if self.symbol not in {"SPCXUSD1", "CLUSD1", "XAUUSD1", "SNDKUSD1", "SKHYNIXUSD1", "MUUSD1"}:
            raise ValueError("symbol is not eligible for USD1 RWA Boost Phase 1")
        if self.maker_side not in {"BUY", "SELL"}:
            raise ValueError("MAKER_SIDE must be BUY or SELL")
        if self.notional <= ZERO or self.notional > self.max_net:
            raise ValueError("order notional must be positive and <= max net position")


class AsterV3:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._last_nonce = 0

    def _nonce(self) -> str:
        current = time.time_ns() // 1000
        self._last_nonce = max(current, self._last_nonce + 1)
        return str(self._last_nonce)

    def _request(self, method: str, path: str, params: dict[str, Any] | None = None) -> Any:
        # Aster's official V3 example signs parameters in insertion order,
        # followed by nonce and signer. The API wallet is already bound to the
        # main user account, so ordinary TRADE/USER_DATA calls do not include
        # user in the signed payload.
        values = {k: str(v) for k, v in (params or {}).items()}
        values.update({"nonce": self._nonce(), "signer": self.cfg.signer})
        message = urllib.parse.urlencode(values)
        typed = {"types": TYPES, "primaryType": "Message", "domain": DOMAIN, "message": {"msg": message}}
        signed = Account.sign_message(encode_typed_data(full_message=typed), self.cfg.private_key)
        values["signature"] = signed.signature.hex()
        url = self.cfg.base_url + path
        data = None
        if method == "GET":
            url += "?" + urllib.parse.urlencode(values)
        else:
            data = urllib.parse.urlencode(values).encode()
        req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/x-www-form-urlencoded"})
        try:
            with urllib.request.urlopen(req, timeout=15) as response:
                return json.loads(response.read())
        except urllib.error.HTTPError as exc:
            body = exc.read().decode(errors="replace")
            raise RuntimeError(f"Aster HTTP {exc.code}: {body[:500]}") from exc

    def public(self, path: str, params: dict[str, Any] | None = None) -> Any:
        query = "?" + urllib.parse.urlencode(params) if params else ""
        with urllib.request.urlopen(self.cfg.base_url + path + query, timeout=15) as response:
            return json.loads(response.read())

    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        return self._request("GET", path, params)

    def post(self, path: str, params: dict[str, Any]) -> Any:
        return self._request("POST", path, params)

    def delete(self, path: str, params: dict[str, Any]) -> Any:
        return self._request("DELETE", path, params)


def symbol_rules(client: AsterV3, symbol: str) -> tuple[Decimal, Decimal]:
    info = client.public("/fapi/v3/exchangeInfo")
    row = next(item for item in info["symbols"] if item["symbol"] == symbol)
    filters = {item["filterType"]: item for item in row["filters"]}
    return dec(filters["PRICE_FILTER"]["tickSize"]), dec(filters["LOT_SIZE"]["stepSize"])


def floor_step(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def nonzero_positions(rows: list[dict[str, Any]], symbol: str | None = None) -> list[dict[str, Any]]:
    return [r for r in rows if dec(r.get("positionAmt", "0")) != ZERO and (symbol is None or r.get("symbol") == symbol)]


def preflight(cfg: Config, client: AsterV3) -> dict[str, Any]:
    cfg.validate()
    mode = client.get("/fapi/v3/multiAssetsMargin")
    position_mode = client.get("/fapi/v3/positionSide/dual")
    balances = client.get("/fapi/v3/balance")
    account = client.get("/fapi/v3/account")
    positions = client.get("/fapi/v3/positionRisk", {"symbol": cfg.symbol})
    orders = client.get("/fapi/v3/openOrders", {"symbol": cfg.symbol})
    commission = client.get("/fapi/v3/commissionRate", {"symbol": cfg.symbol})
    book = client.public("/fapi/v3/depth", {"symbol": cfg.symbol, "limit": 5})
    usd1 = next((x for x in balances if x.get("asset") == "USD1"), {})
    result = {
        "authenticated": True,
        "user_suffix": cfg.user[-6:],
        "signer_suffix": cfg.signer[-6:],
        "symbol": cfg.symbol,
        "single_asset_mode": not bool(mode.get("multiAssetsMargin")),
        "one_way_mode": not bool(position_mode.get("dualSidePosition")),
        "usd1_balance": usd1.get("balance"),
        "usd1_available": usd1.get("availableBalance"),
        "can_trade": bool(account.get("canTrade")),
        "maker_fee": commission.get("makerCommissionRate"),
        "taker_fee": commission.get("takerCommissionRate"),
        "open_orders": len(orders),
        "nonzero_symbol_positions": len(nonzero_positions(positions, cfg.symbol)),
        "best_bid": book["bids"][0][0],
        "best_ask": book["asks"][0][0],
        "dry_run": cfg.dry_run,
    }
    return result


def live_once(cfg: Config, client: AsterV3) -> dict[str, Any]:
    if cfg.dry_run or cfg.live_confirm != "ASTER_LIVE_ONCE_100_USD":
        raise RuntimeError("live execution locked; keep DRY_RUN=true until explicitly approved")
    status = preflight(cfg, client)
    if not status["single_asset_mode"] or not status["one_way_mode"]:
        raise RuntimeError("requires Single-Asset Mode and One-way Mode")
    if not status["can_trade"]:
        raise RuntimeError("Aster account is not permitted to trade")
    if status["open_orders"] or status["nonzero_symbol_positions"]:
        raise RuntimeError("requires zero open XAU orders and zero XAU position")
    if dec(status["usd1_available"] or "0") < cfg.notional:
        raise RuntimeError("insufficient available USD1 for the hard-capped first cycle")
    if dec(status["taker_fee"] or "0") * Decimal("10000") > cfg.max_taker_fee_bps:
        raise RuntimeError("account taker fee exceeds MAX_TAKER_FEE_BPS")
    if cfg.notional != Decimal("100"):
        raise RuntimeError("first live cycle is hard-capped at exactly 100 USD")
    tick, step = symbol_rules(client, cfg.symbol)
    book = client.public("/fapi/v3/depth", {"symbol": cfg.symbol, "limit": 5})
    maker_price = dec(book["bids"][0][0] if cfg.maker_side == "BUY" else book["asks"][0][0])
    maker_price = floor_step(maker_price, tick)
    qty = floor_step(cfg.notional / maker_price, step)
    if qty <= ZERO:
        raise RuntimeError("quantity rounds to zero")
    client_id = f"ASTER_MT_{int(time.time())}"
    order = client.post("/fapi/v3/order", {
        "symbol": cfg.symbol, "side": cfg.maker_side, "type": "LIMIT", "timeInForce": "GTX",
        "quantity": format(qty, "f"), "price": format(maker_price, "f"),
        "newClientOrderId": client_id, "newOrderRespType": "RESULT",
    })
    order_id = order["orderId"]
    deadline = time.monotonic() + cfg.maker_timeout_seconds
    current = order
    while time.monotonic() < deadline:
        current = client.get("/fapi/v3/order", {"symbol": cfg.symbol, "orderId": order_id})
        if current.get("status") in {"FILLED", "CANCELED", "EXPIRED", "REJECTED"}:
            break
        time.sleep(cfg.poll_seconds)
    if current.get("status") not in {"FILLED", "CANCELED", "EXPIRED", "REJECTED"}:
        current = client.delete("/fapi/v3/order", {"symbol": cfg.symbol, "orderId": order_id})
    filled = dec(current.get("executedQty", "0"))
    hedge = None
    if filled > ZERO:
        hedge_side = "SELL" if cfg.maker_side == "BUY" else "BUY"
        hedge = client.post("/fapi/v3/order", {
            "symbol": cfg.symbol, "side": hedge_side, "type": "MARKET",
            "quantity": format(filled, "f"), "reduceOnly": "true", "newOrderRespType": "RESULT",
            "newClientOrderId": client_id + "_H",
        })
    final_positions = client.get("/fapi/v3/positionRisk", {"symbol": cfg.symbol})
    remaining = nonzero_positions(final_positions, cfg.symbol)
    if remaining:
        raise RuntimeError("CRITICAL: live cycle did not finish flat")
    return {"symbol": cfg.symbol, "maker_order_id": order_id, "maker_status": current.get("status"), "maker_filled_qty": str(filled), "hedge_order_id": hedge.get("orderId") if hedge else None, "final_flat": True}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preflight", action="store_true")
    parser.add_argument("--live-once", action="store_true")
    args = parser.parse_args()
    cfg = Config.from_env()
    client = AsterV3(cfg)
    result = live_once(cfg, client) if args.live_once else preflight(cfg, client)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
