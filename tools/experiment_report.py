#!/usr/bin/env python3
"""Print the latest completed SPY maker/taker experiment."""
import argparse, csv, json, shutil
from decimal import Decimal
from datetime import datetime, timezone
from pathlib import Path

def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--data-dir", default="/var/log/robinhood-spy-experiment")
    p.add_argument("--points-before")
    p.add_argument("--points-after")
    p.add_argument("--rank-before")
    p.add_argument("--rank-after")
    args = p.parse_args()
    root = Path(args.data_dir)
    summary = json.loads((root / "latest_summary.json").read_text())
    evaluation = json.loads((root / "evaluation.json").read_text()) if (root / "evaluation.json").exists() else {}
    if args.points_before is not None or args.points_after is not None:
        if args.points_before is None or args.points_after is None:
            raise SystemExit("--points-before and --points-after must be supplied together")
        before, after = Decimal(args.points_before), Decimal(args.points_after)
        delta = after - before
        taker = Decimal(str(summary["taker_volume"]))
        pnl_raw = summary.get("realized_pnl")
        pnl = None if pnl_raw is None else Decimal(str(pnl_raw))
        ppm = None if taker == 0 else delta / taker * Decimal("1000000")
        cpp = None if delta == 0 or pnl is None else max(Decimal("0"), -pnl) / delta
        summary.update({"points_before": float(before), "points_after": float(after), "points_total": float(after), "points_delta": float(delta), "rank_before": args.rank_before, "rank_after": args.rank_after, "points_source": "manual", "points_per_1m_taker": float(ppm) if ppm is not None else None, "cost_per_point": float(cpp) if cpp is not None else None})
        summary["points_status"] = "OK"
        summary_path = root / "latest_summary.json"
        summary_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
        evaluation_name = "GOOD" if pnl is not None and ppm is not None and ppm >= 2 and summary["failed_hedges"] / max(1, summary["cycles"]) < .01 and Decimal(str(summary["max_net_position"])) <= Decimal(str(summary.get("configured_max_net_position", summary["order_notional"]))) and pnl / max(taker, Decimal("1")) > Decimal("-0.0001") else "NEUTRAL" if ppm is not None and ppm >= .5 else "BAD"
        evaluation = {"experiment_id": summary["experiment_id"], "evaluation": evaluation_name, "points_source": "manual", "points_before": float(before), "points_after": float(after), "points_delta": float(delta), "rank_before": args.rank_before, "rank_after": args.rank_after, "points_per_1m_taker": float(ppm) if ppm is not None else None, "cost_per_point": float(cpp) if cpp is not None else None, "summary": summary}
        (root / "evaluation.json").write_text(json.dumps(evaluation, indent=2, default=str), encoding="utf-8")
        next_notional = Decimal(str(summary["order_notional"])) * (Decimal("1.25") if evaluation_name == "GOOD" else Decimal("1") if evaluation_name == "NEUTRAL" else Decimal(".5"))
        (root / "next_experiment_recommendation.json").write_text(json.dumps({"from_experiment_id": summary["experiment_id"], "evaluation": evaluation_name, "recommended": {"order_notional": next_notional, "quote_refresh_interval": summary.get("quote_refresh_interval"), "target_taker_turnover": summary.get("target_taker_turnover")}, "only_approved_fields": ["order_notional", "quote_refresh_interval", "target_taker_turnover"]}, indent=2, default=str), encoding="utf-8")
        history = root / "history.csv"
        if history.exists():
            with history.open(newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f)); fields = list(rows[0].keys()) if rows else []
            extra = ["event_type", "points_source", "points_before", "points_after", "points_delta", "rank_before", "rank_after", "points_per_1m_taker", "cost_per_point"]
            missing = [x for x in extra if x not in fields]
            if missing:
                backup = history.with_name(history.name + ".pre-manual-points-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
                shutil.copy2(history, backup)
                fields += missing
                with history.open("w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=fields); writer.writeheader(); writer.writerows(rows)
            row = {k: "" for k in fields}; row.update({k: v for k, v in {"experiment_id": summary["experiment_id"], "strategy_version": "", "timestamp": datetime.now(timezone.utc).isoformat(), "event_type": "points_snapshot", "points_source": "manual", "points_before": str(before), "points_after": str(after), "points_delta": str(delta), "rank_before": args.rank_before or "", "rank_after": args.rank_after or "", "points_per_1m_taker": str(ppm) if ppm is not None else "", "cost_per_point": str(cpp) if cpp is not None else ""}.items() if k in fields})
            with history.open("a", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=fields).writerow(row)
    print(f"Experiment: {summary['experiment_id']}\n")
    print(f"Points: {summary.get('points_total')}  Delta: {summary.get('points_delta')}")
    if summary.get("points_source"):
        print(f"Points source: {summary['points_source']}")
    print(f"Taker volume: ${summary['taker_volume']:,.2f}")
    print(f"Maker volume: ${summary['maker_volume']:,.2f}")
    print(f"Cycles: {summary['cycles']}\n")
    pnl = summary.get("realized_pnl")
    fees = summary.get("fees")
    print(f"PnL: {'UNKNOWN (fee data unavailable)' if pnl is None else f'${pnl:,.2f}'}")
    print(f"Fees: {'UNKNOWN' if fees is None else f'${fees:,.2f}'}")
    print(f"Points / $1M taker: {summary.get('points_per_1m_taker')}")
    print(f"Cost / point: {summary.get('cost_per_point')}")
    print(f"Avg hedge latency: {summary['avg_hedge_latency_ms']}ms")
    print(f"Failed hedges: {summary['failed_hedges']}")
    print(f"Max position qty: {summary.get('max_net_position_qty', summary['max_net_position'])}")
    print(f"Max position USD: ${summary.get('max_net_position_usd', summary.get('max_net_position', 0))}")
    if evaluation:
        print(f"\nEvaluation: {evaluation.get('evaluation')}")

if __name__ == "__main__":
    main()
