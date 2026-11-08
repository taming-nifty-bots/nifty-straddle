"""Retained executor-job name; no broker-order path exists in this paper pilot."""
import time
import traceback
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ironfly import (END_TIME, ENTRY_TIME, STRATEGY_VERSION, build_candidate,
                     checkpoint, clients, edge, evaluate_trade, notify, open_trade,
                     sync_orders, trade_id, util)


def process_checkpoint(conn, strategies, orders, cutoff, slack, entry_checked=False):
    active = list(strategies.find({"strategy_state": "active"}))
    if len(active) > 1:
        raise ValueError("Multiple active records; manual review required")
    for trade in active:
        if trade.get("strategy_version") != STRATEGY_VERSION or trade.get("simulated") is not True:
            raise ValueError("An active legacy/non-paper trade must be reconciled before this pilot")
        if trade.get("last_check") != cutoff.isoformat():
            fields = evaluate_trade(conn, trade, cutoff)
            result = strategies.update_one(
                {"_id": trade["_id"], "strategy_state": "active",
                 "last_check": {"$ne": cutoff.isoformat()}}, {"$set": fields})
            if result.modified_count and fields.get("strategy_state") == "closed":
                notify(slack, f"Sensex paper fly closed: {fields['exit_reason']}; net {fields['net_pnl']:.2f}, gross {fields['pnl']:.2f}, charges {fields['charges']['total']:.2f}")
    if cutoff.time() != ENTRY_TIME or entry_checked or strategies.find_one({"strategy_state": "active"}):
        return
    candidate = build_candidate(conn, edge.load_instruments(), cutoff)
    if (candidate.get("strategy_version") != STRATEGY_VERSION
            or candidate.get("cutoff") != cutoff
            or candidate.get("entry_date") != cutoff.date().isoformat()):
        raise ValueError("Sensex candidate version/cutoff mismatch")
    if not candidate.get("eligible"):
        notify(slack, f"No Sensex paper entry: {candidate['reason']}; VIX percentile {candidate['vix_percentile']:.2f}")
        return True
    key = trade_id(candidate["expiry"])
    for previous in strategies.find({"expiry": candidate["expiry"]}):
        if previous.get("strategy_version", "").startswith("sensex-ironfly-45dte-"):
            return True
    trade = open_trade(candidate)
    result = strategies.update_one({"_id": key}, {"$setOnInsert": trade}, upsert=True)
    if result.upserted_id is not None:
        sync_orders(trade, orders)
        notify(slack, f"Opened PAPER Sensex iron fly: ATM {trade['atm']}, wings {trade['wing_width']}, expiry {trade['expiry']}, quantity {trade['quantity']}, net credit {trade['total_credit_received']:.2f}")
    return True


def main():
    mongo, strategies, orders, slack = clients()
    notify(slack, f"{STRATEGY_VERSION} executor started; PAPER ONLY; old quantity/instrument settings are not used")
    last_error = None
    last_error_time = None
    last_entry_date = None
    try:
        while True:
            now = util.ist_now()
            if now.time() >= END_TIME:
                break
            try:
                for trade in strategies.find({"strategy_version": STRATEGY_VERSION}):
                    sync_orders(trade, orders)
                cutoff = checkpoint(now)
                if now.weekday() < 5 and cutoff:
                    conn = edge.login_to_dhan(slack_channel="straddlebot")
                    entry_done = process_checkpoint(conn, strategies, orders, cutoff, slack,
                                                    entry_checked=last_entry_date == now.date())
                    if entry_done:
                        last_entry_date = now.date()
                if last_error:
                    notify(slack, "Sensex paper executor recovered")
                    last_error = None
            except Exception as error:
                traceback.print_exc()
                message = util.exception_detail(error)
                if message != last_error or last_error_time is None or (now - last_error_time).total_seconds() >= 900:
                    notify(slack, f"Sensex paper executor failed: {message}")
                    last_error, last_error_time = message, now
            time.sleep(5)
    finally:
        mongo.close()
    notify(slack, "Sensex paper executor finished")


if __name__ == "__main__":
    main()
