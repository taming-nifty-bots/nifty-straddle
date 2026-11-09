"""Retained executor-job name; no broker-order path exists in this paper pilot."""
import traceback
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ironfly import (STRATEGY_VERSION, build_candidate,
                     checkpoint, clients, edge, evaluate_trade, notify, open_trade,
                     sync_orders, trade_id, util)


def process_checkpoint(conn, strategies, orders, cutoff, slack):
    if checkpoint(cutoff) is None:
        raise ValueError("Nifty paper job must run from 15:00 until 15:05 IST")
    active = list(strategies.find({"strategy_state": "active"}))
    if len(active) > 1:
        raise ValueError("Multiple active records; manual review required")
    for trade in active:
        if trade.get("strategy_version") != STRATEGY_VERSION or trade.get("simulated") is not True:
            raise ValueError("An active legacy/non-paper trade must be reconciled before this pilot")
        if trade.get("last_check", "")[:10] != cutoff.date().isoformat():
            fields = evaluate_trade(conn, trade, cutoff)
            result = strategies.update_one(
                {"_id": trade["_id"], "strategy_state": "active",
                 "last_check": {"$ne": cutoff.isoformat()}}, {"$set": fields})
            if result.modified_count:
                saved = strategies.find_one({"_id": trade["_id"]})
                sync_orders(saved, orders)
                if fields.get("strategy_state") == "closed":
                    notify(slack, f"Nifty paper fly closed: {fields['exit_reason']}; net {fields['net_pnl']:.2f}, gross {fields['pnl']:.2f}, charges {fields['charges']['total']:.2f}")
                else:
                    notify(slack, f"Nifty paper fly marked: gross P&L {fields['running_pnl']:.2f}; holding {fields['holding_days']} calendar days; DTE {fields['dte']}")
    if strategies.find_one({"strategy_state": "active"}):
        return
    candidate = build_candidate(conn, edge.load_instruments(), cutoff)
    if (candidate.get("strategy_version") != STRATEGY_VERSION
            or candidate.get("cutoff") != cutoff
            or candidate.get("entry_date") != cutoff.date().isoformat()):
        raise ValueError("Nifty candidate version/cutoff mismatch")
    if not candidate.get("eligible"):
        notify(slack, f"No Nifty paper entry: {candidate['reason']}; VIX percentile {candidate['vix_percentile']:.2f}")
        return True
    key = trade_id(candidate["expiry"])
    for previous in strategies.find({"expiry": candidate["expiry"]}):
        if previous.get("strategy_version", "").startswith("nifty-ironfly-45dte-"):
            notify(slack, f"No Nifty paper re-entry: expiry {candidate['expiry']} already traded")
            return True
    trade = open_trade(candidate)
    result = strategies.update_one({"_id": key}, {"$setOnInsert": trade}, upsert=True)
    if result.upserted_id is not None:
        sync_orders(trade, orders)
        notify(slack, f"Opened PAPER Nifty iron fly: ATM {trade['atm']}, wings {trade['wing_width']}, expiry {trade['expiry']}, quantity {trade['quantity']}, premium credit before charges {trade['total_credit_received']:.2f}")
    return True


def main():
    mongo, strategies, orders, slack = clients()
    notify(slack, f"{STRATEGY_VERSION} executor started; PAPER ONLY; old quantity/instrument settings are not used")
    try:
        now = util.ist_now()
        cutoff = checkpoint(now)
        if now.weekday() >= 5 or cutoff is None:
            notify(slack, "Nifty paper executor skipped: run once on weekdays from 15:00 until 15:05 IST")
            return
        for trade in strategies.find({"strategy_version": STRATEGY_VERSION}):
            sync_orders(trade, orders)
        conn = edge.login_to_dhan(slack_channel="straddlebot")
        process_checkpoint(conn, strategies, orders, cutoff, slack)
        notify(slack, "Nifty paper daily check complete")
    except Exception as error:
        traceback.print_exc()
        notify(slack, "Nifty paper executor failed: " + util.exception_detail(error))
        raise
    finally:
        mongo.close()
        notify(slack, "Nifty paper executor finished")


if __name__ == "__main__":
    main()
