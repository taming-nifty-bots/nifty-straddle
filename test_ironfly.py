"""Offline checks: python test_ironfly.py. No broker, Mongo or Slack calls."""
import importlib.util
import os
import sys
from datetime import date, datetime, time, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parent / "tamingnifty"))
import ironfly as fly

spec = importlib.util.spec_from_file_location("straddle_executor", ROOT / "straddle" / "straddle.py")
executor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(executor)

TODAY = date(2026, 11, 9)
CUTOFF = datetime.combine(TODAY, time(15, 15))
EXPIRY = TODAY + timedelta(days=45)


def assert_raises(call):
    try:
        call()
    except ValueError:
        return
    raise AssertionError("Expected explicit ValueError")


def master():
    rows = [{"EXCH_ID": "BSE", "INSTRUMENT": "INDEX", "SYMBOL_NAME": "SENSEX",
             "SECURITY_ID": 1},
            {"EXCH_ID": "NSE", "INSTRUMENT": "INDEX", "SYMBOL_NAME": "India VIX",
             "SECURITY_ID": 2}]
    for strike, kind, security in [(80000, "CE", 10), (80000, "PE", 11),
                                    (78000, "PE", 12), (82000, "CE", 13)]:
        rows.append({"EXCH_ID": "BSE", "SEGMENT": "D", "INSTRUMENT": "OPTIDX",
                     "UNDERLYING_SYMBOL": "SENSEX", "SYMBOL_NAME": "BSXOPT",
                     "SM_EXPIRY_DATE": EXPIRY.isoformat(), "EXPIRY_FLAG": "M",
                     "STRIKE_PRICE": strike, "OPTION_TYPE": kind, "LOT_SIZE": 20,
                     "SECURITY_ID": security, "DISPLAY_NAME": f"SENSEX {strike} {kind}"})
    return pd.DataFrame(rows)


def history():
    dates = pd.bdate_range(TODAY - timedelta(days=365), TODAY - timedelta(days=1))
    return pd.DataFrame({"datetime": dates, "close": [10.0] * len(dates)})


def candidate():
    prices = {1: 80000, 21: 20, 10: 1100, 11: 900, 12: 250, 13: 250}

    def quote(conn, security, segment, instrument, cutoff):
        if instrument == "OPTIDX":
            assert segment == "BSE_FNO"
        return prices[security], cutoff - timedelta(minutes=1)

    with patch.object(fly, "candle_price", side_effect=quote), \
            patch.object(fly.edge, "fetch_candles", return_value=history()):
        return fly.build_candidate(None, master(), CUTOFF)


def collection(initial=()):
    documents = {}
    for document in initial:
        documents[document["_id"]] = dict(document)

    def matches(document, query):
        for key, value in query.items():
            if isinstance(value, dict) and "$ne" in value:
                if document.get(key) == value["$ne"]:
                    return False
            elif document.get(key) != value:
                return False
        return True

    def find(query):
        return [dict(document) for document in documents.values() if matches(document, query)]

    def find_one(query):
        values = find(query)
        return values[0] if values else None

    def update_one(query, update, upsert=False):
        found = find_one(query)
        if found:
            old = dict(found)
            found.update(update.get("$set", {}))
            documents[found["_id"]] = found
            return SimpleNamespace(modified_count=int(old != found), upserted_id=None)
        if upsert:
            document = dict(update["$setOnInsert"])
            documents[document["_id"]] = document
            return SimpleNamespace(modified_count=0, upserted_id=document["_id"])
        return SimpleNamespace(modified_count=0, upserted_id=None)

    return SimpleNamespace(find=find, find_one=find_one, update_one=update_one, documents=documents)


def test_monthly_expiry_at_least_45_not_nearest_or_weekly():
    data = master()
    earlier = data.iloc[2:].copy()
    earlier["SM_EXPIRY_DATE"] = (TODAY + timedelta(days=44)).isoformat()
    weekly = data.iloc[2:].copy()
    weekly["EXPIRY_FLAG"] = "W"
    weekly["SM_EXPIRY_DATE"] = (TODAY + timedelta(days=46)).isoformat()
    chosen, expiry = fly.monthly_contracts(pd.concat([data, earlier, weekly]), TODAY)
    assert expiry == EXPIRY and len(chosen) == 4
    assert_raises(lambda: fly.monthly_contracts(earlier, TODAY))


def test_monthly_date_labelled_halfyear_is_not_skipped_for_january():
    data = master()
    december = data.iloc[2:].copy()
    december["SM_EXPIRY_DATE"] = "2026-12-31"
    december["EXPIRY_FLAG"] = "H"
    january = data.iloc[2:].copy()
    january["SM_EXPIRY_DATE"] = "2027-01-28"
    january["LOT_SIZE"] = 25
    chosen, expiry = fly.monthly_contracts(pd.concat([december, january]), TODAY)
    assert expiry == date(2026, 12, 31)
    assert fly.contract(chosen, 80000, "CE", "SELL")["quantity"] == 20


def test_contract_requires_exact_match_and_real_lot_size():
    contracts, expiry = fly.monthly_contracts(master(), TODAY)
    assert fly.contract(contracts, 80000, "CE", "SELL")["quantity"] == 20
    assert_raises(lambda: fly.contract(contracts, 80100, "CE", "SELL"))
    assert_raises(lambda: fly.contract(pd.concat([contracts, contracts]), 80000, "CE", "SELL"))
    contracts["LOT_SIZE"] = 20.5
    assert_raises(lambda: fly.contract(contracts, 80000, "CE", "SELL"))


def test_completed_same_day_traded_candles_only():
    frame = pd.DataFrame({"datetime": [CUTOFF - timedelta(days=1),
                                      CUTOFF - timedelta(minutes=2),
                                      CUTOFF - timedelta(minutes=1), CUTOFF],
                          "close": [100, 200, 300, 999], "volume": [10, 10, 0, 10]})
    assert fly.last_close(frame, CUTOFF, option=True)[0] == 200
    assert fly.last_close(frame, CUTOFF)[0] == 300
    assert_raises(lambda: fly.last_close(frame.iloc[:1], CUTOFF, option=True))
    assert_raises(lambda: fly.last_close(pd.concat([frame, frame]), CUTOFF))
    assert_raises(lambda: fly.positive_price(float("nan")))


def test_vix_prior_year_strict_percentile_and_missing_history():
    data = history()
    assert fly.vix_percentile(data, 10, TODAY) == 0
    assert fly.vix_percentile(data, 20, TODAY) == 100
    future = pd.DataFrame({"datetime": [CUTOFF], "close": [999]})
    assert fly.vix_percentile(pd.concat([data, future]), 20, TODAY) == 100
    assert_raises(lambda: fly.vix_percentile(data.tail(199), 20, TODAY))
    data.loc[data.index[0], "close"] = -1
    assert_raises(lambda: fly.vix_percentile(data, 20, TODAY))


def test_candidate_four_legs_symmetric_premium_wings_one_lot():
    signal = candidate()
    assert signal["eligible"] is True
    assert signal["atm"] == 80000 and signal["wing_width"] == 2000
    assert signal["credit_per_unit"] == 1500 and signal["quantity"] == 20
    assert [leg["side"] for leg in signal["legs"]] == ["BUY", "BUY", "SELL", "SELL"]
    assert [leg["strike"] for leg in signal["legs"]] == [78000, 82000, 80000, 80000]
    assert signal["expiry"] == EXPIRY.isoformat()
    assert fly.open_trade(signal)["total_credit_received"] == 30000


def test_vix_filter_blocks_equal_and_below_50():
    with patch.object(fly, "candle_price", return_value=(80000, CUTOFF - timedelta(minutes=1))), \
            patch.object(fly.edge, "fetch_candles", return_value=history()), \
            patch.object(fly, "vix_percentile", return_value=50):
        signal = fly.build_candidate(None, master(), CUTOFF)
    assert signal["eligible"] is False and "legs" not in signal


def test_exact_price_boundaries_and_21_dte_at_final_checkpoint():
    assert fly.close_reason(1000, 500, EXPIRY, CUTOFF) == "50% net-credit target"
    assert fly.close_reason(1000, 2000, EXPIRY, CUTOFF) is None
    assert fly.close_reason(1000, 3000, EXPIRY, CUTOFF) is None
    assert fly.close_reason(1000, 500.01, EXPIRY, CUTOFF) is None
    assert fly.close_reason(1000, 1999.99, EXPIRY, CUTOFF) is None
    expiry = TODAY + timedelta(days=21)
    assert fly.close_reason(1000, 1000, expiry, CUTOFF) == "21 calendar DTE time exit"
    assert fly.close_reason(1000, 2000, expiry, CUTOFF) == "21 calendar DTE time exit"
    assert fly.close_reason(1000, 1000, expiry, CUTOFF.replace(hour=14, minute=30)) is None
    assert fly.close_reason(1000, 1000, TODAY + timedelta(days=22), CUTOFF) is None


def test_seven_checkpoints_no_minute_by_minute_or_missed_checkpoint_fill():
    assert len(fly.CHECK_TIMES) == 7
    for clock in fly.CHECK_TIMES:
        now = datetime.combine(TODAY, clock).replace(second=45)
        assert fly.checkpoint(now) == now.replace(second=0)
    assert fly.checkpoint(CUTOFF.replace(minute=16)) is None
    assert fly.checkpoint(CUTOFF.replace(hour=14, minute=45)) is None


def test_full_fly_pnl_all_eight_order_charges_and_gross_preserved():
    trade = fly.open_trade(candidate())
    prices = {10: 600, 11: 400, 12: 250, 13: 250}
    with patch.object(fly, "candle_price", side_effect=lambda conn, secid, segment, instrument, cutoff:
                      (prices[secid], cutoff - timedelta(minutes=1))):
        fields = fly.evaluate_trade(None, trade, CUTOFF + timedelta(days=1))
    assert fields["strategy_state"] == "closed" and fields["pnl"] == 20000
    assert fields["charges"]["brokerage"] == 160
    assert fields["net_pnl"] == fly.money(20000 - fields["charges"]["total"])
    assert all("exit_price_time" in leg for leg in fields["legs"])
    assert "running_net_pnl" not in fields


def test_losses_do_not_close_fly_before_21_dte_even_at_maximum_loss():
    signal = candidate()
    for leg in signal["legs"]:
        if leg["side"] == "BUY":
            leg["entry_price"] = 750
    signal["credit_per_unit"] = 500
    trade = fly.open_trade(signal)
    prices = {10: 1100, 11: 900, 12: 500, 13: 500}
    with patch.object(fly, "candle_price", side_effect=lambda conn, secid, segment, instrument, cutoff:
                      (prices[secid], cutoff - timedelta(minutes=1))):
        fields = fly.evaluate_trade(None, trade, CUTOFF + timedelta(days=1))
        assert fields["running_pnl"] == -trade["total_credit_received"]
        assert "strategy_state" not in fields and "exit_reason" not in fields
        prices.update({10: 1500, 11: 1500})
        fields = fly.evaluate_trade(None, trade, CUTOFF + timedelta(days=1))
        assert fields["running_pnl"] == -30000
        assert "strategy_state" not in fields and "exit_reason" not in fields
        fields = fly.evaluate_trade(None, trade, CUTOFF + timedelta(days=24))
    assert fields["strategy_state"] == "closed"
    assert fields["exit_reason"] == "21 calendar DTE time exit"
    assert fields["pnl"] == -30000 and fields["charges"]["brokerage"] == 160


def test_checkpoint_is_only_evaluated_once_after_successful_write():
    trade = fly.open_trade(candidate())
    trade["last_check"] = CUTOFF.isoformat()
    trades = collection([trade])
    with patch.object(executor, "evaluate_trade") as evaluate:
        executor.process_checkpoint(None, trades, collection(), CUTOFF, None)
        evaluate.assert_not_called()


def test_partial_fetch_failure_writes_nothing_and_no_price_fallback():
    trade = fly.open_trade(candidate())
    with patch.object(fly, "candle_price", side_effect=ValueError("no trades")):
        assert_raises(lambda: fly.evaluate_trade(None, trade, CUTOFF))
    assert "exit_date" not in trade and trade["strategy_state"] == "active"


def test_entry_idempotency_and_no_same_expiry_reentry():
    signal = candidate()
    trades, orders = collection(), collection()
    with patch.object(executor, "notify"), \
            patch.object(executor.edge, "load_instruments", return_value=master()), \
            patch.object(executor, "build_candidate", return_value=signal):
        executor.process_checkpoint(None, trades, orders, CUTOFF, None)
    assert len(trades.documents) == 1 and len(orders.documents) == 4
    key = fly.trade_id(signal["expiry"])
    trade = trades.documents[key]
    trade.update(strategy_state="closed", legs=[dict(leg, exit_price=leg["entry_price"],
                 exit_price_time=leg["price_time"]) for leg in trade["legs"]])
    with patch.object(executor, "notify"), \
            patch.object(executor.edge, "load_instruments", return_value=master()), \
            patch.object(executor, "build_candidate", return_value=signal):
        executor.process_checkpoint(None, trades, orders, CUTOFF, None)
    assert len(trades.documents) == 1
    fly.sync_orders(trade, orders)
    fly.sync_orders(trade, orders)
    assert len(orders.documents) == 8


def test_restart_repairs_missing_paper_orders_and_never_calls_broker():
    trade = fly.open_trade(candidate())
    orders = collection()
    fly.sync_orders(trade, orders)
    first = next(iter(orders.documents))
    del orders.documents[first]
    fly.sync_orders(trade, orders)
    assert len(orders.documents) == 4
    for path in (ROOT / "ironfly.py", ROOT / "straddle" / "straddle.py"):
        assert "place_order(" not in path.read_text(encoding="utf-8")


def test_stale_candidate_and_active_legacy_record_block_entries():
    signal = candidate()
    signal["entry_date"] = (TODAY - timedelta(days=1)).isoformat()
    trades, orders = collection(), collection()
    with patch.object(executor.edge, "load_instruments", return_value=master()), \
            patch.object(executor, "build_candidate", return_value=signal):
        assert_raises(lambda: executor.process_checkpoint(None, trades, orders, CUTOFF, None))
    assert not trades.documents
    legacy = {"_id": "old", "strategy_state": "active"}
    assert_raises(lambda: executor.process_checkpoint(None, collection([legacy]), orders, CUTOFF, None))


def test_entry_selection_runs_in_same_job_without_signal_storage():
    trades, orders = collection(), collection()
    with patch.object(executor.edge, "load_instruments", return_value=master()) as load, \
            patch.object(executor, "build_candidate", return_value=candidate()) as build, \
            patch.object(executor, "notify"):
        result = executor.process_checkpoint("conn", trades, orders, CUTOFF, None)
    assert result is True and len(trades.documents) == 1 and len(orders.documents) == 4
    load.assert_called_once()
    build.assert_called_once_with("conn", load.return_value, CUTOFF)


def test_vix_rejection_is_not_repeated_during_entry_window():
    entry = candidate()
    entry.update(eligible=False, reason="VIX filter blocked", vix_percentile=50)
    trades, orders = collection(), collection()
    with patch.object(executor.edge, "load_instruments", return_value=master()), \
            patch.object(executor, "build_candidate", return_value=entry) as build, \
            patch.object(executor, "notify"):
        done = executor.process_checkpoint(None, trades, orders, CUTOFF, None)
        executor.process_checkpoint(None, trades, orders, CUTOFF, None, entry_checked=done)
    assert done is True and not trades.documents and not orders.documents
    build.assert_called_once()


def test_entry_data_failure_can_retry_without_partial_trade():
    trades, orders = collection(), collection()
    with patch.object(executor.edge, "load_instruments", return_value=master()), \
            patch.object(executor, "build_candidate", side_effect=[ValueError("missing candles"), candidate()]), \
            patch.object(executor, "notify"):
        assert_raises(lambda: executor.process_checkpoint(None, trades, orders, CUTOFF, None))
        assert not trades.documents and not orders.documents
        assert executor.process_checkpoint(None, trades, orders, CUTOFF, None) is True
    assert len(trades.documents) == 1 and len(orders.documents) == 4


def test_no_entry_selection_before_1515_or_while_holding():
    trade = fly.open_trade(candidate())
    trade["last_check"] = CUTOFF.isoformat()
    with patch.object(executor, "build_candidate") as build:
        executor.process_checkpoint(None, collection(), collection(),
                                    CUTOFF.replace(hour=14, minute=30), None)
        executor.process_checkpoint(None, collection([trade]), collection(), CUTOFF, None)
        build.assert_not_called()


def test_completed_entry_check_does_not_skip_position_exits():
    trade = fly.open_trade(candidate())
    trades = collection([trade])
    fields = {"strategy_state": "closed", "exit_reason": "21 calendar DTE time exit",
              "pnl": -50, "net_pnl": -100, "charges": {"total": 50},
              "last_check": CUTOFF.isoformat()}
    with patch.object(executor, "evaluate_trade", return_value=fields) as evaluate, \
            patch.object(executor, "build_candidate") as build, \
            patch.object(executor, "notify"):
        executor.process_checkpoint(None, trades, collection(), CUTOFF, None, entry_checked=True)
    evaluate.assert_called_once()
    build.assert_not_called()
    assert trades.documents[trade["_id"]]["strategy_state"] == "closed"


def test_clients_reject_live_mode_before_any_connection():
    with patch.object(fly, "load_dotenv"), patch.dict(os.environ, {"live_trading": "true"}), \
            patch.object(fly, "MongoClient") as mongo:
        assert_raises(fly.clients)
        mongo.assert_not_called()


def test_container_and_deployment_defaults_are_paper_only():
    dockerfile = (ROOT / "straddle" / "Dockerfile").read_text(encoding="utf-8")
    assert "ENV user_name=sugam" in dockerfile
    assert "ENV live_trading=false" in dockerfile
    for name in ("INTEGRATE_API_TOKEN", "INTEGRATE_API_SECRET", "ENV TOTP", "ENV slack_token"):
        assert name not in dockerfile
    workflow = (ROOT / ".github" / "workflows" / "straddle-CI-CD.yml").read_text(encoding="utf-8")
    assert "--set-env-vars live_trading=false" in workflow


if __name__ == "__main__":
    tests = [value for key, value in sorted(globals().items()) if key.startswith("test_")]
    for test in tests:
        test()
        print("PASS", test.__name__)
    print(len(tests), "passed")
