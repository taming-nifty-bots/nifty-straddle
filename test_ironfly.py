"""Offline checks: python test_ironfly.py. No broker, Mongo or Slack calls."""
import importlib.util
import os
import sys
from datetime import date, datetime, time, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parent / "tamingnifty"))
import ironfly as fly

spec = importlib.util.spec_from_file_location("straddle_executor", ROOT / "straddle" / "straddle.py")
executor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(executor)

TODAY = date(2026, 11, 9)
CUTOFF = datetime.combine(TODAY, time(15, 0))
EXPIRY = TODAY + timedelta(days=45)


def assert_raises(call):
    try:
        call()
    except ValueError:
        return
    raise AssertionError("Expected explicit ValueError")


def master(wing_width=1000):
    rows = [{"EXCH_ID": "NSE", "INSTRUMENT": "INDEX", "SYMBOL_NAME": "NIFTY",
             "SECURITY_ID": 1},
            {"EXCH_ID": "NSE", "INSTRUMENT": "INDEX", "SYMBOL_NAME": "India VIX",
             "SECURITY_ID": 2}]
    for strike, kind, security in [(80000, "CE", 10), (80000, "PE", 11),
                                    (80000 - wing_width, "PE", 12), (80000 + wing_width, "CE", 13)]:
        rows.append({"EXCH_ID": "NSE", "SEGMENT": "D", "INSTRUMENT": "OPTIDX",
                     "UNDERLYING_SYMBOL": "NIFTY", "SYMBOL_NAME": "NIFTY",
                     "SM_EXPIRY_DATE": EXPIRY.isoformat(), "EXPIRY_FLAG": "M",
                     "STRIKE_PRICE": strike, "OPTION_TYPE": kind, "LOT_SIZE": 65,
                     "SECURITY_ID": security, "DISPLAY_NAME": f"NIFTY {strike} {kind}"})
    return pd.DataFrame(rows)


def history():
    dates = pd.bdate_range(TODAY - timedelta(days=365), TODAY - timedelta(days=1))
    return pd.DataFrame({"datetime": dates, "close": [10.0] * len(dates)})


def candidate(wing_width=1000, short_ce=1100, short_pe=900):
    prices = {1: 80000, 21: 20, 10: short_ce, 11: short_pe, 12: 750, 13: 750}

    def quote(conn, security, segment, instrument, cutoff):
        if instrument == "OPTIDX":
            assert segment == "NSE_FNO"
        return prices[security], cutoff - timedelta(minutes=1)

    with patch.object(fly, "candle_price", side_effect=quote), \
            patch.object(fly.edge, "fetch_candles", return_value=history()):
        return fly.build_candidate(None, master(wing_width), CUTOFF)


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
    assert fly.contract(chosen, 80000, "CE", "SELL")["quantity"] == 65


def test_contract_requires_exact_match_and_real_lot_size():
    contracts, expiry = fly.monthly_contracts(master(), TODAY)
    assert fly.contract(contracts, 80000, "CE", "SELL")["quantity"] == 65
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


def test_candle_price_failure_identifies_security_and_preserves_validation():
    empty = pd.DataFrame()
    untraded = pd.DataFrame({"datetime": [CUTOFF - timedelta(minutes=1)],
                            "close": [100], "volume": [0]})
    for frame in (empty, untraded):
        with patch.object(fly.edge, "fetch_candles", return_value=frame):
            try:
                fly.candle_price(None, 1135111, "BSE_FNO", "OPTIDX", CUTOFF)
            except ValueError as error:
                assert "security_id=1135111 BSE_FNO OPTIDX" in str(error)
                assert "2026-11-09 15:00:00" in str(error)
                assert isinstance(error.__cause__, ValueError)
            else:
                raise AssertionError("Missing/untraded candles must block pricing")
    traded = untraded.copy()
    traded["volume"] = 20
    with patch.object(fly.edge, "fetch_candles", return_value=traded):
        assert fly.candle_price(None, 1135111, "BSE_FNO", "OPTIDX", CUTOFF) == (
            100, CUTOFF - timedelta(minutes=1))


def test_candidate_four_legs_symmetric_half_premium_wings_one_lot():
    signal = candidate()
    assert signal["eligible"] is True
    assert signal["atm"] == 80000 and signal["wing_width"] == 1000
    assert signal["credit_per_unit"] == 500 and signal["quantity"] == 65
    assert [leg["side"] for leg in signal["legs"]] == ["BUY", "BUY", "SELL", "SELL"]
    assert [leg["strike"] for leg in signal["legs"]] == [79000, 81000, 80000, 80000]
    assert signal["expiry"] == EXPIRY.isoformat()
    trade = fly.open_trade(signal)
    assert trade["total_credit_received"] == 32500
    assert signal["strategy_version"] == "nifty-ironfly-45dte-v1"
    assert trade["instrument_name"] == "NIFTY" and trade["exchange_segment"] == "NSE_FNO"
    assert trade["entry_time"] == "15:00" and trade["holding_days"] == 0
    assert trade["dte"] == 45 and trade["last_check"] == CUTOFF.isoformat()


def test_vix_filter_blocks_at_or_below_25_and_allows_above():
    for percentile in (0, 24.9999, 25):
        with patch.object(fly, "vix_percentile", return_value=percentile):
            signal = candidate()
        assert signal["eligible"] is False and "legs" not in signal
        assert "25th percentile" in signal["reason"]
    for percentile in (25.0001, 49.9, 50, 100):
        with patch.object(fly, "vix_percentile", return_value=percentile):
            assert candidate()["eligible"] is True


def test_half_premium_wing_distance_rounds_up_at_exact_decimal_boundaries():
    for short_ce, short_pe, width in ((1100, 900, 1000),
                                     (1099.99, 900, 1000),
                                     (1100.01, 899.99, 1000),
                                     (1100.01, 900, 1100),
                                     (1101, 900, 1100)):
        signal = candidate(width, short_ce, short_pe)
        assert signal["wing_width"] == width
        assert [leg["strike"] for leg in signal["legs"][:2]] == [80000 - width, 80000 + width]


def test_half_premium_wings_reject_net_credit_outside_width():
    for credit in (0, 1000, 1500):
        with patch.object(fly, "fly_credit", return_value=credit):
            assert_raises(candidate)


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


def test_daily_startup_window_uses_actual_minute_without_retrospective_fill():
    for minute in range(5):
        now = CUTOFF.replace(minute=minute, second=45)
        assert fly.checkpoint(now) == now.replace(second=0)
    assert fly.checkpoint(CUTOFF.replace(minute=5)) is None
    assert fly.checkpoint(CUTOFF.replace(hour=9, minute=30)) is None
    assert fly.checkpoint(CUTOFF.replace(hour=14, minute=45)) is None
    assert fly.close_reason(1000, 1000, TODAY + timedelta(days=21),
                            CUTOFF.replace(minute=4)) == "21 calendar DTE time exit"
    signal = candidate()
    signal["cutoff"] = CUTOFF.replace(minute=3)
    assert fly.open_trade(signal)["entry_time"] == "15:03"


def test_full_fly_pnl_all_eight_order_charges_and_gross_preserved():
    trade = fly.open_trade(candidate())
    prices = {10: 600, 11: 400, 12: 375, 13: 375}
    with patch.object(fly, "candle_price", side_effect=lambda conn, secid, segment, instrument, cutoff:
                      (prices[secid], cutoff - timedelta(minutes=1))):
        fields = fly.evaluate_trade(None, trade, CUTOFF + timedelta(days=1))
    assert fields["strategy_state"] == "closed" and fields["pnl"] == 16250
    assert fields["charges"]["brokerage"] == 160
    assert fields["net_pnl"] == fly.money(16250 - fields["charges"]["total"])
    assert all("exit_price_time" in leg for leg in fields["legs"])
    assert "running_net_pnl" not in fields


def test_losses_do_not_close_fly_before_21_dte_even_at_maximum_loss():
    signal = candidate()
    for leg in signal["legs"]:
        if leg["side"] == "BUY":
            leg["entry_price"] = 875
    signal["credit_per_unit"] = 250
    trade = fly.open_trade(signal)
    prices = {10: 600, 11: 400, 12: 250, 13: 250}
    with patch.object(fly, "candle_price", side_effect=lambda conn, secid, segment, instrument, cutoff:
                      (prices[secid], cutoff - timedelta(minutes=1))):
        fields = fly.evaluate_trade(None, trade, CUTOFF + timedelta(days=1))
        assert fields["running_pnl"] == -trade["total_credit_received"]
        assert "strategy_state" not in fields and "exit_reason" not in fields
        prices.update({10: 1100, 11: 900, 12: 500, 13: 500})
        fields = fly.evaluate_trade(None, trade, CUTOFF + timedelta(days=1))
        assert fields["running_pnl"] == -48750
        assert fields["pnl"] == -48750
        assert fields["holding_days"] == 1 and fields["dte"] == 44
        assert len(fields["mark_prices"]) == 4
        assert fields["closing_value_per_unit"] == 1000
        assert "strategy_state" not in fields and "exit_reason" not in fields
        fields = fly.evaluate_trade(None, trade, CUTOFF + timedelta(days=24))
    assert fields["strategy_state"] == "closed"
    assert fields["exit_reason"] == "21 calendar DTE time exit"
    assert fields["pnl"] == -48750 and fields["charges"]["brokerage"] == 160


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


def test_same_expiry_nifty_history_blocks_and_active_sensex_blocks():
    signal = candidate()
    previous = fly.open_trade(signal)
    previous.update(_id=f"nifty-ironfly-45dte-old:{signal['expiry']}",
                    strategy_version="nifty-ironfly-45dte-old", strategy_state="closed")
    trades, orders = collection([previous]), collection()
    with patch.object(executor.edge, "load_instruments", return_value=master()), \
            patch.object(executor, "build_candidate", return_value=signal), \
            patch.object(executor, "notify"):
        assert executor.process_checkpoint(None, trades, orders, CUTOFF, None) is True
    assert len(trades.documents) == 1 and not orders.documents
    previous["strategy_state"] = "active"
    previous["strategy_version"] = "sensex-ironfly-45dte-v2"
    assert_raises(lambda: executor.process_checkpoint(None, collection([previous]), orders, CUTOFF, None))


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


def test_vix_rejection_writes_no_trade():
    entry = candidate()
    entry.update(eligible=False, reason="VIX filter blocked", vix_percentile=25)
    trades, orders = collection(), collection()
    with patch.object(executor.edge, "load_instruments", return_value=master()), \
            patch.object(executor, "build_candidate", return_value=entry) as build, \
            patch.object(executor, "notify"):
        done = executor.process_checkpoint(None, trades, orders, CUTOFF, None)
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


def test_no_entry_selection_before_1500_or_while_holding():
    trade = fly.open_trade(candidate())
    trade["last_check"] = CUTOFF.isoformat()
    with patch.object(executor, "build_candidate") as build:
        assert_raises(lambda: executor.process_checkpoint(None, collection(), collection(),
                                                          CUTOFF.replace(hour=14, minute=30), None))
        executor.process_checkpoint(None, collection([trade]), collection(), CUTOFF, None)
        build.assert_not_called()


def test_exit_repairs_orders_and_considers_new_expiry_immediately():
    trade = fly.open_trade(candidate())
    trades = collection([trade])
    trade["last_check"] = (CUTOFF - timedelta(days=1)).isoformat()
    trades = collection([trade])
    fields = {"strategy_state": "closed", "exit_reason": "21 calendar DTE time exit",
              "pnl": -50, "net_pnl": -100, "charges": {"total": 50},
              "last_check": CUTOFF.isoformat(),
              "legs": [dict(leg, exit_price=leg["entry_price"],
                            exit_price_time=CUTOFF) for leg in trade["legs"]]}
    new = candidate()
    new["expiry"] = (EXPIRY + timedelta(days=30)).isoformat()
    orders = collection()
    with patch.object(executor, "evaluate_trade", return_value=fields) as evaluate, \
            patch.object(executor.edge, "load_instruments", return_value=master()), \
            patch.object(executor, "build_candidate", return_value=new) as build, \
            patch.object(executor, "notify"):
        executor.process_checkpoint(None, trades, orders, CUTOFF, None)
    evaluate.assert_called_once()
    build.assert_called_once()
    assert trades.documents[trade["_id"]]["strategy_state"] == "closed"
    assert trades.documents[fly.trade_id(new["expiry"])]["strategy_state"] == "active"
    assert len(orders.documents) == 12
    assert all(order["exchangeSegment"] == "NSE_FNO" for order in orders.documents.values())


def test_clients_reject_live_mode_before_any_connection():
    with patch.object(fly, "load_dotenv"), patch.dict(os.environ, {"live_trading": "true"}), \
            patch.object(fly, "MongoClient") as mongo:
        assert_raises(fly.clients)
        mongo.assert_not_called()


def test_daily_main_runs_once_closes_client_and_returns_without_sleep():
    trades, orders = collection(), collection()
    mongo = Mock()
    with patch.object(executor, "clients", return_value=(mongo, trades, orders, None)), \
            patch.object(executor, "check_trading_day", return_value=True), \
            patch.object(executor.util, "ist_now", return_value=CUTOFF.replace(second=20)), \
            patch.object(executor.edge, "login_to_dhan", return_value="conn") as login, \
            patch.object(executor, "process_checkpoint") as process, \
            patch.object(executor, "notify"):
        executor.main()
    login.assert_called_once()
    process.assert_called_once_with("conn", trades, orders, CUTOFF, None)
    mongo.close.assert_called_once()
    source = (ROOT / "straddle" / "straddle.py").read_text(encoding="utf-8")
    assert "while " not in source and "time.sleep" not in source


def test_daily_main_skips_weekends_and_outside_window_without_broker_calls():
    for now in (CUTOFF.replace(hour=14), CUTOFF.replace(minute=5),
                datetime(2026, 11, 14, 15)):
        mongo = Mock()
        with patch.object(executor, "clients",
                          return_value=(mongo, collection(), collection(), None)), \
                patch.object(executor, "check_trading_day", return_value=True), \
                patch.object(executor.util, "ist_now", return_value=now), \
                patch.object(executor.edge, "login_to_dhan") as login, \
                patch.object(executor, "process_checkpoint") as process, \
                patch.object(executor, "notify") as notify:
            executor.main()
        login.assert_not_called()
        process.assert_not_called()
        mongo.close.assert_called_once()
        assert any("skipped" in call.args[1] for call in notify.call_args_list)


def test_daily_main_reports_failure_and_exits_unsuccessfully():
    mongo = Mock()
    with patch.object(executor, "clients",
                      return_value=(mongo, collection(), collection(), None)), \
            patch.object(executor, "check_trading_day", return_value=True), \
            patch.object(executor.util, "ist_now", return_value=CUTOFF), \
            patch.object(executor.edge, "login_to_dhan", return_value="conn"), \
            patch.object(executor, "process_checkpoint", side_effect=ValueError("missing data")) as process, \
            patch.object(executor, "notify") as notify:
        assert_raises(executor.main)
    process.assert_called_once()
    mongo.close.assert_called_once()
    assert any("failed" in call.args[1] for call in notify.call_args_list)


def test_daily_existing_position_marks_without_creating_another_position():
    trade = fly.open_trade(candidate())
    cutoff = CUTOFF + timedelta(days=1)
    trades, orders = collection([trade]), collection()
    prices = {10: 1100, 11: 900, 12: 750, 13: 750}
    with patch.object(fly, "candle_price", side_effect=lambda conn, secid, segment, instrument, time:
                      (prices[secid], time - timedelta(minutes=1))), \
            patch.object(executor, "build_candidate") as build, \
            patch.object(executor, "notify"):
        executor.process_checkpoint(None, trades, orders, cutoff, None)
        executor.process_checkpoint(None, trades, orders, cutoff.replace(minute=3), None)
    build.assert_not_called()
    marked = trades.documents[trade["_id"]]
    assert marked["holding_days"] == 1 and marked["dte"] == 44
    assert marked["pnl"] == marked["running_pnl"] == 0
    assert marked["last_check"] == cutoff.isoformat()
    assert len(marked["mark_prices"]) == 4
    assert marked["net_pnl"] is None and marked["strategy_state"] == "active"
    assert len(orders.documents) == 4 and len(trades.documents) == 1


def test_closed_sensex_history_does_not_block_a_nifty_entry():
    signal = candidate()
    previous = {"_id": "sensex-history", "strategy_state": "closed",
                "strategy_version": "sensex-ironfly-45dte-v2", "expiry": signal["expiry"]}
    trades, orders = collection([previous]), collection()
    with patch.object(executor.edge, "load_instruments", return_value=master()), \
            patch.object(executor, "build_candidate", return_value=signal), \
            patch.object(executor, "notify"):
        executor.process_checkpoint(None, trades, orders, CUTOFF, None)
    assert len(trades.documents) == 2
    assert trades.documents["sensex-history"] == previous


def test_container_and_deployment_defaults_are_paper_only():
    dockerfile = (ROOT / "straddle" / "Dockerfile").read_text(encoding="utf-8")
    assert "ENV user_name=sugam" in dockerfile
    assert "ENV live_trading=false" in dockerfile
    for name in ("INTEGRATE_API_TOKEN", "INTEGRATE_API_SECRET", "ENV TOTP", "ENV slack_token"):
        assert name not in dockerfile
    workflow = (ROOT / ".github" / "workflows" / "straddle-CI-CD.yml").read_text(encoding="utf-8")
    assert "--set-env-vars live_trading=false" in workflow
    assert '--cron-expression "30 9 * * 1-5"' in workflow
    assert "--replica-timeout 900" in workflow and "--replica-retry-limit 0" in workflow


if __name__ == "__main__":
    tests = [value for key, value in sorted(globals().items()) if key.startswith("test_")]
    for test in tests:
        test()
        print("PASS", test.__name__)
    print(len(tests), "passed")
