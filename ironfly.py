"""Paper-only Sensex iron fly; approved adaptation of Zerodha's 45 DTE rules."""
import math
import os
from datetime import datetime, time, timedelta
from decimal import Decimal, ROUND_HALF_UP

import pandas as pd
from dotenv import find_dotenv, load_dotenv
from pymongo import MongoClient
from slack_sdk import WebClient
from tamingnifty import connect_dhan as edge
from tamingnifty import utils as util
from tamingnifty.charges import money, order_charges, sum_charges

STRATEGY_VERSION = "sensex-ironfly-45dte-v1"
ENTRY_TIME = time(15, 15)
CHECK_TIMES = (time(9, 30), time(10, 30), time(11, 30), time(12, 30),
               time(13, 30), time(14, 30), ENTRY_TIME)
END_TIME = time(15, 16)
STRIKE_INTERVAL = 100
SLACK_CHANNEL = "straddlebot"


def notify(slack, message):
    try:
        util.notify(message=message, slack_client=slack, slack_channel=SLACK_CHANNEL)
    except Exception as error:
        print(f"Slack notification failed: {util.exception_detail(error)}", flush=True)


def clients():
    load_dotenv(find_dotenv())
    if os.environ.get("live_trading", "false").lower() != "false":
        raise ValueError("Sensex iron fly is paper-only; live_trading must be false")
    connection = os.environ.get("CONNECTION_STRING")
    user = os.environ.get("user_name", "sugam").strip().lower()
    if not connection or not user:
        raise ValueError("CONNECTION_STRING and a nonempty user_name are required")
    mongo = MongoClient(connection)
    db = mongo["Bots"]
    slack = WebClient(token=os.environ.get("slack_token"))
    return mongo, db[f"nifty_Straddle_{user}"], db[f"orders_nifty_Straddle_{user}"], slack


def positive_price(value):
    value = float(value)
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"Expected a finite positive price, got {value}")
    return value


def last_close(frame, cutoff, option=False):
    """Use only completed candles: a 15:14 candle is available at 15:15."""
    if frame.empty:
        raise ValueError(f"No candles available at {cutoff}")
    data = frame.copy()
    data["datetime"] = pd.to_datetime(data["datetime"], errors="raise")
    if data["datetime"].isna().any() or data["datetime"].duplicated().any():
        raise ValueError("Candle timestamps are missing or duplicated")
    data = data[(data["datetime"].dt.date == cutoff.date())
                & (data["datetime"] + timedelta(minutes=1) <= cutoff)]
    if option:
        data = data[pd.to_numeric(data["volume"], errors="raise") > 0]
    if data.empty:
        raise ValueError(f"No same-session completed {'traded option' if option else 'index'} candle at {cutoff}")
    row = data.sort_values("datetime").iloc[-1]
    return positive_price(row["close"]), row["datetime"].to_pydatetime()


def candle_price(conn, security_id, segment, instrument, cutoff):
    start = datetime.combine(cutoff.date(), time(9, 15))
    # The shared API helper sends dates; include tomorrow then filter locally.
    end = datetime.combine(cutoff.date() + timedelta(days=1), time())
    frame = edge.fetch_candles(conn, security_id, segment, instrument, start, end, "min")
    return last_close(frame, cutoff, option=instrument == "OPTIDX")


def index_id(master, exchange, symbol):
    matches = master[(master["EXCH_ID"] == exchange)
                     & (master["INSTRUMENT"] == "INDEX")
                     & (master["SYMBOL_NAME"] == symbol)]
    if len(matches) != 1:
        raise ValueError(f"Expected one {exchange} {symbol} index, found {len(matches)}")
    return int(matches.iloc[0]["SECURITY_ID"])


def monthly_contracts(master, today):
    data = master[(master["EXCH_ID"] == "BSE") & (master["SEGMENT"] == "D")
                  & (master["INSTRUMENT"] == "OPTIDX")
                  & (master["UNDERLYING_SYMBOL"] == "SENSEX")
                  & (master["EXPIRY_FLAG"].isin(("M", "Q", "H")))].copy()
    data["expiry"] = pd.to_datetime(data["SM_EXPIRY_DATE"], errors="raise").dt.date
    if data["expiry"].isna().any():
        raise ValueError("Sensex monthly expiry is missing")
    data = data[data["expiry"] >= today + timedelta(days=45)]
    if data.empty:
        raise ValueError("No Sensex monthly expiry at least 45 calendar DTE")
    expiry = min(data["expiry"])
    return data[data["expiry"] == expiry], expiry


def contract(data, strike, option_type, side):
    matches = data[(data["STRIKE_PRICE"] == strike) & (data["OPTION_TYPE"] == option_type)]
    if len(matches) != 1:
        raise ValueError(f"Expected one Sensex {strike} {option_type}, found {len(matches)}")
    row = matches.iloc[0]
    lot = float(row["LOT_SIZE"])
    if not math.isfinite(lot) or lot <= 0 or not lot.is_integer():
        raise ValueError("Contract lot size must be a positive whole number")
    return {"symbol": str(row["DISPLAY_NAME"]), "security_id": int(row["SECURITY_ID"]),
            "strike": int(strike), "option_type": option_type, "side": side,
            "quantity": int(lot)}


def vix_percentile(history, current, today):
    data = history.copy()
    data["datetime"] = pd.to_datetime(data["datetime"], errors="raise")
    lower = pd.Timestamp(today) - pd.DateOffset(years=1)
    data = data[(data["datetime"] >= lower) & (data["datetime"].dt.date < today)]
    if data["datetime"].duplicated().any() or len(data) < 200:
        raise ValueError("VIX percentile requires at least 200 distinct prior-year daily observations")
    closes = pd.to_numeric(data["close"], errors="raise")
    if not all(math.isfinite(value) and value > 0 for value in closes):
        raise ValueError("VIX history contains invalid closes")
    if (today - data["datetime"].min().date()).days < 350:
        raise ValueError("VIX history does not cover the preceding year")
    return 100.0 * int((closes < positive_price(current)).sum()) / len(closes)


def fly_credit(legs, field):
    credit = Decimal("0")
    for leg in legs:
        price = Decimal(str(positive_price(leg[field])))
        credit += price if leg["side"] == "SELL" else -price
    return money(credit)


def build_candidate(conn, master, cutoff):
    contracts, expiry = monthly_contracts(master, cutoff.date())
    spot, spot_time = candle_price(conn, index_id(master, "BSE", "SENSEX"), "IDX_I", "INDEX", cutoff)
    if cutoff - spot_time > timedelta(minutes=2):
        raise ValueError("Sensex index candle is stale")
    vix_id = edge.get_security_id("India VIX")
    vix, vix_time = candle_price(conn, vix_id, "IDX_I", "INDEX", cutoff)
    if cutoff - vix_time > timedelta(minutes=2):
        raise ValueError("India VIX candle is stale")
    history = edge.fetch_candles(conn, vix_id, "IDX_I", "INDEX",
                                 cutoff - timedelta(days=380), cutoff, "day")
    percentile = vix_percentile(history, vix, cutoff.date())
    base = {"strategy_version": STRATEGY_VERSION,
            "entry_date": cutoff.date().isoformat(), "cutoff": cutoff,
            "expiry": expiry.isoformat(), "vix": vix, "vix_percentile": percentile}
    if percentile <= 50:
        base.update(eligible=False, reason="VIX is not strictly above the prior-year 50th percentile")
        return base
    atm = int((Decimal(str(spot)) / STRIKE_INTERVAL).quantize(Decimal("1"), rounding=ROUND_HALF_UP)) * STRIKE_INTERVAL
    shorts = [contract(contracts, atm, "CE", "SELL"), contract(contracts, atm, "PE", "SELL")]
    for leg in shorts:
        leg["entry_price"], leg["price_time"] = candle_price(
            conn, leg["security_id"], "BSE_FNO", "OPTIDX", cutoff)
    width = math.ceil((shorts[0]["entry_price"] + shorts[1]["entry_price"]) / STRIKE_INTERVAL) * STRIKE_INTERVAL
    legs = [contract(contracts, atm - width, "PE", "BUY"),
            contract(contracts, atm + width, "CE", "BUY")] + shorts
    for leg in legs[:2]:
        leg["entry_price"], leg["price_time"] = candle_price(
            conn, leg["security_id"], "BSE_FNO", "OPTIDX", cutoff)
    if len({leg["quantity"] for leg in legs}) != 1:
        raise ValueError("The four Sensex contracts have inconsistent lot sizes")
    credit = fly_credit(legs, "entry_price")
    if not 0 < credit < width:
        raise ValueError(f"Invalid iron-fly net credit {credit} for wing width {width}")
    base.update(eligible=True, reason="VIX filter passed", spot=spot, atm=atm,
                wing_width=width, quantity=legs[0]["quantity"],
                credit_per_unit=credit, legs=legs)
    return base


def trade_id(expiry):
    return f"{STRATEGY_VERSION}:{expiry}"


def leg_charges(legs, price_field, closing=False):
    charges = []
    for leg in legs:
        side = leg["side"]
        if closing:
            side = "BUY" if side == "SELL" else "SELL"
        charges.append(order_charges("options", side, leg[price_field],
                                     leg["quantity"], exchange="BSE"))
    return sum_charges(*charges)


def open_trade(candidate):
    legs = candidate["legs"]
    return {"_id": trade_id(candidate["expiry"]), "strategy_version": STRATEGY_VERSION,
            "instrument_name": "SENSEX", "exchange_segment": "BSE_FNO", "simulated": True,
            "strategy_state": "active", "entry_date": candidate["entry_date"],
            "entry_time": "15:15", "expiry": candidate["expiry"],
            "quantity": candidate["quantity"], "lot_size": candidate["quantity"],
            "atm": candidate["atm"], "wing_width": candidate["wing_width"],
            "credit_per_unit": candidate["credit_per_unit"],
            "total_credit_received": money(candidate["credit_per_unit"] * candidate["quantity"]),
            "vix_percentile": candidate["vix_percentile"], "legs": legs,
            "pnl": 0.0, "running_pnl": 0.0, "net_pnl": None,
            "charges": leg_charges(legs, "entry_price")}


def checkpoint(now):
    clock = now.time().replace(second=0, microsecond=0)
    if clock not in CHECK_TIMES:
        return None
    return datetime.combine(now.date(), clock)


def close_reason(credit, closing_value, expiry, cutoff):
    if closing_value <= money(Decimal(str(credit)) * Decimal("0.5")):
        return "50% net-credit target"
    if cutoff.time() == ENTRY_TIME and (expiry - cutoff.date()).days <= 21:
        return "21 calendar DTE time exit"
    return None


def evaluate_trade(conn, trade, cutoff):
    legs = []
    for stored in trade["legs"]:
        leg = dict(stored)
        leg["exit_price"], leg["exit_price_time"] = candle_price(
            conn, leg["security_id"], "BSE_FNO", "OPTIDX", cutoff)
        legs.append(leg)
    closing_value = fly_credit(legs, "exit_price")
    if not 0 <= closing_value <= trade["wing_width"]:
        raise ValueError("Observed fly closing value is outside its structural bounds")
    pnl = money((Decimal(str(trade["credit_per_unit"])) - Decimal(str(closing_value))) * trade["quantity"])
    reason = close_reason(trade["credit_per_unit"], closing_value,
                          datetime.fromisoformat(trade["expiry"]).date(), cutoff)
    fields = {"last_check": cutoff.isoformat(), "running_pnl": pnl}
    if reason:
        charges = sum_charges(leg_charges(legs, "entry_price"),
                              leg_charges(legs, "exit_price", closing=True))
        fields.update(strategy_state="closed", exit_date=cutoff.date().isoformat(),
                      exit_time=cutoff.strftime("%H:%M"), exit_reason=reason,
                      legs=legs, pnl=pnl, charges=charges,
                      net_pnl=money(Decimal(str(pnl)) - Decimal(str(charges["total"]))))
    return fields


def sync_orders(trade, orders):
    """Paper fills are replayable from the trade; restart cannot duplicate orders."""
    for stage in ("entry", "exit"):
        if stage == "exit" and trade["strategy_state"] != "closed":
            continue
        for index, leg in enumerate(trade["legs"]):
            side = leg["side"]
            if stage == "exit":
                side = "BUY" if side == "SELL" else "SELL"
            key = f"{trade['_id']}:{stage}:{index}"
            record = {"_id": key, "trade_id": trade["_id"], "simulated": True,
                      "orderStatus": "TRADED", "exchangeSegment": "BSE_FNO",
                      "transactionType": side, "tradingSymbol": leg["symbol"],
                      "securityId": leg["security_id"], "quantity": leg["quantity"],
                      "filledQty": leg["quantity"], "averageTradedPrice": leg[f"{stage}_price"],
                      "price_time": leg["price_time" if stage == "entry" else "exit_price_time"]}
            orders.update_one({"_id": key}, {"$setOnInsert": record}, upsert=True)
