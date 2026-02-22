import sys
import os
from tamingnifty import connect_definedge as edge
from tamingnifty import utils as util
from tamingnifty import ta
from datetime import datetime, timedelta
from dateutil import parser
import pandas as pd
pd.set_option('display.max_rows', None)
import requests
import time
import zipfile
import sys
import os
import io
from retry import retry
from slack_sdk import WebClient
from pymongo import MongoClient
from dotenv import (  # pip install python-dotenv
    find_dotenv,
    load_dotenv,
)

dotenv_file: str = find_dotenv()
load_dotenv(dotenv_file)
slack_channel = "straddlebot"
slack_client = WebClient(token=os.environ.get('slack_token'))
user_name = os.environ.get('user_name')
CONNECTION_STRING = os.environ.get('CONNECTION_STRING')  #Mongo Connection
trade_end_time = parser.parse("15:28:00").time()
trade_start_time = parser.parse("09:16:00").time()

mongo_client = MongoClient(CONNECTION_STRING)
collection_name = "supertrend"

supertrend_collection = mongo_client['Bots'][collection_name]
instrument_name = "NIFTY"

strategies_collection_name = instrument_name.lower() + "_Straddle_" + user_name
orders_collection_name = "orders_" + instrument_name.lower() + "_Straddle_" + user_name

# trades collection
strategies = mongo_client['Bots'][strategies_collection_name]

# @retry(tries=5, delay=5, backoff=2)
def load_csv_from_zip(url='https://app.definedgesecurities.com/public/allmaster.zip'):
    column_names = ['SEGMENT', 'TOKEN', 'SYMBOL', 'TRADINGSYM', 'INSTRUMENT TYPE', 'EXPIRY', 'TICKSIZE', 'LOTSIZE', 'OPTIONTYPE', 'STRIKE', 'PRICEPREC', 'MULTIPLIER', 'ISIN', 'PRICEMULT', 'UnKnown']
    # Send a GET request to download the zip file
    response = requests.get(url)
    response.raise_for_status()  # This will raise an exception for HTTP errors
    # Open the zip file from the bytes-like object
    with zipfile.ZipFile(io.BytesIO(response.content)) as thezip:
        # Extract the name of the first CSV file in the zip archive
        csv_name = thezip.namelist()[0]
        # Extract and read the CSV file into a pandas DataFrame
        with thezip.open(csv_name) as csv_file:
            df = pd.read_csv(csv_file, header=None, names=column_names, low_memory=False, on_bad_lines='skip')
    df = df[(df['SEGMENT'] == 'NFO') & (df['INSTRUMENT TYPE'] == 'OPTIDX')]
    df = df[(df['SYMBOL'].str.startswith('NIFTY'))]
    df = df[df['SYMBOL'] == 'NIFTY']
    df['EXPIRY'] = df['EXPIRY'].astype(str).apply(lambda x: x.zfill(8))
    df['EXPIRY'] = pd.to_datetime(df['EXPIRY'], format='%d%m%Y', errors='coerce')
    df = df.sort_values(by='EXPIRY', ascending=True)
    # Return the loaded DataFrame
    return df



# @retry(tries=5, delay=5, backoff=2)
def get_option_symbol(strike=19950, option_type = "PE" ):
    df = load_csv_from_zip()
    df = df[df['TRADINGSYM'].str.contains(str(strike))]
    df = df[df['OPTIONTYPE'].str.match(option_type)]
    # Get the current date
    current_date = datetime.now()

    df= df[(df['EXPIRY'] > (current_date + timedelta(days=6)))]
    df = df.head(1)
    print("Getting options Symbol...")
    print(f"Symbol: {df['TRADINGSYM'].values[0]} , Expiry: {df['EXPIRY'].values[0]}")
    return df['TRADINGSYM'].values[0], df['EXPIRY'].values[0]

def get_straddle_chart(conn, strike=19950, CE_option_symbol=None, PE_option_symbol=None, expiry=None):
    if CE_option_symbol is None or PE_option_symbol is None:
        print("Fetching option symbols for the given strike...")
        # Fetch the option symbols for the given strike
        CE_option_symbol, expiry = get_option_symbol(strike=strike, option_type="CE")
        PE_option_symbol, expiry = get_option_symbol(strike=strike, option_type="PE")
        expiry = str(expiry)
    expiry = parser.parse(expiry).date()
    print(expiry)
    days_ago = expiry - timedelta(days=60)
    start = days_ago
    CE_ohlc = edge.fetch_historical_data(conn, 'NFO', CE_option_symbol, start, datetime.today(), 'min')
    PE_ohlc = edge.fetch_historical_data(conn, 'NFO', PE_option_symbol, start, datetime.today(), 'min')
    print(CE_ohlc[-20:])
    print(PE_ohlc[-20:])
    straddle = ta.straddle_chart(CE_ohlc,PE_ohlc)
    straddle = ta.convert_to_pnf(brick_size=0.5, df=straddle)
    return straddle, CE_option_symbol, PE_option_symbol


@retry(tries=5, delay=5, backoff=2)
def fetch_oi(conn, trading_symbol: str):
    try:
        quote = edge.fetch_historical_data(conn, 'NFO', trading_symbol, (datetime.now() - timedelta(days=7)), datetime.today(), 'min')
        return quote['oi'].iloc[-1]
    except Exception as e:
        print(f"Exception encountered: {e}. Retrying...")

def pcr(conn,atm=25700,multiple=100):
    atm_strike = atm
    call_oi = 0
    put_oi = 0
    for i in range(5):
        symbol, expiry = get_option_symbol(strike=atm, option_type="CE")
        call_oi += fetch_oi(conn, symbol)
        atm += multiple

    for i in range(5):
        symbol, expiry = get_option_symbol(strike=atm_strike, option_type="PE")
        put_oi += fetch_oi(conn, symbol)
        atm_strike -= multiple
    pcr_value = put_oi / call_oi if call_oi != 0 else float('inf')
    return round(pcr_value, 2)



def main():
    print("Option Straddle Signal is running...")
    util.notify(message=f"{instrument_name} Option Straddle Signal is Alive!", slack_client=slack_client, slack_channel=slack_channel)
    # Track the time when the last notification was sent
    last_notification_time = datetime.now()
    initialization_needed = True
    while True:
        current_time = datetime.now().time()
        # Calculate elapsed time since the last notification
        notification_time = datetime.now()

        # Calculate elapsed time since the last notification
        elapsed_time = notification_time - last_notification_time
        print(f"elapsed time: {elapsed_time}")
        if elapsed_time >= timedelta(hours=1):
            util.notify(message=f"{instrument_name} Option Straddle Signal is Alive!", slack_client=slack_client, slack_channel=slack_channel)
            util.notify(message=f"current time from {instrument_name} Option Straddle Signal: {current_time}", slack_client=slack_client, slack_channel=slack_channel)
            # Update the last notification time
            last_notification_time = notification_time
        conn = edge.login_to_integrate()
        if current_time > trade_start_time:
            if strategies.count_documents({'strategy_state': 'active'}) > 0:
                print("Active Option Position found. No analysis will be done.")
            elif current_time > parser.parse("09:29:00").time():
                if initialization_needed:
                    Nifty_ltp = edge.fetch_ltp(conn, "NSE", "Nifty 50")

                    delta = Nifty_ltp * 0.01 #can be used to calculate AT_PREV and ATM_NEXT strikes
                    atm_next_strike = util.round_to_nearest(Nifty_ltp + delta, base=50)
                    atm_prev_strike = util.round_to_nearest(Nifty_ltp - delta, base=50)

                    pcr_value = pcr(conn, atm=util.round_to_nearest(Nifty_ltp, base=50), multiple=50)

                    atm_next_AFT = False
                    atm_prev_AFT = False   

                    initialization_needed = False

                atm_next_straddle, atm_next_CE_option_symbol, atm_next_PE_option_symbol = get_straddle_chart(conn, strike=atm_next_strike)
                atm_prev_straddle, atm_prev_CE_option_symbol, atm_prev_PE_option_symbol = get_straddle_chart(conn, strike=atm_prev_strike)

                print(f"\n***** Printing ATM Next Straddle {atm_next_strike} *****\n")
                print(atm_next_straddle.iloc[-20:])
                print(f"\n***** Printing ATM Previous Straddle {atm_prev_strike} *****\n")
                print(atm_prev_straddle.iloc[-20:])
             
                if atm_next_straddle.iloc[-1]['double_bottom_sell'] == True:
                    # Check for Anchor coloumn at -3
                    if atm_next_straddle.iloc[-3]['count'] >=25:
                        atm_next_AFT = True
                    # Check for Anchor Coloumn at -5
                    elif atm_next_straddle.iloc[-5]['count'] >=25 and atm_next_straddle.iloc[-4:]['high'].max() <= atm_next_straddle.iloc[-5]['high']:
                        atm_next_AFT = True

                else:
                    atm_next_AFT = False

                if atm_prev_straddle.iloc[-1]['double_bottom_sell'] == True:
                    # Check for Anchor coloumn at -3
                    if atm_prev_straddle.iloc[-3]['count'] >=25:
                        atm_prev_AFT = True
                    # Check for Anchor Coloumn at -5
                    elif atm_prev_straddle.iloc[-5]['count'] >=25 and atm_prev_straddle.iloc[-4:]['high'].max() <= atm_prev_straddle.iloc[-5]['high']:
                        atm_prev_AFT = True
                else:
                    atm_prev_AFT = False


                if supertrend_collection.count_documents({"_id": "next_straddle"}) == 0:
                    st = {"_id": "next_straddle", "datetime": atm_next_straddle.iloc[-1]['datetime'], "straddle_close": atm_next_straddle.iloc[-1]['close'], "strike": atm_next_strike, "CE_option_symbol": atm_next_CE_option_symbol, "PE_option_symbol": atm_next_PE_option_symbol,"pcr": pcr_value,"AFT": atm_next_AFT}
                    supertrend_collection.insert_one(st)
                else:
                    supertrend_collection.update_one({"_id": "next_straddle"}, {"$set": {"datetime": atm_next_straddle.iloc[-1]['datetime'], "straddle_close": atm_next_straddle.iloc[-1]['close'], "strike": atm_next_strike, "CE_option_symbol": atm_next_CE_option_symbol, "PE_option_symbol": atm_next_PE_option_symbol, "pcr": pcr_value, "AFT": atm_next_AFT}})
                
                if supertrend_collection.count_documents({"_id": "prev_straddle"}) == 0:
                    st = {"_id": "prev_straddle", "datetime": atm_prev_straddle.iloc[-1]['datetime'], "straddle_close": atm_prev_straddle.iloc[-1]['close'], "strike": atm_prev_strike, "CE_option_symbol": atm_prev_CE_option_symbol, "PE_option_symbol": atm_prev_PE_option_symbol,"pcr": pcr_value,"AFT": atm_prev_AFT}
                    supertrend_collection.insert_one(st)    
                else:
                    supertrend_collection.update_one({"_id": "prev_straddle"}, {"$set": {"datetime": atm_prev_straddle.iloc[-1]['datetime'], "straddle_close": atm_prev_straddle.iloc[-1]['close'], "strike": atm_prev_strike, "CE_option_symbol": atm_prev_CE_option_symbol, "PE_option_symbol": atm_prev_PE_option_symbol, "pcr": pcr_value, "AFT": atm_prev_AFT}})

                
        print("repeating loop for Supertrend")
        if current_time > trade_end_time:
            util.notify("Closing Bell, Signal will exit now",slack_client=slack_client, slack_channel=slack_channel)
            return
        
        time.sleep(10)

if __name__ == "__main__":
    main()
