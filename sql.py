import ast
import time
import threading
from datetime import datetime, timedelta
import pytz
from SmartApi import SmartConnect
import pyotp
from smartWebSocketV2 import SmartWebSocketV2
from psql import insert_data, stock_token, insert_ohlc_data
import pandas as pd
import logging

# Timezone
ist = pytz.timezone("Asia/Kolkata")

# Credentials
from creds import *  # api_key, username, pwd, token

# SmartAPI Setup
obj = SmartConnect(api_key=api_key)
data = obj.generateSession(username, pwd, pyotp.TOTP(token).now())

AUTH_TOKEN = data['data']['jwtToken']
FEED_TOKEN = data['data']['feedToken']
refresh_token = data['data']['refreshToken']
res = obj.getProfile(refresh_token)

# Stock Map
stock_map = stock_token()
print("Subscribed tokens:", list(stock_map.keys()))

# Candle State
interval_minutes = 5
current_candle = {}
current_interval_start = {}

for token in stock_map.keys():
    current_candle[token] = None
    current_interval_start[token] = None

# WebSocket Setup
subscribe_token_list = [{"exchangeType": 1, "tokens": list(stock_map.keys())}]
sws = SmartWebSocketV2(auth_token=AUTH_TOKEN, api_key=api_key, feed_token=FEED_TOKEN, client_code=data['data']['clientcode'])

def round_down_time(dt, delta_minutes=5):
    return dt - timedelta(
        minutes=dt.minute % delta_minutes,
        seconds=dt.second,
        microseconds=dt.microsecond
    )

def get_historical_data(
    smart_api_obj,
    exchange,
    symboltoken,
    interval,
    fromdate,
    todate,
    max_retries=5,
    save_to_csv=False,
    retry_delay=1.5
):
    """Fetch historical data with retries."""
    historic_param = {
        "exchange": exchange,
        "symboltoken": symboltoken,
        "interval": interval,
        "fromdate": fromdate,
        "todate": todate,
    }

    for attempt in range(1, max_retries + 1):
        try:
            raw_data = smart_api_obj.getCandleData(historic_param)

            if not raw_data or 'data' not in raw_data or not raw_data['data']:
                print(f"[{symboltoken}] No data returned.")
                return None

            df = pd.DataFrame(raw_data['data'], columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'])

            if save_to_csv:
                file_name = f"{symboltoken}.csv"
                df.to_csv(file_name, index=False)
                print(f"[{symboltoken}] Data saved to {file_name}")

            return df

        except Exception as e:
            err_msg = str(e)
            print(f"[{symboltoken}] Error: {err_msg}")

            if "Access denied" in err_msg or "exceeding access rate" in err_msg:
                time.sleep(retry_delay * attempt)
            else:
                time.sleep(retry_delay)

    print(f"[{symboltoken}] Failed after {max_retries} retries.")
    return None

def on_data(wsapp, message):
    try:
        token = str(message.get('token'))
        ltp = message.get('last_traded_price')
        ts_epoch = message.get('exchange_timestamp')

        if token not in stock_map or ltp is None:
            return

        ltp = float(ltp) / 100
        stock_name = stock_map[token]
        insert_data(token, stock_name, ltp)

        tick_time = datetime.utcfromtimestamp(ts_epoch / 1000).replace(tzinfo=pytz.utc).astimezone(ist)
        interval_start = round_down_time(tick_time, interval_minutes)

        if current_interval_start[token] is None:
            current_interval_start[token] = interval_start
            current_candle[token] = {
                'start_time': interval_start.strftime("%Y-%m-%d %H:%M:%S"),
                'open': ltp,
                'high': ltp,
                'low': ltp,
                'close': ltp
            }
            return

        if interval_start > current_interval_start[token]:
            completed_candle = current_candle[token]

            insert_ohlc_data(
                token=token,
                start_time=completed_candle['start_time'],
                open_=completed_candle['open'],
                high=completed_candle['high'],
                low=completed_candle['low'],
                close=completed_candle['close'],
                interval='5m'
            )

            print(f"[REALTIME CANDLE SAVED] {token}: {completed_candle}")

            current_interval_start[token] = interval_start
            current_candle[token] = {
                'start_time': interval_start.strftime("%Y-%m-%d %H:%M:%S"),
                'open': ltp,
                'high': ltp,
                'low': ltp,
                'close': ltp
            }
            return

        candle = current_candle[token]
        candle['high'] = max(candle['high'], ltp)
        candle['low'] = min(candle['low'], ltp)
        candle['close'] = ltp

    except Exception as e:
        print(f"[ERROR] {e}")

def fetch_and_store_historical():
    while True:
        try:
            now = datetime.now(ist)
            from_time = (now - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M")
            to_time = now.strftime("%Y-%m-%d %H:%M")

            for token in stock_map.keys():
                df = get_historical_data(
                    smart_api_obj=obj,
                    exchange="NSE",
                    symboltoken=token,
                    interval="FIVE_MINUTE",
                    fromdate=from_time,
                    todate=to_time,
                    save_to_csv=False
                )

                if df is not None and not df.empty:
                    last_row = df.iloc[-1]
                    insert_ohlc_data(
                        token=token,
                        start_time=str(last_row['timestamp']),
                        open_=float(last_row['open']),
                        high=float(last_row['high']),
                        low=float(last_row['low']),
                        close=float(last_row['close']),
                        interval='5m'
                    )
                    print(f"[HISTORICAL CANDLE SAVED] {token}: {last_row.to_dict()}")
                else:
                    print(f"[{token}] No historical data.")

            time.sleep(300)  # Wait for 5 minutes

        except Exception as e:
            print(f"[HISTORICAL THREAD ERROR] {e}")
            time.sleep(300)

def on_open(wsapp):
    print("WebSocket connection opened")
    sws.subscribe('abc123', 1, subscribe_token_list)

def on_error(wsapp, error):
    print(f"WebSocket error: {error}")

def on_close(wsapp):
    print("WebSocket closed")

# WebSocket callbacks
sws.on_open = on_open
sws.on_data = on_data
sws.on_error = on_error
sws.on_close = on_close


def wait_until_915():
    now = datetime.now()
    target = now.replace(hour=9, minute=15, second=0, microsecond=0)

    if now < target:
        wait_seconds = (target - now).total_seconds()
        print(f"Waiting until 9:15 AM to start... ({int(wait_seconds)} seconds)")
        time.sleep(wait_seconds)
    else:
        print("It's 9:15 AM or later, starting immediately.")


if __name__ == "__main__":
    wait_until_915()
    threading.Thread(target=fetch_and_store_historical, daemon=True).start()

    sws.connect()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Process interrupted, closing WebSocket.")
        sws.close_connection()
