import ast
import sqlite3
from datetime import datetime
from SmartApi import SmartConnect
import pyotp
from smartWebSocketV2 import SmartWebSocketV2
from psql import insert_data,stock_token,insert_ohlc_data
import threading
import time
import json
from datetime import datetime, timedelta
import pytz
ist = pytz.timezone("Asia/Kolkata")

stock_map=stock_token()
print(list(stock_map.keys()))
current_candle = {}
current_interval_start = {}
candles = {}
interval_minutes = 5
# Initialize per-token state
for token in list(stock_map.keys()):
    current_candle[token] = None
    current_interval_start[token] = None
    candles[token] = []



# --- SmartAPI Setup ---
from creds import *  # Assuming `api_key`, `username`, `pwd`, `token` are defined here.

obj = SmartConnect(api_key=api_key)
data = obj.generateSession(username, pwd, pyotp.TOTP(token).now())

AUTH_TOKEN = data['data']['jwtToken']
FEED_TOKEN = data['data']['feedToken']
refresh_token = data['data']['refreshToken']

res = obj.getProfile(refresh_token)

# --- WebSocket Setup ---
subscribe_token_list = [
    {"exchangeType": 1, "tokens": list(stock_map.keys())},  # Example: NSE tokens for RELIANCE and TCS
]

# Create SmartWebSocketV2 instance
sws = SmartWebSocketV2(auth_token=AUTH_TOKEN, api_key=api_key, feed_token=FEED_TOKEN, client_code=data['data']['clientcode'])

def round_down_time(dt, delta_minutes=5):
    return dt - timedelta(
        minutes=dt.minute % delta_minutes,
        seconds=dt.second,
        microseconds=dt.microsecond
    )

def get_stock_name_from_token(token):
    return stock_map.get(str(token), "Unknown Stock")



# --- WebSocket Callbacks ---
def on_data(wsapp, message):
    print("Ticks received: {}".format(message))

    try:
        # Directly parse the message as we no longer expect a 'data' field
        token = message.get('token', None)
        ltp = message.get('last_traded_price', None)
        ts_epoch = message.get('exchange_timestamp')

        if token and ltp:
            ltp = float(ltp)/100
            stock_name = get_stock_name_from_token(token)  # You can implement a mapping function
            print(stock_name)
            uuid = str(token)  # Replace this with actual UUID logic if necessary
            insert_data(uuid, stock_name, ltp)
            # ------------------------------
            # Start OHLC logic (on_tick part)
            # ------------------------------
            ts = datetime.utcfromtimestamp(ts_epoch / 1000).replace(tzinfo=pytz.utc).astimezone(ist)
            interval_start = round_down_time(ts, interval_minutes)
            print(current_interval_start)
            print(interval_start)
            print(current_interval_start[token])
            if current_interval_start[token] is None or interval_start > current_interval_start[token]:
                if current_candle[token] is not None:
                    candles[token].append(current_candle[token])
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
                    print('hi')

                current_interval_start[token] = interval_start
                current_candle[token] = {
                    'start_time': interval_start.strftime("%Y-%m-%d %H:%M:%S"),
                    'open': ltp,
                    'high': ltp,
                    'low': ltp,
                    'close': ltp
                }
            else:
                candle = current_candle[token]
                candle['high'] = max(candle['high'], ltp)
                candle['low'] = min(candle['low'], ltp)
                candle['close'] = ltp
        else:
            print(f"Missing data for token {token}, LTP {ltp}")
    except Exception as e:
        print(f"Error processing message: {e}")
        time.sleep(10)


def on_open(wsapp):
    print("WebSocket connection opened",dir(wsapp))
    sws.subscribe('abc123', 1, subscribe_token_list)  # Subscribe to token list

def on_error(wsapp, error):
    print(f"WebSocket error: {error}")

def on_close(wsapp):
    print("WebSocket connection closed")

# Assign the callback functions to WebSocket events
sws.on_open = on_open
sws.on_data = on_data
sws.on_error = on_error
sws.on_close = on_close

# --- Main Execution ---
if __name__ == "__main__":
    # 'for testing'
    # with open(r'11-06-2025\99926037.txt', 'r') as file:
    #     data=file.read()
    # data = data.split('\n')
    # dict_list = [ast.literal_eval(item) for item in data]
    # for i,item in enumerate(dict_list):
    #     on_data(message=item,wsapp=None)
    

    'for production'
    sws.connect()    # Start WebSocket connection
    try:
        while True:
            time.sleep(1)  # Keeps the main thread alive to keep receiving data
    except KeyboardInterrupt:
        print("Process interrupted, closing WebSocket connection.")
        sws.close_connection()