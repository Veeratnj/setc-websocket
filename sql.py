import sqlite3
from datetime import datetime
from SmartApi import SmartConnect
import pyotp
from smartWebSocketV2 import SmartWebSocketV2
from psql import insert_data
import threading
import time
import json
from datetime import datetime, timedelta

# --- Database Setup ---
def create_table():
    conn = sqlite3.connect('stock_data.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            uuid TEXT,
            stock_name TEXT,
            ltp REAL,
            last_update TEXT
        )
    ''')
    conn.commit()
    conn.close()

# def insert_data(uuid, stock_name, ltp):
#     conn = sqlite3.connect('stock_data.db')
#     cursor = conn.cursor()
#     last_update = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
#     # Check if the uuid already exists in the database
#     cursor.execute('SELECT * FROM stock_prices WHERE uuid = ?', (uuid,))
#     existing_record = cursor.fetchone()
    
#     if existing_record:
#         # If the record exists, update it
#         cursor.execute('''
#             UPDATE stock_prices
#             SET stock_name = ?, ltp = ?, last_update = ?
#             WHERE uuid = ?
#         ''', (stock_name, ltp, last_update, uuid))
#     else:
#         # If the record does not exist, insert it
#         cursor.execute('''
#             INSERT INTO stock_prices (uuid, stock_name, ltp, last_update)
#             VALUES (?, ?, ?, ?)
#         ''', (uuid, stock_name, ltp, last_update))
    
#     conn.commit()
#     conn.close()

# --- SmartAPI Setup ---
from creds import *  # Assuming `api_key`, `username`, `pwd`, `token` are defined here.

obj = SmartConnect(api_key=api_key)
data = obj.generateSession(username, pwd, pyotp.TOTP(token).now())

AUTH_TOKEN = data['data']['jwtToken']
FEED_TOKEN = data['data']['feedToken']
refresh_token = data['data']['refreshToken']

res = obj.getProfile(refresh_token)
print(res)

nifty_50_tokens = [
    "2885", "3045", "2765", "2855", "2775", "2675", "2535", "2785", "2735", "2925", 
    "3065", "3115", "3155", "3135", "3205", "3255", "3305", "3355", "3405", "3455", 
    "3505", "3555", "3605", "3655", "3705", "3755", "3805", "3855", "3905", "3955", 
    "4005", "4055", "4105", "4155", "4205", "4255", "4305", "4355", "4405", "4455", 
    "4505", "4555", "4605", "4655", "4705", "4755", "4805", "4855", "4905", "4955", 
    "5005", "5055", "5105", "5155", "5205", "5255", "5305", "5355", "5405", "5455"
]

stock_map={"99926000":"NIFTY50",
    "99926009":"BANKNIFTY",
    "99926037":"FINNIFTY",
    "3045":"SBIN-EQ"
}

# --- WebSocket Setup ---
token_list = [
    {"exchangeType": 1, "tokens": list(stock_map.keys())},  # Example: NSE tokens for RELIANCE and TCS
    # {"exchangeType": 1, "tokens": ["2885", "3045"]}
]

# Create SmartWebSocketV2 instance
sws = SmartWebSocketV2(auth_token=AUTH_TOKEN, api_key=api_key, feed_token=FEED_TOKEN, client_code=data['data']['clientcode'])

close_price_map = {}

def get_candle_key(timestamp):
    dt = datetime.fromtimestamp(timestamp / 1000)
    return dt.replace(second=0, microsecond=0, minute=(dt.minute // 5) * 5)


def get_candle_key(timestamp):
    dt = datetime.fromtimestamp(timestamp / 1000)
    # Get the next 5-minute mark, then subtract 1 second
    next_five_min = dt.replace(second=0, microsecond=0, minute=(dt.minute // 5) * 5) + timedelta(minutes=5)
    return next_five_min - timedelta(seconds=1)


def get_adjusted_close_time():
    now = datetime.now()
    # Round to the nearest 5th minute, then subtract 1 second
    minute = (now.minute // 5) * 5 + 4  # Get the 4th minute of the 5-min window
    adjusted_time = now.replace(minute=minute, second=59, microsecond=0)
    if adjusted_time < now:
        return adjusted_time
    else:
        return adjusted_time - timedelta(minutes=5)



def cleanup_old_closes():
    now = datetime.now()
    expired_keys = []

    for key, record in close_price_map.items():
        # If this 5-min bucket is older than 5 minutes, save and clean it
        if now - record['time'] > timedelta(minutes=5):
            stock_name = get_stock_name_from_token(record['token'])  # Your mapping function
            insert_data(str(record['token']), stock_name, record['ltp'])  # Your existing DB function
            print(f"[Saved] {stock_name} | ₹{record['ltp']} at {record['time']}")
            expired_keys.append(key)

    # Remove expired records from memory (only store the latest)
    for key in expired_keys:
        del close_price_map[key]

# --- WebSocket Callbacks ---
def on_data_v1(wsapp, message):
    print("Ticks received: {}".format(message))
    
    try:
        # Directly parse the message as we no longer expect a 'data' field
        token = message.get('token', None)
        ltp = message.get('last_traded_price', None)

        if token and ltp:
            # Assuming you can map `token` to a stock name, here it's just a placeholder
            stock_name = get_stock_name_from_token(token)  # You can implement a mapping function
            
            # Assuming `uuid` as the token or a generated value (you can adjust this)
            uuid = str(token)  # Replace this with actual UUID logic if necessary

            # Insert into the database
            insert_data(uuid, stock_name, ltp)
        else:
            print(f"Missing data for token {token}, LTP {ltp}")
    except Exception as e:
        print(f"Error processing message: {e}")

def get_stock_name_from_token(token):
    # This function can map token to stock name. For now, it's a simple placeholder.
    # You'll need to replace it with a more accurate mapping.
    stock_map = {
        "2885": "RELIANCE",
        "3045": "TCS",
        # Add more tokens and corresponding stock names here
    }
    stock_map = {
    "2885": "ADANIENT",
    "3045": "ADANIPORTS",
    "2765": "ASIANPAINT",
    "2855": "AXISBANK",
    "2775": "BAJAJ-AUTO",
    "2675": "BAJFINANCE",
    "2535": "BAJAJFINSV",
    "2785": "BPCL",
    "2735": "BHARTIARTL",
    "2925": "BRITANNIA",
    "3065": "CIPLA",
    "3115": "COALINDIA",
    "3155": "DIVISLAB",
    "3135": "DRREDDY",
    "3205": "EICHERMOT",
    "3255": "GRASIM",
    "3305": "HCLTECH",
    "3355": "HDFC",
    "3405": "HDFCBANK",
    "3455": "HEROMOTOCO",
    "3505": "HINDALCO",
    "3555": "HINDUNILVR",
    "3605": "ICICIBANK",
    "3655": "ITC",
    "3705": "INDUSINDBK",
    "3755": "INFY",
    "3805": "JSWSTEEL",
    "3855": "KOTAKBANK",
    "3905": "LT",
    "3955": "M&M",
    "4005": "MARUTI",
    "4055": "NTPC",
    "4105": "NESTLEIND",
    "4155": "ONGC",
    "4205": "POWERGRID",
    "4255": "RELIANCE",
    "4305": "SBILIFE",
    "4355": "SBIN",
    "4405": "SUNPHARMA",
    "4455": "TCS",
    "4505": "TATACONSUM",
    "4555": "TATAMOTORS",
    "4605": "TATASTEEL",
    "4655": "TECHM",
    "4705": "TITAN",
    "4755": "UPL",
    "4805": "ULTRACEMCO",
    "4855": "WIPRO",
    "4905": "HDFCLIFE",
    "4955": "APOLLOHOSP",
    "5005": "BAJAJHLDNG",
    "5055": "SBICARD",
    "5105": "ICICIPRULI",
    "5155": "SHREECEM",
    "5205": "PEL",
    "5255": "DLF",
    "5305": "NAUKRI",
    "5355": "VOLTAS",
    "5405": "LTI",
    "5455": "LTIM",}
    stock_map={"99926000":"NIFTY50",
    "99926009":"BANKNIFTY",
    "99926037":"FINNIFTY",
    "3045":"SBIN-EQ",
}
    

    return stock_map.get(str(token), "Unknown Stock")


def on_data(wsapp, message):
    try:
        # Check if message is a string (it needs to be parsed), otherwise assume it's already a dict
        if isinstance(message, str):
            data = json.loads(message)
        else:
            data = message  # If it's already a dict, use it directly

        # Handle if message is a list of ticks or a single tick
        ticks = data if isinstance(data, list) else [data]

        for tick in ticks:
            token = tick.get('token')
            ltp = tick.get('last_traded_price')
            timestamp = tick.get('exchange_timestamp')

            if token and ltp and timestamp:
                # Generate 5-min interval key
                candle_time = get_candle_key(timestamp)
                key = f"{token}_{candle_time}"

                # Convert to actual price
                price = ltp / 100  # Assuming LTP is in paise (like cents)

                # Update the latest LTP for this token in the current 5-min interval
                close_price_map[key] = {
                    'time': candle_time,
                    'token': token,
                    'ltp': price
                }

                # Clean up old (expired) entries and store the most recent close price
                cleanup_old_closes()

            else:
                print(f"Missing fields in tick: {tick}")
    except Exception as e:
        print(f"Error in on_data: {e}")


def on_open(wsapp):
    print("WebSocket connection opened")
    sws.subscribe('abc123', 1, token_list)  # Subscribe to token list

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
    create_table()  # Ensure the table exists
    sws.connect()    # Start WebSocket connection

    # Optionally, you can use threading to keep the WebSocket connection open
    # threading.Thread(target=sws.connect).start()

    # Keep the script running
    try:
        while True:
            time.sleep(1)  # Keeps the main thread alive to keep receiving data
    except KeyboardInterrupt:
        print("Process interrupted, closing WebSocket connection.")
        sws.close_connection()
