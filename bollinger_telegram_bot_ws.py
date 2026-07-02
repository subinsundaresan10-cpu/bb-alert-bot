"""
Bollinger Bands + 200 EMA Telegram Alert Bot (REAL-TIME / WebSocket version)
------------------------------------------------------------------------------
Monitors BTCUSDT, SOLUSDT, LTCUSDT on Binance (5m timeframe) using a live
WebSocket feed, so alerts fire the INSTANT price touches a Bollinger Band
(no REST polling delay, no waiting for candle close).

Logic:
  - Price ABOVE 200 EMA + touches LOWER band -> alert (uptrend pullback)
  - Price BELOW 200 EMA + touches UPPER band -> alert (downtrend pullback)
  - Only ONE alert per candle (per symbol), no matter how many times it
    touches the band within that same candle.

SETUP STEPS:
1. Create a Telegram bot via @BotFather -> get TOKEN.
2. Message the bot once, then open:
   https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates
   to find your CHAT_ID.
3. Install requirements:
   pip install requests pandas numpy websocket-client
4. Fill in TELEGRAM_TOKEN and TELEGRAM_CHAT_ID below.
5. Run: python bollinger_telegram_bot_ws.py
   (Keep it running in background - VPS / nohup / screen recommended for 24x7)
"""

import os
import json
import threading
import time
import requests
import pandas as pd
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
import websocket  # pip install websocket-client

# ============ CONFIG ============
# On Render: set these as Environment Variables in the dashboard (do NOT
# hardcode secrets if this repo is public on GitHub).
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "PUT_YOUR_BOT_TOKEN_HERE")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "PUT_YOUR_CHAT_ID_HERE")
PORT = int(os.environ.get("PORT", 10000))  # Render provides PORT automatically

SYMBOLS = ["BTCUSDT", "SOLUSDT", "LTCUSDT"]
TIMEFRAME = "5m"          # Binance kline interval
BB_LENGTH = 30             # Bollinger Bands period (SMA length)
BB_STDDEV = 2              # Standard deviation multiplier
EMA_LENGTH = 200           # EMA trend filter length
HISTORY_LIMIT = 300        # historical candles to preload (must be > EMA_LENGTH)
# ================================================

BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
BINANCE_WS_URL = "wss://stream.binance.com:9443/stream?streams=" + "/".join(
    f"{s.lower()}@kline_{TIMEFRAME}" for s in SYMBOLS
)

# Per-symbol historical candle DataFrame (closed candles + live-updating last row)
history = {}
lock = threading.Lock()

# Per-symbol: open_time of the candle we already alerted for (one alert per candle)
last_alerted_candle = {symbol: None for symbol in SYMBOLS}


def send_telegram_message(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        resp = requests.post(url, data=payload, timeout=10)
        if resp.status_code != 200:
            print(f"[Telegram Error] {resp.status_code}: {resp.text}")
    except Exception as e:
        print(f"[Telegram Exception] {e}")


def preload_history(symbol: str):
    """Fetch closed historical candles once at startup so BB(30) and EMA(200) are accurate."""
    params = {"symbol": symbol, "interval": TIMEFRAME, "limit": HISTORY_LIMIT}
    resp = requests.get(BINANCE_KLINES_URL, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    df = pd.DataFrame(data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "num_trades",
        "taker_buy_base", "taker_buy_quote", "ignore"
    ])
    for col in ["open", "high", "low", "close"]:
        df[col] = df[col].astype(float)
    return df[["open_time", "open", "high", "low", "close"]]


def calculate_indicators(df: pd.DataFrame):
    sma = df["close"].rolling(window=BB_LENGTH).mean()
    std = df["close"].rolling(window=BB_LENGTH).std()
    upper_band = sma + (std * BB_STDDEV)
    lower_band = sma - (std * BB_STDDEV)
    ema200 = df["close"].ewm(span=EMA_LENGTH, adjust=False).mean()
    return upper_band, lower_band, ema200


def evaluate(symbol: str):
    df = history[symbol]
    upper, lower, ema200 = calculate_indicators(df)

    last_open_time = df["open_time"].iloc[-1]
    last_high = df["high"].iloc[-1]
    last_low = df["low"].iloc[-1]
    last_close = df["close"].iloc[-1]
    last_upper = upper.iloc[-1]
    last_lower = lower.iloc[-1]
    last_ema = ema200.iloc[-1]

    if pd.isna(last_upper) or pd.isna(last_lower) or pd.isna(last_ema):
        return

    above_ema = last_close > last_ema
    below_ema = last_close < last_ema

    touched_lower = last_low <= last_lower
    touched_upper = last_high >= last_upper

    triggered_state = None
    if above_ema and touched_lower:
        triggered_state = "lower"
    elif below_ema and touched_upper:
        triggered_state = "upper"

    if triggered_state and last_alerted_candle[symbol] != last_open_time:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        if triggered_state == "lower":
            msg = (f"🟢 <b>{symbol}</b> — LOWER Band touch (price ABOVE 200 EMA)\n"
                   f"Price: {last_close:.4f} | Low: {last_low:.4f}\n"
                   f"Lower Band: {last_lower:.4f}\n"
                   f"200 EMA: {last_ema:.4f}\n"
                   f"Timeframe: {TIMEFRAME}\n{now}")
        else:
            msg = (f"🔴 <b>{symbol}</b> — UPPER Band touch (price BELOW 200 EMA)\n"
                   f"Price: {last_close:.4f} | High: {last_high:.4f}\n"
                   f"Upper Band: {last_upper:.4f}\n"
                   f"200 EMA: {last_ema:.4f}\n"
                   f"Timeframe: {TIMEFRAME}\n{now}")
        print(msg.replace("\n", " | "))
        send_telegram_message(msg)
        last_alerted_candle[symbol] = last_open_time


def on_message(ws, message):
    payload = json.loads(message)
    data = payload.get("data", {})
    k = data.get("k")
    if not k:
        return

    symbol = data["s"]  # e.g. BTCUSDT
    open_time = k["t"]
    candle = {
        "open_time": open_time,
        "open": float(k["o"]),
        "high": float(k["h"]),
        "low": float(k["l"]),
        "close": float(k["c"]),
    }

    with lock:
        df = history[symbol]
        if df["open_time"].iloc[-1] == open_time:
            # same candle still forming -> update last row in place (live tick)
            df.iloc[-1, df.columns.get_loc("high")] = candle["high"]
            df.iloc[-1, df.columns.get_loc("low")] = candle["low"]
            df.iloc[-1, df.columns.get_loc("close")] = candle["close"]
        else:
            # new candle started -> append, drop oldest to keep window bounded
            history[symbol] = pd.concat(
                [df, pd.DataFrame([candle])], ignore_index=True
            ).iloc[-HISTORY_LIMIT:].reset_index(drop=True)

        evaluate(symbol)


def on_error(ws, error):
    print(f"[WebSocket Error] {error}")


def on_close(ws, close_status_code, close_msg):
    print("[WebSocket] Connection closed.")


def on_open(ws):
    print("[WebSocket] Connected. Watching:", ", ".join(SYMBOLS))
    send_telegram_message(
        f"✅ Real-time BB(30,2) + 200 EMA alert bot connected.\nWatching: {', '.join(SYMBOLS)}"
    )


class HealthCheckHandler(BaseHTTPRequestHandler):
    """Minimal HTTP endpoint so Render sees an open port, and UptimeRobot
    can ping this URL every few minutes to keep the free instance awake."""
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Bollinger bot is running.")

    def do_HEAD(self):
        # UptimeRobot (and many uptime monitors) send HEAD requests by
        # default. Without this, Python's BaseHTTPRequestHandler replies
        # 501 Not Implemented, which UptimeRobot reports as "Down".
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()

    def log_message(self, format, *args):
        pass  # silence default request logging


def start_health_server():
    server = HTTPServer(("0.0.0.0", PORT), HealthCheckHandler)
    print(f"[HealthCheck] Listening on port {PORT}")
    server.serve_forever()


def main():
    # Start the tiny HTTP server in a background thread (Render requires
    # a web service to bind to $PORT; also used as the UptimeRobot ping target)
    threading.Thread(target=start_health_server, daemon=True).start()

    print("Preloading historical candles for accurate BB/EMA...")
    for symbol in SYMBOLS:
        history[symbol] = preload_history(symbol)
        print(f"  {symbol}: {len(history[symbol])} candles loaded")

    print(f"Connecting to Binance WebSocket ({TIMEFRAME} klines)...")
    ws = websocket.WebSocketApp(
        BINANCE_WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    # ping_interval keeps the connection alive; auto-reconnect loop below
    while True:
        try:
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            print(f"[Reconnect] WebSocket dropped: {e}. Reconnecting in 5s...")
        time.sleep(5)


if __name__ == "__main__":
    main()
