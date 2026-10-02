import os
import time
import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from aiohttp import web


# ============================================================
# CONFIG
# ============================================================

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")

THRESHOLD = 23.0

INTERVAL = "5m"
WINDOW_CANDLES = 25

SCAN_INTERVAL = 300  # 5 minutes

MAX_WORKERS = 20

SPOT_URL = "https://data-api.binance.vision"
FUTURES_URL = "https://fapi.binance.com"

PORT = int(os.environ.get("PORT", "10000"))

HEADERS = {
    "User-Agent": "TheKingdomRender/1.0"
}


# ============================================================
# STATE
# ============================================================

alerted = set()

state_lock = threading.Lock()

last_scan_time = 0
last_scan_duration = 0

spot_count = 0
futures_count = 0

scanner_running = False


# ============================================================
# HTTP SESSION
# ============================================================

session = requests.Session()

session.headers.update(HEADERS)


# ============================================================
# BINANCE API
# ============================================================

def api_get(base_url, path, params=None):

    url = base_url + path

    for attempt in range(3):

        try:

            response = session.get(
                url,
                params=params,
                timeout=20
            )

            if response.status_code == 200:

                return response.json()

            if response.status_code in (418, 429):

                retry_after = response.headers.get(
                    "Retry-After",
                    "5"
                )

                try:
                    wait = int(retry_after)
                except Exception:
                    wait = 5

                print(
                    f"Rate limited: {url} "
                    f"waiting {wait}s"
                )

                time.sleep(wait)

                continue

            print(
                f"API error {response.status_code}: "
                f"{url}"
            )

            return None

        except Exception as e:

            print(
                f"Request error: "
                f"{url} -> {e}"
            )

            time.sleep(2)

    return None


# ============================================================
# TELEGRAM
# ============================================================

def send_telegram(message):

    if not TELEGRAM_TOKEN or not CHAT_ID:

        print(
            "Telegram variables are not configured."
        )

        return False

    url = (
        "https://api.telegram.org/"
        f"bot{TELEGRAM_TOKEN}/sendMessage"
    )

    try:

        response = requests.post(
            url,
            data={
                "chat_id": CHAT_ID,
                "text": message
            },
            timeout=20
        )

        response.raise_for_status()

        result = response.json()

        if not result.get("ok"):

            print(
                "Telegram error:",
                result
            )

            return False

        return True

    except Exception as e:

        print(
            f"Telegram request failed: {e}"
        )

        return False


# ============================================================
# GET SPOT SYMBOLS
# ============================================================

def get_spot_symbols():

    print("Getting Binance Spot symbols...")

    data = api_get(
        SPOT_URL,
        "/api/v3/exchangeInfo"
    )

    if not data:

        raise Exception(
            "Could not get Spot exchangeInfo."
        )

    symbols = []

    for item in data.get("symbols", []):

        if (
            item.get("status") == "TRADING"
            and item.get("quoteAsset") == "USDT"
            and item.get("isSpotTradingAllowed") is True
        ):

            symbols.append(
                item["symbol"]
            )

    return symbols


# ============================================================
# GET FUTURES SYMBOLS
# ============================================================

def get_futures_symbols():

    print("Getting Binance Futures symbols...")

    data = api_get(
        FUTURES_URL,
        "/fapi/v1/exchangeInfo"
    )

    if not data:

        raise Exception(
            "Could not get Futures exchangeInfo."
        )

    symbols = []

    for item in data.get("symbols", []):

        if (
            item.get("status") == "TRADING"
            and item.get("quoteAsset") == "USDT"
            and item.get("contractType") == "PERPETUAL"
        ):

            symbols.append(
                item["symbol"]
            )

    return symbols


# ============================================================
# CHECK ONE COIN
# ============================================================

def check_coin(
    market,
    symbol
):

    if market == "SPOT":

        base_url = SPOT_URL
        path = "/api/v3/klines"

    else:

        base_url = FUTURES_URL
        path = "/fapi/v1/klines"

    candles = api_get(
        base_url,
        path,
        {
            "symbol": symbol,
            "interval": INTERVAL,
            "limit": WINDOW_CANDLES
        }
    )

    if not candles:

        return None

    if len(candles) < 20:

        return None

    try:

        # Price at beginning
        start_price = float(
            candles[0][1]
        )

        # Highest price during window
        highest_price = max(
            float(candle[2])
            for candle in candles
        )

        # Latest candle close
        current_price = float(
            candles[-1][4]
        )

        # Maximum movement
        change = (
            (highest_price - start_price)
            / start_price
        ) * 100

        return {
            "market": market,
            "symbol": symbol,
            "change": change,
            "current_price": current_price,
            "highest_price": highest_price,
            "start_price": start_price
        }

    except Exception as e:

        print(
            f"{market} {symbol}: "
            f"calculation error: {e}"
        )

        return None


# ============================================================
# FORMAT PRICE
# ============================================================

def format_price(price):

    if price >= 1000:
        return f"{price:.2f}"

    if price >= 1:
        return f"{price:.4f}"

    if price >= 0.01:
        return f"{price:.6f}"

    if price >= 0.0001:
        return f"{price:.8f}"

    return f"{price:.10f}"


# ============================================================
# CREATE ALERT MESSAGE
# ============================================================

def create_message(result):

    market = result["market"]
    symbol = result["symbol"]

    change = result["change"]

    current_price = result["current_price"]
    highest_price = result["highest_price"]
    start_price = result["start_price"]

    if market == "SPOT":

        market_icon = "🟢"
        market_name = "SPOT"

    else:

        market_icon = "🔴"
        market_name = "FUTURES"

    message = (
        "⚡ The Kingdom Alert\n\n"

        f"{market_icon} Market: {market_name}\n"
        f"🪙 {symbol}\n\n"

        f"📈 2H Move: +{change:.2f}%\n"
        f"💰 Current: {format_price(current_price)}\n"
        f"🔥 2H High: {format_price(highest_price)}\n"
        f"📍 2H Start: {format_price(start_price)}"
    )

    return message


# ============================================================
# PROCESS RESULT
# ============================================================

def process_result(result, new_alerts):

    if result is None:

        return

    market = result["market"]
    symbol = result["symbol"]

    key = f"{market}:{symbol}"

    change = result["change"]

    with state_lock:

        # ====================================================
        # NEW ALERT
        # ====================================================

        if change >= THRESHOLD:

            if key not in alerted:

                alerted.add(key)

                new_alerts.append(
                    result
                )

                print(
                    f"NEW ALERT: "
                    f"{market} "
                    f"{symbol} "
                    f"+{change:.2f}%"
                )

        # ====================================================
        # RESET
        # ====================================================

        else:

            if key in alerted:

                alerted.remove(key)

                print(
                    f"RESET: "
                    f"{market} "
                    f"{symbol}"
                )


# ============================================================
# SCAN MARKET
# ============================================================

def scan_market(
    market,
    symbols
):

    new_alerts = []

    total = len(symbols)

    completed = 0

    print(
        f"Scanning {market}: "
        f"{total} symbols..."
    )

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:

        futures = {
            executor.submit(
                check_coin,
                market,
                symbol
            ): symbol

            for symbol in symbols
        }

        for future in as_completed(
            futures
        ):

            symbol = futures[future]

            completed += 1

            try:

                result = future.result()

                process_result(
                    result,
                    new_alerts
                )

            except Exception as e:

                print(
                    f"{market} "
                    f"{symbol}: {e}"
                )

            if (
                completed % 100 == 0
                or completed == total
            ):

                print(
                    f"{market}: "
                    f"{completed}/{total}"
                )

    return new_alerts


# ============================================================
# COMPLETE SCAN
# ============================================================

def run_full_scan():

    global last_scan_time
    global last_scan_duration
    global spot_count
    global futures_count
    global scanner_running

    if scanner_running:

        print(
            "Previous scan is still running."
        )

        return

    scanner_running = True

    scan_start = time.time()

    print()
    print("========================================")
    print("THE KINGDOM RENDER SCANNER")
    print("========================================")

    print(
        f"Threshold: +{THRESHOLD}%"
    )

    print(
        "Window: approximately 2 hours"
    )

    print(
        "Candle: 5 minutes"
    )

    print(
        f"Workers: {MAX_WORKERS}"
    )

    print("========================================")

    try:

        # ----------------------------------------------------
        # GET SYMBOLS
        # ----------------------------------------------------

        spot_symbols = get_spot_symbols()

        print(
            f"Spot symbols: "
            f"{len(spot_symbols)}"
        )

        futures_symbols = get_futures_symbols()

        print(
            f"Futures symbols: "
            f"{len(futures_symbols)}"
        )

        spot_count = len(
            spot_symbols
        )

        futures_count = len(
            futures_symbols
        )

        # ----------------------------------------------------
        # SCAN SPOT
        # ----------------------------------------------------

        spot_alerts = scan_market(
            "SPOT",
            spot_symbols
        )

        # ----------------------------------------------------
        # SCAN FUTURES
        # ----------------------------------------------------

        futures_alerts = scan_market(
            "FUTURES",
            futures_symbols
        )

        new_alerts = (
            spot_alerts
            + futures_alerts
        )

        # ----------------------------------------------------
        # SEND TELEGRAM
        # ----------------------------------------------------

        print(
            f"New alerts: "
            f"{len(new_alerts)}"
        )

        for result in new_alerts:

            message = create_message(
                result
            )

            sent = send_telegram(
                message
            )

            if sent:

                print(
                    f"Telegram sent: "
                    f"{result['market']} "
                    f"{result['symbol']}"
                )

            else:

                print(
                    f"Telegram failed: "
                    f"{result['market']} "
                    f"{result['symbol']}"
                )

        # ----------------------------------------------------
        # FINISH
        # ----------------------------------------------------

        elapsed = (
            time.time()
            - scan_start
        )

        last_scan_duration = elapsed
        last_scan_time = time.time()

        print(
            "========================================"
        )

        print(
            f"Scan completed in "
            f"{elapsed:.1f}s"
        )

        print(
            f"Spot: {spot_count}"
        )

        print(
            f"Futures: {futures_count}"
        )

        print(
            f"New alerts: {len(new_alerts)}"
        )

        print(
            "========================================"
        )

    except Exception as e:

        print(
            "SCAN ERROR:",
            repr(e)
        )

    finally:

        scanner_running = False


# ============================================================
# BACKGROUND SCANNER
# ============================================================

async def scanner_loop():

    print(
        "Background scanner started."
    )

    # First scan immediately

    await asyncio.to_thread(
        run_full_scan
    )

    # Then every 5 minutes

    while True:

        await asyncio.sleep(
            SCAN_INTERVAL
        )

        await asyncio.to_thread(
            run_full_scan
        )


# ============================================================
# HEALTH ENDPOINT
# ============================================================

async def health(request):

    uptime = time.time()

    if last_scan_time:

        seconds_since_scan = (
            uptime - last_scan_time
        )

    else:

        seconds_since_scan = None

    with state_lock:

        alert_count = len(
            alerted
        )

    return web.json_response(
        {
            "status": "ok",
            "service": "the-kingdom-render",
            "scanner_running": scanner_running,
            "spot_symbols": spot_count,
            "futures_symbols": futures_count,
            "active_alerts": alert_count,
            "last_scan_seconds_ago": (
                round(seconds_since_scan, 1)
                if seconds_since_scan is not None
                else None
            ),
            "last_scan_duration": (
                round(last_scan_duration, 1)
            )
        }
    )


# ============================================================
# HOME PAGE
# ============================================================

async def home(request):

    return web.Response(
        text=(
            "The Kingdom Render Bot is running.\n\n"
            "Scanner: Spot + Futures\n"
            "Threshold: +23%\n"
            "Window: 2 hours\n"
            "Interval: 5 minutes\n\n"
            "Health: /health"
        ),
        content_type="text/plain"
    )


# ============================================================
# START SERVER
# ============================================================

async def main():

    app = web.Application()

    app.router.add_get(
        "/",
        home
    )

    app.router.add_get(
        "/health",
        health
    )

    # Start background scanner

    asyncio.create_task(
        scanner_loop()
    )

    runner = web.AppRunner(
        app
    )

    await runner.setup()

    site = web.TCPSite(
        runner,
        "0.0.0.0",
        PORT
    )

    await site.start()

    print()
    print("========================================")
    print("The Kingdom Render Bot is ONLINE")
    print(
        f"Port: {PORT}"
    )
    print("Health: /health")
    print("========================================")

    # Keep server alive

    while True:

        await asyncio.sleep(
            3600
        )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        print(
            "Bot stopped."
        )
