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

SCAN_INTERVAL = 300

MAX_WORKERS = 15

PORT = int(os.environ.get("PORT", "10000"))


# ============================================================
# BINANCE ENDPOINTS
# ============================================================

SPOT_ENDPOINTS = [
    "https://data-api.binance.vision",
    "https://api.binance.com",
]

FUTURES_ENDPOINTS = [
    "https://fapi.binance.com",
]


# ============================================================
# HEADERS
# ============================================================

HEADERS = {
    "User-Agent": "TheKingdomRender/2.0",
    "Accept": "application/json",
}


# ============================================================
# STATE
# ============================================================

alerted = set()

state_lock = threading.Lock()

scanner_running = False

last_scan_time = 0
last_scan_duration = 0

spot_count = 0
futures_count = 0

spot_api_status = "not tested"
futures_api_status = "not tested"

last_error = ""


# ============================================================
# HTTP SESSION
# ============================================================

session = requests.Session()

session.headers.update(HEADERS)


# ============================================================
# LOGGING
# ============================================================

def log(message):

    print(
        message,
        flush=True
    )


# ============================================================
# BINANCE REQUEST
# ============================================================

def api_get(
    endpoints,
    path,
    params=None,
    label="API"
):

    last_error_message = None

    for base_url in endpoints:

        url = base_url + path

        log(
            f"[{label}] GET {url}"
        )

        for attempt in range(2):

            try:

                response = session.get(
                    url,
                    params=params,
                    timeout=10
                )

                log(
                    f"[{label}] "
                    f"HTTP {response.status_code} "
                    f"from {base_url}"
                )

                if response.status_code == 200:

                    return response.json()

                if response.status_code in (418, 429):

                    wait = int(
                        response.headers.get(
                            "Retry-After",
                            "5"
                        )
                    )

                    log(
                        f"[{label}] "
                        f"Rate limited. "
                        f"Waiting {wait}s"
                    )

                    time.sleep(
                        min(wait, 15)
                    )

                    continue

                text = response.text[:300]

                log(
                    f"[{label}] "
                    f"Error body: {text}"
                )

                last_error_message = (
                    f"HTTP {response.status_code} "
                    f"from {base_url}"
                )

                break

            except requests.exceptions.Timeout:

                log(
                    f"[{label}] "
                    f"TIMEOUT from {base_url}"
                )

                last_error_message = (
                    f"Timeout from {base_url}"
                )

            except requests.exceptions.RequestException as e:

                log(
                    f"[{label}] "
                    f"REQUEST ERROR: {e}"
                )

                last_error_message = str(e)

            except Exception as e:

                log(
                    f"[{label}] "
                    f"ERROR: {e}"
                )

                last_error_message = str(e)

            if attempt == 0:

                time.sleep(1)

    raise Exception(
        last_error_message
        or f"{label} request failed"
    )


# ============================================================
# TELEGRAM
# ============================================================

def send_telegram(message):

    if not TELEGRAM_TOKEN:

        log(
            "[TELEGRAM] "
            "TELEGRAM_TOKEN is missing"
        )

        return False

    if not CHAT_ID:

        log(
            "[TELEGRAM] "
            "CHAT_ID is missing"
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
            timeout=10
        )

        result = response.json()

        if result.get("ok"):

            return True

        log(
            f"[TELEGRAM] Error: "
            f"{result}"
        )

        return False

    except Exception as e:

        log(
            f"[TELEGRAM] "
            f"Request error: {e}"
        )

        return False


# ============================================================
# SPOT SYMBOLS
# ============================================================

def get_spot_symbols():

    global spot_api_status
    global last_error

    log("")
    log(
        "================================"
    )
    log(
        "TESTING BINANCE SPOT API"
    )
    log(
        "================================"
    )

    try:

        data = api_get(
            SPOT_ENDPOINTS,
            "/api/v3/exchangeInfo",
            label="SPOT"
        )

        symbols = []

        for item in data.get(
            "symbols",
            []
        ):

            if (
                item.get("status")
                == "TRADING"
                and item.get("quoteAsset")
                == "USDT"
                and item.get(
                    "isSpotTradingAllowed"
                ) is True
            ):

                symbols.append(
                    item["symbol"]
                )

        spot_api_status = (
            f"OK ({len(symbols)} symbols)"
        )

        log(
            f"[SPOT] SUCCESS: "
            f"{len(symbols)} symbols"
        )

        return symbols

    except Exception as e:

        spot_api_status = (
            f"FAILED: {e}"
        )

        last_error = (
            f"Spot API: {e}"
        )

        log(
            f"[SPOT] FAILED: {e}"
        )

        return []


# ============================================================
# FUTURES SYMBOLS
# ============================================================

def get_futures_symbols():

    global futures_api_status
    global last_error

    log("")
    log(
        "================================"
    )
    log(
        "TESTING BINANCE FUTURES API"
    )
    log(
        "================================"
    )

    try:

        data = api_get(
            FUTURES_ENDPOINTS,
            "/fapi/v1/exchangeInfo",
            label="FUTURES"
        )

        symbols = []

        for item in data.get(
            "symbols",
            []
        ):

            if (
                item.get("status")
                == "TRADING"
                and item.get("quoteAsset")
                == "USDT"
                and item.get("contractType")
                == "PERPETUAL"
            ):

                symbols.append(
                    item["symbol"]
                )

        futures_api_status = (
            f"OK ({len(symbols)} symbols)"
        )

        log(
            f"[FUTURES] SUCCESS: "
            f"{len(symbols)} symbols"
        )

        return symbols

    except Exception as e:

        futures_api_status = (
            f"FAILED: {e}"
        )

        last_error = (
            f"Futures API: {e}"
        )

        log(
            f"[FUTURES] FAILED: {e}"
        )

        return []


# ============================================================
# CHECK ONE COIN
# ============================================================

def check_coin(
    market,
    symbol
):

    if market == "SPOT":

        endpoints = SPOT_ENDPOINTS
        path = "/api/v3/klines"

    else:

        endpoints = FUTURES_ENDPOINTS
        path = "/fapi/v1/klines"

    try:

        candles = api_get(
            endpoints,
            path,
            {
                "symbol": symbol,
                "interval": INTERVAL,
                "limit": WINDOW_CANDLES
            },
            label=f"{market}-{symbol}"
        )

        if not candles:

            return None

        if len(candles) < 20:

            return None

        start_price = float(
            candles[0][1]
        )

        highest_price = max(
            float(candle[2])
            for candle in candles
        )

        current_price = float(
            candles[-1][4]
        )

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

        return None


# ============================================================
# PRICE FORMAT
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
# TELEGRAM MESSAGE
# ============================================================

def create_message(result):

    market = result["market"]

    if market == "SPOT":

        icon = "🟢"
        name = "SPOT"

    else:

        icon = "🔴"
        name = "FUTURES"

    return (
        "⚡ The Kingdom Alert\n\n"
        f"{icon} Market: {name}\n"
        f"🪙 {result['symbol']}\n\n"
        f"📈 2H Move: "
        f"+{result['change']:.2f}%\n"
        f"💰 Current: "
        f"{format_price(result['current_price'])}\n"
        f"🔥 2H High: "
        f"{format_price(result['highest_price'])}\n"
        f"📍 2H Start: "
        f"{format_price(result['start_price'])}"
    )


# ============================================================
# PROCESS RESULT
# ============================================================

def process_result(
    result,
    new_alerts
):

    if result is None:

        return

    market = result["market"]
    symbol = result["symbol"]

    key = (
        f"{market}:{symbol}"
    )

    change = result["change"]

    with state_lock:

        if change >= THRESHOLD:

            if key not in alerted:

                alerted.add(key)

                new_alerts.append(
                    result
                )

                log(
                    f"🚨 NEW ALERT "
                    f"{market} "
                    f"{symbol} "
                    f"+{change:.2f}%"
                )

        else:

            if key in alerted:

                alerted.remove(key)

                log(
                    f"RESET "
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

    if not symbols:

        log(
            f"[{market}] "
            "No symbols to scan."
        )

        return []

    new_alerts = []

    total = len(symbols)

    completed = 0

    log("")
    log(
        f"========== {market} SCAN =========="
    )

    log(
        f"[{market}] "
        f"Symbols: {total}"
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

            completed += 1

            try:

                result = future.result()

                process_result(
                    result,
                    new_alerts
                )

            except Exception as e:

                symbol = futures[
                    future
                ]

                log(
                    f"[{market}] "
                    f"{symbol}: {e}"
                )

            if (
                completed % 100 == 0
                or completed == total
            ):

                log(
                    f"[{market}] "
                    f"{completed}/{total}"
                )

    log(
        f"[{market}] Scan finished."
    )

    return new_alerts


# ============================================================
# FULL SCAN
# ============================================================

def run_full_scan():

    global scanner_running
    global last_scan_time
    global last_scan_duration
    global spot_count
    global futures_count

    if scanner_running:

        log(
            "Previous scan is still running."
        )

        return

    scanner_running = True

    start_time = time.time()

    log("")
    log(
        "========================================"
    )
    log(
        "THE KINGDOM RENDER SCANNER"
    )
    log(
        "========================================"
    )

    log(
        f"Threshold: +{THRESHOLD}%"
    )

    log(
        "Window: approximately 2 hours"
    )

    log(
        "Candle: 5 minutes"
    )

    log(
        f"Workers: {MAX_WORKERS}"
    )

    log(
        "========================================"
    )

    try:

        # ----------------------------------------------------
        # SPOT
        # ----------------------------------------------------

        spot_symbols = get_spot_symbols()

        spot_count = len(
            spot_symbols
        )

        # ----------------------------------------------------
        # FUTURES
        # ----------------------------------------------------

        futures_symbols = (
            get_futures_symbols()
        )

        futures_count = len(
            futures_symbols
        )

        log("")
        log(
            "========================================"
        )

        log(
            f"Spot symbols: {spot_count}"
        )

        log(
            f"Futures symbols: {futures_count}"
        )

        log(
            "========================================"
        )

        # ----------------------------------------------------
        # SCAN
        # ----------------------------------------------------

        all_alerts = []

        if spot_symbols:

            spot_alerts = scan_market(
                "SPOT",
                spot_symbols
            )

            all_alerts.extend(
                spot_alerts
            )

        else:

            log(
                "Spot scan skipped."
            )

        if futures_symbols:

            futures_alerts = scan_market(
                "FUTURES",
                futures_symbols
            )

            all_alerts.extend(
                futures_alerts
            )

        else:

            log(
                "Futures scan skipped."
            )

        # ----------------------------------------------------
        # TELEGRAM
        # ----------------------------------------------------

        log("")
        log(
            f"New alerts: "
            f"{len(all_alerts)}"
        )

        for result in all_alerts:

            message = create_message(
                result
            )

            if send_telegram(
                message
            ):

                log(
                    f"Telegram sent: "
                    f"{result['market']} "
                    f"{result['symbol']}"
                )

            else:

                log(
                    f"Telegram failed: "
                    f"{result['market']} "
                    f"{result['symbol']}"
                )

        # ----------------------------------------------------
        # FINISH
        # ----------------------------------------------------

        elapsed = (
            time.time()
            - start_time
        )

        last_scan_duration = elapsed
        last_scan_time = time.time()

        log("")
        log(
            "========================================"
        )

        log(
            f"SCAN COMPLETED "
            f"in {elapsed:.1f}s"
        )

        log(
            f"Spot: {spot_count}"
        )

        log(
            f"Futures: {futures_count}"
        )

        log(
            f"New alerts: {len(all_alerts)}"
        )

        log(
            "========================================"
        )

    except Exception as e:

        log(
            f"FATAL SCAN ERROR: {e}"
        )

    finally:

        scanner_running = False


# ============================================================
# BACKGROUND LOOP
# ============================================================

async def scanner_loop():

    log(
        "Background scanner started."
    )

    # First scan immediately

    await asyncio.to_thread(
        run_full_scan
    )

    # Repeat every 5 minutes

    while True:

        log(
            "Waiting 5 minutes "
            "before next scan..."
        )

        await asyncio.sleep(
            SCAN_INTERVAL
        )

        await asyncio.to_thread(
            run_full_scan
        )


# ============================================================
# HEALTH
# ============================================================

async def health(request):

    now = time.time()

    if last_scan_time:

        seconds_ago = (
            now - last_scan_time
        )

    else:

        seconds_ago = None

    with state_lock:

        active_alerts = len(
            alerted
        )

    return web.json_response(
        {
            "status": "ok",
            "service": "the-kingdom-render",

            "scanner_running":
                scanner_running,

            "spot_symbols":
                spot_count,

            "futures_symbols":
                futures_count,

            "spot_api":
                spot_api_status,

            "futures_api":
                futures_api_status,

            "active_alerts":
                active_alerts,

            "last_scan_seconds_ago":
                (
                    round(
                        seconds_ago,
                        1
                    )
                    if seconds_ago is not None
                    else None
                ),

            "last_scan_duration":
                round(
                    last_scan_duration,
                    1
                ),

            "last_error":
                last_error
        }
    )


# ============================================================
# HOME
# ============================================================

async def home(request):

    return web.Response(
        text=(
            "The Kingdom Render Bot is running.\n\n"
            "Spot + Futures\n"
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

    # Start scanner

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

    log("")
    log(
        "========================================"
    )

    log(
        "THE KINGDOM RENDER BOT IS ONLINE"
    )

    log(
        f"Port: {PORT}"
    )

    log(
        "Health: /health"
    )

    log(
        "========================================"
    )

    while True:

        await asyncio.sleep(
            3600
        )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":

    asyncio.run(
        main()
    )
