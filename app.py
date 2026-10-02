import asyncio
import json
import os
import time
from collections import defaultdict, deque

import aiohttp
import websockets


# =========================================================
# THE KINGDOM - RENDER WEBSOCKET SCANNER
# =========================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

THRESHOLD = 23.0

# 2 hours
WINDOW_SECONDS = 2 * 60 * 60

# 5-minute candle
INTERVAL = "5m"

# Binance allows max 1024 streams per connection.
# Keep some safety margin.
STREAMS_PER_CONNECTION = 900


SPOT_EXCHANGE_INFO = "https://data-api.binance.vision/api/v3/exchangeInfo"
FUTURES_EXCHANGE_INFO = "https://fapi.binance.com/fapi/v1/exchangeInfo"

SPOT_WS = "wss://stream.binance.com:9443/ws"
FUTURES_WS = "wss://fstream.binance.com/market/ws"


# ---------------------------------------------------------
# Global state
# ---------------------------------------------------------

history = {
    "SPOT": defaultdict(lambda: deque(maxlen=30)),
    "FUTURES": defaultdict(lambda: deque(maxlen=30)),
}

alerted = {
    "SPOT": set(),
    "FUTURES": set(),
}

stats = {
    "spot_symbols": 0,
    "futures_symbols": 0,
    "spot_connections": 0,
    "futures_connections": 0,
    "last_spot_event": None,
    "last_futures_event": None,
    "alerts_sent": 0,
}

start_time = time.time()


# =========================================================
# Logging
# =========================================================

def log(message):
    print(message, flush=True)


# =========================================================
# Telegram
# =========================================================

async def send_telegram(message):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        log("[TELEGRAM] Missing TELEGRAM_TOKEN or CHAT_ID")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    try:
        timeout = aiohttp.ClientTimeout(total=15)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, json=payload) as response:
                text = await response.text()

                if response.status == 200:
                    stats["alerts_sent"] += 1
                    log("[TELEGRAM] Alert sent")
                    return True

                log(f"[TELEGRAM] HTTP {response.status}: {text}")
                return False

    except Exception as e:
        log(f"[TELEGRAM] Error: {e}")
        return False


# =========================================================
# Binance symbol discovery
# =========================================================

async def get_exchange_info(url, market):
    """
    Only used once during startup to discover symbols.
    No kline REST requests are made.
    """

    log(f"[{market}] Getting exchange info...")

    timeout = aiohttp.ClientTimeout(total=20)

    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as response:

                if response.status != 200:
                    text = await response.text()

                    log(
                        f"[{market}] ExchangeInfo HTTP "
                        f"{response.status}: {text[:300]}"
                    )

                    return []

                data = await response.json()

        symbols = []

        for item in data.get("symbols", []):

            symbol = item.get("symbol", "")
            status = item.get("status", "")
            quote_asset = item.get("quoteAsset", "")

            # Only active USDT markets
            if (
                status == "TRADING"
                and quote_asset == "USDT"
                and symbol.endswith("USDT")
            ):
                symbols.append(symbol.lower())

        symbols = sorted(set(symbols))

        log(
            f"[{market}] Found {len(symbols)} active USDT symbols"
        )

        return symbols

    except Exception as e:
        log(f"[{market}] ExchangeInfo error: {e}")
        return []


# =========================================================
# Alert calculation
# =========================================================

async def process_candle(market, symbol, candle):
    """
    Store closed 5m candles.

    We use:
        start price = oldest candle OPEN
        highest     = highest HIGH during the window

    Same basic idea as the original Kingdom scanner.
    """

    if not candle.get("x"):
        # Ignore unfinished candle
        return

    open_price = float(candle["o"])
    high_price = float(candle["h"])
    close_price = float(candle["c"])

    now = int(time.time())

    q = history[market][symbol]

    q.append(
        {
            "time": now,
            "open": open_price,
            "high": high_price,
            "close": close_price,
        }
    )

    # Need roughly 2 hours of 5m candles
    if len(q) < 25:
        return

    # Remove candles older than 2 hours
    cutoff = now - WINDOW_SECONDS

    while q and q[0]["time"] < cutoff:
        q.popleft()

    if len(q) < 20:
        return

    start_price = q[0]["open"]

    highest_price = max(
        item["high"]
        for item in q
    )

    current_price = close_price

    if start_price <= 0:
        return

    change = (
        (highest_price - start_price)
        / start_price
    ) * 100

    # -----------------------------------------------------
    # Alert
    # -----------------------------------------------------

    if change >= THRESHOLD:

        if symbol not in alerted[market]:

            alerted[market].add(symbol)

            display_symbol = symbol.upper()

            message = (
                f"🚨 <b>THE KINGDOM ALERT</b>\n\n"
                f"📊 Market: <b>{market}</b>\n"
                f"🪙 Coin: <b>{display_symbol}</b>\n"
                f"📈 Move: <b>+{change:.2f}%</b>\n"
                f"💰 Start: <b>{start_price:g}</b>\n"
                f"🔥 High: <b>{highest_price:g}</b>\n"
                f"💵 Current: <b>{current_price:g}</b>\n"
                f"⏱ Window: <b>~2 Hours</b>"
            )

            log(
                f"[ALERT] {market} {display_symbol} "
                f"+{change:.2f}%"
            )

            await send_telegram(message)

    else:

        # Reset after dropping below threshold.
        if symbol in alerted[market]:
            alerted[market].remove(symbol)


# =========================================================
# WebSocket worker
# =========================================================

async def websocket_worker(market, symbols, connection_number):
    if market == "SPOT":
        base_url = SPOT_WS
    else:
        base_url = FUTURES_WS

    log(
        f"[{market}] Connection #{connection_number} "
        f"starting with {len(symbols)} streams"
    )

    # Binance stream names
    streams = [
        f"{symbol}@kline_{INTERVAL}"
        for symbol in symbols
    ]

    while True:

        try:

            log(
                f"[{market}] Connection #{connection_number} "
                f"connecting..."
            )

            async with websockets.connect(
                base_url,
                ping_interval=20,
                ping_timeout=60,
                close_timeout=10,
                max_size=4 * 1024 * 1024,
            ) as ws:

                log(
                    f"[{market}] Connection #{connection_number} "
                    f"CONNECTED"
                )

                if market == "SPOT":
                    stats["spot_connections"] += 1
                else:
                    stats["futures_connections"] += 1

                # Subscribe in one message.
                # This avoids sending hundreds of messages.
                subscribe_message = {
                    "method": "SUBSCRIBE",
                    "params": streams,
                    "id": connection_number,
                }

                await ws.send(
                    json.dumps(subscribe_message)
                )

                log(
                    f"[{market}] Connection #{connection_number} "
                    f"subscribed to {len(streams)} streams"
                )

                async for raw_message in ws:

                    try:
                        data = json.loads(raw_message)

                        # Subscription response
                        if "result" in data:
                            continue

                        # Combined stream
                        if "data" in data:
                            event = data["data"]
                        else:
                            event = data

                        if event.get("e") != "kline":
                            continue

                        candle = event.get("k")

                        if not candle:
                            continue

                        symbol = event.get("s", "").lower()

                        if not symbol:
                            continue

                        if market == "SPOT":
                            stats["last_spot_event"] = time.time()
                        else:
                            stats["last_futures_event"] = time.time()

                        await process_candle(
                            market,
                            symbol,
                            candle,
                        )

                    except json.JSONDecodeError:
                        continue

                    except Exception as e:
                        log(
                            f"[{market}] Event processing error: {e}"
                        )

        except asyncio.CancelledError:
            raise

        except Exception as e:

            log(
                f"[{market}] Connection #{connection_number} "
                f"ERROR: {e}"
            )

            log(
                f"[{market}] Connection #{connection_number} "
                f"reconnecting in 10 seconds..."
            )

            await asyncio.sleep(10)


# =========================================================
# Start WebSocket connections
# =========================================================

async def start_market_streams(market, symbols):

    if not symbols:
        log(f"[{market}] No symbols available.")
        return

    chunks = [
        symbols[i:i + STREAMS_PER_CONNECTION]
        for i in range(
            0,
            len(symbols),
            STREAMS_PER_CONNECTION
        )
    ]

    log(
        f"[{market}] Total connections required: "
        f"{len(chunks)}"
    )

    tasks = []

    for index, chunk in enumerate(chunks, start=1):

        task = asyncio.create_task(
            websocket_worker(
                market,
                chunk,
                index,
            )
        )

        tasks.append(task)

        # Small delay between connections
        await asyncio.sleep(2)

    await asyncio.gather(*tasks)


# =========================================================
# Health server
# =========================================================

async def health_handler(request):

    now = time.time()

    def age(timestamp):
        if timestamp is None:
            return None

        return round(now - timestamp, 1)

    return aiohttp.web.json_response(
        {
            "status": "ok",
            "service": "the-kingdom-render",

            "scanner": {
                "running": True,
                "uptime_seconds": round(
                    now - start_time,
                    1
                ),
            },

            "markets": {
                "spot_symbols": stats["spot_symbols"],
                "futures_symbols": stats["futures_symbols"],
                "spot_connections": stats["spot_connections"],
                "futures_connections": stats["futures_connections"],
            },

            "websocket": {
                "last_spot_event_seconds_ago":
                    age(stats["last_spot_event"]),

                "last_futures_event_seconds_ago":
                    age(stats["last_futures_event"]),
            },

            "alerts_sent": stats["alerts_sent"],

            "active_alerts": {
                "spot": len(alerted["SPOT"]),
                "futures": len(alerted["FUTURES"]),
            },

            "warmup": {
                "message":
                    "Scanner needs about 2 hours of live candle history after a fresh restart."
            },
        }
    )


async def start_health_server():

    app = aiohttp.web.Application()

    app.router.add_get(
        "/",
        health_handler,
    )

    app.router.add_get(
        "/health",
        health_handler,
    )

    port = int(
        os.getenv(
            "PORT",
            "10000"
        )
    )

    runner = aiohttp.web.AppRunner(app)

    await runner.setup()

    site = aiohttp.web.TCPSite(
        runner,
        "0.0.0.0",
        port,
    )

    await site.start()

    log(
        f"[HEALTH] Server running on port {port}"
    )


# =========================================================
# Main
# =========================================================

async def main():

    log("")
    log("==============================================")
    log("       THE KINGDOM RENDER BOT")
    log("==============================================")
    log("WebSocket scanner starting...")
    log("")

    if not TELEGRAM_TOKEN:
        log("[WARNING] TELEGRAM_TOKEN missing")

    if not CHAT_ID:
        log("[WARNING] CHAT_ID missing")

    await start_health_server()

    # -----------------------------------------------------
    # Get symbols
    # -----------------------------------------------------

    spot_symbols_task = asyncio.create_task(
        get_exchange_info(
            SPOT_EXCHANGE_INFO,
            "SPOT",
        )
    )

    futures_symbols_task = asyncio.create_task(
        get_exchange_info(
            FUTURES_EXCHANGE_INFO,
            "FUTURES",
        )
    )

    spot_symbols, futures_symbols = await asyncio.gather(
        spot_symbols_task,
        futures_symbols_task,
    )

    stats["spot_symbols"] = len(spot_symbols)
    stats["futures_symbols"] = len(futures_symbols)

    log("")
    log(
        f"[START] Spot symbols: {len(spot_symbols)}"
    )
    log(
        f"[START] Futures symbols: {len(futures_symbols)}"
    )
    log("")

    if not spot_symbols and not futures_symbols:

        log(
            "[FATAL] No symbols received from Binance."
        )

        # Keep health endpoint alive.
        while True:
            await asyncio.sleep(60)

    tasks = []

    if spot_symbols:
        tasks.append(
            asyncio.create_task(
                start_market_streams(
                    "SPOT",
                    spot_symbols,
                )
            )
        )

    if futures_symbols:
        tasks.append(
            asyncio.create_task(
                start_market_streams(
                    "FUTURES",
                    futures_symbols,
                )
            )
        )

    await asyncio.gather(*tasks)


# =========================================================
# Run
# =========================================================

if __name__ == "__main__":

    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        log("Bot stopped.")

    except Exception as e:
        log(f"[FATAL] {e}")
