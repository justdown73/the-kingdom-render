import asyncio
import json
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque, Dict, Optional

import aiohttp
from aiohttp import web
import websockets


# =========================================================
# THE KINGDOM - RENDER EDITION
# =========================================================
#
# Spot + USDⓈ-M Futures
# Threshold: +23%
# Window: 25 x 5-minute candles (~2h 5m)
#
# IMPORTANT:
# This version does NOT use Binance REST market-data
# requests for scanning.
#
# It uses Binance WebSocket all-market mini ticker streams
# and builds local 5-minute candles from live prices.
# =========================================================


# =========================================================
# CONFIG
# =========================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
CHAT_ID = os.getenv("CHAT_ID", "").strip()

THRESHOLD_PERCENT = 23.0

CANDLE_SECONDS = 5 * 60

# Same setting as the original Kingdom bot.
# 25 x 5 minutes = 125 minutes (~2 hours)
WINDOW_CANDLES = 25


# Spot market-data WebSocket.
# Binance provides a market-data-only endpoint here.
SPOT_WS_URL = (
    "wss://data-stream.binance.vision/ws/!miniTicker@arr"
)

# Current USDⓈ-M Futures market-data WebSocket structure.
FUTURES_WS_URL = (
    "wss://fstream.binance.com/market/ws/!miniTicker@arr"
)


# Prevent Telegram alert bursts.
TELEGRAM_MIN_INTERVAL = 0.55


# Render provides PORT automatically.
PORT = int(os.getenv("PORT", "10000"))


# =========================================================
# DATA STRUCTURES
# =========================================================


@dataclass
class Candle:
    start_ms: int
    open: float
    high: float
    close: float


class SymbolState:
    def __init__(self) -> None:
        self.candles: Deque[Candle] = deque(
            maxlen=WINDOW_CANDLES
        )

        self.current_bucket: Optional[int] = None

        self.last_price: Optional[float] = None

        self.last_event_ms: Optional[int] = None

        # Prevent repeated alerts while the same symbol
        # remains above the threshold.
        self.alerted: bool = False


# Separate Spot and Futures state.
states: Dict[str, Dict[str, SymbolState]] = {
    "SPOT": defaultdict(SymbolState),
    "FUTURES": defaultdict(SymbolState),
}


# =========================================================
# STATS
# =========================================================


stats = {
    "started_at": time.time(),

    "last_spot_event": None,
    "last_futures_event": None,

    "spot_messages": 0,
    "futures_messages": 0,

    "alerts_queued": 0,
    "alerts_sent": 0,

    "telegram_errors": 0,

    "spot_reconnects": 0,
    "futures_reconnects": 0,
}


# =========================================================
# TELEGRAM QUEUE
# =========================================================


telegram_queue: asyncio.Queue[str] = asyncio.Queue()

telegram_session: Optional[
    aiohttp.ClientSession
] = None


# =========================================================
# LOGGING
# =========================================================


def log(message: str) -> None:
    print(message, flush=True)


# =========================================================
# HELPERS
# =========================================================


def format_price(value: float) -> str:
    """
    Make crypto prices readable without unnecessary zeros.
    """

    if value >= 1000:
        return (
            f"{value:,.4f}"
            .rstrip("0")
            .rstrip(".")
        )

    if value >= 1:
        return (
            f"{value:.8f}"
            .rstrip("0")
            .rstrip(".")
        )

    if value >= 0.01:
        return (
            f"{value:.10f}"
            .rstrip("0")
            .rstrip(".")
        )

    return (
        f"{value:.12f}"
        .rstrip("0")
        .rstrip(".")
    )


def is_usdt_symbol(symbol: str) -> bool:
    return (
        bool(symbol)
        and symbol.upper().endswith("USDT")
    )


def symbol_count(market: str) -> int:
    return len(states[market])


def warmed_symbol_count(market: str) -> int:
    return sum(
        1
        for state in states[market].values()
        if len(state.candles) >= WINDOW_CANDLES
    )


def seconds_ago(timestamp) -> Optional[float]:
    if timestamp is None:
        return None

    return round(
        time.time() - timestamp,
        1
    )


# =========================================================
# TELEGRAM MESSAGE
# =========================================================


def build_alert_message(
    market: str,
    symbol: str,
    change: float,
    start_price: float,
    high_price: float,
    current_price: float,
) -> str:

    market_label = (
        "SPOT"
        if market == "SPOT"
        else "FUTURES"
    )

    return (
        "🚨 <b>THE KINGDOM ALERT</b>\n\n"

        f"📊 Market: <b>{market_label}</b>\n"

        f"🪙 Coin: <b>{symbol}</b>\n"

        f"📈 Move: <b>+{change:.2f}%</b>\n"

        f"💰 Start: "
        f"<b>{format_price(start_price)}</b>\n"

        f"🔥 High: "
        f"<b>{format_price(high_price)}</b>\n"

        f"💵 Current: "
        f"<b>{format_price(current_price)}</b>\n"

        f"⏱ Window: <b>~2 Hours</b>\n"

        "📡 Source: "
        "<b>Binance WebSocket</b>"
    )


# =========================================================
# TELEGRAM WORKER
# =========================================================


async def telegram_worker() -> None:

    global telegram_session

    while True:

        message = await telegram_queue.get()

        try:

            if (
                not TELEGRAM_TOKEN
                or not CHAT_ID
            ):
                log(
                    "[TELEGRAM] "
                    "TELEGRAM_TOKEN or CHAT_ID is missing"
                )
                continue


            # Create session only when needed.
            if (
                telegram_session is None
                or telegram_session.closed
            ):
                timeout = aiohttp.ClientTimeout(
                    total=20
                )

                telegram_session = (
                    aiohttp.ClientSession(
                        timeout=timeout
                    )
                )


            url = (
                "https://api.telegram.org/"
                f"bot{TELEGRAM_TOKEN}/sendMessage"
            )


            payload = {
                "chat_id": CHAT_ID,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }


            # Try up to 3 times.
            for attempt in range(3):

                try:

                    async with telegram_session.post(
                        url,
                        json=payload
                    ) as response:

                        body = await response.text()


                        # Success.
                        if response.status == 200:

                            stats["alerts_sent"] += 1

                            log(
                                "[TELEGRAM] "
                                "Alert sent"
                            )

                            break


                        # Telegram rate limit.
                        if response.status == 429:

                            retry_after = 3

                            try:

                                data = json.loads(body)

                                retry_after = int(
                                    data
                                    .get("parameters", {})
                                    .get(
                                        "retry_after",
                                        3
                                    )
                                )

                            except Exception:
                                pass


                            log(
                                "[TELEGRAM] "
                                f"Rate limited. "
                                f"Waiting {retry_after}s"
                            )


                            await asyncio.sleep(
                                max(
                                    1,
                                    retry_after
                                )
                            )

                            continue


                        # Other Telegram error.
                        stats[
                            "telegram_errors"
                        ] += 1

                        log(
                            "[TELEGRAM] "
                            f"HTTP {response.status}: "
                            f"{body[:300]}"
                        )

                        break


                except (
                    aiohttp.ClientError,
                    asyncio.TimeoutError
                ) as exc:

                    if attempt < 2:

                        await asyncio.sleep(
                            2 * (attempt + 1)
                        )

                    else:

                        stats[
                            "telegram_errors"
                        ] += 1

                        log(
                            "[TELEGRAM] "
                            f"Request error: {exc}"
                        )


            # Keep messages separated.
            await asyncio.sleep(
                TELEGRAM_MIN_INTERVAL
            )


        finally:

            telegram_queue.task_done()


# =========================================================
# ROLLING 5-MINUTE CANDLE ENGINE
# =========================================================


def add_price_update(
    market: str,
    symbol: str,
    event_ms: int,
    price: float,
) -> Optional[
    tuple[float, float, float]
]:

    """
    Convert live mini-ticker updates into local
    5-minute OHLC candles.

    Returns:

        start_price
        highest_price
        current_price

    once enough candles are available.
    """

    if price <= 0:
        return None


    state = states[market][symbol]


    # Find 5-minute bucket.
    bucket_size_ms = (
        CANDLE_SECONDS * 1000
    )

    bucket = (
        event_ms // bucket_size_ms
    ) * bucket_size_ms


    # -----------------------------------------------------
    # FIRST UPDATE
    # -----------------------------------------------------

    if state.current_bucket is None:

        state.current_bucket = bucket

        state.last_price = price

        state.last_event_ms = event_ms

        state.candles.append(
            Candle(
                start_ms=bucket,
                open=price,
                high=price,
                close=price,
            )
        )

        return None


    # -----------------------------------------------------
    # SAME CANDLE
    # -----------------------------------------------------

    if bucket == state.current_bucket:

        candle = state.candles[-1]

        if price > candle.high:
            candle.high = price

        candle.close = price

        state.last_price = price

        state.last_event_ms = event_ms


    # -----------------------------------------------------
    # NEW CANDLE
    # -----------------------------------------------------

    else:

        previous_price = (
            state.last_price
            if state.last_price is not None
            else price
        )


        gap_buckets = max(
            1,
            (
                bucket
                - state.current_bucket
            )
            // bucket_size_ms
        )


        # If the coin disappeared from the stream
        # for a very long time, start a fresh window.
        if gap_buckets >= WINDOW_CANDLES:

            state.candles.clear()

            state.candles.append(
                Candle(
                    start_ms=bucket,
                    open=price,
                    high=price,
                    close=price,
                )
            )


        else:

            # Fill missing buckets with a flat candle
            # using the last known price.
            next_bucket = (
                state.current_bucket
                + bucket_size_ms
            )


            while next_bucket < bucket:

                state.candles.append(
                    Candle(
                        start_ms=next_bucket,
                        open=previous_price,
                        high=previous_price,
                        close=previous_price,
                    )
                )

                next_bucket += bucket_size_ms


            # Current bucket.
            state.candles.append(
                Candle(
                    start_ms=bucket,
                    open=price,
                    high=price,
                    close=price,
                )
            )


        state.current_bucket = bucket

        state.last_price = price

        state.last_event_ms = event_ms


    # -----------------------------------------------------
    # NOT ENOUGH HISTORY
    # -----------------------------------------------------

    if (
        len(state.candles)
        < WINDOW_CANDLES
    ):
        return None


    # -----------------------------------------------------
    # CALCULATE MOVE
    # -----------------------------------------------------

    start_price = (
        state.candles[0].open
    )

    highest_price = max(
        candle.high
        for candle in state.candles
    )

    current_price = (
        state.candles[-1].close
    )


    if start_price <= 0:
        return None


    return (
        start_price,
        highest_price,
        current_price,
    )


# =========================================================
# ALERT EVALUATION
# =========================================================


async def evaluate_update(
    market: str,
    symbol: str,
    event_ms: int,
    price: float,
) -> None:

    result = add_price_update(
        market,
        symbol,
        event_ms,
        price,
    )


    if result is None:
        return


    (
        start_price,
        highest_price,
        current_price,
    ) = result


    change = (
        (
            highest_price
            - start_price
        )
        / start_price
    ) * 100.0


    state = states[market][symbol]


    # -----------------------------------------------------
    # ABOVE THRESHOLD
    # -----------------------------------------------------

    if change >= THRESHOLD_PERCENT:

        # Only one alert while this rolling window
        # remains above 23%.
        if not state.alerted:

            state.alerted = True


            message = build_alert_message(
                market=market,
                symbol=symbol,
                change=change,
                start_price=start_price,
                high_price=highest_price,
                current_price=current_price,
            )


            await telegram_queue.put(
                message
            )


            stats[
                "alerts_queued"
            ] += 1


            log(
                "[ALERT] "
                f"{market} "
                f"{symbol} "
                f"+{change:.2f}% "
                f"queue="
                f"{telegram_queue.qsize()}"
            )


    # -----------------------------------------------------
    # RESET
    # -----------------------------------------------------

    else:

        # Once the rolling window falls below
        # 23%, the symbol can alert again later.
        state.alerted = False


# =========================================================
# BINANCE WEBSOCKET WORKER
# =========================================================


async def market_stream_worker(
    market: str,
    url: str,
) -> None:

    reconnect_delay = 5


    while True:

        try:

            log(
                f"[{market}] "
                "Connecting to Binance WebSocket..."
            )


            async with websockets.connect(
                url,

                # Client-side ping/pong support.
                ping_interval=20,
                ping_timeout=60,

                close_timeout=10,

                max_size=16 * 1024 * 1024,
            ) as ws:

                log(
                    f"[{market}] "
                    "WebSocket CONNECTED"
                )


                # Reset reconnect delay after success.
                reconnect_delay = 5


                async for raw_message in ws:

                    try:

                        data = json.loads(
                            raw_message
                        )


                        # !miniTicker@arr sends an array.
                        if not isinstance(
                            data,
                            list
                        ):
                            continue


                        # -------------------------------------------------
                        # STATS
                        # -------------------------------------------------

                        if market == "SPOT":

                            stats[
                                "spot_messages"
                            ] += 1

                            stats[
                                "last_spot_event"
                            ] = time.time()

                        else:

                            stats[
                                "futures_messages"
                            ] += 1

                            stats[
                                "last_futures_event"
                            ] = time.time()


                        # -------------------------------------------------
                        # PROCESS CHANGED TICKERS
                        # -------------------------------------------------

                        for item in data:

                            if not isinstance(
                                item,
                                dict
                            ):
                                continue


                            symbol = str(
                                item.get(
                                    "s",
                                    ""
                                )
                            ).upper()


                            # We only want USDT markets.
                            if not is_usdt_symbol(
                                symbol
                            ):
                                continue


                            try:

                                price = float(
                                    item["c"]
                                )


                                event_ms = int(
                                    item.get(
                                        "E",
                                        int(
                                            time.time()
                                            * 1000
                                        )
                                    )
                                )


                            except (
                                KeyError,
                                TypeError,
                                ValueError
                            ):

                                continue


                            await evaluate_update(
                                market,
                                symbol,
                                event_ms,
                                price,
                            )


                    except json.JSONDecodeError:

                        continue


                    except Exception as exc:

                        log(
                            f"[{market}] "
                            "Message processing error: "
                            f"{exc}"
                        )


        except asyncio.CancelledError:

            raise


        except Exception as exc:

            if market == "SPOT":

                stats[
                    "spot_reconnects"
                ] += 1

            else:

                stats[
                    "futures_reconnects"
                ] += 1


            log(
                f"[{market}] "
                f"WebSocket error: {exc}"
            )


            log(
                f"[{market}] "
                f"Reconnecting in "
                f"{reconnect_delay}s..."
            )


            await asyncio.sleep(
                reconnect_delay
            )


            # Exponential backoff.
            reconnect_delay = min(
                reconnect_delay * 2,
                60
            )


# =========================================================
# HEALTH ENDPOINT
# =========================================================


async def health(
    request: web.Request
) -> web.Response:

    return web.json_response(
        {
            "status": "ok",

            "service":
                "the-kingdom-render",

            "scanner_running":
                True,

            "uptime_seconds":
                round(
                    time.time()
                    - stats["started_at"],
                    1
                ),


            "markets": {

                "spot_symbols_seen":
                    symbol_count("SPOT"),

                "futures_symbols_seen":
                    symbol_count("FUTURES"),

                "spot_warmed_symbols":
                    warmed_symbol_count("SPOT"),

                "futures_warmed_symbols":
                    warmed_symbol_count(
                        "FUTURES"
                    ),
            },


            "websocket": {

                "spot_last_event_seconds_ago":
                    seconds_ago(
                        stats[
                            "last_spot_event"
                        ]
                    ),

                "futures_last_event_seconds_ago":
                    seconds_ago(
                        stats[
                            "last_futures_event"
                        ]
                    ),

                "spot_reconnects":
                    stats[
                        "spot_reconnects"
                    ],

                "futures_reconnects":
                    stats[
                        "futures_reconnects"
                    ],
            },


            "alerts": {

                "queued":
                    stats[
                        "alerts_queued"
                    ],

                "sent":
                    stats[
                        "alerts_sent"
                    ],

                "queue_size":
                    telegram_queue.qsize(),

                "telegram_errors":
                    stats[
                        "telegram_errors"
                    ],
            },


            "config": {

                "threshold_percent":
                    THRESHOLD_PERCENT,

                "candle_minutes":
                    5,

                "window_candles":
                    WINDOW_CANDLES,

                "window_approx_minutes":
                    WINDOW_CANDLES * 5,

                "binance_rest_market_requests":
                    0,
            },


            "note":
                (
                    "A fresh process needs "
                    "about 25 live 5-minute "
                    "buckets before the full "
                    "rolling-window detector "
                    "becomes active."
                ),
        }
    )


# =========================================================
# HEALTH SERVER
# =========================================================


async def start_health_server() -> web.AppRunner:

    app = web.Application()


    app.router.add_get(
        "/",
        health
    )


    app.router.add_get(
        "/health",
        health
    )


    runner = web.AppRunner(
        app
    )


    await runner.setup()


    site = web.TCPSite(
        runner,

        host="0.0.0.0",

        port=PORT,
    )


    await site.start()


    log(
        f"[HEALTH] "
        f"Listening on "
        f"0.0.0.0:{PORT}"
    )


    return runner


# =========================================================
# CLEANUP
# =========================================================


async def shutdown_health_server(
    runner: Optional[web.AppRunner]
) -> None:

    global telegram_session


    if runner is not None:

        await runner.cleanup()


    if (
        telegram_session is not None
        and not telegram_session.closed
    ):

        await telegram_session.close()


# =========================================================
# MAIN
# =========================================================


async def main() -> None:

    log("")

    log(
        "=============================================="
    )

    log(
        "        THE KINGDOM RENDER BOT"
    )

    log(
        "=============================================="
    )

    log(
        "Starting WebSocket-only scanner..."
    )

    log(
        f"Threshold: "
        f"+{THRESHOLD_PERCENT:.1f}%"
    )

    log(
        f"Window: "
        f"{WINDOW_CANDLES} x 5m "
        f"(~{WINDOW_CANDLES * 5} minutes)"
    )

    log(
        "Binance REST market-data requests: 0"
    )

    log("")


    if not TELEGRAM_TOKEN:

        log(
            "[WARNING] "
            "TELEGRAM_TOKEN is not set"
        )


    if not CHAT_ID:

        log(
            "[WARNING] "
            "CHAT_ID is not set"
        )


    runner = None

    tasks = []


    try:

        # Start health endpoint.
        runner = await start_health_server()


        # Telegram worker.
        tasks.append(
            asyncio.create_task(
                telegram_worker()
            )
        )


        # Spot WebSocket.
        tasks.append(
            asyncio.create_task(
                market_stream_worker(
                    "SPOT",
                    SPOT_WS_URL,
                )
            )
        )


        # Futures WebSocket.
        tasks.append(
            asyncio.create_task(
                market_stream_worker(
                    "FUTURES",
                    FUTURES_WS_URL,
                )
            )
        )


        await asyncio.gather(
            *tasks
        )


    finally:

        # Cancel workers on shutdown.
        for task in tasks:
            task.cancel()


        if tasks:

            await asyncio.gather(
                *tasks,
                return_exceptions=True
            )


        await shutdown_health_server(
            runner
        )


# =========================================================
# RUN
# =========================================================


if __name__ == "__main__":

    try:

        asyncio.run(main())

    except KeyboardInterrupt:

        log(
            "Bot stopped."
        )

    except Exception as exc:

        log(
            f"[FATAL] {exc}"
        )
