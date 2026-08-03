"""
data_source.py — Pluggable data-source abstraction
====================================================

HOW TO ADD A NEW BROKER
-----------------------
1.  Write three async functions matching these signatures:

        async def my_broker_get_historical(symbol: str, timeframe: str,
                                            limit: int = 300) -> List[dict]:
            '''Return a list of OHLCV dicts:
               [{"time": <unix_sec>, "open": …, "high": …,
                 "low": …, "close": …, "volume": …}, …]
            '''

        async def my_broker_subscribe_live(symbol: str, timeframe: str):
            '''Async generator that yields dicts:
               {"symbol": …, "price": …, "volume": …, "timestamp": …,
                "ohlcv": <optional OHLCV dict>}
            '''

        async def my_broker_get_symbols() -> List[dict]:
            '''Return [{"symbol": …, "name": …, "exchange": …,
                        "asset_type": …}, …]'''

2.  Register them in DATA_SOURCES at the bottom of this file.

That's it – the rest of the application picks up the new source
automatically (REST endpoints, WebSocket handler, frontend dropdown).
"""

from __future__ import annotations

import asyncio
import json
import time
import logging
from datetime import datetime, timedelta
from typing import AsyncGenerator, Dict, List, Optional

import aiohttp

logger = logging.getLogger("data_source")

# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  HYPERLIQUID  —  Crypto (free, no API key, real-time WebSocket)        ║
# ╚══════════════════════════════════════════════════════════════════════════╝

HYPERLIQUID_WS = "wss://api.hyperliquid.xyz/ws"
HYPERLIQUID_API = "https://api.hyperliquid.xyz/info"

HYPERLIQUID_SYMBOLS: List[str] = [
    "BTC", "ETH", "SOL", "ARB", "DOGE", "WIF", "PEPE", "AVAX",
    "MATIC", "LINK", "OP", "SUI", "APT", "INJ", "SEI", "TIA",
    "JUP", "ONDO", "RENDER", "FET",
]

# Map our Timeframe enum values to Hyperliquid candle intervals
_HL_INTERVAL_MAP: Dict[str, str] = {
    "1m": "1m", "5m": "5m", "15m": "15m",
    "1h": "1h", "4h": "4h", "1D": "1d", "1W": "1w",
}


async def hyperliquid_get_historical(
    symbol: str, timeframe: str, limit: int = 300
) -> List[dict]:
    """Fetch historical candles from Hyperliquid REST API."""
    interval = _HL_INTERVAL_MAP.get(timeframe, "1h")
    # Calculate start time based on limit and interval
    interval_seconds = {
        "1m": 60, "5m": 300, "15m": 900,
        "1h": 3600, "4h": 14400, "1d": 86400, "1w": 604800,
    }
    secs = interval_seconds.get(interval, 3600)
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - (limit * secs * 1000)

    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin": symbol,
            "interval": interval,
            "startTime": start_ms,
            "endTime": end_ms,
        },
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                HYPERLIQUID_API, json=payload,
                headers={"Content-Type": "application/json"},
            ) as resp:
                if resp.status != 200:
                    logger.warning("Hyperliquid candle API returned %s", resp.status)
                    return []
                data = await resp.json()

        candles: List[dict] = []
        for c in data:
            candles.append({
                "time": int(c["t"]) // 1000,
                "open": float(c["o"]),
                "high": float(c["h"]),
                "low": float(c["l"]),
                "close": float(c["c"]),
                "volume": float(c["v"]),
            })
        return candles
    except Exception as exc:
        logger.error("hyperliquid_get_historical error: %s", exc)
        return []


async def hyperliquid_subscribe_live(
    symbol: str, timeframe: str
) -> AsyncGenerator[dict, None]:
    """
    Connect to Hyperliquid WS → subscribe to trades for *symbol*.
    Yields price-tick dicts as they arrive.
    """
    interval = _HL_INTERVAL_MAP.get(timeframe, "1m")
    backoff = 1
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    HYPERLIQUID_WS, heartbeat=20, timeout=30,
                ) as ws:
                    # Subscribe to candle updates
                    await ws.send_json({
                        "method": "subscribe",
                        "subscription": {
                            "type": "candle",
                            "coin": symbol,
                            "interval": interval,
                        },
                    })
                    # Also subscribe to trades for tick-level prices
                    await ws.send_json({
                        "method": "subscribe",
                        "subscription": {
                            "type": "trades",
                            "coin": symbol,
                        },
                    })
                    backoff = 1  # reset on success
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            channel = data.get("channel")
                            tick_data = data.get("data")
                            if not tick_data:
                                continue

                            if channel == "trades":
                                for trade in tick_data:
                                    yield {
                                        "symbol": symbol,
                                        "price": float(trade["px"]),
                                        "volume": float(trade["sz"]),
                                        "timestamp": int(
                                            trade.get("time", time.time() * 1000)
                                        ) // 1000,
                                        "side": trade.get("side", ""),
                                    }
                            elif channel == "candle":
                                c = tick_data
                                yield {
                                    "symbol": symbol,
                                    "price": float(c["c"]),
                                    "timestamp": int(c["t"]) // 1000,
                                    "ohlcv": {
                                        "time": int(c["t"]) // 1000,
                                        "open": float(c["o"]),
                                        "high": float(c["h"]),
                                        "low": float(c["l"]),
                                        "close": float(c["c"]),
                                        "volume": float(c["v"]),
                                    },
                                }
                        elif msg.type in (
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.ERROR,
                        ):
                            break
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.warning("Hyperliquid WS error (%s): %s — reconnecting in %ss",
                           symbol, exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


async def hyperliquid_get_symbols() -> List[dict]:
    """Return list of available Hyperliquid perpetual symbols."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                HYPERLIQUID_API,
                json={"type": "meta"},
                headers={"Content-Type": "application/json"},
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    symbols = []
                    for item in data.get("universe", []):
                        sym = item.get("name", "")
                        if sym:
                            symbols.append({
                                "symbol": sym,
                                "name": f"{sym}-USD Perp",
                                "exchange": "Hyperliquid",
                                "asset_type": "crypto",
                            })
                    return symbols
    except Exception as exc:
        logger.error("hyperliquid_get_symbols error: %s", exc)

    # Fallback to static list
    return [
        {"symbol": s, "name": f"{s}-USD Perp",
         "exchange": "Hyperliquid", "asset_type": "crypto"}
        for s in HYPERLIQUID_SYMBOLS
    ]


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  YAHOO FINANCE (US)  —  Stocks & ETFs (polled, ~15s interval)          ║
# ╚══════════════════════════════════════════════════════════════════════════╝

YAHOO_US_SYMBOLS: List[str] = [
    "GOOGL", "GOOG", "AAPL", "MSFT", "AMZN", "NVDA", "TSLA", "META", "SPY",
    "QQQ", "AMD", "NFLX", "JPM", "V", "DIS", "PYPL", "BA",
    "CRM", "INTC", "UBER", "COIN",
]

_YF_DIRECT_PARAMS: Dict[str, dict] = {
    "1m":  {"range": "7d",   "interval": "1m"},
    "5m":  {"range": "60d",  "interval": "5m"},
    "15m": {"range": "60d",  "interval": "15m"},
    "1h":  {"range": "2y",   "interval": "1h"},
    "4h":  {"range": "2y",   "interval": "1h"},
    "1D":  {"range": "10y",  "interval": "1d"},
    "1W":  {"range": "max",  "interval": "1wk"},
}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


async def _fetch_yahoo_chart_direct(symbol: str, timeframe: str) -> List[dict]:
    """Fetch candles directly from Yahoo Finance API without yfinance scraper bugs."""
    p = _YF_DIRECT_PARAMS.get(timeframe, _YF_DIRECT_PARAMS["1D"])
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range={p['range']}&interval={p['interval']}"
    headers = {"User-Agent": USER_AGENT}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    logger.warning("Yahoo chart direct API returned %s for %s", resp.status, symbol)
                    return []
                data = await resp.json()

        result = data.get("chart", {}).get("result")
        if not result:
            return []

        chart_data = result[0]
        timestamps = chart_data.get("timestamp", [])
        quote = chart_data.get("indicators", {}).get("quote", [{}])[0]

        opens = quote.get("open", [])
        highs = quote.get("high", [])
        lows = quote.get("low", [])
        closes = quote.get("close", [])
        volumes = quote.get("volume", [])

        candles = []
        for i in range(len(timestamps)):
            c = closes[i] if i < len(closes) else None
            if c is None:
                continue
            o = opens[i] if i < len(opens) and opens[i] is not None else c
            h = highs[i] if i < len(highs) and highs[i] is not None else c
            l = lows[i] if i < len(lows) and lows[i] is not None else c
            v = volumes[i] if i < len(volumes) and volumes[i] is not None else 0
            candles.append({
                "time": timestamps[i],
                "open": round(float(o), 4),
                "high": round(float(h), 4),
                "low": round(float(l), 4),
                "close": round(float(c), 4),
                "volume": float(v),
            })
        return candles
    except Exception as exc:
        logger.error("_fetch_yahoo_chart_direct error (%s): %s", symbol, exc)
        return []


async def get_fundamentals(symbol: str, source: str) -> dict:
    """Fetch key fundamental data (TTM PE, Forward PE, EPS, PEG, Market Cap, Dividend Yield, Profit Margin)."""
    if source in ("yahoo_us", "yahoo_india"):
        try:
            headers = {"User-Agent": USER_AGENT}
            async with aiohttp.ClientSession(headers=headers) as session:
                # Obtain cookie & crumb
                try:
                    async with session.get("https://fc.yahoo.com") as _:
                        pass
                    async with session.get("https://query1.finance.yahoo.com/v1/test/getcrumb") as r:
                        crumb = await r.text()
                except Exception:
                    crumb = ""

                url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}?modules=defaultKeyStatistics,financialData,summaryDetail"
                if crumb and "Invalid" not in crumb:
                    url += f"&crumb={crumb}"

                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        result = data.get("quoteSummary", {}).get("result", [{}])[0]
                        ks = result.get("defaultKeyStatistics", {})
                        sd = result.get("summaryDetail", {})
                        fd = result.get("financialData", {})

                        def _fmt(d, default=None):
                            if isinstance(d, dict):
                                return d.get("raw", default)
                            return d if d is not None else default

                        pe_ttm = _fmt(sd.get("trailingPE"))
                        pe_forward = _fmt(ks.get("forwardPE")) or _fmt(sd.get("forwardPE"))
                        eps_ttm = _fmt(ks.get("trailingEps"))
                        peg_ratio = _fmt(ks.get("pegRatio"))
                        market_cap = _fmt(sd.get("marketCap"))
                        div_yield = _fmt(sd.get("dividendYield"))
                        profit_margins = _fmt(fd.get("profitMargins"))
                        pb_ratio = _fmt(ks.get("priceToBook"))

                        return {
                            "symbol": symbol,
                            "source": source,
                            "pe_ttm": round(float(pe_ttm), 2) if pe_ttm is not None else None,
                            "pe_forward": round(float(pe_forward), 2) if pe_forward is not None else None,
                            "eps_ttm": round(float(eps_ttm), 2) if eps_ttm is not None else None,
                            "peg_ratio": round(float(peg_ratio), 2) if peg_ratio is not None else None,
                            "pb_ratio": round(float(pb_ratio), 2) if pb_ratio is not None else None,
                            "market_cap": float(market_cap) if market_cap is not None else None,
                            "dividend_yield": round(float(div_yield) * 100, 2) if div_yield is not None else None,
                            "profit_margins": round(float(profit_margins) * 100, 2) if profit_margins is not None else None,
                        }
        except Exception as exc:
            logger.warning("get_fundamentals error (%s): %s", symbol, exc)

    return {
        "symbol": symbol,
        "source": source,
        "pe_ttm": None,
        "pe_forward": None,
        "eps_ttm": None,
        "peg_ratio": None,
        "market_cap": None,
        "dividend_yield": None,
    }


async def search_symbols(query: str, source: str) -> List[dict]:
    """Search available stock or crypto symbols matching query."""
    if not query:
        return []

    q_str = query.strip()

    if source in ("yahoo_us", "yahoo_india"):
        url = f"https://query1.finance.yahoo.com/v1/finance/search?q={q_str}&quotesCount=10&newsCount=0"
        headers = {"User-Agent": USER_AGENT}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        quotes = data.get("quotes", [])
                        results = []
                        for item in quotes:
                            sym = item.get("symbol")
                            if not sym:
                                continue

                            # For Yahoo India, append .NS if user searched without suffix
                            if source == "yahoo_india" and not (sym.endswith(".NS") or sym.endswith(".BO")):
                                if not any(char in sym for char in (".", "=")):
                                    sym = f"{sym}.NS"

                            short_name = item.get("shortname") or item.get("longname") or sym
                            exch = item.get("exchDisp") or item.get("exchange") or ""
                            results.append({
                                "symbol": sym,
                                "name": f"{sym} — {short_name} ({exch})",
                                "exchange": exch,
                            })
                        return results
        except Exception as exc:
            logger.warning("search_symbols error (%s/%s): %s", source, q_str, exc)
            return []

    elif source == "hyperliquid":
        symbols = await hyperliquid_get_symbols()
        query_upper = q_str.upper()
        return [s for s in symbols if query_upper in s["symbol"].upper()]

    elif source == "binance":
        symbols = await binance_get_symbols()
        query_upper = q_str.upper()
        return [s for s in symbols if query_upper in s["symbol"].upper()]

    return []


async def yahoo_us_get_historical(
    symbol: str, timeframe: str, limit: int = 3000
) -> List[dict]:
    """Fetch historical data from Yahoo Finance for US stocks."""
    candles = await _fetch_yahoo_chart_direct(symbol, timeframe)
    return candles[-limit:] if limit else candles


async def yahoo_us_subscribe_live(
    symbol: str, timeframe: str
) -> AsyncGenerator[dict, None]:
    """
    Poll Yahoo Finance every 15 seconds to simulate live updates.
    """
    last_price: Optional[float] = None
    while True:
        try:
            candles = await _fetch_yahoo_chart_direct(symbol, "1m")
            if candles:
                latest = candles[-1]
                price = latest["close"]
                change = round(price - last_price, 4) if last_price else 0.0
                change_pct = round(
                    (change / last_price * 100) if last_price and last_price != 0 else 0.0,
                    4,
                )
                last_price = price
                ts = latest["time"]

                yield {
                    "symbol": symbol,
                    "price": price,
                    "change": change,
                    "change_pct": change_pct,
                    "volume": latest["volume"],
                    "timestamp": ts,
                    "ohlcv": latest,
                }
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.warning("yahoo_us poll error (%s): %s", symbol, exc)

        await asyncio.sleep(15)


async def yahoo_us_get_symbols() -> List[dict]:
    """Return list of popular US stock symbols."""
    return [
        {"symbol": s, "name": s, "exchange": "NASDAQ/NYSE", "asset_type": "stock"}
        for s in YAHOO_US_SYMBOLS
    ]


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  YAHOO FINANCE (INDIA)  —  NSE/BSE stocks (polled, ~15s interval)      ║
# ╚══════════════════════════════════════════════════════════════════════════╝

YAHOO_INDIA_SYMBOLS: List[str] = [
    "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS",
    "ICICIBANK.NS", "SBIN.NS", "BHARTIARTL.NS", "ITC.NS",
    "KOTAKBANK.NS", "LT.NS", "HINDUNILVR.NS", "AXISBANK.NS",
    "BAJFINANCE.NS", "MARUTI.NS", "TITAN.NS", "SUNPHARMA.NS",
    "WIPRO.NS", "TATAMOTORS.NS", "ADANIENT.NS", "HCLTECH.NS",
]


async def yahoo_india_get_historical(
    symbol: str, timeframe: str, limit: int = 3000
) -> List[dict]:
    """Fetch historical data from Yahoo Finance for Indian stocks."""
    candles = await _fetch_yahoo_chart_direct(symbol, timeframe)
    return candles[-limit:] if limit else candles


async def yahoo_india_subscribe_live(
    symbol: str, timeframe: str
) -> AsyncGenerator[dict, None]:
    """
    Poll Yahoo Finance every 15 seconds for Indian stock updates.
    """
    last_price: Optional[float] = None
    while True:
        try:
            candles = await _fetch_yahoo_chart_direct(symbol, "1m")
            if candles:
                latest = candles[-1]
                price = latest["close"]
                change = round(price - last_price, 4) if last_price else 0.0
                change_pct = round(
                    (change / last_price * 100) if last_price and last_price != 0 else 0.0,
                    4,
                )
                last_price = price
                ts = latest["time"]

                yield {
                    "symbol": symbol,
                    "price": price,
                    "change": change,
                    "change_pct": change_pct,
                    "volume": latest["volume"],
                    "timestamp": ts,
                    "ohlcv": latest,
                }
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.warning("yahoo_india poll error (%s): %s", symbol, exc)

        await asyncio.sleep(15)


async def yahoo_india_get_symbols() -> List[dict]:
    """Return list of popular Indian stock symbols."""
    return [
        {"symbol": s, "name": s.replace(".NS", ""),
         "exchange": "NSE", "asset_type": "stock"}
        for s in YAHOO_INDIA_SYMBOLS
    ]


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  BINANCE  —  Crypto (free public WebSocket & REST)                       ║
# ╚══════════════════════════════════════════════════════════════════════════╝

BINANCE_SYMBOLS: List[str] = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "ADAUSDT", "DOGEUSDT",
    "XRPUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT", "NEARUSDT", "PEPEUSDT",
]

_BINANCE_INTERVAL_MAP: Dict[str, str] = {
    "1m": "1m", "5m": "5m", "15m": "15m",
    "1h": "1h", "4h": "4h", "1D": "1d", "1W": "1w",
}


async def binance_get_historical(
    symbol: str, timeframe: str, limit: int = 300
) -> List[dict]:
    """Fetch historical candles from Binance public REST API."""
    interval = _BINANCE_INTERVAL_MAP.get(timeframe, "1h")
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    logger.warning("Binance REST error: status %s for %s", resp.status, symbol)
                    return []
                data = await resp.json()

        candles: List[dict] = []
        for item in data:
            candles.append({
                "time": int(item[0]) // 1000,
                "open": float(item[1]),
                "high": float(item[2]),
                "low": float(item[3]),
                "close": float(item[4]),
                "volume": float(item[5]),
            })
        return candles
    except Exception as exc:
        logger.error("binance_get_historical error (%s): %s", symbol, exc)
        return []


async def binance_subscribe_live(
    symbol: str, timeframe: str
) -> AsyncGenerator[dict, None]:
    """
    Connect to Binance public WebSocket for real-time kline updates.
    """
    interval = _BINANCE_INTERVAL_MAP.get(timeframe, "1m")
    ws_url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@kline_{interval}"
    backoff = 1
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(ws_url, heartbeat=20, timeout=30) as ws:
                    backoff = 1
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            k = data.get("k", {})
                            if k:
                                price = float(k["c"])
                                ts = int(k["t"]) // 1000
                                yield {
                                    "symbol": symbol,
                                    "price": price,
                                    "volume": float(k["v"]),
                                    "timestamp": ts,
                                    "ohlcv": {
                                        "time": ts,
                                        "open": float(k["o"]),
                                        "high": float(k["h"]),
                                        "low": float(k["l"]),
                                        "close": price,
                                        "volume": float(k["v"]),
                                    },
                                }
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.warning("Binance WS error (%s): %s — reconnecting in %ss", symbol, exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


async def binance_get_symbols() -> List[dict]:
    """Return list of popular Binance USDT trading pairs."""
    return [
        {"symbol": s, "name": f"{s.replace('USDT', '')}/USDT", "exchange": "Binance", "asset_type": "crypto"}
        for s in BINANCE_SYMBOLS
    ]


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  DATA_SOURCES  —  The registry. Add new brokers here.                  ║
# ╚══════════════════════════════════════════════════════════════════════════╝

DATA_SOURCES: Dict[str, dict] = {
    "hyperliquid": {
        "get_historical": hyperliquid_get_historical,
        "subscribe_live": hyperliquid_subscribe_live,
        "get_symbols": hyperliquid_get_symbols,
        "label": "Hyperliquid (Crypto)",
    },
    "binance": {
        "get_historical": binance_get_historical,
        "subscribe_live": binance_subscribe_live,
        "get_symbols": binance_get_symbols,
        "label": "Binance (Crypto)",
    },
    "yahoo_us": {
        "get_historical": yahoo_us_get_historical,
        "subscribe_live": yahoo_us_subscribe_live,
        "get_symbols": yahoo_us_get_symbols,
        "label": "Yahoo Finance (US)",
    },
    "yahoo_india": {
        "get_historical": yahoo_india_get_historical,
        "subscribe_live": yahoo_india_subscribe_live,
        "get_symbols": yahoo_india_get_symbols,
        "label": "Yahoo Finance (India)",
    },
}
