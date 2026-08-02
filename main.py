"""
main.py — FastAPI application entry point
==========================================

Serves the trading dashboard and provides REST + WebSocket APIs
for historical data, symbol discovery, and live price streaming.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

from data_source import DATA_SOURCES
from models import DataSourceType, Timeframe
from ws_manager import ConnectionManager

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("main")

BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Stock Analyzer — Live Trading Dashboard",
    description="Multi-chart real-time trading dashboard with pluggable data sources",
    version="1.0.0",
)

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
manager = ConnectionManager()


# ---------------------------------------------------------------------------
# REST endpoints
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Serve the main dashboard page."""
    sources = {
        key: src["label"] for key, src in DATA_SOURCES.items()
    }
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "sources": sources},
    )


@app.get("/api/sources")
async def get_sources():
    """Return available data sources and their labels."""
    return {
        key: {"label": src["label"]} for key, src in DATA_SOURCES.items()
    }


@app.get("/api/symbols/{source}")
async def get_symbols(source: str):
    """Return available symbols for a given data source."""
    src = DATA_SOURCES.get(source)
    if not src:
        return {"error": f"Unknown source: {source}", "symbols": []}
    try:
        symbols = await src["get_symbols"]()
        return {"source": source, "symbols": symbols}
    except Exception as exc:
        logger.error("get_symbols error (%s): %s", source, exc)
        return {"error": str(exc), "symbols": []}


@app.get("/api/historical/{source}/{symbol}")
async def get_historical(
    source: str,
    symbol: str,
    timeframe: str = Query(default="1D", description="Candle timeframe"),
    limit: int = Query(default=300, ge=1, le=1000),
):
    """Fetch historical OHLCV candles for a symbol."""
    src = DATA_SOURCES.get(source)
    if not src:
        return {"error": f"Unknown source: {source}", "candles": []}
    try:
        candles = await src["get_historical"](symbol, timeframe, limit)
        return {
            "source": source,
            "symbol": symbol,
            "timeframe": timeframe,
            "candles": candles,
        }
    except Exception as exc:
        logger.error("get_historical error (%s/%s): %s", source, symbol, exc)
        return {"error": str(exc), "candles": []}


# ---------------------------------------------------------------------------
# WebSocket endpoint
# ---------------------------------------------------------------------------

@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    """
    Bidirectional WebSocket channel for live data.

    Client → Server messages:
        {"action": "subscribe",   "source": "...", "symbol": "...", "timeframe": "..."}
        {"action": "unsubscribe", "source": "...", "symbol": "..."}

    Server → Client messages:
        {"symbol": "...", "source": "...", "price": ..., "change": ...,
         "change_pct": ..., "volume": ..., "timestamp": ..., "ohlcv": {...}}
    """
    await manager.connect(client_id, websocket)
    try:
        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await websocket.send_text(json.dumps({"error": "Invalid JSON"}))
                continue

            action = msg.get("action")
            source = msg.get("source", "")
            symbol = msg.get("symbol", "")
            timeframe = msg.get("timeframe", "1m")

            if action == "subscribe" and source and symbol:
                await manager.subscribe(client_id, source, symbol, timeframe)
                await websocket.send_text(json.dumps({
                    "status": "subscribed",
                    "source": source,
                    "symbol": symbol,
                    "timeframe": timeframe,
                }))
            elif action == "unsubscribe" and source and symbol:
                await manager.unsubscribe(client_id, source, symbol)
                await websocket.send_text(json.dumps({
                    "status": "unsubscribed",
                    "source": source,
                    "symbol": symbol,
                }))
            else:
                await websocket.send_text(json.dumps({
                    "error": "Unknown action or missing fields",
                }))

    except WebSocketDisconnect:
        await manager.disconnect(client_id)
    except Exception as exc:
        logger.error("WebSocket error for %s: %s", client_id, exc)
        await manager.disconnect(client_id)


# ---------------------------------------------------------------------------
# Health check (for Render / monitoring)
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "clients": len(manager.clients),
        "streams": len(manager.streams),
    }


# ---------------------------------------------------------------------------
# Run with uvicorn (for local dev)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "10000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
