"""
Pydantic models for the Stock Analyzer trading dashboard.
Defines the data contracts used across the backend.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class DataSourceType(str, Enum):
    HYPERLIQUID = "hyperliquid"
    YAHOO_US = "yahoo_us"
    YAHOO_INDIA = "yahoo_india"
    # Future sources – add here when you wire them up
    # ALPACA = "alpaca"
    # BINANCE = "binance"
    # ZERODHA = "zerodha"


class Timeframe(str, Enum):
    M1 = "1m"
    M5 = "5m"
    M15 = "15m"
    H1 = "1h"
    H4 = "4h"
    D1 = "1D"
    W1 = "1W"


# ---------------------------------------------------------------------------
# Core data models
# ---------------------------------------------------------------------------

class OHLCV(BaseModel):
    """Single candlestick bar."""
    time: int = Field(..., description="Unix timestamp in seconds")
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0


class PriceTick(BaseModel):
    """Real-time price update pushed to the browser."""
    symbol: str
    source: DataSourceType
    price: float
    change: float = 0.0          # absolute change from previous tick
    change_pct: float = 0.0      # percentage change
    volume: Optional[float] = None
    timestamp: int = Field(
        default_factory=lambda: int(datetime.utcnow().timestamp())
    )
    # Optional OHLCV for candle updates
    ohlcv: Optional[OHLCV] = None


# ---------------------------------------------------------------------------
# WebSocket message contracts
# ---------------------------------------------------------------------------

class SubscriptionRequest(BaseModel):
    action: str = "subscribe"       # "subscribe" | "unsubscribe"
    source: DataSourceType
    symbol: str
    timeframe: Timeframe = Timeframe.M1


class UnsubscribeRequest(BaseModel):
    action: str = "unsubscribe"
    symbol: str
    source: DataSourceType


# ---------------------------------------------------------------------------
# REST API response models
# ---------------------------------------------------------------------------

class SymbolInfo(BaseModel):
    symbol: str
    name: str = ""
    exchange: str = ""
    asset_type: str = ""           # "crypto", "stock", "etf"


class HistoricalResponse(BaseModel):
    symbol: str
    source: DataSourceType
    timeframe: Timeframe
    candles: List[OHLCV]
