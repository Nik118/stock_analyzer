# 📊 Stock Analyzer — Live Trading Dashboard

A real-time multi-chart trading dashboard built with **FastAPI**, **TradingView Lightweight Charts**, and pluggable data sources.

![Python](https://img.shields.io/badge/Python-3.11+-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-0.115-green)
![License](https://img.shields.io/badge/License-MIT-yellow)

## ✨ Features

- **Multi-Chart Layout** — 1, 2, 4, 6, or 8 charts in a responsive grid
- **Real-Time Crypto** — Hyperliquid WebSocket for live crypto prices (BTC, ETH, SOL, 20+ pairs)
- **US & Indian Stocks** — Yahoo Finance for AAPL, NVDA, RELIANCE.NS, TCS.NS, and more
- **Technical Analysis** — SMA, EMA, RSI, MACD, Bollinger Bands, VWAP — all computed client-side
- **Color-Coded Tickers** — Green/red flash animation on every price change
- **Pluggable Architecture** — Add any broker (Alpaca, Binance, Zerodha) by adding 3 functions to `data_source.py`
- **Dark Glassmorphism UI** — Premium dark theme with gradient accents and smooth animations
- **Deployment Ready** — Render, Heroku, Railway — one-click deploy

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- pip

### Setup

```bash
# Clone the repo
git clone https://github.com/yourusername/stock_analyzer.git
cd stock_analyzer

# Create virtual environment
python -m venv venv

# Activate (Windows)
venv\Scripts\activate
# Activate (macOS/Linux)
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Copy environment template
copy .env.example .env     # Windows
cp .env.example .env       # macOS/Linux

# Run the server
python main.py
```

Open **http://localhost:10000** in your browser.

## 📐 Architecture

```
stock_analyzer/
├── main.py                  # FastAPI app, routes, WebSocket endpoint
├── data_source.py           # Pluggable data source abstraction
├── ws_manager.py            # WebSocket connection manager
├── models.py                # Pydantic data models
├── templates/
│   └── index.html           # Full SPA dashboard
├── requirements.txt         # Python dependencies
├── render.yaml              # Render deploy blueprint
├── Procfile                 # Heroku/Railway compatibility
└── .env.example             # Environment template
```

## 🔌 Adding a New Data Source

Edit `data_source.py` and add a new entry to the `DATA_SOURCES` dict:

```python
DATA_SOURCES["alpaca"] = {
    "get_historical": alpaca_get_historical,    # async (symbol, tf, limit) -> [OHLCV]
    "subscribe_live": alpaca_subscribe_live,     # async generator -> price ticks
    "get_symbols": alpaca_get_symbols,           # async () -> [SymbolInfo]
    "label": "Alpaca (US Stocks)",
}
```

Each function follows a documented interface — see the docstrings at the top of `data_source.py`.

## 📊 Supported Indicators

| Indicator | Parameters | Rendering |
|-----------|-----------|-----------|
| SMA | Period: 20, 50 | Line overlay |
| EMA | Period: 12, 26 | Line overlay |
| RSI | Period: 14 | Separate mini-pane |
| MACD | 12, 26, 9 | Histogram + lines |
| Bollinger Bands | Period: 20, StdDev: 2 | Upper/middle/lower bands |
| VWAP | Session-based | Line overlay |

## 🌐 Deployment

### Render (Recommended)

1. Push to GitHub
2. Go to [render.com](https://render.com) → **New** → **Blueprint**
3. Connect your repo — `render.yaml` handles everything

### Heroku / Railway

```bash
# The Procfile handles the start command
heroku create stock-analyzer
git push heroku main
```

### Docker (Optional)

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "10000"]
```

## ⚠️ Notes

- **Yahoo Finance data is delayed** (~15 min). For true real-time Indian stock data, integrate Zerodha Kite Connect via `data_source.py`.
- **Hyperliquid is real-time** — crypto charts update tick-by-tick via WebSocket.
- Free Render instances sleep after 15 min of inactivity; first request after sleep may take 30–60 seconds.

## 📄 License

MIT