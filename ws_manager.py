"""
ws_manager.py — WebSocket connection manager
=============================================

Manages browser ↔ server ↔ upstream data-source connections:
- Tracks active browser clients and their subscriptions
- Spins up / tears down data-source streaming tasks per (source, symbol)
- Routes incoming ticks to the correct browser clients
- Handles graceful cleanup on disconnect
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from typing import Dict, Optional, Set, Tuple

from fastapi import WebSocket

from data_source import DATA_SOURCES
from models import DataSourceType

logger = logging.getLogger("ws_manager")


@dataclass
class Subscription:
    """Tracks one client's subscription to a (source, symbol, timeframe)."""
    source: str
    symbol: str
    timeframe: str


@dataclass
class ClientState:
    """Per-client state."""
    websocket: WebSocket
    subscriptions: Dict[str, Subscription] = field(default_factory=dict)
    # key = f"{source}:{symbol}"


@dataclass
class StreamTask:
    """A single upstream data-stream task shared across clients."""
    task: asyncio.Task
    subscribers: Set[str] = field(default_factory=set)  # client_ids
    last_price: Optional[float] = None


class ConnectionManager:
    """
    Manages all WebSocket clients and the upstream data-source streams.

    For each unique (source, symbol, timeframe) that at least one client
    cares about, we maintain ONE upstream streaming task. When the last
    client unsubscribes, the task is cancelled.
    """

    def __init__(self) -> None:
        self.clients: Dict[str, ClientState] = {}
        self.streams: Dict[str, StreamTask] = {}  # key = "source:symbol:timeframe"
        self._lock = asyncio.Lock()

    # ── Client lifecycle ───────────────────────────────────────────────

    async def connect(self, client_id: str, websocket: WebSocket) -> None:
        await websocket.accept()
        self.clients[client_id] = ClientState(websocket=websocket)
        logger.info("Client %s connected. Total: %d", client_id, len(self.clients))

    async def disconnect(self, client_id: str) -> None:
        client = self.clients.pop(client_id, None)
        if not client:
            return

        # Clean up all subscriptions for this client
        for sub_key in list(client.subscriptions.keys()):
            sub = client.subscriptions[sub_key]
            stream_key = f"{sub.source}:{sub.symbol}:{sub.timeframe}"
            await self._remove_subscriber(stream_key, client_id)

        logger.info("Client %s disconnected. Total: %d", client_id, len(self.clients))

    # ── Subscription management ────────────────────────────────────────

    async def subscribe(
        self, client_id: str, source: str, symbol: str, timeframe: str
    ) -> None:
        client = self.clients.get(client_id)
        if not client:
            return

        sub_key = f"{source}:{symbol}"

        # Unsubscribe from any existing subscription on the same source:symbol
        if sub_key in client.subscriptions:
            old_sub = client.subscriptions[sub_key]
            old_stream_key = f"{old_sub.source}:{old_sub.symbol}:{old_sub.timeframe}"
            await self._remove_subscriber(old_stream_key, client_id)

        # Register new subscription
        client.subscriptions[sub_key] = Subscription(
            source=source, symbol=symbol, timeframe=timeframe
        )

        stream_key = f"{source}:{symbol}:{timeframe}"
        await self._add_subscriber(stream_key, client_id, source, symbol, timeframe)

        logger.info(
            "Client %s subscribed: %s/%s @%s", client_id, source, symbol, timeframe
        )

    async def unsubscribe(self, client_id: str, source: str, symbol: str) -> None:
        client = self.clients.get(client_id)
        if not client:
            return

        sub_key = f"{source}:{symbol}"
        sub = client.subscriptions.pop(sub_key, None)
        if sub:
            stream_key = f"{sub.source}:{sub.symbol}:{sub.timeframe}"
            await self._remove_subscriber(stream_key, client_id)
            logger.info("Client %s unsubscribed: %s/%s", client_id, source, symbol)

    # ── Stream task management ─────────────────────────────────────────

    async def _add_subscriber(
        self,
        stream_key: str,
        client_id: str,
        source: str,
        symbol: str,
        timeframe: str,
    ) -> None:
        async with self._lock:
            if stream_key in self.streams:
                self.streams[stream_key].subscribers.add(client_id)
            else:
                # Start a new stream task
                task = asyncio.create_task(
                    self._run_stream(stream_key, source, symbol, timeframe)
                )
                self.streams[stream_key] = StreamTask(
                    task=task, subscribers={client_id}
                )

    async def _remove_subscriber(self, stream_key: str, client_id: str) -> None:
        async with self._lock:
            stream = self.streams.get(stream_key)
            if not stream:
                return
            stream.subscribers.discard(client_id)
            if not stream.subscribers:
                stream.task.cancel()
                del self.streams[stream_key]
                logger.info("Stream %s stopped (no subscribers)", stream_key)

    async def _run_stream(
        self, stream_key: str, source: str, symbol: str, timeframe: str
    ) -> None:
        """
        Long-running task that consumes the data-source async generator
        and fans out ticks to all subscribed browser clients.
        """
        src = DATA_SOURCES.get(source)
        if not src:
            logger.error("Unknown data source: %s", source)
            return

        subscribe_live = src["subscribe_live"]
        try:
            async for tick in subscribe_live(symbol, timeframe):
                stream = self.streams.get(stream_key)
                if not stream:
                    break

                # Calculate change from last price
                last = stream.last_price
                price = tick.get("price", 0)
                if last is not None and last != 0:
                    tick["change"] = round(price - last, 6)
                    tick["change_pct"] = round((price - last) / last * 100, 4)
                else:
                    tick["change"] = 0.0
                    tick["change_pct"] = 0.0
                stream.last_price = price

                tick["source"] = source

                # Fan out to all subscribers
                dead_clients: list[str] = []
                for cid in list(stream.subscribers):
                    client = self.clients.get(cid)
                    if not client:
                        dead_clients.append(cid)
                        continue
                    try:
                        await client.websocket.send_text(json.dumps(tick))
                    except Exception:
                        dead_clients.append(cid)

                # Clean up dead clients
                for cid in dead_clients:
                    stream.subscribers.discard(cid)
                    await self.disconnect(cid)

        except asyncio.CancelledError:
            logger.info("Stream %s cancelled", stream_key)
        except Exception as exc:
            logger.error("Stream %s error: %s", stream_key, exc)

    # ── Broadcast helpers ──────────────────────────────────────────────

    async def send_to_client(self, client_id: str, data: dict) -> None:
        client = self.clients.get(client_id)
        if client:
            try:
                await client.websocket.send_text(json.dumps(data))
            except Exception:
                await self.disconnect(client_id)
