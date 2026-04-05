"""Tests for OrderBook state management and KalshiWebSocket message dispatch."""

from __future__ import annotations

import json
import time
from unittest.mock import MagicMock, patch

import pytest

from src.orderbook.ws_client import KalshiWebSocket, OrderBook


# ── OrderBook.apply_snapshot ─────────────────────────────────────────


class TestOrderBookSnapshot:
    def test_apply_snapshot_basic(self) -> None:
        book = OrderBook(ticker="TEST-TICKER")
        book.apply_snapshot(
            yes=[["0.45", "100.00"], ["0.44", "200.00"], ["0.43", "300.00"]],
            no=[["0.54", "150.00"], ["0.53", "250.00"], ["0.52", "50.00"]],
        )
        # YES bids sorted descending
        assert book.yes_bids[0][0] == 0.45
        assert book.yes_bids[1][0] == 0.44
        assert book.yes_bids[2][0] == 0.43

        # NO bids sorted descending
        assert book.no_bids[0][0] == 0.54
        assert book.no_bids[1][0] == 0.53

    def test_best_bid_ask(self) -> None:
        book = OrderBook(ticker="TEST")
        book.apply_snapshot(
            yes=[["0.48", "100.00"]],
            no=[["0.51", "200.00"]],
        )
        assert book.best_bid == 0.48
        assert book.best_ask == pytest.approx(0.49, abs=1e-4)  # 1.0 - 0.51
        assert book.mid == pytest.approx(0.485, abs=1e-4)
        assert book.spread == pytest.approx(0.01, abs=1e-4)

    def test_depth_properties(self) -> None:
        book = OrderBook(ticker="TEST")
        book.apply_snapshot(
            yes=[["0.48", "1000.00"], ["0.47", "500.00"]],
            no=[["0.51", "2000.00"], ["0.50", "300.00"]],
        )
        assert book.bid_depth == 1000
        assert book.ask_depth == 2000
        assert book.depth_ratio == pytest.approx(1000 / 3000, abs=1e-4)

    def test_empty_book(self) -> None:
        book = OrderBook(ticker="TEST")
        assert book.is_empty
        assert book.best_bid == 0.0
        assert book.best_ask == 1.0
        assert book.mid == 0.5
        assert book.bid_depth == 0
        assert book.ask_depth == 0
        assert book.depth_ratio == 0.5

    def test_snapshot_replaces_previous(self) -> None:
        book = OrderBook(ticker="TEST")
        book.apply_snapshot(
            yes=[["0.30", "100.00"]],
            no=[["0.70", "100.00"]],
        )
        assert book.best_bid == 0.30

        book.apply_snapshot(
            yes=[["0.50", "200.00"]],
            no=[["0.50", "200.00"]],
        )
        assert book.best_bid == 0.50
        assert len(book.yes_bids) == 1

    def test_ask_levels_derived(self) -> None:
        book = OrderBook(ticker="TEST")
        book.apply_snapshot(
            yes=[["0.48", "100.00"]],
            no=[["0.55", "200.00"], ["0.52", "150.00"]],
        )
        ask_levels = book.ask_levels
        # Sorted ascending by YES ask price
        assert ask_levels[0][0] == pytest.approx(0.45, abs=1e-4)  # 1 - 0.55
        assert ask_levels[1][0] == pytest.approx(0.48, abs=1e-4)  # 1 - 0.52


# ── OrderBook.apply_delta ────────────────────────────────────────────


class TestOrderBookDelta:
    def test_add_new_level(self) -> None:
        book = OrderBook(ticker="TEST")
        book.apply_snapshot(yes=[["0.48", "100.00"]], no=[["0.52", "200.00"]])

        book.apply_delta(price=0.47, delta=50.0, side="yes")
        assert len(book.yes_bids) == 2
        assert book.yes_bids[0][0] == 0.48  # still best
        assert book.yes_bids[1] == [0.47, 50.0]

    def test_update_existing_level(self) -> None:
        book = OrderBook(ticker="TEST")
        book.apply_snapshot(yes=[["0.48", "100.00"]], no=[])

        book.apply_delta(price=0.48, delta=50.0, side="yes")
        assert book.yes_bids[0][1] == 150.0  # 100 + 50

    def test_remove_level_on_zero(self) -> None:
        book = OrderBook(ticker="TEST")
        book.apply_snapshot(yes=[["0.48", "100.00"], ["0.47", "50.00"]], no=[])

        book.apply_delta(price=0.48, delta=-100.0, side="yes")
        assert len(book.yes_bids) == 1
        assert book.yes_bids[0][0] == 0.47

    def test_delta_negative_below_zero_removes(self) -> None:
        book = OrderBook(ticker="TEST")
        book.apply_snapshot(yes=[["0.48", "30.00"]], no=[["0.52", "100.00"]])

        book.apply_delta(price=0.48, delta=-50.0, side="yes")
        assert len(book.yes_bids) == 0
        assert book.is_empty is False  # no_bids still present

    def test_delta_no_side(self) -> None:
        book = OrderBook(ticker="TEST")
        book.apply_snapshot(yes=[], no=[["0.55", "100.00"]])

        book.apply_delta(price=0.55, delta=-30.0, side="no")
        assert book.no_bids[0][1] == 70.0

    def test_ignore_negative_delta_for_missing_level(self) -> None:
        book = OrderBook(ticker="TEST")
        book.apply_snapshot(yes=[["0.48", "100.00"]], no=[])

        # Delta for a price that doesn't exist — should not add negative qty
        book.apply_delta(price=0.40, delta=-50.0, side="yes")
        assert len(book.yes_bids) == 1  # unchanged


# ── KalshiWebSocket message dispatch ────────────────────────────────


class TestWebSocketDispatch:
    def _make_ws(self) -> KalshiWebSocket:
        from src.config import Config
        config = Config()
        ws = KalshiWebSocket(config)
        return ws

    def test_dispatch_orderbook_snapshot(self) -> None:
        ws = self._make_ws()
        callback = MagicMock()
        ws.on_orderbook(callback)

        msg = {
            "type": "orderbook_snapshot",
            "sid": 1,
            "seq": 1,
            "msg": {
                "market_ticker": "KXMLBGAME-26APR04-NYM",
                "yes_dollars_fp": [["0.48", "100.00"], ["0.47", "200.00"]],
                "no_dollars_fp": [["0.52", "150.00"]],
            },
        }
        ws._dispatch(msg)

        book = ws.get_book("KXMLBGAME-26APR04-NYM")
        assert book is not None
        assert book.best_bid == 0.48
        assert book.best_ask == pytest.approx(0.48, abs=1e-4)
        callback.assert_called_once()

    def test_dispatch_orderbook_delta(self) -> None:
        ws = self._make_ws()

        # First set up a snapshot
        ws._dispatch({
            "type": "orderbook_snapshot",
            "sid": 1,
            "seq": 1,
            "msg": {
                "market_ticker": "TEST",
                "yes_dollars_fp": [["0.48", "100.00"]],
                "no_dollars_fp": [["0.52", "200.00"]],
            },
        })

        # Then apply delta
        ws._dispatch({
            "type": "orderbook_delta",
            "sid": 1,
            "seq": 2,
            "msg": {
                "market_ticker": "TEST",
                "price_dollars": "0.49",
                "delta_fp": "50.00",
                "side": "yes",
            },
        })

        book = ws.get_book("TEST")
        assert book is not None
        assert len(book.yes_bids) == 2
        assert book.best_bid == 0.49  # new best

    def test_dispatch_trade(self) -> None:
        ws = self._make_ws()
        callback = MagicMock()
        ws.on_trade(callback)

        ws._dispatch({
            "type": "trade",
            "sid": 2,
            "msg": {
                "trade_id": "abc123",
                "market_ticker": "TEST",
                "yes_price_dollars": "0.48",
                "no_price_dollars": "0.52",
                "count_fp": "25.00",
                "taker_side": "yes",
                "ts": 1700000000,
            },
        })

        callback.assert_called_once()
        trade = callback.call_args[0][0]
        assert trade["market_ticker"] == "TEST"
        assert trade["yes_price"] == 0.48
        assert trade["count"] == 25
        assert trade["taker_side"] == "yes"

    def test_unknown_message_type_ignored(self) -> None:
        ws = self._make_ws()
        callback = MagicMock()
        ws.on_orderbook(callback)
        ws.on_trade(callback)

        ws._dispatch({"type": "subscription_ack", "id": 1})
        callback.assert_not_called()

    def test_seq_gap_detection_logs_warning(self) -> None:
        ws = self._make_ws()

        # Snapshot at seq=1
        ws._dispatch({
            "type": "orderbook_snapshot",
            "sid": 1,
            "seq": 1,
            "msg": {
                "market_ticker": "TEST",
                "yes_dollars_fp": [["0.48", "100.00"]],
                "no_dollars_fp": [],
            },
        })
        assert ws._last_seq[1] == 1

        # Delta at seq=3 (gap — seq=2 missing)
        ws._dispatch({
            "type": "orderbook_delta",
            "sid": 1,
            "seq": 3,
            "msg": {
                "market_ticker": "TEST",
                "price_dollars": "0.47",
                "delta_fp": "50.00",
                "side": "yes",
            },
        })
        # Seq updated despite gap (delta still applied)
        assert ws._last_seq[1] == 3

    def test_seq_gap_triggers_resubscribe(self) -> None:
        import asyncio

        ws = self._make_ws()

        # Snapshot at seq=1
        ws._dispatch({
            "type": "orderbook_snapshot",
            "sid": 1,
            "seq": 1,
            "msg": {
                "market_ticker": "TEST",
                "yes_dollars_fp": [["0.48", "100.00"]],
                "no_dollars_fp": [],
            },
        })

        # Delta at seq=3 within a running loop
        resubscribed = False
        original = ws._resubscribe_for_snapshot

        async def mock_resub(sid: int) -> None:
            nonlocal resubscribed
            resubscribed = True

        ws._resubscribe_for_snapshot = mock_resub  # type: ignore[assignment]

        async def run_test() -> None:
            ws._dispatch({
                "type": "orderbook_delta",
                "sid": 1,
                "seq": 3,
                "msg": {
                    "market_ticker": "TEST",
                    "price_dollars": "0.47",
                    "delta_fp": "50.00",
                    "side": "yes",
                },
            })
            # Let the created task execute
            await asyncio.sleep(0.01)

        asyncio.run(run_test())
        assert resubscribed


# ── Subscription message format ──────────────────────────────────────


class TestSubscriptionFormat:
    def test_subscribe_builds_correct_message(self) -> None:
        """Verify the subscription JSON matches Kalshi's expected format."""
        import asyncio
        from unittest.mock import AsyncMock

        ws = TestWebSocketDispatch._make_ws(TestWebSocketDispatch())
        ws._ws = AsyncMock()
        ws._connected = True

        asyncio.run(ws.subscribe(["KXMLBGAME-26APR04-NYM"]))

        ws._ws.send.assert_called_once()
        sent = json.loads(ws._ws.send.call_args[0][0])
        assert sent["cmd"] == "subscribe"
        assert "orderbook_delta" in sent["params"]["channels"]
        assert "trade" in sent["params"]["channels"]
        assert "KXMLBGAME-26APR04-NYM" in sent["params"]["market_tickers"]

    def test_subscribe_skips_already_subscribed(self) -> None:
        import asyncio
        from unittest.mock import AsyncMock

        async def run_test() -> None:
            ws = TestWebSocketDispatch._make_ws(TestWebSocketDispatch())
            ws._ws = AsyncMock()
            ws._connected = True

            await ws.subscribe(["TICKER-A"])
            assert ws._ws.send.call_count == 1

            # Subscribe again — should be skipped
            await ws.subscribe(["TICKER-A"])
            assert ws._ws.send.call_count == 1  # no new call

        asyncio.run(run_test())
