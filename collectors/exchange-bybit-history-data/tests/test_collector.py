"""Тесты exchange-bybit-history-data — без БД и Kafka."""
import sys, os, unittest.mock, importlib.util, datetime

for mod in ["kafka", "dotenv", "tqdm", "psycopg2", "psycopg2.extras", "requests"]:
    sys.modules[mod] = unittest.mock.MagicMock()

os.environ.update({
    "DB_HOST": "localhost", "DB_PORT": "5432", "DB_NAME": "botdb",
    "DB_USER": "bot", "DB_PASSWORD": "bot", "KAFKA_URL": "x",
    "SYMBOL": "ETHUSDT",
})

spec = importlib.util.spec_from_file_location(
    "bybit", os.path.join(os.path.dirname(__file__), "../src/main.py"))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def make_row():
    return {
        "symbol":      "ETHUSDT",
        "date":        datetime.date(2024, 1, 15),
        "open_price":  2200.0,
        "high_price":  2400.0,
        "low_price":   2100.0,
        "close_price": 2350.0,
        "volume":      50000.0,
        "time_open":   datetime.datetime(2024, 1, 15, 0, 0, 0),
    }


def test_build_price_event_fields():
    event = m.build_price_event(make_row())
    for f in ["asset", "quote", "exchange", "open", "high", "low",
              "close", "volume", "interval", "timestamp_ms", "candle_id"]:
        assert f in event


def test_build_price_event_asset():
    event = m.build_price_event(make_row())
    assert event["asset"] == "ETH"
    assert event["quote"] == "USDT"
    assert event["exchange"] == "bybit"


def test_candle_to_ticks_count():
    ticks = m.candle_to_ticks(make_row())
    assert len(ticks) == 5


def test_ticks_bullish_order():
    """Бычий день: первый тик = open, последний = close (выше open)."""
    row   = make_row()  # close=2350 > open=2200 → бычий
    ticks = m.candle_to_ticks(row)
    assert ticks[0]["price"] == row["open_price"]
    assert ticks[-1]["price"] == row["close_price"]


def test_ticks_bearish_order():
    """Медвежий день: первый тик = open, последний = close (ниже open)."""
    row = make_row()
    row["close_price"] = 2000.0  # медвежий
    ticks = m.candle_to_ticks(row)
    assert ticks[0]["price"] == row["open_price"]
    assert ticks[-1]["price"] == row["close_price"]


def test_ticks_have_price_field():
    for tick in m.candle_to_ticks(make_row()):
        assert "price" in tick
        assert "timestamp_ms" in tick
        assert "candle_id" in tick


def test_ticks_timestamps_ascending():
    ticks = m.candle_to_ticks(make_row())
    timestamps = [t["timestamp_ms"] for t in ticks]
    assert timestamps == sorted(timestamps)


def test_ticks_volume_split():
    row   = make_row()
    ticks = m.candle_to_ticks(row)
    assert all(t["volume"] == row["volume"] / 5 for t in ticks)
