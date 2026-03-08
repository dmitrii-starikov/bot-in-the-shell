"""
Тесты historical_collector.
Только чистые функции — без БД, без Kafka, без сети.
"""
import sys, os, unittest.mock, importlib.util, datetime

sys.modules["kafka"]   = unittest.mock.MagicMock()
sys.modules["dotenv"]  = unittest.mock.MagicMock()
sys.modules["tqdm"]    = unittest.mock.MagicMock()
sys.modules["psycopg2"] = unittest.mock.MagicMock()
sys.modules["psycopg2.extras"] = unittest.mock.MagicMock()
sys.modules["requests"] = unittest.mock.MagicMock()
sys.modules["orjson"]  = unittest.mock.MagicMock()

# Прокидываем нужные ENV чтобы модуль загрузился
os.environ.update({
    "DB_HOST": "localhost", "DB_PORT": "5432", "DB_NAME": "botdb",
    "DB_USER": "bot", "DB_PASSWORD": "bot",
    "KAFKA_URL": "localhost:9092",
    "COINMARKETCAP_BTC_ID": "1",
    "COINMARKETCAP_ETH_ID": "1027",
    "COINMARKETCAP_BNB_ID": "1839",
})

spec = importlib.util.spec_from_file_location(
    "collector", os.path.join(os.path.dirname(__file__), "../src/main.py"))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


# ─── convert_date ─────────────────────────────────────────────────────────────

def test_convert_date_format():
    result = m.convert_date("2023-01-15T00:00:00.000Z")
    assert result == "2023-01-15 00:00:00"


def test_convert_date_returns_string():
    result = m.convert_date("2020-06-01T12:00:00.000Z")
    assert isinstance(result, str)


# ─── build_price_event ────────────────────────────────────────────────────────

def make_row(coin="eth", date="2023-01-15"):
    return {
        "coin":        coin,
        "date":        datetime.date(2023, 1, 15),
        "open_price":  1500.0,
        "high_price":  1600.0,
        "low_price":   1450.0,
        "close_price": 1550.0,
        "volume":      9000000,
        "time_open":   datetime.datetime(2023, 1, 15, 0, 0, 0),
    }


def test_build_price_event_required_fields():
    event = m.build_price_event(make_row())
    for f in ["asset", "quote", "exchange", "open", "high", "low",
              "close", "volume", "interval", "timestamp_ms", "candle_id"]:
        assert f in event, f"Missing: {f}"


def test_build_price_event_asset_uppercased():
    event = m.build_price_event(make_row(coin="eth"))
    assert event["asset"] == "ETH"


def test_build_price_event_interval_is_1d():
    event = m.build_price_event(make_row())
    assert event["interval"] == "1d"


def test_build_price_event_exchange_is_historical():
    event = m.build_price_event(make_row())
    assert event["exchange"] == "historical"


def test_build_price_event_candle_id_contains_asset():
    event = m.build_price_event(make_row(coin="btc"))
    assert "BTC" in event["candle_id"]


def test_build_price_event_timestamp_ms_is_int():
    event = m.build_price_event(make_row())
    assert isinstance(event["timestamp_ms"], int)
    assert event["timestamp_ms"] > 0
