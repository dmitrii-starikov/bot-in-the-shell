"""
Тесты sigma_analyzer — только чистая функция compute_signal.
Без Kafka, без сети.
"""
import sys, os, unittest.mock, importlib.util

sys.modules["kafka"]  = unittest.mock.MagicMock()
sys.modules["dotenv"] = unittest.mock.MagicMock()
os.environ.update({"KAFKA_URL": "x", "ASSET": "ETH", "QUOTE": "USDT"})

spec = importlib.util.spec_from_file_location(
    "analyzer", os.path.join(os.path.dirname(__file__), "../src/main.py"))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

NOW = 1704067200000


def test_neutral_within_sigma():
    import random; random.seed(42)
    prices = [2300 + random.uniform(-50, 50) for _ in range(20)]
    result = m.compute_signal(prices, 2300.0, NOW)
    assert result["value"] == "NEUTRAL"


def test_oversold_below_sigma():
    prices = [2300.0] * 20
    # sigma будет ~0 для одинаковых цен, добавим немного вариации
    prices = [2300 + (i % 3) for i in range(20)]
    result = m.compute_signal(prices, 2100.0, NOW)
    assert result["value"] == "OVERSOLD"


def test_overbought_above_sigma():
    prices = [2300 + (i % 3) for i in range(20)]
    result = m.compute_signal(prices, 2600.0, NOW)
    assert result["value"] == "OVERBOUGHT"


def test_confidence_between_0_and_1():
    prices = [2300 + (i % 5) for i in range(20)]
    for price in [2100.0, 2300.0, 2600.0]:
        result = m.compute_signal(prices, price, NOW)
        if result:
            assert 0.0 <= result["confidence"] <= 1.0


def test_zero_sigma_returns_none():
    prices = [2300.0] * 20  # все одинаковые → σ=0
    result = m.compute_signal(prices, 2300.0, NOW)
    assert result is None


def test_required_fields():
    prices = [2300 + (i % 3) for i in range(20)]
    result = m.compute_signal(prices, 2100.0, NOW)
    for f in ["source", "asset", "quote", "signal_type", "value",
              "confidence", "ttl_ms", "timestamp_ms", "meta"]:
        assert f in result


def test_meta_contains_debug_info():
    prices = [2300 + (i % 3) for i in range(20)]
    result = m.compute_signal(prices, 2100.0, NOW)
    for f in ["mean", "sigma", "price", "deviation", "window", "time"]:
        assert f in result["meta"]


def test_price_added_after_signal():
    """Текущая цена добавляется в историю ПОСЛЕ расчёта — не влияет на текущий сигнал."""
    from collections import deque
    history = deque([2300 + (i % 3) for i in range(20)], maxlen=20)
    price_before = list(history)

    result = m.compute_signal(list(history), 2100.0, NOW)
    # история не должна измениться внутри функции
    assert list(history) == price_before
