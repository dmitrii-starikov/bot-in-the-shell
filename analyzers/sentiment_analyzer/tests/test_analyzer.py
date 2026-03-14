"""Тесты sentiment-analyzer — только чистая функция compute_signal."""
import sys, os, unittest.mock, importlib.util

sys.modules["kafka"]  = unittest.mock.MagicMock()
sys.modules["dotenv"] = unittest.mock.MagicMock()
os.environ.update({"KAFKA_URL": "x"})

spec = importlib.util.spec_from_file_location(
    "sa", os.path.join(os.path.dirname(__file__), "../src/main.py"))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

NOW = 1704067200000


def test_extreme_fear_is_bullish():
    r = m.compute_signal(10, "Extreme Fear", NOW)
    assert r["value"] == "BULLISH"

def test_fear_is_bullish():
    r = m.compute_signal(35, "Fear", NOW)
    assert r["value"] == "BULLISH"

def test_neutral_is_neutral():
    r = m.compute_signal(50, "Neutral", NOW)
    assert r["value"] == "NEUTRAL"

def test_greed_is_bearish():
    r = m.compute_signal(65, "Greed", NOW)
    assert r["value"] == "BEARISH"

def test_extreme_greed_is_bearish():
    r = m.compute_signal(90, "Extreme Greed", NOW)
    assert r["value"] == "BEARISH"

def test_extreme_fear_high_confidence():
    low  = m.compute_signal(5,  "Extreme Fear", NOW)
    high = m.compute_signal(20, "Extreme Fear", NOW)
    assert low["confidence"] > high["confidence"]

def test_extreme_greed_high_confidence():
    low  = m.compute_signal(80, "Extreme Greed", NOW)
    high = m.compute_signal(95, "Extreme Greed", NOW)
    assert high["confidence"] > low["confidence"]

def test_confidence_range():
    for v in [5, 25, 35, 50, 65, 80, 95]:
        r = m.compute_signal(v, "", NOW)
        assert 0.0 <= r["confidence"] <= 1.0, f"confidence out of range for {v}"

def test_required_fields():
    r = m.compute_signal(10, "Extreme Fear", NOW)
    for f in ["source", "asset", "quote", "signal_type", "value",
              "confidence", "ttl_ms", "timestamp_ms", "meta"]:
        assert f in r

def test_signal_type_is_trend():
    r = m.compute_signal(50, "Neutral", NOW)
    assert r["signal_type"] == "TREND"

def test_meta_fields():
    r = m.compute_signal(25, "Fear", NOW)
    assert "fg_value" in r["meta"]
    assert "classification" in r["meta"]
    assert "date" in r["meta"]

def test_ttl_is_24h():
    r = m.compute_signal(50, "Neutral", NOW)
    assert r["ttl_ms"] == 24 * 3600 * 1000
