"""
Тесты volatility_analyzer — только чистые функции.
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

def make_long_amps(base=5.0, n=20):
    return [base + (i % 3) * 0.5 for i in range(n)]

def test_amplitude_basic():
    assert round(m.amplitude([100.0, 110.0]), 4) == 10.0

def test_amplitude_flat():
    assert m.amplitude([200.0, 200.0, 200.0]) == 0.0

def test_amplitude_zero_low():
    assert m.amplitude([0.0, 100.0]) == 0.0

def test_high_signal_on_spike():
    short  = [1000.0, 2000.0] + [1500.0] * 8
    long_a = make_long_amps(5.0, 50)
    result = m.compute_signal(short, long_a, 1500.0, NOW)
    assert result["value"] == "HIGH"

def test_low_signal_on_flat():
    short  = [2300.0] * 10
    long_a = make_long_amps(5.0, 50)
    result = m.compute_signal(short, long_a, 2300.0, NOW)
    assert result["value"] == "LOW"

def test_not_enough_history():
    result = m.compute_signal([2300.0] * 10, [5.0], 2300.0, NOW)
    assert result is None

def test_zero_sigma():
    result = m.compute_signal([2300.0] * 10, [5.0] * 20, 2300.0, NOW)
    assert result is None

def test_required_fields():
    short  = [1000.0, 2000.0] + [1500.0] * 8
    long_a = make_long_amps(5.0, 50)
    result = m.compute_signal(short, long_a, 1500.0, NOW)
    for f in ["source","asset","quote","signal_type","value","confidence","ttl_ms","timestamp_ms","meta"]:
        assert f in result

def test_meta_fields():
    short  = [1000.0, 2000.0] + [1500.0] * 8
    long_a = make_long_amps(5.0, 50)
    result = m.compute_signal(short, long_a, 1500.0, NOW)
    for f in ["amplitude","mean_amplitude","sigma","deviation","short_window","long_window","time"]:
        assert f in result["meta"]

def test_confidence_range():
    short  = [1000.0, 2000.0] + [1500.0] * 8
    long_a = make_long_amps(5.0, 50)
    result = m.compute_signal(short, long_a, 1500.0, NOW)
    assert 0.0 <= result["confidence"] <= 1.0

def test_signal_type():
    short  = [1000.0, 2000.0] + [1500.0] * 8
    long_a = make_long_amps(5.0, 50)
    result = m.compute_signal(short, long_a, 1500.0, NOW)
    assert result["signal_type"] == "VOLATILITY"
