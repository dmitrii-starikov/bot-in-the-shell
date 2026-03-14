"""Тесты market-stats — только чистые функции."""
import sys, os, unittest.mock, importlib.util

sys.modules["psycopg2"]        = unittest.mock.MagicMock()
sys.modules["psycopg2.extras"] = unittest.mock.MagicMock()
sys.modules["dotenv"]          = unittest.mock.MagicMock()
os.environ.update({"DB_HOST":"x","DB_PORT":"5432","DB_NAME":"x","DB_USER":"x","DB_PASSWORD":"x"})

spec = importlib.util.spec_from_file_location(
    "ms", os.path.join(os.path.dirname(__file__), "../src/main.py"))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def test_mean():
    assert m.mean([1, 2, 3, 4, 5]) == 3.0

def test_mean_empty():
    assert m.mean([]) == 0

def test_stdev():
    assert round(m.stdev([2, 4, 4, 4, 5, 5, 7, 9]), 4) == 2.0

def test_stdev_single():
    assert m.stdev([5]) == 0

def test_median_odd():
    assert m.median([1, 3, 5]) == 3

def test_median_even():
    assert m.median([1, 2, 3, 4]) == 2.5
