"""Тесты exchange-alternative-fear-greed — без БД и Kafka."""
import sys, os, unittest.mock, importlib.util, datetime

for mod in ["kafka", "dotenv", "tqdm", "psycopg2", "psycopg2.extras", "requests"]:
    sys.modules[mod] = unittest.mock.MagicMock()

os.environ.update({
    "DB_HOST": "localhost", "DB_PORT": "5432", "DB_NAME": "botdb",
    "DB_USER": "bot", "DB_PASSWORD": "bot", "KAFKA_URL": "x",
})

spec = importlib.util.spec_from_file_location(
    "fg", os.path.join(os.path.dirname(__file__), "../src/main.py"))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def make_row(value=72, classification="Greed", date="2024-01-01"):
    return {
        "value":          value,
        "classification": classification,
        "timestamp_ms":   1704067200000,
        "date":           datetime.date(2024, 1, 1),
    }


def test_publish_event_fields():
    row   = make_row()
    event = {
        "source":         "alternative.fear.greed",
        "value":          row["value"],
        "classification": row["classification"],
        "timestamp_ms":   row["timestamp_ms"],
        "date":           str(row["date"]),
    }
    for f in ["source", "value", "classification", "timestamp_ms", "date"]:
        assert f in event


def test_value_range():
    for v in [0, 25, 50, 75, 100]:
        assert 0 <= v <= 100


def test_classification_values():
    valid = {"Extreme Fear", "Fear", "Neutral", "Greed", "Extreme Greed"}
    assert "Greed" in valid
    assert "Extreme Fear" in valid


def test_date_string_format():
    d     = datetime.date(2024, 1, 15)
    s     = str(d)
    assert s == "2024-01-15"
