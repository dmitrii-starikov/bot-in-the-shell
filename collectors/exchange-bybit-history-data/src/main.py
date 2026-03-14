"""
exchange-bybit-history-data — загружает исторические дневные свечи с Bybit
и публикует в Kafka.

Команды:
    python src/main.py fetch    — скачать и сохранить в БД
    python src/main.py publish  — прочитать из БД и запушить в Kafka
    python src/main.py simulate — пушить тики (5 на свечу) как симуляция биржи
    python src/main.py all      — fetch + publish (по умолчанию)

API: https://api.bybit.com/v5/market/kline (публичный, без ключа)
Лимит: 1000 свечей за запрос, пагинация через end timestamp.
"""
import os, sys, time, json, datetime, requests
import psycopg2
import psycopg2.extras
from kafka import KafkaProducer
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

DB_HOST   = os.environ["DB_HOST"]
DB_PORT   = os.environ.get("DB_PORT", "5432")
DB_NAME   = os.environ["DB_NAME"]
DB_USER   = os.environ["DB_USER"]
DB_PASS   = os.environ["DB_PASSWORD"]

KAFKA_URL    = os.environ.get("KAFKA_URL",    "localhost:9092")
TOPIC        = os.environ.get("KAFKA_TOPIC",  "collectors.market.price")
SYMBOL       = os.environ.get("SYMBOL",       "ETHUSDT")
YEAR_FROM    = int(os.environ.get("YEAR_FROM", "2019"))  # Bybit запустился в 2019
YEAR_TO      = int(os.environ.get("YEAR_TO",   "2026"))

QUOTE_SYMBOL = SYMBOL[len(SYMBOL.rstrip("USDTBUSD")):]  # USDT из ETHUSDT
ASSET_SYMBOL = SYMBOL[:len(SYMBOL) - len(QUOTE_SYMBOL)] # ETH  из ETHUSDT

API_URL  = "https://api.bybit.com/v5/market/kline"
LIMIT    = 1000
INTERVAL = "D"


# ─── БД ──────────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )


def init_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bybit_daily_candles (
                id          SERIAL PRIMARY KEY,
                symbol      VARCHAR(20)  NOT NULL,
                date        DATE         NOT NULL,
                open_price  FLOAT        NOT NULL,
                high_price  FLOAT        NOT NULL,
                low_price   FLOAT        NOT NULL,
                close_price FLOAT        NOT NULL,
                volume      FLOAT        NOT NULL,
                time_open   TIMESTAMP    NOT NULL,
                created_at  TIMESTAMP    DEFAULT NOW(),
                UNIQUE (symbol, date)
            )
        """)
        conn.commit()
    print("[db] ✓ Таблица bybit_daily_candles готова")


# ─── Загрузка ────────────────────────────────────────────────────────────────

def fetch_candles(symbol: str, start_ms: int, end_ms: int) -> list:
    """Скачивает свечи за период. Пагинация в обратном порядке (новые → старые)."""
    all_candles = []
    cursor_end  = end_ms

    while cursor_end > start_ms:
        params = {
            "category": "spot",
            "symbol":   symbol,
            "interval": INTERVAL,
            "start":    start_ms,
            "end":      cursor_end,
            "limit":    LIMIT,
        }
        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            resp = requests.get(API_URL, params=params, headers=headers, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            if data["retCode"] != 0:
                print(f"  [fetch] API ошибка: {data['retMsg']}")
                break

            candles = data["result"]["list"]
            if not candles:
                break

            all_candles.extend(candles)

            # Bybit возвращает [timestamp, open, high, low, close, volume, turnover]
            # самая старая свеча — последняя в списке
            oldest_ts = int(candles[-1][0])
            if oldest_ts <= start_ms or len(candles) < LIMIT:
                break

            cursor_end = oldest_ts - 1
            time.sleep(0.2)

        except Exception as e:
            print(f"  [fetch] Ошибка запроса: {e}")
            break

    return all_candles


def do_fetch():
    conn = get_conn()
    init_db(conn)

    start_ms = int(datetime.datetime(YEAR_FROM, 1, 1).timestamp() * 1000)
    end_ms   = int(datetime.datetime(YEAR_TO,   1, 1).timestamp() * 1000)

    print(f"[fetch] {SYMBOL}  {YEAR_FROM} → {YEAR_TO}")
    candles  = fetch_candles(SYMBOL, start_ms, end_ms)
    inserted = 0

    with conn.cursor() as cur:
        for c in tqdm(candles, desc="insert"):
            ts_ms      = int(c[0])
            time_open  = datetime.datetime.fromtimestamp(ts_ms / 1000)
            cur.execute("""
                INSERT INTO bybit_daily_candles
                    (symbol, date, open_price, high_price, low_price,
                     close_price, volume, time_open)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, date) DO NOTHING
            """, (
                SYMBOL,
                time_open.date(),
                float(c[1]),  # open
                float(c[2]),  # high
                float(c[3]),  # low
                float(c[4]),  # close
                float(c[5]),  # volume
                time_open,
            ))
            inserted += 1
        conn.commit()

    conn.close()
    print(f"[fetch] ✓ Сохранено: {inserted} свечей")


# ─── Публикация ──────────────────────────────────────────────────────────────

def build_price_event(row: dict) -> dict:
    ts_ms = int(row["time_open"].timestamp() * 1000)
    return {
        "asset":        ASSET_SYMBOL,
        "quote":        QUOTE_SYMBOL,
        "exchange":     "bybit",
        "open":         row["open_price"],
        "high":         row["high_price"],
        "low":          row["low_price"],
        "close":        row["close_price"],
        "volume":       row["volume"],
        "interval":     "1d",
        "timestamp_ms": ts_ms,
        "candle_id":    f"{SYMBOL}_1d_{ts_ms}",
    }


def candle_to_ticks(row: dict) -> list[dict]:
    """5 тиков из одной свечи. Бычий: open→low→mid→high→close. Медвежий: наоборот."""
    ts_open = int(row["time_open"].timestamp() * 1000)
    step_ms = 86400 * 1000 // 5

    o, h, l, c = row["open_price"], row["high_price"], row["low_price"], row["close_price"]
    mid    = round((h + l) / 2, 8)
    prices = [o, l, mid, h, c] if c >= o else [o, h, mid, l, c]

    return [{
        "asset":        ASSET_SYMBOL,
        "quote":        QUOTE_SYMBOL,
        "exchange":     "simulation",
        "price":        price,
        "volume":       row["volume"] / 5,
        "interval":     "tick",
        "timestamp_ms": ts_open + i * step_ms,
        "candle_id":    f"{SYMBOL}_tick_{ts_open + i * step_ms}",
    } for i, price in enumerate(prices)]


def _get_rows(date_from, date_to):
    conn = get_conn()
    filters, params = [], {}
    if date_from:
        filters.append("date >= %(date_from)s"); params["date_from"] = date_from
    if date_to:
        filters.append("date <= %(date_to)s");   params["date_to"]   = date_to
    filters.append("symbol = %(symbol)s");       params["symbol"]    = SYMBOL
    where = "WHERE " + " AND ".join(filters)

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"SELECT * FROM bybit_daily_candles {where} ORDER BY date", params)
        rows = cur.fetchall()
    conn.close()
    return rows


def do_publish(date_from=None, date_to=None):
    rows = _get_rows(date_from, date_to)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_URL,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print(f"[publish] Публикую {len(rows)} свечей в {TOPIC}...")
    for row in tqdm(rows, desc="publish"):
        producer.send(TOPIC, value=build_price_event(dict(row)))
    producer.flush()
    print(f"[publish] ✓ Готово")


def do_simulate(date_from=None, date_to=None):
    rows = _get_rows(date_from, date_to)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_URL,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    delay_ms = int(os.environ.get("SIMULATE_DELAY_MS", "0"))
    total    = len(rows) * 5
    print(f"[simulate] Свечей: {len(rows)} → тиков: {total}  delay={delay_ms}ms")
    for row in tqdm(rows, desc="simulate"):
        for tick in candle_to_ticks(dict(row)):
            producer.send(TOPIC, value=tick)
            if delay_ms > 0:
                time.sleep(delay_ms / 1000)
    producer.flush()
    print(f"[simulate] ✓ Отправлено тиков: {total}")


# ─── Точка входа ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    cmd       = sys.argv[1] if len(sys.argv) > 1 else "all"
    date_from = os.environ.get("PUBLISH_DATE_FROM")
    date_to   = os.environ.get("PUBLISH_DATE_TO")

    if cmd == "fetch":
        do_fetch()
    elif cmd == "publish":
        do_publish(date_from, date_to)
    elif cmd == "simulate":
        do_simulate(date_from, date_to)
    elif cmd == "all":
        do_fetch()
        do_publish(date_from, date_to)
    else:
        print("Использование: python src/main.py [fetch|publish|simulate|all]")
        sys.exit(1)
