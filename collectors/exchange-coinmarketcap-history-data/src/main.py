"""
historical_collector — загружает исторические дневные свечи с CoinMarketCap
и сохраняет в Postgres. Если данные уже есть — пушит в Kafka.

Команды:
    python src/main.py fetch   — скачать и сохранить в БД (по умолчанию)
    python src/main.py publish — прочитать из БД и запушить в Kafka
    python src/main.py all     — fetch + publish
"""
import os, sys, time, json, datetime, requests, orjson
import psycopg2
import psycopg2.extras
from kafka import KafkaProducer
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

# ─── Конфиг ──────────────────────────────────────────────────────────────────

DB_HOST   = os.environ.get("DB_HOST", "postgres")
DB_PORT   = "5432"
DB_NAME   = os.environ["DB_NAME"]
DB_USER   = os.environ["DB_USER"]
DB_PASS   = os.environ["DB_PASSWORD"]

KAFKA_URL = os.environ.get("KAFKA_URL", "kafka:9092")
TOPIC     = os.environ.get("KAFKA_TOPIC", "market.price")

YEAR_FROM = int(os.environ.get("YEAR_FROM", "2017"))
YEAR_TO   = int(os.environ.get("YEAR_TO",   "2026"))

TMP_DIR   = os.path.join(os.path.dirname(__file__), "../tmp")

COINS = {
    "btc": os.environ["COINMARKETCAP_BTC_ID"],
    "eth": os.environ["COINMARKETCAP_ETH_ID"],
    "bnb": os.environ.get("COINMARKETCAP_BNB_ID", "1839"),
}

QUOTE_SYMBOL = "USDT"


# ─── БД ───────────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )


def init_db(conn):
    """Создаёт таблицу если не существует."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS historic_coinmarketcap_daily_candles (
                id             SERIAL PRIMARY KEY,
                coin           VARCHAR(10)  NOT NULL,
                date           DATE         NOT NULL,
                open_price     FLOAT        NOT NULL,
                high_price     FLOAT        NOT NULL,
                low_price      FLOAT        NOT NULL,
                close_price    FLOAT        NOT NULL,
                avg_price      FLOAT        NOT NULL,
                volume         BIGINT       NOT NULL,
                market_cap     BIGINT,
                time_open      TIMESTAMP    NOT NULL,
                time_close     TIMESTAMP    NOT NULL,
                created_at     TIMESTAMP    DEFAULT NOW(),
                UNIQUE (coin, date)
            )
        """)
        conn.commit()
    print("[db] ✓ Таблица historic_coinmarketcap_daily_candles готова")


def insert_candle(cur, coin: str, doc: dict):
    cur.execute("""
        INSERT INTO historic_coinmarketcap_daily_candles
            (coin, date, open_price, high_price, low_price, close_price,
             avg_price, volume, market_cap, time_open, time_close)
        VALUES
            (%(coin)s, %(date)s, %(open_price)s, %(high_price)s, %(low_price)s,
             %(close_price)s, %(avg_price)s, %(volume)s, %(market_cap)s,
             %(time_open)s, %(time_close)s)
        ON CONFLICT (coin, date) DO NOTHING
    """, {
        "coin":        coin,
        "date":        doc["date"],
        "open_price":  doc["openPrice"],
        "high_price":  doc["highPrice"],
        "low_price":   doc["lowPrice"],
        "close_price": doc["closePrice"],
        "avg_price":   round((doc["highPrice"] + doc["lowPrice"]) / 2, 4),
        "volume":      int(doc["volume"]),
        "market_cap":  int(doc.get("marketCap") or 0),
        "time_open":   doc["timeOpen"],
        "time_close":  doc["timeClose"],
    })


# ─── Загрузка данных ──────────────────────────────────────────────────────────

def convert_date(iso: str) -> str:
    """ISO8601 → 'YYYY-MM-DD HH:MM:SS'"""
    return datetime.datetime.strptime(
        iso, '%Y-%m-%dT%H:%M:%S.%f%z'
    ).strftime("%Y-%m-%d %H:%M:%S")


def fetch_year(coin_id: str, year: int) -> dict | None:
    """Скачивает данные за год с CoinMarketCap (с кешем в tmp/)."""
    os.makedirs(TMP_DIR, exist_ok=True)
    cache_file = os.path.join(TMP_DIR, f"{coin_id}_{year}.json")

    if os.path.exists(cache_file):
        with open(cache_file, "rb") as f:
            return orjson.loads(f.read())

    start = int(datetime.datetime(year,     1, 1).timestamp())
    end   = int(datetime.datetime(year + 1, 1, 1).timestamp())
    url   = (f"https://api.coinmarketcap.com/data-api/v3/cryptocurrency/historical"
             f"?id={coin_id}&convertId=2781&timeStart={start}&timeEnd={end}")

    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.content
        with open(cache_file, "wb") as f:
            f.write(data)
        return orjson.loads(data)
    except Exception as e:
        print(f"  [fetch] ✗ ошибка: {e}")
        return None


def do_fetch():
    """Скачивает все данные и сохраняет в Postgres."""
    conn = get_conn()
    init_db(conn)

    total_inserted = 0

    for coin_symbol, coin_id in COINS.items():
        print(f"\n[fetch] Монета: {coin_symbol.upper()} (id={coin_id})")

        for year in tqdm(range(YEAR_FROM, YEAR_TO), desc=f"  {coin_symbol}"):
            content = fetch_year(coin_id, year)

            if not content or content.get("status", {}).get("error_message") != "SUCCESS":
                print(f"  [fetch] ✗ нет данных за {year}")
                continue

            with conn.cursor() as cur:
                for q in content["data"]["quotes"]:
                    doc = {
                        "timeOpen":  convert_date(q["timeOpen"]),
                        "timeClose": convert_date(q["timeClose"]),
                        "openPrice": q["quote"]["open"],
                        "highPrice": q["quote"]["high"],
                        "lowPrice":  q["quote"]["low"],
                        "closePrice":q["quote"]["close"],
                        "volume":    q["quote"]["volume"],
                        "marketCap": q["quote"].get("marketCap"),
                        "date": datetime.datetime.strptime(
                            convert_date(q["timeOpen"]), "%Y-%m-%d %H:%M:%S"
                        ).date(),
                    }
                    insert_candle(cur, coin_symbol, doc)
                    total_inserted += 1
                conn.commit()

            time.sleep(0.5)  # не флудим CoinMarketCap

    conn.close()
    print(f"\n[fetch] ✓ Загружено записей: {total_inserted}")


# ─── Публикация в Kafka ───────────────────────────────────────────────────────

def build_price_event(row: dict) -> dict:
    """Конвертирует строку из БД в PriceEvent для Kafka."""
    return {
        "asset":        row["coin"].upper(),
        "quote":        QUOTE_SYMBOL,
        "exchange":     "historical",
        "open":         row["open_price"],
        "high":         row["high_price"],
        "low":          row["low_price"],
        "close":        row["close_price"],
        "volume":       row["volume"],
        "interval":     "1d",
        "timestamp_ms": int(row["time_open"].timestamp() * 1000),
        "candle_id":    f"{row['coin'].upper()}{QUOTE_SYMBOL}_1d_{int(row['time_open'].timestamp() * 1000)}",
    }


def do_publish(coin: str | None = None, date_from: str | None = None, date_to: str | None = None):
    """Читает из БД и пушит в Kafka."""
    conn = get_conn()

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_URL,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    filters = []
    params  = {}
    if coin:
        filters.append("coin = %(coin)s")
        params["coin"] = coin
    if date_from:
        filters.append("date >= %(date_from)s")
        params["date_from"] = date_from
    if date_to:
        filters.append("date <= %(date_to)s")
        params["date_to"] = date_to

    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"SELECT * FROM historic_coinmarketcap_daily_candles {where} ORDER BY coin, date", params)
        rows = cur.fetchall()

    print(f"[publish] Публикую {len(rows)} свечей в {TOPIC}...")

    for row in tqdm(rows, desc="publish"):
        event = build_price_event(dict(row))
        producer.send(TOPIC, value=event)

    producer.flush()
    conn.close()
    print(f"[publish] ✓ Готово")


# ─── Симуляция биржевого потока ───────────────────────────────────────────────

def candle_to_ticks(row: dict) -> list[dict]:
    """
    Из одной дневной свечи генерирует 5 тиков в хронологическом порядке.

    Логика как в старом TestStrategy.getPriceGenerator:
    - Бычий день (close > open): open → low → mid → high → close
    - Медвежий день (close < open): open → high → mid → low → close

    Временны́е метки равномерно распределены по дню (каждые ~4.8 часа).
    """
    asset      = row["coin"].upper()
    ts_open    = int(row["time_open"].timestamp() * 1000)
    day_ms     = 86400 * 1000
    step_ms    = day_ms // 5  # ~4.8 часа между тиками

    o = row["open_price"]
    h = row["high_price"]
    l = row["low_price"]
    c = row["close_price"]
    v = row["volume"]
    mid = round((h + l) / 2, 8)

    bullish = c >= o
    prices = [o, l, mid, h, c] if bullish else [o, h, mid, l, c]

    ticks = []
    for i, price in enumerate(prices):
        ts = ts_open + i * step_ms
        ticks.append({
            "asset":        asset,
            "quote":        QUOTE_SYMBOL,
            "exchange":     "simulation",
            "price":        price,
            "volume":       v // 5,
            "interval":     "tick",
            "timestamp_ms": ts,
            "date": datetime.datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M:%S"),
            "candle_id":    f"{asset}{QUOTE_SYMBOL}_tick_{ts}",
        })

    return ticks


def do_publish_simulation(
    coin: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    delay_ms: int = 0,          # пауза между тиками в мс (0 = максимальная скорость)
):
    """Читает свечи из БД и пушит тики в Kafka как симуляция биржи."""
    conn = get_conn()

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_URL,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    filters = []
    params  = {}
    if coin:
        filters.append("coin = %(coin)s")
        params["coin"] = coin
    if date_from:
        filters.append("date >= %(date_from)s")
        params["date_from"] = date_from
    if date_to:
        filters.append("date <= %(date_to)s")
        params["date_to"] = date_to

    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"SELECT * FROM historic_coinmarketcap_daily_candles {where} ORDER BY coin, date", params)
        rows = cur.fetchall()

    total_ticks = len(rows) * 5
    print(f"[simulate] Свечей: {len(rows)} → тиков: {total_ticks}")
    print(f"[simulate] Топик: {TOPIC}  delay={delay_ms}ms")

    for row in tqdm(rows, desc="simulate"):
        for tick in candle_to_ticks(dict(row)):
            producer.send(TOPIC, value=tick)
            if delay_ms > 0:
                time.sleep(delay_ms / 1000)

    producer.flush()
    conn.close()
    print(f"[simulate] ✓ Готово. Отправлено тиков: {total_ticks}")

# ─── Точка входа ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "all"

    if cmd == "fetch":
        do_fetch()
    elif cmd == "publish":
        coin      = os.environ.get("PUBLISH_COIN")       # BTC | ETH | BNB | пусто = все
        date_from = os.environ.get("PUBLISH_DATE_FROM")  # 2023-01-01
        date_to   = os.environ.get("PUBLISH_DATE_TO")    # 2024-01-01
        do_publish(coin, date_from, date_to)
    elif cmd == "simulate":
        coin      = os.environ.get("PUBLISH_COIN")
        date_from = os.environ.get("PUBLISH_DATE_FROM")
        date_to   = os.environ.get("PUBLISH_DATE_TO")
        delay_ms  = int(os.environ.get("SIMULATE_DELAY_MS", "0"))
        do_publish_simulation(coin, date_from, date_to, delay_ms)
    elif cmd == "all":
        do_fetch()
        do_publish()
    else:
        print(f"Неизвестная команда: {cmd}")
        print("Использование: python src/main.py [fetch|publish|all]")
        sys.exit(1)