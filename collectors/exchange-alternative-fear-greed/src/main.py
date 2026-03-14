"""
exchange-alternative-fear-greed — загружает исторический индекс страха/жадности
с alternative.me и публикует в Kafka.

Команды:
    python src/main.py fetch    — скачать и сохранить в БД
    python src/main.py publish  — прочитать из БД и запушить в Kafka
    python src/main.py all      — fetch + publish (по умолчанию)

Топик: collectors.fear.greed
Формат сообщения:
    {
        "source":         "alternative.fear.greed",
        "value":          72,
        "classification": "Greed",
        "timestamp_ms":   1704067200000,
        "date":           "2024-01-01"
    }
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

KAFKA_URL = os.environ.get("KAFKA_URL", "localhost:9092")
TOPIC     = os.environ.get("KAFKA_TOPIC", "collectors.fear.greed")

API_URL   = "https://api.alternative.me/fng/?limit=2000&format=json"
TMP_FILE  = os.path.join(os.path.dirname(__file__), "../tmp/fear_greed.json")


# ─── БД ──────────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )


def init_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS collectors_fear_greed_index (
                id             SERIAL PRIMARY KEY,
                date           DATE         NOT NULL UNIQUE,
                value          INT          NOT NULL,
                classification VARCHAR(20)  NOT NULL,
                timestamp_ms   BIGINT       NOT NULL,
                created_at     TIMESTAMP    DEFAULT NOW()
            )
        """)
        conn.commit()
    print("[db] ✓ Таблица collectors_fear_greed_index готова")


# ─── Загрузка ────────────────────────────────────────────────────────────────

def fetch_data() -> list[dict]:
    """Скачивает с alternative.me. Кеш обновляется раз в 23 часа."""
    os.makedirs(os.path.dirname(TMP_FILE), exist_ok=True)

    if os.path.exists(TMP_FILE):
        age_hours = (time.time() - os.path.getmtime(TMP_FILE)) / 3600
        if age_hours < 23:
            print(f"[fetch] Кеш свежий ({age_hours:.1f}ч), используем его")
            with open(TMP_FILE) as f:
                return json.load(f)

    print("[fetch] Скачиваем с alternative.me...")
    resp = requests.get(API_URL, timeout=15)
    resp.raise_for_status()
    data = resp.json()["data"]

    with open(TMP_FILE, "w") as f:
        json.dump(data, f)

    print(f"[fetch] Получено записей: {len(data)}")
    return data


def do_fetch():
    conn = get_conn()
    init_db(conn)

    records  = fetch_data()
    inserted = 0

    with conn.cursor() as cur:
        for r in tqdm(records, desc="insert"):
            ts   = int(r["timestamp"])
            date = datetime.datetime.fromtimestamp(ts).date()
            cur.execute("""
                INSERT INTO collectors_fear_greed_index (date, value, classification, timestamp_ms)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (date) DO NOTHING
            """, (date, int(r["value"]), r["value_classification"], ts * 1000))
            inserted += 1
        conn.commit()

    conn.close()
    print(f"[fetch] ✓ Сохранено: {inserted} записей")


# ─── Публикация ──────────────────────────────────────────────────────────────

def do_publish(date_from: str | None = None, date_to: str | None = None):
    conn = get_conn()
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_URL,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    filters, params = [], {}
    if date_from:
        filters.append("date >= %(date_from)s"); params["date_from"] = date_from
    if date_to:
        filters.append("date <= %(date_to)s");   params["date_to"]   = date_to

    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"SELECT * FROM collectors_fear_greed_index {where} ORDER BY date", params)
        rows = cur.fetchall()

    print(f"[publish] Публикую {len(rows)} записей в {TOPIC}...")

    for row in tqdm(rows, desc="publish"):
        producer.send(TOPIC, value={
            "source":         "alternative.fear.greed",
            "value":          row["value"],
            "classification": row["classification"],
            "timestamp_ms":   row["timestamp_ms"],
            "date":           str(row["date"]),
        })

    producer.flush()
    conn.close()
    print(f"[publish] ✓ Готово")


# ─── Точка входа ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    cmd       = sys.argv[1] if len(sys.argv) > 1 else "all"
    date_from = os.environ.get("PUBLISH_DATE_FROM")
    date_to   = os.environ.get("PUBLISH_DATE_TO")

    if cmd == "fetch":
        do_fetch()
    elif cmd == "publish":
        do_publish(date_from, date_to)
    elif cmd == "all":
        do_fetch()
        do_publish(date_from, date_to)
    else:
        print("Использование: python src/main.py [fetch|publish|all]")
        sys.exit(1)