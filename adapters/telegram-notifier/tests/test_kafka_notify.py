"""
test_kafka_notify.py — тест через Kafka.

Публикует сообщения в топик adapters.notifications,
нотифаер подхватывает и шлёт в Telegram.

"""
import os, json, time
from kafka import KafkaProducer

KAFKA_URL  = os.environ.get("KAFKA_URL", "localhost:9092")
TOPIC      = os.environ.get("KAFKA_TOPIC", "adapters.notifications")
CHANNEL_ID = os.environ.get("TELEGRAM_DEFAULT_CHANNEL_ID", "")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_URL,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

messages = [
    {
        "level":      "INFO",
        "message":    "✅ Kafka test: INFO",
        "channel_id": CHANNEL_ID,
        "context":    {"source": "test_kafka_notify.py"},
    },
    {
        "level":      "ERROR",
        "message":    "🔴 Kafka test: ERROR",
        "channel_id": CHANNEL_ID,
        "context":    {"source": "test_kafka_notify.py", "detail": "test error"},
    },
    {
        "level":      "CRITICAL",
        "message":    "🚨 Kafka test: CRITICAL",
        "channel_id": CHANNEL_ID,
        "context":    {"source": "test_kafka_notify.py"},
    },
]

print(f"[test] Kafka: {KAFKA_URL}  топик: {TOPIC}")
print(f"[test] Публикую {len(messages)} сообщений...")

for msg in messages:
    producer.send(TOPIC, value=msg)
    print(f"  → [{msg['level']}] {msg['message']}")
    time.sleep(0.5)

producer.flush()
print()
print("[test] ✓ Готово. Проверь Telegram — должно прийти 3 сообщения (DEBUG игнорируется).")