"""
telegram_notifier — слушает Kafka-топик и пушит сообщения в Telegram.

Формат сообщения в топике:
{
    "level":      "INFO" | "ERROR" | "CRITICAL" | ...,
    "message":    "текст",
    "channel_id": "-100123456789",   # опционально, иначе берётся из ENV
    "context":    {}                 # опционально, для отладки
}
"""
import os, json, time
from kafka import KafkaConsumer

KAFKA_URL   = os.environ["KAFKA_URL"]
TOPIC       = os.environ.get("KAFKA_TOPIC", "notifications")
TOKEN       = os.environ["TELEGRAM_TOKEN"]
CHANNEL_ID  = os.environ.get("TELEGRAM_DEFAULT_CHANNEL_ID", "")

# Какие уровни пускать в Telegram (остальные игнорируем)
LOGGED_LEVELS = set(os.environ.get("TELEGRAM_LOGGED_LIST", "INFO,ERROR,CRITICAL").split(","))

def send(channel_id: str, text: str) -> None:
    import urllib.request
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = json.dumps({"chat_id": channel_id, "text": text}).encode()
    req = urllib.request.Request(url, data=payload,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        resp.read()


def format_message(event: dict) -> str:
    level   = event.get("level", "INFO")
    message = event.get("message", "")
    context = event.get("context", {})

    text = f"[{level}] {message}"
    if context:
        text += "\n" + "\n".join(f"  {k}: {v}" for k, v in context.items())
    return text


def run():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_URL,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",   # только новые сообщения
        group_id="telegram_notifier",
    )

    print(f"[telegram_notifier] Listening: {TOPIC} → Telegram")
    print(f"[telegram_notifier] Levels: {LOGGED_LEVELS}")

    for msg in consumer:
        event = msg.value
        level = event.get("level", "INFO")

        if level not in LOGGED_LEVELS:
            continue

        channel_id = event.get("channel_id") or CHANNEL_ID
        if not channel_id:
            print(f"[telegram_notifier] No channel_id, skipping: {event}")
            continue

        text = format_message(event)

        try:
            send(channel_id, text)
            print(f"[telegram_notifier] ✓ {level}: {event.get('message', '')[:60]}")
        except Exception as e:
            print(f"[telegram_notifier] ✗ Failed to send: {e}")


if __name__ == "__main__":
    # Небольшая пауза чтобы Kafka успела подняться
    time.sleep(5)
    run()
