"""
sentiment-analyzer — TREND сигнал на основе Fear & Greed индекса.

Читает:  collectors.fear.greed
Пишет:  analyzers.signal.out

Логика (на основе исторических данных ETH 2020-2026):
  Extreme Fear (0-24)   → BULLISH   confidence высокий  (лучшие точки входа)
  Fear         (25-46)  → BULLISH   confidence низкий
  Neutral      (47-54)  → NEUTRAL   (46.4% бычьих — хуже монетки)
  Greed        (55-75)  → BEARISH   confidence низкий
  Extreme Greed (76-100)→ BEARISH   confidence высокий  (эйфория = выход)

Данные суточные — TTL сигнала 24 часа.

Режимы:
    python src/main.py       — обычный, слушает бесконечно
    python src/main.py eval  — оценка точности на исторических данных
"""
import os, sys, json, datetime
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_URL = os.environ.get("KAFKA_URL", "localhost:9092")
IN_TOPIC  = os.environ.get("IN_TOPIC",  "collectors.fear.greed")
OUT_TOPIC = os.environ.get("OUT_TOPIC", "analyzers.signal.out")
TTL_MS    = int(os.environ.get("TTL_MS", str(24 * 3600 * 1000)))  # 24 часа

# Границы зон (можно переопределить через .env)
EXTREME_FEAR_MAX  = int(os.environ.get("EXTREME_FEAR_MAX",  "24"))
FEAR_MAX          = int(os.environ.get("FEAR_MAX",          "46"))
NEUTRAL_MAX       = int(os.environ.get("NEUTRAL_MAX",       "54"))
GREED_MAX         = int(os.environ.get("GREED_MAX",         "75"))
# > GREED_MAX → Extreme Greed


# ─── Чистая функция сигнала ───────────────────────────────────────────────────

def compute_signal(fg_value: int, classification: str, timestamp_ms: int) -> dict:
    """
    Считает TREND сигнал по значению Fear & Greed.
    Контр-трендовая логика: страх → покупать, жадность → продавать.

    Целевые диапазоны confidence:
      Extreme Fear/Greed → 0.7–0.9  (экстремум = высокая уверенность)
      Fear/Greed         → 0.3–0.5  (умеренный сигнал)
      Neutral            → 0.1–0.3  (слабый, солвер почти игнорирует)
    """
    if fg_value <= EXTREME_FEAR_MAX:
        value      = "BULLISH"
        # 0 → 0.9,  EXTREME_FEAR_MAX → 0.7
        confidence = 0.9 - 0.2 * (fg_value / EXTREME_FEAR_MAX)
    elif fg_value <= FEAR_MAX:
        value      = "BULLISH"
        # EXTREME_FEAR_MAX+1 → 0.5,  FEAR_MAX → 0.3
        t          = (fg_value - EXTREME_FEAR_MAX) / (FEAR_MAX - EXTREME_FEAR_MAX)
        confidence = 0.5 - 0.2 * t
    elif fg_value <= NEUTRAL_MAX:
        value      = "NEUTRAL"
        # середина зоны → 0.1,  края → 0.3
        mid        = (FEAR_MAX + NEUTRAL_MAX) / 2
        t          = abs(fg_value - mid) / ((NEUTRAL_MAX - FEAR_MAX) / 2)
        confidence = 0.1 + 0.2 * t
    elif fg_value <= GREED_MAX:
        value      = "BEARISH"
        # NEUTRAL_MAX+1 → 0.3,  GREED_MAX → 0.5
        t          = (fg_value - NEUTRAL_MAX) / (GREED_MAX - NEUTRAL_MAX)
        confidence = 0.3 + 0.2 * t
    else:
        value      = "BEARISH"
        # GREED_MAX+1 → 0.7,  100 → 0.9
        t          = (fg_value - GREED_MAX) / (100 - GREED_MAX)
        confidence = 0.7 + 0.2 * t

    confidence = round(max(0.0, min(1.0, confidence)), 4)
    ts_human   = datetime.datetime.fromtimestamp(timestamp_ms / 1000).strftime("%Y-%m-%d")

    return {
        "source":       "sentiment_analyzer",
        "asset":        "CRYPTO",   # глобальный сигнал, не привязан к паре
        "quote":        "MARKET",
        "signal_type":  "TREND",
        "value":        value,
        "confidence":   round(confidence, 4),
        "ttl_ms":       TTL_MS,
        "timestamp_ms": timestamp_ms,
        "meta": {
            "fg_value":         str(fg_value),
            "classification":   classification,
            "date":             ts_human,
        }
    }


# ─── Consumer factory ────────────────────────────────────────────────────────

def make_consumer(group_id: str, from_beginning: bool = False) -> KafkaConsumer:
    if from_beginning:
        from kafka import TopicPartition
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_URL,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            consumer_timeout_ms=3000,
        )
        tp = TopicPartition(IN_TOPIC, 0)
        consumer.assign([tp])
        consumer.seek_to_beginning(tp)
        return consumer
    return KafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=KAFKA_URL,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id=group_id,
    )


# ─── Обычный режим ───────────────────────────────────────────────────────────

def run():
    consumer = make_consumer("sentiment_analyzer")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_URL,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    total = signals = 0
    print(f"[sentiment_analyzer] {IN_TOPIC} → {OUT_TOPIC}")

    for msg in consumer:
        tick = msg.value
        fg_value       = tick.get("value")
        classification = tick.get("classification", "")
        timestamp_ms   = tick.get("timestamp_ms")

        if fg_value is None or timestamp_ms is None:
            continue

        total += 1
        result = compute_signal(int(fg_value), classification, timestamp_ms)
        producer.send(OUT_TOPIC, value=result)
        signals += 1

        print(f"[sentiment_analyzer] {result['meta']['date']}  "
              f"FG={fg_value:>3} ({classification:<14})  "
              f"{result['value']:<8} conf={result['confidence']:.2f}")


# ─── Eval режим ──────────────────────────────────────────────────────────────

def run_eval():
    """
    Оценка: проверяем точность TREND сигнала.
    Для каждого сигнала смотрим движение цены в Kafka (market.price) — но
    поскольку Fear & Greed суточный, оцениваем по самому сигналу:
    считаем статистику по зонам.
    """
    consumer = make_consumer("", from_beginning=True)

    signals_log   = []
    signals_count = {"BULLISH": 0, "NEUTRAL": 0, "BEARISH": 0}
    total = 0

    print(f"[sentiment_analyzer] EVAL MODE")
    print(f"[sentiment_analyzer] Читаю {IN_TOPIC}...")

    for msg in consumer:
        tick = msg.value
        fg_value       = tick.get("value")
        classification = tick.get("classification", "")
        timestamp_ms   = tick.get("timestamp_ms")

        if fg_value is None or timestamp_ms is None:
            continue

        total += 1
        result = compute_signal(int(fg_value), classification, timestamp_ms)
        v = result["value"]
        signals_count[v] = signals_count.get(v, 0) + 1
        signals_log.append({
            "date":           result["meta"]["date"],
            "fg_value":       fg_value,
            "classification": classification,
            "value":          v,
            "confidence":     result["confidence"],
        })

    _print_eval_report(signals_log, signals_count, total)


def _print_eval_report(signals_log, signals_count, total):
    print()
    print("═" * 60)
    print("  SENTIMENT ANALYZER — ОТЧЁТ ПО ОЦЕНКЕ")
    print("═" * 60)
    print(f"  Записей обработано: {total}")
    total_sig = sum(signals_count.values())
    print(f"  Сигналов всего:     {total_sig}")
    for v, c in signals_count.items():
        pct = c / total_sig * 100 if total_sig > 0 else 0
        bar = "█" * int(pct / 2)
        print(f"    {v:<8}: {c:>5}  ({pct:>5.1f}%)  {bar}")

    print()
    print("  Распределение confidence по зонам:")
    zones = {}
    for s in signals_log:
        key = f"{s['value']} / {s['classification']}"
        zones.setdefault(key, []).append(s["confidence"])

    for key in sorted(zones):
        vals = zones[key]
        avg  = sum(vals) / len(vals)
        print(f"    {key:<30} avg conf={avg:.2f}  n={len(vals)}")

    print()
    print("  Сигналы по месяцам (последние 12):")
    monthly = {}
    for s in signals_log:
        month = s["date"][:7]
        monthly.setdefault(month, {"BULLISH": 0, "NEUTRAL": 0, "BEARISH": 0})
        monthly[month][s["value"]] += 1

    for month in sorted(monthly)[-12:]:
        m = monthly[month]
        print(f"    {month}  🐂{m['BULLISH']:>3}  ➖{m['NEUTRAL']:>3}  🐻{m['BEARISH']:>3}")

    print("═" * 60)


# ─── Точка входа ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "run"
    if mode == "eval":
        run_eval()
    else:
        run()