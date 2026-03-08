"""
sigma-analyzer — DEVIATION сигнал на основе стандартного отклонения цены.

Читает:  market.price  (тики от simulation или реальный поток)
Пишет:  signal.out

Логика (из старого кода, поле deviationAvgPrice):
  - Накапливает цены в скользящем окне N тиков
  - Считает mean и σ по окну
  - Если текущая цена выходит за mean ± σ → OVERBOUGHT / OVERSOLD
  - Иначе → NEUTRAL

Принцип: анализатор видит только текущий тик и историю прошлых.
Никакого заглядывания в будущее.
"""
import os, json, math, datetime
from collections import deque
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_URL  = os.environ.get("KAFKA_URL",  "kafka:9092")
IN_TOPIC   = os.environ.get("IN_TOPIC",   "market.price")
OUT_TOPIC  = os.environ.get("OUT_TOPIC",  "signal.out")
ASSET      = os.environ.get("ASSET",      "ETH")
QUOTE      = os.environ.get("QUOTE",      "USDT")
WINDOW     = int(os.environ.get("WINDOW", "20"))   # тиков в окне
TTL_MS     = int(os.environ.get("TTL_MS", str(5 * 86400 * 1000)))  # 5 дней


# ─── Чистая функция сигнала ───────────────────────────────────────────────────

def compute_signal(prices: list[float], current_price: float, timestamp_ms: int) -> dict:
    """
    Считает DEVIATION сигнал по окну цен.
    Принимает только прошлые цены + текущую — не смотрит в будущее.

    OVERBOUGHT: current > mean + σ
    OVERSOLD:   current < mean - σ
    NEUTRAL:    внутри диапазона
    """
    n    = len(prices)
    mean = sum(prices) / n
    variance = sum((x - mean) ** 2 for x in prices) / n
    sigma = math.sqrt(variance)

    if sigma == 0:
        return None

    deviation = (current_price - mean) / sigma  # в единицах σ

    if deviation > 1.0:
        value      = "OVERBOUGHT"
        confidence = min(abs(deviation) / 3.0, 1.0)
    elif deviation < -1.0:
        value      = "OVERSOLD"
        confidence = min(abs(deviation) / 3.0, 1.0)
    else:
        value      = "NEUTRAL"
        confidence = round(1.0 - abs(deviation), 4)

    ts_human = datetime.datetime.fromtimestamp(timestamp_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")

    return {
        "source":       "sigma_analyzer",
        "asset":        ASSET,
        "quote":        QUOTE,
        "signal_type":  "DEVIATION",
        "value":        value,
        "confidence":   round(confidence, 4),
        "ttl_ms":       TTL_MS,
        "timestamp_ms": timestamp_ms,
        "meta": {
            "mean":      str(round(mean, 4)),
            "sigma":     str(round(sigma, 4)),
            "price":     str(round(current_price, 4)),
            "deviation": str(round(deviation, 3)),
            "window":    str(WINDOW),
            "n_samples": str(n),
            "time":      ts_human,
        }
    }


# ─── Kafka loop ───────────────────────────────────────────────────────────────

def run():
    consumer = KafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=KAFKA_URL,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id=f"sigma_analyzer_{ASSET}_{QUOTE}",
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_URL,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    # Скользящее окно — только прошлые цены
    history = deque(maxlen=WINDOW)

    total   = 0
    signals = 0

    print(f"[sigma_analyzer] asset={ASSET}/{QUOTE} window={WINDOW}")
    print(f"[sigma_analyzer] Listening: {IN_TOPIC} → {OUT_TOPIC}")

    for msg in consumer:
        tick = msg.value

        # Фильтруем только нашу пару
        if tick.get("asset") != ASSET or tick.get("quote") != QUOTE:
            continue

        price = tick.get("price")
        if price is None:
            continue

        total += 1

        # Считаем сигнал если накопили достаточно истории
        if len(history) >= WINDOW:
            result = compute_signal(list(history), price, tick["timestamp_ms"])
            if result:
                producer.send(OUT_TOPIC, value=result)
                signals += 1

                ts = result["meta"]["time"]
                print(f"[sigma_analyzer] {ts}  {result['value']:<12} "
                      f"confidence={result['confidence']:.2f}  "
                      f"price={result['meta']['price']}  "
                      f"mean={result['meta']['mean']}  "
                      f"σ={result['meta']['sigma']}")

        # Добавляем текущую цену в историю ПОСЛЕ расчёта
        # (не заглядываем в будущее)
        history.append(price)

        if total % 500 == 0:
            print(f"[sigma_analyzer] Обработано тиков: {total}, сигналов: {signals}")


if __name__ == "__main__":
    run()
