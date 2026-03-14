"""
volatility-analyzer — VOLATILITY сигнал на основе амплитуды цен в окне.

Читает:  collectors.market.price
Пишет:  analyzers.signal.out

Логика:
  - Накапливает цены в скользящем окне SHORT_WINDOW тиков
  - Считает амплитуду: (max - min) / min * 100
  - Сравнивает с историей амплитуд за LONG_WINDOW
  - HIGH:   амплитуда > mean + σ  (скачок, опасно)
  - LOW:    амплитуда < mean - σ  (тихий рынок)
  - NORMAL: внутри диапазона

Режимы:
    python src/main.py       — обычный, слушает бесконечно
    python src/main.py eval  — оценка точности
"""
import os, sys, json, math, datetime
from collections import deque
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_URL    = os.environ.get("KAFKA_URL",    "kafka:9092")
IN_TOPIC     = os.environ.get("IN_TOPIC",     "collectors.market.price")
OUT_TOPIC    = os.environ.get("OUT_TOPIC",    "analyzers.signal.out")
ASSET        = os.environ.get("ASSET",        "ETH")
QUOTE        = os.environ.get("QUOTE",        "USDT")
SHORT_WINDOW = int(os.environ.get("SHORT_WINDOW", "10"))
LONG_WINDOW  = int(os.environ.get("LONG_WINDOW",  "100"))
TTL_MS       = int(os.environ.get("TTL_MS", str(5 * 86400 * 1000)))

EVAL_HORIZONS = [5, 10, 20]


# ─── Чистые функции ──────────────────────────────────────────────────────────

def amplitude(prices: list[float]) -> float:
    """Амплитуда окна: (max - min) / min * 100"""
    lo = min(prices)
    if lo == 0:
        return 0.0
    return (max(prices) - lo) / lo * 100


def compute_signal(
    short_prices: list[float],
    long_amplitudes: list[float],
    current_price: float,
    timestamp_ms: int,
) -> dict | None:
    """
    short_prices    — последние SHORT_WINDOW цен (текущий момент)
    long_amplitudes — история амплитуд (норма)
    """
    if len(long_amplitudes) < 2:
        return None

    current_amp = amplitude(short_prices)
    n    = len(long_amplitudes)
    mean = sum(long_amplitudes) / n
    variance = sum((x - mean) ** 2 for x in long_amplitudes) / n
    sigma = math.sqrt(variance)

    if sigma == 0:
        return None

    deviation = (current_amp - mean) / sigma

    if deviation > 1.0:
        value      = "HIGH"
        confidence = min(abs(deviation) / 3.0, 1.0)
    elif deviation < -1.0:
        value      = "LOW"
        confidence = min(abs(deviation) / 3.0, 1.0)
    else:
        value      = "NORMAL"
        confidence = round(1.0 - abs(deviation), 4)

    ts_human = datetime.datetime.fromtimestamp(timestamp_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")

    return {
        "source":       "volatility_analyzer",
        "asset":        ASSET,
        "quote":        QUOTE,
        "signal_type":  "VOLATILITY",
        "value":        value,
        "confidence":   round(confidence, 4),
        "ttl_ms":       TTL_MS,
        "timestamp_ms": timestamp_ms,
        "meta": {
            "amplitude":      str(round(current_amp, 4)),
            "mean_amplitude": str(round(mean, 4)),
            "sigma":          str(round(sigma, 4)),
            "deviation":      str(round(deviation, 3)),
            "short_window":   str(SHORT_WINDOW),
            "long_window":    str(LONG_WINDOW),
            "price":          str(round(current_price, 4)),
            "time":           ts_human,
        }
    }


# ─── Consumer factory ─────────────────────────────────────────────────────────

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
    consumer = make_consumer(f"volatility_analyzer_{ASSET}_{QUOTE}")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_URL,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    short_buf    = deque(maxlen=SHORT_WINDOW)
    long_amp_buf = deque(maxlen=LONG_WINDOW)
    total = signals = 0

    print(f"[volatility_analyzer] asset={ASSET}/{QUOTE}")
    print(f"[volatility_analyzer] short={SHORT_WINDOW} long={LONG_WINDOW}")
    print(f"[volatility_analyzer] {IN_TOPIC} → {OUT_TOPIC}")

    for msg in consumer:
        tick = msg.value
        if tick.get("asset") != ASSET or tick.get("quote") != QUOTE:
            continue
        price = tick.get("price")
        if price is None:
            continue

        total += 1
        short_buf.append(price)

        if len(short_buf) == SHORT_WINDOW:
            amp = amplitude(list(short_buf))
            long_amp_buf.append(amp)

            if len(long_amp_buf) >= SHORT_WINDOW:
                result = compute_signal(
                    list(short_buf), list(long_amp_buf),
                    price, tick["timestamp_ms"]
                )
                if result:
                    producer.send(OUT_TOPIC, value=result)
                    signals += 1
                    ts = result["meta"]["time"]
                    print(f"[volatility_analyzer] {ts}  {result['value']:<6}  "
                          f"conf={result['confidence']:.2f}  "
                          f"amp={result['meta']['amplitude']}%  "
                          f"mean={result['meta']['mean_amplitude']}%")

        if total % 500 == 0:
            print(f"[volatility_analyzer] тиков: {total}  сигналов: {signals}")


# ─── Eval режим ──────────────────────────────────────────────────────────────

def run_eval():
    """
    HIGH должен предшествовать большим движениям цены.
    LOW должен предшествовать малым движениям.
    Смотрим среднее абсолютное движение цены после каждого типа сигнала.
    """
    consumer = make_consumer("", from_beginning=True)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_URL,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    short_buf     = deque(maxlen=SHORT_WINDOW)
    long_amp_buf  = deque(maxlen=LONG_WINDOW)
    price_history = []
    signals_log   = []
    signals_count = {"HIGH": 0, "NORMAL": 0, "LOW": 0}
    total = tick_idx = 0

    print(f"[volatility_analyzer] EVAL MODE  asset={ASSET}/{QUOTE}")
    print(f"[volatility_analyzer] short={SHORT_WINDOW} long={LONG_WINDOW}")
    print(f"[volatility_analyzer] Читаю {IN_TOPIC}...")

    for msg in consumer:
        tick = msg.value
        if tick.get("asset") != ASSET or tick.get("quote") != QUOTE:
            continue
        price = tick.get("price")
        if price is None:
            continue

        total    += 1
        tick_idx += 1
        price_history.append(price)
        short_buf.append(price)

        if len(short_buf) == SHORT_WINDOW:
            amp = amplitude(list(short_buf))
            long_amp_buf.append(amp)

            if len(long_amp_buf) >= SHORT_WINDOW:
                result = compute_signal(
                    list(short_buf), list(long_amp_buf),
                    price, tick["timestamp_ms"]
                )
                if result:
                    producer.send(OUT_TOPIC, value=result)
                    v = result["value"]
                    signals_count[v] = signals_count.get(v, 0) + 1
                    signals_log.append({
                        "tick_idx":   tick_idx,
                        "value":      v,
                        "confidence": result["confidence"],
                        "price":      price,
                        "amp":        float(result["meta"]["amplitude"]),
                        "time":       result["meta"]["time"],
                    })

    producer.flush()
    _print_eval_report(signals_log, signals_count, price_history, total)


def _print_eval_report(signals_log, signals_count, prices, total):
    print()
    print("═" * 60)
    print("  VOLATILITY ANALYZER — ОТЧЁТ ПО ОЦЕНКЕ")
    print("═" * 60)
    print(f"  Тиков обработано: {total}")
    total_sig = sum(signals_count.values())
    print(f"  Сигналов всего:   {total_sig}")
    for v, c in signals_count.items():
        pct = c / total_sig * 100 if total_sig > 0 else 0
        print(f"    {v:<6}: {c:>5}  ({pct:.1f}%)")
    print()
    print("  Среднее абс. движение цены после сигнала:")
    print("  (HIGH должен → большое движение, LOW → малое)")
    print()

    for h in EVAL_HORIZONS:
        buckets = {"HIGH": [], "NORMAL": [], "LOW": []}
        for s in signals_log:
            future_idx = s["tick_idx"] - 1 + h
            if future_idx >= len(prices):
                continue
            move = abs(prices[future_idx] - s["price"]) / s["price"] * 100
            buckets[s["value"]].append(move)

        print(f"  Горизонт +{h} тиков:")
        for v, moves in buckets.items():
            if moves:
                avg = sum(moves) / len(moves)
                mx  = max(moves)
                print(f"    {v:<6}: avg={avg:>5.2f}%  max={mx:>6.2f}%  (n={len(moves)})")
        print()

    print("═" * 60)


# ─── Точка входа ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "run"
    if mode == "eval":
        run_eval()
    else:
        run()
