"""
sigma-analyzer — DEVIATION сигнал на основе стандартного отклонения цены.

Читает:  collectors.market.price
Пишет:  analyzers.signal.out

Логика (из старого кода, поле deviationAvgPrice):
  - Накапливает цены в скользящем окне N тиков
  - Считает mean и σ по окну
  - Если текущая цена выходит за mean ± σ → OVERBOUGHT / OVERSOLD
  - Иначе → NEUTRAL

Принцип: анализатор видит только текущий тик и историю прошлых.
Никакого заглядывания в будущее.

Режимы запуска:
    python src/main.py          — обычный режим, слушает бесконечно
    python src/main.py eval     — режим оценки: считает сигналы + метрики точности
"""
import os, sys, json, math, datetime
from collections import deque
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_URL  = os.environ.get("KAFKA_URL",  "kafka:9092")
IN_TOPIC   = os.environ.get("IN_TOPIC",   "collectors.market.price")
OUT_TOPIC  = os.environ.get("OUT_TOPIC",  "analyzers.signal.out")
ASSET      = os.environ.get("ASSET",      "ETH")
QUOTE      = os.environ.get("QUOTE",      "USDT")
WINDOW     = int(os.environ.get("WINDOW", "20"))   # тиков в окне
THRESHOLD  = float(os.environ.get("THRESHOLD", "1.0"))  # порог в единицах σ
TTL_MS     = int(os.environ.get("TTL_MS", str(5 * 86400 * 1000)))
# Через сколько тиков проверяем результат сигнала
EVAL_HORIZONS = [5, 10, 20]


# ─── Чистая функция сигнала ───────────────────────────────────────────────────

def compute_signal(prices: list[float], current_price: float, timestamp_ms: int) -> dict:
    """
    DEVIATION сигнал по окну цен.
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

    if deviation > THRESHOLD:
        value      = "OVERBOUGHT"
        confidence = min(abs(deviation) / (THRESHOLD * 3.0), 1.0)
    elif deviation < -THRESHOLD:
        value      = "OVERSOLD"
        confidence = min(abs(deviation) / (THRESHOLD * 3.0), 1.0)
    else:
        value      = "NEUTRAL"
        confidence = round(1.0 - abs(deviation) / THRESHOLD, 4)

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


# ─── Оценка точности ─────────────────────────────────────────────────────────

def evaluate_pending(pending: list, current_price: float, tick_idx: int) -> list:
    """
    Проверяет ожидающие сигналы — наступил ли горизонт оценки.
    Возвращает список ещё не закрытых сигналов.
    """
    still_pending = []
    for entry in pending:
        for h in EVAL_HORIZONS:
            if tick_idx == entry["tick_idx"] + h and f"result_{h}" not in entry:
                price_then  = entry["price"]
                pct_change  = (current_price - price_then) / price_then * 100
                correct = (
                    (entry["value"] == "OVERSOLD"   and current_price > price_then) or
                    (entry["value"] == "OVERBOUGHT" and current_price < price_then)
                )
                entry[f"result_{h}"]  = round(pct_change, 3)
                entry[f"correct_{h}"] = correct

        still_pending.append(entry)

    return still_pending


def print_eval_report(pending: list, total: int, signals_count: dict):
    """Печатает итоговый отчёт по метрикам."""

    # Только сигналы с направлением (не NEUTRAL)
    directional = [e for e in pending if e["value"] in ("OVERSOLD", "OVERBOUGHT")]

    print()
    print("═" * 60)
    print(f"  SIGMA ANALYZER — ОТЧЁТ ПО ОЦЕНКЕ")
    print("═" * 60)
    print(f"  Тиков обработано:   {total}")
    print(f"  Сигналов всего:     {sum(signals_count.values())}")
    total_signals = sum(signals_count.values())
    for v, c in signals_count.items():
        pct = c / total_signals * 100 if total_signals > 0 else 0
        print(f"    {v:<12}: {c:>5}  ({pct:.1f}%)")
    print()

    for h in EVAL_HORIZONS:
        scored = [e for e in directional if f"correct_{h}" in e]
        if not scored:
            continue

        correct  = sum(1 for e in scored if e[f"correct_{h}"])
        accuracy = correct / len(scored) * 100

        changes  = [e[f"result_{h}"] for e in scored]
        oversold_changes  = [e[f"result_{h}"] for e in scored if e["value"] == "OVERSOLD"]
        overbought_changes = [e[f"result_{h}"] for e in scored if e["value"] == "OVERBOUGHT"]

        avg_change = sum(changes) / len(changes)
        avg_os  = sum(oversold_changes)  / len(oversold_changes)  if oversold_changes  else 0
        avg_ob  = sum(overbought_changes) / len(overbought_changes) if overbought_changes else 0

        print(f"  Горизонт +{h} тиков  (оценено: {len(scored)})")
        print(f"    Точность:          {accuracy:.1f}%")
        print(f"    Средн. изменение:  {avg_change:+.2f}%")
        print(f"    OVERSOLD  → avg:   {avg_os:+.2f}%  (ожидаем +)")
        print(f"    OVERBOUGHT→ avg:   {avg_ob:+.2f}%  (ожидаем -)")
        print()

    # Confidence vs точность
    high_conf = [e for e in directional if e["confidence"] >= 0.6 and f"correct_{EVAL_HORIZONS[1]}" in e]
    low_conf  = [e for e in directional if e["confidence"] <  0.6 and f"correct_{EVAL_HORIZONS[1]}" in e]
    h = EVAL_HORIZONS[1]
    if high_conf and low_conf:
        acc_high = sum(1 for e in high_conf if e[f"correct_{h}"]) / len(high_conf) * 100
        acc_low  = sum(1 for e in low_conf  if e[f"correct_{h}"]) / len(low_conf)  * 100
        print(f"  Confidence vs точность (горизонт +{h}):")
        print(f"    confidence ≥ 0.6:  {acc_high:.1f}%  ({len(high_conf)} сигналов)")
        print(f"    confidence <  0.6: {acc_low:.1f}%  ({len(low_conf)} сигналов)")
        print()

    print("═" * 60)


# ─── Kafka loops ──────────────────────────────────────────────────────────────

def run():
    """Обычный режим — слушает бесконечно, пишет сигналы в Kafka."""
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
    total = signals = 0

    print(f"[sigma_analyzer] asset={ASSET}/{QUOTE} window={WINDOW}")
    print(f"[sigma_analyzer] {IN_TOPIC} → {OUT_TOPIC}")

    for msg in consumer:
        tick = msg.value
        if tick.get("asset") != ASSET or tick.get("quote") != QUOTE:
            continue
        price = tick.get("price") or tick.get("close")
        if price is None:
            continue

        total += 1

        if len(history) >= WINDOW:
            result = compute_signal(list(history), price, tick["timestamp_ms"])
            if result:
                producer.send(OUT_TOPIC, value=result)
                signals += 1
                ts = result["meta"]["time"]
                print(f"[sigma_analyzer] {ts}  {result['value']:<12} "
                      f"confidence={result['confidence']:.2f}  "
                      f"price={result['meta']['price']}  "
                      f"mean={result['meta']['mean']}  σ={result['meta']['sigma']}")

        # Добавляем текущую цену в историю ПОСЛЕ расчёта
        # (не заглядываем в будущее)
        history.append(price)

        if total % 500 == 0:
            print(f"[sigma_analyzer] тиков: {total}  сигналов: {signals}")


def run_eval():
    """
    Режим оценки — читает топик до конца, считает метрики точности.
    Каждый раз читает с самого начала — без group_id, без запоминания offset.
    """
    from kafka import TopicPartition
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_URL,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=3000,   # завершить когда нет новых сообщений 3с
    )
    # Назначаем партицию вручную и сразу идём в начало
    tp = TopicPartition(IN_TOPIC, 0)
    consumer.assign([tp])
    consumer.seek_to_beginning(tp)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_URL,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    history       = deque(maxlen=WINDOW)
    pending       = []   # сигналы ожидающие оценки
    signals_count = {"OVERSOLD": 0, "OVERBOUGHT": 0, "NEUTRAL": 0}
    total = tick_idx = 0

    print(f"[sigma_analyzer] EVAL MODE  asset={ASSET}/{QUOTE} window={WINDOW}")
    print(f"[sigma_analyzer] horizons={EVAL_HORIZONS} тиков")
    print(f"[sigma_analyzer] Читаю {IN_TOPIC}...")

    for msg in consumer:
        tick = msg.value
        if tick.get("asset") != ASSET or tick.get("quote") != QUOTE:
            continue
        price = tick.get("price") or tick.get("close")
        if price is None:
            continue

        total    += 1
        tick_idx += 1

        # Оцениваем pending сигналы текущей ценой
        pending = evaluate_pending(pending, price, tick_idx)

        if len(history) >= WINDOW:
            result = compute_signal(list(history), price, tick["timestamp_ms"])
            if result:
                producer.send(OUT_TOPIC, value=result)
                v = result["value"]
                signals_count[v] = signals_count.get(v, 0) + 1

                ts = result["meta"]["time"]
                print(f"[eval] {ts}  {v:<12} conf={result['confidence']:.2f}  "
                      f"price={result['meta']['price']}")

                # Запоминаем для оценки (только OVERSOLD/OVERBOUGHT)
                if v in ("OVERSOLD", "OVERBOUGHT"):
                    pending.append({
                        "tick_idx":  tick_idx,
                        "value":     v,
                        "price":     price,
                        "confidence": result["confidence"],
                        "time":      result["meta"]["time"],
                    })

        history.append(price)

    producer.flush()
    print_eval_report(pending, total, signals_count)


# ─── Точка входа ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "run"
    if mode == "eval":
        run_eval()
    else:
        run()