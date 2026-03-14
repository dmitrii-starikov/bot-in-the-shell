# sigma-analyzer

Анализатор отклонения цены от скользящего среднего (DEVIATION сигнал).

## Что делает

Читает тики из топика `collectors.market.price`, вычисляет стандартное отклонение цены
за скользящее окно и публикует сигнал в `analyzers.signal.out`.

```
collectors.market.price → [sigma-analyzer] → analyzers.signal.out
```

## Логика

Для каждого тика считает отклонение текущей цены от среднего по окну:

```
deviation = (price - mean) / σ
```

| deviation       | сигнал      | интерпретация             |
|-----------------|-------------|---------------------------|
| > THRESHOLD     | OVERBOUGHT  | цена выше нормы → ожидаем откат вниз |
| < -THRESHOLD    | OVERSOLD    | цена ниже нормы → ожидаем отскок вверх |
| иначе           | NEUTRAL     | цена в норме              |

**Принцип:** анализатор видит только текущий тик и историю прошлых.
Никакого заглядывания в будущее (при бэктесте).

## Конфигурация (.env)

```env
KAFKA_URL=kafka:9092
ASSET=ETH
QUOTE=USDT
WINDOW=50          # тиков в скользящем окне
THRESHOLD=2.0      # порог срабатывания в единицах σ
IN_TOPIC=collectors.market.price
OUT_TOPIC=analyzers.signal.out
TTL_MS=432000000   # 5 дней
```

## Команды

```bash
make sigma-analyzer          # запустить как демон
make sigma-analyzer-down     # остановить
make sigma-analyzer-logs     # смотреть логи
make sigma-analyzer-eval     # оценка точности на исторических данных
make sigma-analyzer-test     # тесты
make sigma-analyzer-build    # пересобрать образ
```

## Режим оценки (eval)

Читает топик с начала, генерирует сигналы и проверяет точность предсказания
через 5, 10 и 20 тиков после каждого сигнала.

```bash
make sigma-analyzer-eval
```

Пример отчёта:
```
  Горизонт +10 тиков  (оценено: 265)
    Точность:          62.6%
    OVERSOLD  → avg:   +2.78%  (ожидаем +)
    OVERBOUGHT→ avg:   -0.81%  (ожидаем -)
```

## Формат сигнала

```json
{
  "source":       "sigma_analyzer",
  "asset":        "ETH",
  "quote":        "USDT",
  "signal_type":  "DEVIATION",
  "value":        "OVERSOLD",
  "confidence":   0.74,
  "ttl_ms":       432000000,
  "timestamp_ms": 1704067200000,
  "meta": {
    "mean":      "2310.5",
    "sigma":     "87.3",
    "price":     "2100.0",
    "deviation": "-2.41",
    "window":    "50",
    "time":      "2024-01-01 00:00:00"
  }
}
```

## Подбор параметров

| WINDOW | THRESHOLD | характер                        |
|--------|-----------|---------------------------------|
| 20     | 1.0       | много сигналов, слабая точность |
| 50     | 2.0       | умеренно, хорошо на боковике    |
| 100    | 2.0       | редкие сигналы, только экстремумы |

Стратегия **mean reversion** — работает на боковом рынке,
плохо работает на сильном тренде. Рекомендуется комбинировать
с `volatility-analyzer` (фильтр по амплитуде).