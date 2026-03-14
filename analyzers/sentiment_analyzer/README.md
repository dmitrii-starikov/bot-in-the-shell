# sentiment-analyzer

Анализатор рыночного настроения на основе Fear & Greed индекса (TREND сигнал).

## Что делает

```
collectors.fear.greed → [sentiment-analyzer] → analyzers.signal.out
```

## Логика

Контр-трендовая стратегия — страх = покупать, жадность = продавать.
Основана на исторических данных ETH 2020-2026 из `market-stats`.

| Зона            | FG value | Сигнал   | Confidence |
|-----------------|----------|----------|------------|
| Extreme Fear    | 0–24     | BULLISH  | высокий    |
| Fear            | 25–46    | BULLISH  | низкий     |
| Neutral         | 47–54    | NEUTRAL  | низкий     |
| Greed           | 55–75    | BEARISH  | низкий     |
| Extreme Greed   | 76–100   | BEARISH  | высокий    |

Почему Neutral → NEUTRAL: исторически при Neutral рынок бычий только 46.4% дней
— хуже случайного угадывания. Сигнал не надёжен.

## Конфигурация (.env)

```env
KAFKA_URL=kafka:9092
IN_TOPIC=collectors.fear.greed
OUT_TOPIC=analyzers.signal.out
TTL_MS=86400000      # 24 часа — данные суточные

# Границы зон (можно подстроить под другие активы)
EXTREME_FEAR_MAX=24
FEAR_MAX=46
NEUTRAL_MAX=54
GREED_MAX=75
```

## Команды

```bash
make sentiment-analyzer          # запустить как демон
make sentiment-analyzer-down
make sentiment-analyzer-logs
make sentiment-analyzer-eval     # статистика по зонам
make sentiment-analyzer-test
make sentiment-analyzer-build
```

## Формат сигнала

```json
{
  "source":       "sentiment_analyzer",
  "asset":        "CRYPTO",
  "quote":        "MARKET",
  "signal_type":  "TREND",
  "value":        "BULLISH",
  "confidence":   0.72,
  "ttl_ms":       86400000,
  "timestamp_ms": 1704067200000,
  "meta": {
    "fg_value":       "12",
    "classification": "Extreme Fear",
    "date":           "2024-01-01"
  }
}
```

`asset: CRYPTO` и `quote: MARKET` — сигнал глобальный, не привязан к паре.
Solver применяет его ко всем парам.

## Комбинирование с другими сигналами

```
OVERSOLD (sigma) + BULLISH (sentiment) → сильный BUY
OVERSOLD (sigma) + BEARISH (sentiment) → слабый BUY или пропуск
OVERBOUGHT       + BEARISH (sentiment) → сильный SELL
любой            + NEUTRAL (sentiment) → игнорируем sentiment
```
