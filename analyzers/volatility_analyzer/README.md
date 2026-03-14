# volatility-analyzer

Анализатор волатильности на основе амплитуды цен в скользящем окне (VOLATILITY сигнал).

## Что делает

Читает тики из топика `collectors.market.price`, вычисляет амплитуду цен за короткое окно
и сравнивает с исторической нормой за длинное окно. Публикует сигнал в `analyzers.signal.out`.

```
collectors.market.price → [volatility-analyzer] → analyzers.signal.out
```

## Логика

```
amplitude = (max - min) / min * 100  — за SHORT_WINDOW тиков
```

Сравнивает текущую амплитуду со средней по истории амплитуд (LONG_WINDOW):

| отклонение      | сигнал  | интерпретация                        |
|-----------------|---------|--------------------------------------|
| > mean + σ      | HIGH    | скачок, опасно — лучше не входить    |
| < mean - σ      | LOW     | тихий рынок — мало движения          |
| иначе           | NORMAL  | нормальная волатильность — торгуем   |

**Два окна** — ключевое отличие от sigma-analyzer:
- `SHORT_WINDOW` — амплитуда прямо сейчас
- `LONG_WINDOW` — что считается нормой за последние N тиков

## Конфигурация (.env)

```env
KAFKA_URL=kafka:9092
ASSET=ETH
QUOTE=USDT
SHORT_WINDOW=10    # тиков для текущей амплитуды
LONG_WINDOW=100    # тиков для нормы
IN_TOPIC=collectors.market.price
OUT_TOPIC=analyzers.signal.out
TTL_MS=432000000   # 5 дней
```

## Команды

```bash
make volatility-analyzer          # запустить как демон
make volatility-analyzer-down     # остановить
make volatility-analyzer-logs     # смотреть логи
make volatility-analyzer-eval     # оценка на исторических данных
make volatility-analyzer-test     # тесты
make volatility-analyzer-build    # пересобрать образ
```

## Режим оценки (eval)

Проверяет насколько хорошо сигнал предсказывает движение цены.
HIGH должен предшествовать большим движениям, LOW — малым.

```bash
make volatility-analyzer-eval
```

Пример отчёта:
```
  Горизонт +10 тиков — среднее абс. движение цены:
    HIGH  : avg= 3.21%  max= 18.40%  (n=89)
    NORMAL: avg= 1.54%  max=  9.20%  (n=412)
    LOW   : avg= 0.61%  max=  2.10%  (n=54)
```

## Формат сигнала

```json
{
  "source":       "volatility_analyzer",
  "asset":        "ETH",
  "quote":        "USDT",
  "signal_type":  "VOLATILITY",
  "value":        "HIGH",
  "confidence":   0.81,
  "ttl_ms":       432000000,
  "timestamp_ms": 1704067200000,
  "meta": {
    "amplitude":      "4.21",
    "mean_amplitude": "1.87",
    "sigma":          "0.94",
    "deviation":      "2.49",
    "short_window":   "10",
    "long_window":    "100",
    "time":           "2024-01-01 00:00:00"
  }
}
```

## Комбинирование с sigma-analyzer

Оба анализатора пишут в один топик `analyzers.signal.out`. Солвер комбинирует сигналы:

```
OVERSOLD  + NORMAL → BUY
OVERBOUGHT + NORMAL → SELL
любой     + HIGH   → пропускаем (скачок)
любой     + LOW    → пропускаем (нет движения)
```

Это фильтрует ложные срабатывания sigma в периоды аномальной волатильности.