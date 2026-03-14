# exchange-coinmarketcap-history-data

Загружает исторические дневные свечи с CoinMarketCap, сохраняет в Postgres
и публикует в Kafka.

## Что делает

```
CoinMarketCap API → Postgres → collectors.market.price
```

Поддерживает два режима публикации:
- **publish** — дневные свечи OHLCV как есть
- **simulate** — разбивает каждую свечу на 5 тиков, имитируя биржевой поток

## Конфигурация (.env)

```env
# Переопределяем из корневого (внутри Docker)
DB_PORT=5432
KAFKA_URL=kafka:9092

# Настройки сборщика
KAFKA_TOPIC=collectors.market.price
YEAR_FROM=2017
YEAR_TO=2026
COINMARKETCAP_BTC_ID=1
COINMARKETCAP_ETH_ID=1027
COINMARKETCAP_BNB_ID=1839

# Фильтры для publish/simulate (опционально)
# PUBLISH_COIN=eth
# PUBLISH_DATE_FROM=2023-01-01
# PUBLISH_DATE_TO=2024-01-01

# Пауза между тиками в режиме simulate (0 = максимальная скорость)
SIMULATE_DELAY_MS=0
```

## Команды

```bash
make exchange-coinmarketcap-history-data-fetch     # скачать с CMC → сохранить в БД
make exchange-coinmarketcap-history-data-publish   # из БД → collectors.market.price (свечи)
make exchange-coinmarketcap-history-data-simulate  # из БД → collectors.market.price (тики)
make exchange-coinmarketcap-history-data           # fetch + publish
make exchange-coinmarketcap-history-data-build     # пересобрать образ
make exchange-coinmarketcap-history-data-test      # тесты
```

## Формат свечи (publish)

```json
{
    "asset":        "ETH",
    "quote":        "USDT",
    "exchange":     "historical",
    "open":         2200.0,
    "high":         2400.0,
    "low":          2100.0,
    "close":        2350.0,
    "volume":       9000000,
    "interval":     "1d",
    "timestamp_ms": 1704067200000,
    "candle_id":    "ETHUSDT_1d_1704067200000"
}
```

## Формат тика (simulate)

Из каждой дневной свечи генерируется 5 тиков в хронологическом порядке:
- Бычий день (close ≥ open): open → low → mid → high → close
- Медвежий день: open → high → mid → low → close

```json
{
    "asset":        "ETH",
    "quote":        "USDT",
    "exchange":     "simulation",
    "price":        2350.0,
    "volume":       1800000,
    "interval":     "tick",
    "timestamp_ms": 1704067200000,
    "candle_id":    "ETHUSDT_tick_1704067200000"
}
```

## Кеш

Скачанные JSON файлы кешируются в `./tmp/` — повторный `fetch` не перекачивает
уже загруженные данные. Для принудительного обновления:

## Таблица в БД

`collectors_history_coinmarketcap_daily_candles (coin, date, open_price, high_price, low_price, close_price, avg_price, volume, market_cap, time_open, time_close)`

Индекс `UNIQUE (coin, date)` — повторный fetch безопасен.