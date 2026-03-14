# bot-in-the-shell

Автоматический торговый бот. Проект учебный — основная цель: практика event-driven архитектуры, Kafka, микросервисов.

---

## Архитектура

Event-driven микросервисы. Каждый сервис — отдельный контейнер с одной задачей.
Общение только через Kafka, никакого прямого вызова между сервисами.

```
Слой 0 — Collectors
  Скачивают данные → пишут в Kafka
  Одноразовые (run --rm) или постоянные

Слой 1 — Analyzers
  Читают market.price → пишут сигналы в signal.out
  Чистые функции. Не знают о деньгах и ордерах.

Слой 2 — Solver
  Читает signal.out + trading.orders → пишет trading.commands
  Единственный кто знает о деньгах, позициях, риск-менеджменте.

Слой 3 — Adapters
  Читают trading.commands → исполняют на бирже → пишут trading.orders
  Единственный кто работает с API биржи и знает ключи.

   Также различные служебные адаптеры: оповещения и др.
```

### Топики Kafka

| Топик                     | Кто пишет  | Кто читает       |
|---------------------------|------------|------------------|
| `collectors.*`            | collectors | analyzers        |
| `analyzers.signal.out`    | analyzers  | solver           |
| `solver.trading.commands` | solver     | adapters         |
| `adapters.trading.orders` | adapters   | solver           |
| `adapters.notifications`  | любой      | adapters|

---

## Инфраструктура

- **Kafka** (KRaft, без Zookeeper) — шина событий
- **Postgres** — хранение исторических данных сборщиков
- **Kafka UI** — мониторинг топиков

---

## Запуск

```bash
make up
# Загрузить исторические данные ETH
make exchange-coinmarketcap-history-data-fetch
# Запустить симуляцию биржевого потока
make exchange-coinmarketcap-history-data-simulate
# Запустить анализатор
make sigma-analyzer
# Оценить качество сигналов
make sigma-analyzer-eval
make volatility-analyzer-eval
```

---

## Конфигурация

Корневой `.env` — общие параметры (DB, Kafka, Telegram).
Каждый сервис имеет свой `.env` — специфичные параметры, переопределяет общие где нужно
(например `DB_PORT=5432` внутри Docker vs внешний порт).

```
.env                                    ← корневой, общий
adapters/telegram-notifier/.env
analyzers/sigma_analyzer/.env
analyzers/volatility_analyzer/.env
collectors/exchange-*/.env
```

---

## Принципы

- **Один бот = одна торговая пара.** Масштабирование через контейнеры.
- **Анализатор видит только прошлое.** Никакого заглядывания в будущее.
- **Solver — единственный кто знает о деньгах.** Анализаторы про деньги не знают.
- **Adapter — единственный с API-ключами.** Остальные сервисы ключей не видят.
- **Каждый сервис тестируется изолированно.** Чистые функции, моки для Kafka/DB.