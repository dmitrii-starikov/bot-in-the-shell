# telegram-notifier

Слушает топик `adapters.notifications` и отправляет сообщения в Telegram.

## Что делает

Любой сервис системы может опубликовать сообщение в `adapters.notifications` —
нотифаер доставит его в нужный Telegram-канал.

```
adapters.notifications → [telegram-notifier] → Telegram
```
## Формат сообщения в топике

```json
{
    "level":      "INFO",
    "message":    "Buy order placed: ETH 0.5 @ 2350.0",
    "channel_id": "-100123456789",
    "context":    {"order_id": 42, "price": 2350.0}
}
```

- `channel_id` — опциональный, если не задан берётся `TELEGRAM_CHANNEL_ID` из ENV
- `context` — опциональный, выводится построчно под сообщением
- `level` — фильтруется по `LOGGED_LEVELS`

## Уровни

| level    | когда использовать              |
|----------|---------------------------------|
| INFO     | обычные события (ордер, сигнал) |
| ERROR    | ошибки не критичные             |
| CRITICAL | требует немедленного внимания   |
| DEBUG    | не логируется по умолчанию      |

## Команды

```bash
make telegram-notifier-logs     # смотреть логи
make telegram-notifier-test-credentials     # проверка работы с API Telegram
make telegram-notifier-test-kafka     # отправит 3 тестовых сообщений через kafka
```

## Как отправить сообщение из другого сервиса

```python
producer.send("adapters.notifications", value={
    "level":   "INFO",
    "message": "Solver: BUY signal executed",
    "context": {"asset": "ETH", "price": 2350.0}
})
```

## Особенности реализации

- Использует только stdlib (`urllib`) — без `python-telegram-bot`
- `auto_offset_reset=latest` — читает только новые сообщения, не историю
- `restart: unless-stopped` — поднимается автоматически вместе с инфрой