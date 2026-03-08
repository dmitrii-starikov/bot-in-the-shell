"""
Тестовый скрипт — пушит несколько видов сообщений в Telegram напрямую.
Без Kafka, без бота. Просто проверить что токен и channel_id рабочие.

Использование:
    TELEGRAM_TOKEN=xxx TELEGRAM_DEFAULT_CHANNEL_ID=yyy python3 test_telegram.py
"""
import os, json, urllib.request, urllib.error

TOKEN      = os.environ.get("TELEGRAM_TOKEN", "")
CHANNEL_ID = os.environ.get("TELEGRAM_DEFAULT_CHANNEL_ID", "")

if not TOKEN or not CHANNEL_ID:
    print("Нужны переменные: TELEGRAM_TOKEN и TELEGRAM_DEFAULT_CHANNEL_ID")
    exit(1)


def send(text: str) -> bool:
    url     = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = json.dumps({"chat_id": CHANNEL_ID, "text": text, "parse_mode": "HTML"}).encode()
    req     = urllib.request.Request(url, data=payload,
                                     headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.load(resp)
            return result.get("ok", False)
    except urllib.error.HTTPError as e:
        print(f"  HTTP {e.code}: {e.read().decode()}")
        return False


MESSAGES = [
    # Обычный лог
    "ℹ️ <b>[INFO]</b>  Бот запущен\nПара: ETH/USDT  |  Депозит: $1000",

    # Сигнал
    "📊 <b>[SIGNAL]</b>  sigma_analyzer\nDEVIATION=OVERSOLD  |  confidence=0.81\nmean=2300  σ=85  last=2050",

    # Сделка
    "✅ <b>[TRADE]</b>  BUY исполнен\nETH/USDT  |  цена: $2051  |  объём: 0.24 ETH\nкоманда: #a3f1bc",

    # Ошибка
    "⚠️ <b>[ERROR]</b>  Адаптер не смог подключиться к ByBit\nRetry 1/3...",

    # Критическое
    "🚨 <b>[CRITICAL]</b>  Сработал стоп-лосс\nETH/USDT  |  вход: $2100  |  выход: $1995\nPnL: <b>-4.9%</b>",
]

print(f"Отправляю {len(MESSAGES)} сообщений в {CHANNEL_ID}...\n")

for i, text in enumerate(MESSAGES, 1):
    preview = text.replace("\n", " ")[:60]
    ok = send(text)
    status = "✓" if ok else "✗"
    print(f"  {status}  [{i}/{len(MESSAGES)}] {preview}...")
