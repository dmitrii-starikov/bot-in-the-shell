

class TradingBot:
    def __init__(self, strategies: List[SignalStrategy]):
        self.strategies = strategies

    def evaluate(self, ticker: str, current_price: float) -> Dict[str, float]:
        results = {
            "buy": 0,
            "sell": 0,
            "hold": 0,
            "modifiers": [],
            "blockers": []
        }

        for strategy in self.strategies:
            probabilities = strategy.calculate(ticker, current_price)
            results["buy"] += probabilities["buy"]
            results["sell"] += probabilities["sell"]
            results["hold"] += probabilities["hold"]
            results["modifiers"].extend(probabilities.get("modifiers", []))
            results["blockers"].extend(probabilities.get("blockers", []))

        # Нормализуем вероятности
        total = results["buy"] + results["sell"] + results["hold"]
        if total > 0:
            results["buy"] /= total
            results["sell"] /= total
            results["hold"] /= total

        return results

if __name__ == "__main__":
    strategies = load_strategies()
    bot = TradingBot(strategies)

    ticker = "BTCUSDT"
    current_price = 45000.0

    decision = bot.evaluate(ticker, current_price)
    print(decision)






from typing import List, Dict

class TradingBot:
    def __init__(self, strategies: List[SignalStrategy]):
        self.strategies = strategies

    def evaluate(self, ticker: str, current_price: float) -> Dict[str, float]:
        results = {
            "buy": 0,
            "sell": 0,
            "hold": 0,
            "modifiers": [],
            "blockers": []
        }

        for strategy in self.strategies:
            probabilities = strategy.calculate(ticker, current_price)
            results["buy"] += probabilities["buy"]
            results["sell"] += probabilities["sell"]
            results["hold"] += probabilities["hold"]
            results["modifiers"].extend(probabilities.get("modifiers", []))
            results["blockers"].extend(probabilities.get("blockers", []))

        # Нормализуем вероятности
        total = results["buy"] + results["sell"] + results["hold"]
        if total > 0:
            results["buy"] /= total
            results["sell"] /= total
            results["hold"] /= total

        return results

if __name__ == "__main__":
    strategies = load_strategies()
    bot = TradingBot(strategies)

    ticker = "BTCUSDT"
    current_price = 45000.0

    decision = bot.evaluate(ticker, current_price)
    print(decision)




import time
import schedule
import psycopg2
import redis
import requests

# Конфигурация для подключения к сервисам
DB_PARAMS = {
    'dbname': 'your_database_name',
    'user': 'your_username',
    'password': 'your_password',
    'host': 'your_host',
    'port': 'your_port'
}

REDIS_PARAMS = {
    'host': 'localhost',
    'port': 6389
}

ELASTICSEARCH_URL = 'http://localhost:9201'

class TradingBot:
    def __init__(self, strategies):
        self.strategies = strategies
        self.start_time = time.time()

    def evaluate(self, ticker, current_price):
        results = {
            "buy": 0,
            "sell": 0,
            "hold": 0,
            "modifiers": [],
            "blockers": []
        }

        for strategy in self.strategies:
            probabilities = strategy.calculate(ticker, current_price)
            results["buy"] += probabilities["buy"]
            results["sell"] += probabilities["sell"]
            results["hold"] += probabilities["hold"]
            results["modifiers"].extend(probabilities.get("modifiers", []))
            results["blockers"].extend(probabilities.get("blockers", []))

        total = results["buy"] + results["sell"] + results["hold"]
        if total > 0:
            results["buy"] /= total
            results["sell"] /= total
            results["hold"] /= total

        return results

    def selfCheck(self):
        print("Performing self-check...")

        # Проверка PostgreSQL
        try:
            conn = psycopg2.connect(**DB_PARAMS)
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
            conn.close()
            print("PostgreSQL is running.")
        except Exception as e:
            print(f"PostgreSQL check failed: {e}")

        # Проверка Redis
        try:
            r = redis.Redis(**REDIS_PARAMS)
            r.ping()
            print("Redis is running.")
        except Exception as e:
            print(f"Redis check failed: {e}")

        # Проверка Elasticsearch
        try:
            response = requests.get(ELASTICSEARCH_URL)
            if response.status_code == 200:
                print("Elasticsearch is running.")
            else:
                print(f"Elasticsearch check failed: {response.status_code}")
        except Exception as e:
            print(f"Elasticsearch check failed: {e}")

def main():
    # Инициализация стратегий (пример)
    strategies = load_strategies()
    bot = TradingBot(strategies)

    # Планирование выполнения selfCheck раз в час
    schedule.every().hour.do(bot.selfCheck)

    ticker = "BTCUSDT"
    current_price = 45000.0

    while True:
        # Выполнение основных задач
        decision = bot.evaluate(ticker, current_price)
        print(decision)

        # Выполнение запланированных задач (selfCheck)
        schedule.run_pending()

        # Пауза на 10 секунд
        time.sleep(10)

if __name__ == "__main__":
    main()

