DIR  := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
ENV  := $(DIR).env
DC := docker compose --env-file $(DIR)../../.env --env-file $(DIR).env -f $(DIR)compose.yml
SVC  := exchange-coinmarketcap-history-data

# fetch — скачать с CoinMarketCap → сохранить в БД
$(SVC)-fetch:
	$(DC) run --rm $(SVC) python src/main.py fetch

# publish — прочитать из БД → запушить в Kafka
$(SVC)-publish:
	$(DC) run --rm $(SVC) python src/main.py publish

# all — fetch + publish
$(SVC):
	$(DC) run --rm $(SVC) python src/main.py all

# test
$(SVC)-test:
	$(DC) run --rm $(SVC) python -m pytest tests/ -v

# build
$(SVC)-build:
	$(DC) build

# simulate — пушит тики (5 на свечу) как симуляция биржи
$(SVC)-simulate:
	$(DC) run --rm $(SVC) python src/main.py simulate