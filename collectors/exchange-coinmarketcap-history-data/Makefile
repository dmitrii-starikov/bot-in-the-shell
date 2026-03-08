_CMC_DIR := $(dir $(abspath $(firstword $(filter %exchange-coinmarketcap-history-data/Makefile, $(MAKEFILE_LIST)))))
_CMC_DC  := docker compose --env-file $(_CMC_DIR)../../.env --env-file $(_CMC_DIR).env -f $(_CMC_DIR)compose.yml

exchange-coinmarketcap-history-data-fetch:
	$(_CMC_DC) run --rm exchange-coinmarketcap-history-data python src/main.py fetch

exchange-coinmarketcap-history-data-publish:
	$(_CMC_DC) run --rm exchange-coinmarketcap-history-data python src/main.py publish

exchange-coinmarketcap-history-data:
	$(_CMC_DC) run --rm exchange-coinmarketcap-history-data python src/main.py all

# simulate — пушит тики (5 на свечу) как симуляция биржи
exchange-coinmarketcap-history-data-simulate:
	$(_CMC_DC) run --rm exchange-coinmarketcap-history-data python src/main.py simulate

exchange-coinmarketcap-history-data-test:
	$(_CMC_DC) run --rm exchange-coinmarketcap-history-data python -m pytest tests/ -v

exchange-coinmarketcap-history-data-build:
	$(_CMC_DC) build