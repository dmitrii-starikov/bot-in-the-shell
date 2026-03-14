_BYB_DIR := $(dir $(abspath $(firstword $(filter %exchange-bybit-history-data/Makefile, $(MAKEFILE_LIST)))))
_BYB_DC  := docker compose --env-file $(_BYB_DIR)../../.env --env-file $(_BYB_DIR).env -f $(_BYB_DIR)compose.yml

exchange-bybit-history-data-fetch:
	$(_BYB_DC) run --rm exchange-bybit-history-data python src/main.py fetch

exchange-bybit-history-data-publish:
	$(_BYB_DC) run --rm exchange-bybit-history-data python src/main.py publish

exchange-bybit-history-data-simulate:
	$(_BYB_DC) run --rm exchange-bybit-history-data python src/main.py simulate

exchange-bybit-history-data:
	$(_BYB_DC) run --rm exchange-bybit-history-data python src/main.py all

exchange-bybit-history-data-test:
	$(_BYB_DC) run --rm exchange-bybit-history-data python -m pytest tests/ -v

exchange-bybit-history-data-build:
	$(_BYB_DC) build
