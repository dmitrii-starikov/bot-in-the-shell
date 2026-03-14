_FG_DIR := $(dir $(abspath $(firstword $(filter %exchange-alternative-fear-greed/Makefile, $(MAKEFILE_LIST)))))
_FG_DC  := docker compose --env-file $(_FG_DIR)../../.env --env-file $(_FG_DIR).env -f $(_FG_DIR)compose.yml

exchange-alternative-fear-greed-fetch:
	$(_FG_DC) run --rm exchange-alternative-fear-greed python src/main.py fetch

exchange-alternative-fear-greed-publish:
	$(_FG_DC) run --rm exchange-alternative-fear-greed python src/main.py publish

exchange-alternative-fear-greed:
	$(_FG_DC) run --rm exchange-alternative-fear-greed python src/main.py all

exchange-alternative-fear-greed-test:
	$(_FG_DC) run --rm exchange-alternative-fear-greed python -m pytest tests/ -v

exchange-alternative-fear-greed-build:
	$(_FG_DC) build
