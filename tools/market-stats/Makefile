_MS_DIR := $(dir $(abspath $(firstword $(filter %market-stats/Makefile, $(MAKEFILE_LIST)))))
_MS_DC  := docker compose --env-file $(_MS_DIR)../../.env --env-file $(_MS_DIR).env -f $(_MS_DIR)compose.yml

market-stats:
	$(_MS_DC) run --rm market-stats python src/main.py all

market-stats-prices:
	$(_MS_DC) run --rm market-stats python src/main.py prices

market-stats-fear:
	$(_MS_DC) run --rm market-stats python src/main.py fear

market-stats-combined:
	$(_MS_DC) run --rm market-stats python src/main.py combined

market-stats-build:
	$(_MS_DC) build

market-stats-test:
	$(_MS_DC) run --rm market-stats python -m pytest tests/ -v
