_VOL_DIR := $(dir $(abspath $(firstword $(filter %volatility-analyzer/Makefile, $(MAKEFILE_LIST)))))
_VOL_DC  := docker compose --env-file .env --env-file $(_VOL_DIR).env -f $(_VOL_DIR)compose.yml

volatility-analyzer:
	$(_VOL_DC) up --build

volatility-analyzer-down:
	$(_VOL_DC) down

volatility-analyzer-logs:
	docker logs -f $$(docker ps -qf name=volatility-analyzer)

volatility-analyzer-eval:
	$(_VOL_DC) run --rm volatility-analyzer python src/main.py eval

volatility-analyzer-test:
	$(_VOL_DC) run --rm volatility-analyzer python -m pytest tests/ -v

volatility-analyzer-build:
	$(_VOL_DC) build
