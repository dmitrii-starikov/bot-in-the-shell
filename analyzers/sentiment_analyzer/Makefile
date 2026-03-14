_SENT_DIR := $(dir $(abspath $(firstword $(filter %sentiment-analyzer/Makefile, $(MAKEFILE_LIST)))))
_SENT_DC  := docker compose --env-file $(_SENT_DIR)../../.env --env-file $(_SENT_DIR).env -f $(_SENT_DIR)compose.yml

sentiment-analyzer:
	$(_SENT_DC) up --build

sentiment-analyzer-down:
	$(_SENT_DC) down

sentiment-analyzer-logs:
	docker logs -f $$(docker ps -qf name=sentiment-analyzer)

sentiment-analyzer-eval:
	$(_SENT_DC) run --rm sentiment-analyzer python src/main.py eval

sentiment-analyzer-test:
	$(_SENT_DC) run --rm sentiment-analyzer python -m pytest tests/ -v

sentiment-analyzer-build:
	$(_SENT_DC) build
