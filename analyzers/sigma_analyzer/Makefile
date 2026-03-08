_SIGMA_DIR := $(dir $(abspath $(firstword $(filter %sigma_analyzer/Makefile, $(MAKEFILE_LIST)))))
_SIGMA_DC  := docker compose --env-file $(_SIGMA_DIR)../../.env --env-file $(_SIGMA_DIR).env -f $(_SIGMA_DIR)compose.yml

sigma-analyzer:
	$(_SIGMA_DC) up --build

sigma-analyzer-down:
	$(_SIGMA_DC) down

sigma-analyzer-logs:
	docker logs -f $$(docker ps -qf name=sigma-analyzer)

sigma-analyzer-test:
	$(_SIGMA_DC) run --rm sigma-analyzer python -m pytest tests/ -v

sigma-analyzer-build:
	$(_SIGMA_DC) build