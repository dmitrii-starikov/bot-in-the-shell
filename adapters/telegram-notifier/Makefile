_T_N_DIR := $(dir $(abspath $(firstword $(filter %telegram-notifier/Makefile, $(MAKEFILE_LIST)))))
_T_N_DC  := docker compose --env-file $(_T_N_DIR)../../.env --env-file $(_T_N_DIR).env -f $(_T_N_DIR)compose.yml

telegram-notifier-logs:
	docker logs -f $$(docker ps -qf name=telegram-notifier)

telegram-notifier-test-credentials:
	$(_T_N_DC) run --rm telegram-notifier python tests/test_telegram.py

telegram-notifier-test-kafka:
	$(_T_N_DC) run --rm telegram-notifier python tests/test_kafka_notify.py