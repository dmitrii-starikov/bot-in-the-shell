DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

test-telegram-notifier:
	bash -c "set -a && source $(DIR)/.env && set +a && python3 $(DIR)/test_telegram.py"