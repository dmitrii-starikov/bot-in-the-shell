DOCKER_COMPOSE=docker compose

build:
	$(DOCKER_COMPOSE) pull --ignore-pull-failures
	$(DOCKER_COMPOSE) build --force-rm --pull

up:
	$(DOCKER_COMPOSE) up -d --remove-orphans

stop:
	$(DOCKER_COMPOSE) stop

remove:
	$(DOCKER_COMPOSE) kill
	$(DOCKER_COMPOSE) rm -v --force

start: build up

reset: remove start

restart: stop start

save-context:
	ls

##Makefile.local
-include Makefile.local
-include Makefile.children