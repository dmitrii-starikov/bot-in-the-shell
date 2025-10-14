DOCKER_COMPOSE=docker-compose

help: 																													##Commands description
	@grep -hE '(^[a-zA-Z_-]+:.*?##.*$$)|(^##)' | awk 'BEGIN {FS = ":.*?##"}; {printf "\033[	32m%-30s\033[0m %s\n", $$1, $$2}' | sed -e 's/\[	32m##/[31m/'

build:																													##Building containers
	$(DOCKER_COMPOSE) pull --ignore-pull-failures
	$(DOCKER_COMPOSE) build --force-rm --pull

up:																														##Starting containers
	$(DOCKER_COMPOSE) up -d --remove-orphans

stop:																													##Stop containers
	$(DOCKER_COMPOSE) stop

remove:																													##Delete containers and clear database
	$(DOCKER_COMPOSE) kill
	$(DOCKER_COMPOSE) rm -v --force

start: build up																											##Build and start containers

reset: remove start                          																			##Delete containers, then start

restart: stop start																										##Stop containers, then start

