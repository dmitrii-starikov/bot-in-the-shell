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
	@mkdir -p _context
	@OUT=_context/context.md; \
	echo "# Project Context" > $$OUT; \
	echo "_Generated: $$(date '+%Y-%m-%d %H:%M:%S')_" >> $$OUT; \
	echo "" >> $$OUT; \
	\
	echo "## Structure" >> $$OUT; \
	echo '```' >> $$OUT; \
	find . -not -path '*/\.*' \
	       -not -path '*/node_modules/*' \
	       -not -path '*/tmp/*' \
	       -not -path '*/_context/*' \
	       -not -path '*/__pycache__/*' \
	       -not -path '*/_archives/*' \
	       | sort | sed 's|^\./||' >> $$OUT; \
	echo '```' >> $$OUT; \
	echo "" >> $$OUT; \
	\
	echo "## Files" >> $$OUT; \
	for f in $$(find . \
	        \( -name "*.py" -o -name "*.yml" -o -name "Makefile" -o -name "Dockerfile" -o -name "*.proto" -o -name "*.md" \) \
	        -not -path '*/\.*' \
	        -not -path '*/tmp/*' \
	        -not -path '*/_context/*' \
	        -not -path '*/__pycache__/*' \
	        -not -path '*/_archives/*' \
	        | sort); do \
	    echo "" >> $$OUT; \
	    echo "### $$f" >> $$OUT; \
	    echo '```' >> $$OUT; \
	    cat $$f >> $$OUT; \
	    echo '```' >> $$OUT; \
	done; \
	echo ""; \
	echo "Контекст сохранён в $$OUT ($$(wc -l < $$OUT) строк)"

##Makefile.local
-include Makefile.local
-include Makefile.children