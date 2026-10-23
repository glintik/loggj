# Tests run in Docker, NODE selects node:<version>-alpine image
NODE ?= 24
NODE_VERSIONS = 22 24 26

DOCKER_RUN = docker run --rm -e NODE_ENV=test \
	-v "$(CURDIR)":/app -v /app/node_modules -w /app node:$(NODE)-alpine
NPM_CI = npm ci --no-audit --no-fund --loglevel=error

test:
	$(DOCKER_RUN) sh -c "$(NPM_CI) && npm test"

test-cov:
	$(DOCKER_RUN) sh -c "$(NPM_CI) && npm run coverage"

test-all:
	@for v in $(NODE_VERSIONS); do \
		echo "=== Node $$v"; \
		$(MAKE) --no-print-directory test NODE=$$v || exit 1; \
	done

.PHONY: test test-cov test-all
