test:
	@NODE_ENV=test npm test

test-cov:
	@NODE_ENV=test npm run coverage

jshint:
	./node_modules/.bin/jshint --show-non-errors lib test

.PHONY: test test-cov
