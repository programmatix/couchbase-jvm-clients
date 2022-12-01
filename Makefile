install: deps-only
	@./mvnw install

deps-only:
	@./mvnw -q -f protostellar/pom.xml clean install -B
	@./mvnw -q -f core-io-deps/pom.xml clean install -B
