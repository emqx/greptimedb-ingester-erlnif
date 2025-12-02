SCRIPTS = $(CURDIR)/scripts

.PHONY: all
all: compile

.PHONY: compile
compile:
	rebar3 compile

.PHONY: clean
clean:
	rebar3 clean
	cargo clean

.PHONY: clean-all
clean-all:
	@rm -f rebar.lock
	@rm -rf _build
	@rm -rf target
	@echo "Cleaned all build artifacts."

.PHONY: ct
ct:
	rebar3 ct --readable true -c

.PHONY: ct-docker
ct-docker:
	@echo "Fetching container IPs..."
	$(eval TCP_ADDR := $(shell docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' greptimedb_tcp))
	$(eval TLS_ADDR := $(shell docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' greptimedb_tls))
	$(eval AUTH_ADDR := $(shell docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' greptimedb_auth))
	@echo "Running tests with TCP=$(TCP_ADDR), TLS=$(TLS_ADDR), AUTH=$(AUTH_ADDR)"
	GREPTIMEDB_TCP_ADDR=$(TCP_ADDR) GREPTIMEDB_TLS_ADDR=$(TLS_ADDR) GREPTIMEDB_AUTH_ADDR=$(AUTH_ADDR) rebar3 ct --readable true -c

.PHONY: dialyzer
dialyzer:
	rebar3 as check do dialyzer

.PHONY: fmt
fmt:
	rebar3 fmt
	cargo fmt --all -- --check
