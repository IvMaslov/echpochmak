# echpochmak: Iceberg REST catalog on a raft key-value store.
# Run `make help` to list the targets.

SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

# Tools, pinned. Installed into ./bin so no global installs are needed.
GOLANGCI_LINT_VERSION := v2.15.0

BIN_DIR       := $(CURDIR)/bin
GOLANGCI_LINT := $(BIN_DIR)/golangci-lint

# Build metadata, injected into internal/version.
VERSION    ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT     ?= $(shell git rev-parse --verify --quiet HEAD 2>/dev/null || echo unknown)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS    := -s -w \
	-X echpochmak/internal/version.version=$(VERSION) \
	-X echpochmak/internal/version.commit=$(COMMIT) \
	-X echpochmak/internal/version.buildDate=$(BUILD_DATE)

BINARIES := catalog-server kv-node kvctl

# Iceberg REST spec, pinned by commit. Bump SPEC_COMMIT and run `make spec-update`.
SPEC_COMMIT := 79bc69a51da6d95ccd6da6d46ef156c74e30b8cf
SPEC_URL    := https://raw.githubusercontent.com/apache/iceberg/$(SPEC_COMMIT)/open-api/rest-catalog-open-api.yaml
SPEC_DIR    := api/iceberg

COMPOSE := docker compose -f deployments/compose/docker-compose.yaml --env-file deployments/compose/.env
IMAGE   := echpochmak/catalog-server
# Network for `docker build`; see BUILD_NETWORK in deployments/compose/.env.example.
BUILD_NETWORK ?= default

# Swagger UI, vendored into internal/api/swaggerui/dist and embedded. Bump both
# values (the integrity is the npm registry's dist.integrity) and run
# `make swagger-ui-update`.
SWAGGER_UI_VERSION   ?= 5.34.1
SWAGGER_UI_INTEGRITY ?= sha512-VSr8fXfI93obYaQicLJbLUKGRxDojj/SfRJDDLCioGTUBj4BK/Nj4eE9jCeCAnJxVLi7iS+08PbiZLyrQKBkJQ==
SWAGGER_UI_DIR       := internal/api/swaggerui/dist
SWAGGER_UI_FILES     := swagger-ui-bundle.js swagger-ui-bundle.js.LICENSE.txt swagger-ui.css favicon-32x32.png LICENSE NOTICE

COVER_PROFILE := coverage.out

##@ General

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "Usage: make <target>\n"} \
		/^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2 } \
		/^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)

.PHONY: tools
tools: $(GOLANGCI_LINT) ## Install pinned tools into ./bin

$(GOLANGCI_LINT):
	GOBIN=$(BIN_DIR) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

##@ Code generation

.PHONY: spec-update
spec-update: ## Download the Iceberg REST spec at SPEC_COMMIT
	curl -fsSL "$(SPEC_URL)" -o $(SPEC_DIR)/rest-catalog-open-api.yaml
	printf 'source: %s\ncommit: %s\ndownloaded: %s\n' "$(SPEC_URL)" "$(SPEC_COMMIT)" "$$(date -u +%Y-%m-%d)" \
		> $(SPEC_DIR)/SPEC_VERSION
	@echo "Spec updated; run 'make generate' and review the diff."

.PHONY: swagger-ui-update
swagger-ui-update: ## Download swagger-ui-dist at SWAGGER_UI_VERSION, check its integrity and vendor it
	tmp=$$(mktemp -d); trap 'rm -rf "$$tmp"' EXIT; \
		curl -fsSLo "$$tmp/pkg.tgz" https://registry.npmjs.org/swagger-ui-dist/-/swagger-ui-dist-$(SWAGGER_UI_VERSION).tgz; \
		got="sha512-$$(openssl dgst -sha512 -binary "$$tmp/pkg.tgz" | base64 -w0)"; \
		[ "$$got" = "$(SWAGGER_UI_INTEGRITY)" ] || { echo "integrity mismatch: $$got" >&2; exit 1; }; \
		tar -xzf "$$tmp/pkg.tgz" -C "$$tmp"; \
		rm -rf $(SWAGGER_UI_DIR); mkdir -p $(SWAGGER_UI_DIR); \
		for f in $(SWAGGER_UI_FILES); do cp "$$tmp/package/$$f" $(SWAGGER_UI_DIR)/; done; \
		printf 'swagger-ui-dist %s\nsource: https://registry.npmjs.org/swagger-ui-dist/-/swagger-ui-dist-%s.tgz\nintegrity: %s\nfiles: %s\nupdate with: make swagger-ui-update SWAGGER_UI_VERSION=<version> SWAGGER_UI_INTEGRITY=<sha512-...>\n' \
			$(SWAGGER_UI_VERSION) $(SWAGGER_UI_VERSION) $(SWAGGER_UI_INTEGRITY) "$(SWAGGER_UI_FILES)" > $(SWAGGER_UI_DIR)/VERSION

.PHONY: generate
generate: ## Regenerate code (go generate ./...)
	go generate ./...

.PHONY: check-generate
check-generate: generate ## Fail if generated code is out of date
	@test -z "$$(git status --porcelain -- internal/api/gen internal/kv/kvpb)" || \
		(echo "Generated code is out of date: run 'make generate' and commit the result." && exit 1)

.PHONY: buf-breaking
buf-breaking: ## Fail if the KV protocol changed incompatibly since the last commit
	@if git cat-file -e HEAD:api/kv/buf.yaml 2>/dev/null; then \
		cd api/kv && go tool buf breaking --against '../../.git#subdir=api/kv'; \
	else \
		echo "buf-breaking: api/kv is not committed yet, nothing to compare against"; \
	fi

##@ Build and run

.PHONY: build
build: ## Build all binaries into ./bin
	@mkdir -p $(BIN_DIR)
	@for b in $(BINARIES); do \
		echo "go build $$b"; \
		CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/$$b ./cmd/$$b; \
	done

.PHONY: run
run: build ## Run catalog-server locally against MinIO from compose
	$(BIN_DIR)/catalog-server \
		-log-format text -log-level debug \
		-s3-endpoint http://localhost:9000 -s3-path-style \
		-s3-access-key-id minioadmin -s3-secret-access-key minioadmin

.PHONY: docker-build
docker-build: ## Build the catalog-server image
	docker build -f build/catalog-server/Dockerfile --network $(BUILD_NETWORK) \
		--build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) --build-arg BUILD_DATE=$(BUILD_DATE) \
		-t $(IMAGE):$(VERSION) -t $(IMAGE):dev .

##@ Quality

.PHONY: test
test: ## Run all tests with the race detector
	go test -race -count=1 ./...

.PHONY: test-short
test-short: ## Run fast unit tests only
	go test -short -count=1 ./...

.PHONY: e2e
e2e: deployments/compose/.env ## Start the compose stack and run the Trino end-to-end tests
	$(COMPOSE) up -d --build --wait
	go test -tags e2e -count=1 -v ./test/e2e/

.PHONY: kv-torture
kv-torture: ## Run the in-process linearizability check for longer, with a random seed
	KV_TORTURE_SEED=$${KV_TORTURE_SEED:-$$RANDOM} KV_TORTURE_DURATION=$${KV_TORTURE_DURATION:-60s} \
		go test -race -count=1 -run TestLinearizableUnderFaults -v -timeout 30m ./internal/kv/node/

.PHONY: kv-chaos
kv-chaos: deployments/compose/.env ## Check a 3-node KV cluster in docker for linearizability under chaos, then remove it
	$(COMPOSE) --profile kv up -d --build kv1 kv2 kv3
	status=0; go test -tags kvchaos -count=1 -v -timeout 30m ./test/kvchaos/ || status=$$?; \
		$(COMPOSE) --profile kv rm -fsv kv1 kv2 kv3; \
		docker volume rm -f echpochmak_kv1-data echpochmak_kv2-data echpochmak_kv3-data >/dev/null; \
		exit $$status

HA_COMPOSE := docker compose -f deployments/compose/docker-compose.yaml \
	-f deployments/compose/docker-compose.ha.yaml --env-file deployments/compose/.env --profile kv

.PHONY: ha-up
ha-up: deployments/compose/.env ## Start the HA stack: 3 kv-nodes, 2 catalogs behind HAProxy, MinIO, Trino, PyIceberg
	$(HA_COMPOSE) up -d --build --wait

.PHONY: ha-down
ha-down: deployments/compose/.env ## Remove the HA stack, its containers and volumes
	$(HA_COMPOSE) down -v --remove-orphans

.PHONY: ha-e2e
ha-e2e: deployments/compose/.env ## Run the HA milestone suite and the Trino suite on the HA stack, then remove it
	$(HA_COMPOSE) up -d --build --wait
	status=0; \
		go test -tags ha -count=1 -v -timeout 30m ./test/ha/ || status=$$?; \
		[ $$status -ne 0 ] || go test -tags e2e -count=1 -v ./test/e2e/ || status=$$?; \
		$(HA_COMPOSE) down -v --remove-orphans; \
		exit $$status

# run-rck runs the REST Compatibility Kit on the stack that $(1) starts with
# the services $(2), copies the reports to build/rck-reports/$(3) and removes
# the stack, whatever the result.
define run-rck
	$(1) up -d --build --wait $(2)
	status=0; \
		$(1) --profile rck run --build --name echpochmak-rck rck || status=$$?; \
		rm -rf build/rck-reports/$(3); mkdir -p build/rck-reports/$(3); \
		docker cp echpochmak-rck:/rck/open-api/build/test-results/test build/rck-reports/$(3)/xml >/dev/null 2>&1 || true; \
		docker cp echpochmak-rck:/rck/open-api/build/reports/tests/test build/rck-reports/$(3)/html >/dev/null 2>&1 || true; \
		docker rm -f echpochmak-rck >/dev/null 2>&1 || true; \
		$(1) --profile rck down -v --remove-orphans; \
		echo "RCK reports: build/rck-reports/$(3)/html/index.html"; \
		exit $$status
endef

.PHONY: rck
rck: deployments/compose/.env ## Optional: run Iceberg's REST Compatibility Kit against one in-memory catalog, then remove the stack
	$(call run-rck,$(COMPOSE),catalog,rck)

.PHONY: rck-ha
rck-ha: deployments/compose/.env ## Optional: run Iceberg's REST Compatibility Kit against the HA stack, then remove it
	$(call run-rck,$(HA_COMPOSE),lb,rck-ha)

.PHONY: cover
cover: ## Run tests with coverage and write an HTML report
	go test -race -count=1 -covermode=atomic -coverprofile=$(COVER_PROFILE) ./...
	go tool cover -func=$(COVER_PROFILE) | tail -1
	go tool cover -html=$(COVER_PROFILE) -o coverage.html

.PHONY: lint
lint: $(GOLANGCI_LINT) ## Run golangci-lint
	$(GOLANGCI_LINT) run ./...

.PHONY: fmt
fmt: $(GOLANGCI_LINT) ## Format code with golangci-lint formatters
	$(GOLANGCI_LINT) fmt ./...

.PHONY: tidy
tidy: ## Tidy and verify go.mod
	go mod tidy
	go mod verify
	@git diff --exit-code -- go.mod go.sum || \
		(echo "go.mod/go.sum changed: commit the result of 'go mod tidy'." && exit 1)

.PHONY: ci
ci: tidy check-generate buf-breaking lint test build ## Run every check CI runs

##@ Local environment

deployments/compose/.env:
	cp deployments/compose/.env.example $@

.PHONY: compose-up
compose-up: deployments/compose/.env ## Start MinIO, catalog and Trino
	$(COMPOSE) up -d --build --wait

.PHONY: compose-down
compose-down: deployments/compose/.env ## Stop the environment (keeps data)
	$(COMPOSE) down

.PHONY: compose-clean
compose-clean: deployments/compose/.env ## Stop the environment and delete its data
	$(COMPOSE) down -v

.PHONY: compose-logs
compose-logs: deployments/compose/.env ## Follow logs of all services
	$(COMPOSE) logs -f

.PHONY: trino-cli
trino-cli: deployments/compose/.env ## Open the Trino CLI
	$(COMPOSE) exec trino trino

##@ Cleanup

.PHONY: clean
clean: ## Remove build and coverage output
	rm -rf $(BIN_DIR) $(COVER_PROFILE) coverage.html
