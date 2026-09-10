# ============================================================================
# bedrock — build, test, bench, lint, fuzz, docker
# ============================================================================
SHELL      := /bin/bash
GO         ?= go
MODULE     := bedrock
BENCH_PKGS := ./benchmarks/
BENCHTIME  ?= 1s
BENCH_COUNT?= 1
FUZZTIME   ?= 30s
COVER_MIN  := 90

export CGO_ENABLED ?= 0

.PHONY: help build test test-race cover cover-check bench bench-short fuzz lint vet fmt \
        tidy example docker docker-build clean

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

build: ## Build all packages and the example binary
	$(GO) build -o bin/example ./cmd/example

test: ## Run unit + integration tests
	$(GO) test -count=1 ./...

test-race: ## Run all tests with the race detector
	$(GO) test -race -count=1 ./...

cover: ## Generate coverage profile (cover.out)
	$(GO) test -count=1 -coverprofile=cover.out .
	$(GO) tool cover -func=cover.out | tail -1

cover-check: ## Fail if coverage < 90%
	@$(GO) test -count=1 -coverprofile=cover.out . > /dev/null
	@total=$$($(GO) tool cover -func=cover.out | tail -1 | awk '{print substr($$NF, 1, length($$NF)-1)}'); \
	echo "coverage: $$total%"; \
	awk -v t="$$total" -v m="$(COVER_MIN)" 'BEGIN {exit (t+0 < m) ? 1 : 0}' \
		&& echo "OK: >= $(COVER_MIN)%" || (echo "FAIL: coverage below $(COVER_MIN)%"; exit 1)

bench: ## Run the full benchmark suite
	$(GO) test -run '^$$' -bench . -benchtime $(BENCHTIME) -benchmem -count $(BENCH_COUNT) $(BENCH_PKGS) | tee bench_results.txt

bench-short: ## Quick benchmark pass (100ms per bench)
	$(GO) test -run '^$$' -bench . -benchtime 100ms -benchmem $(BENCH_PKGS)

bench-profile: cpu.out ## Generate CPU + memory profiles for pprof
mem.out: ## Generate memory profile
	$(GO) test -run '^$$' -bench BenchmarkGetRandom -benchtime 2s -memprofile mem.out $(BENCH_PKGS)
cpu.out: ## Generate CPU profile
	$(GO) test -run '^$$' -bench BenchmarkGetRandom -benchtime 2s -cpuprofile cpu.out $(BENCH_PKGS)

fuzz: ## Fuzz store operations for $(FUZZTIME)
	$(GO) test -run '^$$' -fuzz FuzzStoreOperations -fuzztime $(FUZZTIME) ./tests/

lint: vet ## vet + staticcheck-style checks (golangci-lint when available)
	@command -v golangci-lint >/dev/null 2>&1 && golangci-lint run ./... || echo "golangci-lint not installed; vet only"

vet: ## go vet across the module
	$(GO) vet ./...

fmt: ## Format all Go code
	$(GO) fmt ./...

tidy: ## go mod tidy
	$(GO) mod tidy

example: ## Run the lifecycle example
	$(GO) run ./cmd/example ./data/example

docker-build: ## Build the container image
	docker build -t $(MODULE):latest .

clean: ## Remove build artifacts
	rm -rf bin cover.out cpu.out mem.out bench_results.txt data/
