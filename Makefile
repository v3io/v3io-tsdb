GIT_COMMIT_HASH := $(shell git rev-parse HEAD)
GIT_BRANCH=$(shell git rev-parse --abbrev-ref HEAD)
ifeq ($(GIT_BRANCH),)
	GIT_BRANCH="N/A"
endif

ifneq ($(TSDB_LABEL),)
	GIT_REVISION := $(TSDB_LABEL)
else
	GIT_REVISION := $(shell git describe --always)
endif

GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)
GOPATH ?= $(shell go env GOPATH)

TSDBCTL_BIN_NAME := tsdbctl-$(GIT_REVISION)-$(GOOS)-$(GOARCH)

# Use RFC3339 (ISO8601) date format
BUILD_TIME := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

# Use fully qualified package name
CONFIG_PKG=github.com/v3io/v3io-tsdb/pkg/config

# Use Go linker to set the build metadata
BUILD_OPTS := -ldflags " \
  -X $(CONFIG_PKG).buildTime=$(BUILD_TIME) \
  -X $(CONFIG_PKG).osys=$(GOOS) \
  -X $(CONFIG_PKG).architecture=$(GOARCH) \
  -X $(CONFIG_PKG).version=$(GIT_REVISION) \
  -X $(CONFIG_PKG).commitHash=$(GIT_COMMIT_HASH) \
  -X $(CONFIG_PKG).branch=$(GIT_BRANCH)" \
 -v -o "$(GOPATH)/bin/$(TSDBCTL_BIN_NAME)"

TSDB_BUILD_COMMAND ?= GO111MODULE="on" CGO_ENABLED=0 go build $(BUILD_OPTS) ./cmd/tsdbctl

.PHONY: fmt
fmt:
	gofmt -l -s -w .

.PHONY: get
get:
	GO111MODULE="on" go mod tidy

.PHONY: test
test:
	go test -v -race -tags unit -count 1 ./...

.PHONY: integration
integration:
	go test -v -race -tags integration -p 1 -count 1 ./... # p=1 to force Go to run pkg tests serially.

.PHONY: bench
bench:
	go test -run=XXX -bench='^BenchmarkIngest$$' -benchtime 10s -timeout 5m ./test/benchmark/...

.PHONY: build
build:
	docker run \
	  --volume $(shell pwd):/go/src/github.com/v3io/v3io-tsdb \
	  --volume $(shell pwd):/go/bin \
	  --workdir /go/src/github.com/v3io/v3io-tsdb \
	  --env GOOS=$(GOOS) \
	  --env GOARCH=$(GOARCH) \
	  golang:1.12 \
	  make bin

.PHONY: bin
bin:
	${TSDB_BUILD_COMMAND}

PHONY: gofmt
gofmt:
ifeq ($(shell gofmt -l .),)
	# gofmt OK
else
	$(error Please run `go fmt ./...` to format the code)
endif

.PHONY: impi
impi:
	@echo Installing impi...
	GO111MODULE=off go get -u github.com/pavius/impi/cmd/impi
	@echo Verifying imports...
	$(GOPATH)/bin/impi \
		--local github.com/iguazio/provazio \
		--skip pkg/controller/apis \
		--skip pkg/controller/client \
		--ignore-generated \
		--scheme stdLocalThirdParty \
		./...

$(GOPATH)/bin/golangci-lint:
	@echo Installing golangci-lint...
	curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | sh -s v1.10.2
	cp ./bin/golangci-lint $(GOPATH)/bin/

.PHONY: lint
lint: gofmt impi $(GOPATH)/bin/golangci-lint
	@echo Linting...
	@$(GOPATH)/bin/golangci-lint run \
     --disable-all --enable=deadcode --enable=goconst --enable=golint --enable=ineffassign \
     --enable=interfacer --enable=unconvert --enable=varcheck --enable=errcheck --enable=gofmt --enable=misspell \
     --enable=staticcheck --enable=gosimple --enable=govet --enable=goconst \
    cmd/... pkg/... internal/...
	@echo done linting
