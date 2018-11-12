# All top-level dirs except for vendor/.
TOPLEVEL_DIRS=`ls -d ./*/. | grep -v '^./vendor/.$$' | sed 's/\.$$/.../'`
TOPLEVEL_DIRS_GOFMT_SYNTAX=`ls -d ./*/. | grep -v '^./vendor/.$$'`

GIT_SHA_SHORT := $(shell git rev-parse HEAD)
GIT_BRANCH=$(shell git rev-parse --abbrev-ref HEAD)
ifeq ($(GIT_BRANCH),)
	GIT_BRANCH="N/A"
endif

ifneq ($(TRAVIS_TAG),)
	GIT_REVISION := $(TRAVIS_TAG)
else
	GIT_REVISION := $(shell git describe --always)
endif

GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)
GOPATH ?= $(shell go env GOPATH)

TSDBCTL_BIN_NAME := tsdbctl-$(GIT_REVISION)-$(GOOS)-$(GOARCH)

BUILD_TIME := $(shell date +%FT%T%z)
CONFIG_PKG=github.com/v3io/v3io-tsdb/pkg/config

BUILD_OPTS := -ldflags " \
  -X $(CONFIG_PKG).osys=$(GOOS) \
  -X $(CONFIG_PKG).architecture=$(GOARCH) \
  -X $(CONFIG_PKG).version=$(GIT_REVISION) \
  -X $(CONFIG_PKG).revision=$(GIT_SHA_SHORT) \
  -X $(CONFIG_PKG).branch=$(GIT_BRANCH) \
  -X $(CONFIG_PKG).buildTime=$(BUILD_TIME)" \
 -v -o "$(GOPATH)/bin/$(TSDBCTL_BIN_NAME)"

.PHONY: get
get:
	go get -v -t -tags "unit integration" $(TOPLEVEL_DIRS)

.PHONY: test
test: get
	go test -race -tags unit -count 1 $(TOPLEVEL_DIRS)

.PHONY: integration
integration: get
	go test -race -tags integration -p 1 -count 1 $(TOPLEVEL_DIRS) # p=1 to force Go to run pkg tests serially.

.PHONY: bench
bench: get
	go test -run=XXX -bench='^BenchmarkIngest$$' -benchtime 10s -timeout 5m ./test/benchmark/...

.PHONY: build
build: get
	CGO_ENABLED=0 go build $(BUILD_OPTS) ./cmd/tsdbctl

.PHONY: lint
lint:
ifeq ($(shell gofmt -l $(TOPLEVEL_DIRS_GOFMT_SYNTAX)),)
	# lint OK
else
	$(error Please run `go fmt ./...` to format the code)
endif
