# All top-level dirs except for vendor/.
TOPLEVEL_DIRS=`ls -d ./*/. | grep -v '^./vendor/.$$' | sed 's/\.$$/.../'`
TOPLEVEL_DIRS_GOFMT_SYNTAX=`ls -d ./*/. | grep -v '^./vendor/.$$'`

GIT_SHA_SHORT := $(shell git rev-parse HEAD)
GIT_BRANCH=$(shell git rev-parse --abbrev-ref HEAD)
ifeq ($(GIT_BRANCH),)
	GIT_BRANCH="NA"
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

BUILD_OPTS := -ldflags "-X main.opsys=$(GOOS) -X main.arch=$(GOARCH) -X main.version=$(GIT_REVISION) -X main.revision=$(GIT_SHA_SHORT) -X main.branch=$(GIT_BRANCH)" \
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
