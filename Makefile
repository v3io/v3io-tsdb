# All top-level dirs except for vendor/.
TOPLEVEL_DIRS=`ls -d ./*/. | grep -v '^./vendor/.$$' | sed 's/\.$$/.../'`
TOPLEVEL_DIRS_GOFMT_SYNTAX=`ls -d ./*/. | grep -v '^./vendor/.$$'`

ifneq ($(TRAVIS_TAG),)
	GIT_REVISION := $(TRAVIS_TAG)
else
	GIT_REVISION := $(shell git describe --always)
endif

GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)
CGO_ENABLED ?= $(shell go env CGO_ENABLED)
GOHOSTOS ?= $(shell go env GOHOSTOS)

TSDBCTL_BIN_NAME := tsdbctl-$(GIT_REVISION)-$(GOOS)-$(GOARCH)

ifeq ($(GOHOSTOS), $(GOOS))
    ifeq ($(CGO_ENABLED), 0)
        TSDBCTL_BIN_NAME := $(TSDBCTL_BIN_NAME)-no-cgo
    endif
else
    ifeq ($(CGO_ENABLED), 1)
        $(error CGO_ENABLED=$(CGO_ENABLED) is unsupported. Reason: The host OS [$(GOHOSTOS)] doesn't equal to target OS [$(GOOS)])
    endif
endif

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
	go build -v -o "$(GOPATH)/bin/$(TSDBCTL_BIN_NAME)" ./cmd/tsdbctl

.PHONY: lint
lint:
ifeq ($(shell gofmt -l $(TOPLEVEL_DIRS_GOFMT_SYNTAX)),)
	# lint OK
else
	$(error Please run `go fmt ./...` to format the code)
endif
