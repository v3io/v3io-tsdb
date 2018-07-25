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

TSDBCTL_BIN_NAME := tsdbctl-$(GIT_REVISION)-$(GOOS)-$(GOARCH)

.PHONY: get
get:
	go get -v -t $(TOPLEVEL_DIRS)

.PHONY: test
test: get
	go test -v -short $(TOPLEVEL_DIRS)

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
