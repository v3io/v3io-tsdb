.PHONY: test
test:
	go test -short ./...

.PHONY: lint
lint:
ifeq ($(shell gofmt -l .),)
	# lint OK
else
	$(error Please run `go fmt ./...` to format the code)
endif
