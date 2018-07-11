.PHONY: test
test:
	go get -v -t ./...
	go test -short ./...

.PHONY: lint
lint:
ifeq ($(shell gofmt -l .),)
	# lint OK
else
	$(error Please run `go fmt ./...` to format the code)
endif
