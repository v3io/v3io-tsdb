.PHONY: test
test:
	go get -t -v ./...
	go test -short -v ./...

.PHONY: lint
lint:
ifeq ($(shell gofmt -l .),)
	# lint OK
else
	$(error Please run `go fmt ./...` to format the code)
endif
