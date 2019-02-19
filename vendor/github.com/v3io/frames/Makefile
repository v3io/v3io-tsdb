# Copyright 2018 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# -mod=vendor is avaiable from Go 1.11 and up
modflag=$(shell go run _scripts/modflag.go)

all:
	@echo Please pick a target
	@egrep '^[^ :]+:' Makefile | \
	   grep -v all | \
	   sed -e 's/://' -e 's/^/    /' | \
	   sort
	@false

test:
	GO111MODULE=on go test -v $(testflags) $(modflag) ./...

build:
	GO111MODULE=on go build -v $(modflag) ./...

test-python:
	cd clients/py && $(MAKE) test

build-docker:
	docker build -f ./cmd/framesd/Dockerfile -t quay.io/v3io/frames .

wheel:
	cd clients/py && python setup.py bdist_wheel

update-tsdb-dep:
	GO111MODULE=on go get github.com/v3io/v3io-tsdb@frames-integration
	GO111MODULE=on go mod vendor
	@echo "Done. Don't forget to commit ☺"

grpc: grpc-go grpc-py

grpc-go:
	protoc  frames.proto --go_out=plugins=grpc:pb

grpc-py:
	cd clients/py && \
	pipenv run python -m grpc_tools.protoc \
		-I../.. --python_out=v3io_frames\
		--grpc_python_out=v3io_frames \
		../../frames.proto
	python _scripts/fix_pb_import.py \
	    clients/py/v3io_frames/frames_pb2_grpc.py

pypi:
	cd clients/py && \
	    pipenv run make upload

cloc:
	cloc \
	    --exclude-dir=vendor,_t,.ipynb_checkpoints,_examples,_build \
	    .

update-go-deps:
	go mod tidy
	go mod vendor
	git add vendor go.mod go.sum
	@echo "Don't forget to test & commit"

update-py-deps:
	cd clients/py && $(MAKE) update-deps
	git add clients/py/Pipfile*
	@echo "Don't forget to test & commit"

bench-go:
	./_scripts/go_benchmark.py

bench-py:
	./_scripts/py_benchmark.py

bench:
	@echo Go
	$(MAKE) bench-go
	@echo Python
	$(MAKE) bench-py

python-deps:
	cd clients/py && $(MAKE) sync-deps
