# frames

[![Build Status](https://travis-ci.org/v3io/frames.svg?branch=master)](https://travis-ci.org/v3io/frames)
[![GoDoc](https://godoc.org/github.com/v3io/frames?status.svg)](https://godoc.org/github.com/v3io/frames)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Server and client library of streaming data from v3io

## Components

- Go server, both gRPC and HTTP protocols are supported
- Go client
- Python client

### Development

Core is written in [Go](https://golang.org/), we work on `development` branch
and release to `master.

- To run the Go tests run `make test`.
- To run the Python tests run `make test-python`

#### Adding/Changing Dependencies

- If you add Go dependencies run `make update-go-deps`
- If you add Python dependencies, updates `clients/py/Pipfile` and run `make
  update-py-deps`

#### travis CI

We run integration tests on travis. See `.travis.yml` for details.

In [travis settings](https://travis-ci.org/v3io/frames/settings) we have the following environment variables defined:

- Docker
    - `DOCKER_PASSWORD` Password to push images to quay.io
    - `DOCKER_USERNAME` Username to push images to quay.io
- PyPI
    - `V3IO_PYPI_PASSWORD` Password to push new release to pypi
    - `V3IO_PYPI_USER` User to push
- Iguazio
    - `V3IO_SESSION` is a JSON encoded map with session information to run tests.
       Make sure to quote the JSON object with `'`. Here's an example value: `'{"url":"45.39.128.5:8081","container":"mitzi","user":"daffy","password":"rabbit season"}'`


### Docker Image

#### Build

    make build-docker

#### Running

     docker run \
	-v /path/to/config.yaml:/etc/framesd.yaml \
	quay.io/v3io/frames:unstable

## LICENSE

[Apache 2](LICENSE)
