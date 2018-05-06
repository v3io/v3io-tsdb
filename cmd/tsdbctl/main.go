package main

import (
	"github.com/v3io/v3io-tsdb/pkg/tsdbctl"
	"os"
)

func main() {
	if err := Run(); err != nil {

		os.Exit(1)
	}

	os.Exit(0)
}

func Run() error {
	return tsdbctl.NewRootCommandeer().Execute()
}
