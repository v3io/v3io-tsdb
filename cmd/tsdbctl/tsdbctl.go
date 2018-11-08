package main

import (
	"github.com/v3io/v3io-tsdb/pkg/tsdbctl"
	"os"
)

// Note, following variables set by make
var opsys string
var arch string
var version string
var revision string
var branch string

func main() {
	if err := Run(); err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}

func Run() error {
	rootCmd := tsdbctl.NewRootCommandeer(opsys, arch, version, revision, branch)
	defer tearDown(rootCmd)
	return rootCmd.Execute()
}

func tearDown(cmd *tsdbctl.RootCommandeer) {
	if cmd.Reporter != nil { // could be nil if has failed on initialisation
		cmd.Reporter.Stop()
	}
}
