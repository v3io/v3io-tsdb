package main

import (
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdbctl"
	"os"
)

// Note, following variables set by make
var opSys, arch, version, revision, branch string
var buildInfo = &tsdb.BuildInfo{
	Os:           opSys,
	Architecture: arch,
	Version:      version,
	Revision:     revision,
	Branch:       branch,
}

func main() {
	if err := Run(); err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}

func Run() error {
	rootCmd := tsdbctl.NewRootCommandeer(buildInfo)
	defer tearDown(rootCmd)
	return rootCmd.Execute()
}

func tearDown(cmd *tsdbctl.RootCommandeer) {
	if cmd.Reporter != nil { // could be nil if has failed on initialisation
		cmd.Reporter.Stop()
	}
}
