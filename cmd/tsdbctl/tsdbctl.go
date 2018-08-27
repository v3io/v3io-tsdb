package main

import (
	"github.com/v3io/v3io-tsdb/pkg/tsdbctl"
	"os"
	"time"
)

func main() {
	if err := Run(); err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}

func Run() error {
	rootCmd := tsdbctl.NewRootCommandeer()
	defer tearDown(rootCmd)
	return rootCmd.Execute()
}

func tearDown(cmd *tsdbctl.RootCommandeer) {
	time.Sleep(100 * time.Millisecond) // postpone performance report to avoid mixing with other log messages
	cmd.Reporter.Stop()
}
