package main

import (
	"fmt"
	"log"
	"regexp"
	"runtime"
	"strconv"
)

var (
	verRe = regexp.MustCompile("go[0-9]+\\.([0-9]+)")
)

func main() {
	version := runtime.Version()
	matches := verRe.FindStringSubmatch(version)
	if len(matches) != 2 {
		log.Fatalf("error: can't parse version: %s", version)
	}

	minVer := matches[1]
	n, err := strconv.Atoi(minVer)
	if err != nil {
		log.Fatalf("error: bad version - %s", minVer)
	}

	if n >= 11 {
		fmt.Println("-mod=vendor")
	}
}
