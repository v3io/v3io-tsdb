/*
Copyright 2018 Iguazio Systems Ltd.

Licensed under the Apache License, Version 2.0 (the "License") with
an addition restriction as set forth herein. You may not use this
file except in compliance with the License. You may obtain a copy of
the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

In addition, you may not use the software for any purposes that are
illegal under applicable law, and the grant of the foregoing license
under the Apache 2.0 license is conditioned upon your compliance with
such restriction.
*/

package http

import (
	"fmt"
	"time"

	"github.com/ghodss/yaml"
	"github.com/v3io/frames"
)

var configData = []byte(`
log:
  level: "info"

backends:
  - type: "kv"
  - type: "csv"
    rootDir = "/tmp"
`)

func ExampleServer() {
	cfg := &frames.Config{}
	if err := yaml.Unmarshal(configData, cfg); err != nil {
		fmt.Printf("error: can't read config - %s", err)
		return
	}

	srv, err := NewServer(cfg, ":8080", nil)
	if err != nil {
		fmt.Printf("error: can't create server - %s", err)
		return
	}

	if err = srv.Start(); err != nil {
		fmt.Printf("error: can't start server - %s", err)
		return
	}

	fmt.Println("server running")
	for {
		time.Sleep(60 * time.Second)
	}
}
