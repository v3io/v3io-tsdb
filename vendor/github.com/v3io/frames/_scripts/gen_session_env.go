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

// Generate session information environment variable
// $ export V3IO_SESSION=$(go run _scripts/gen_session_env.go \
//		-user iguazio \
//		-password t0ps3cr3t \
//		-address backend353.iguazio.com:8081 \
//		-container bigdata)

package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"path"

	"github.com/v3io/frames/pb"
)

func main() {
	var session pb.Session

	flag.StringVar(&session.Url, "url", "", "web API url")
	flag.StringVar(&session.Container, "container", "", "container name")
	flag.StringVar(&session.Password, "password", "", "password")
	flag.StringVar(&session.Path, "path", "", "path in container")
	flag.StringVar(&session.Token, "token", "", "authentication token")
	flag.StringVar(&session.User, "user", "", "login user")
	flag.Parse()

	log.SetFlags(0) // Remove time ... prefix
	if flag.NArg() != 0 {
		log.Fatalf("error: %s takes no arguments", path.Base(os.Args[0]))
	}

	json.NewEncoder(os.Stdout).Encode(session)
}
