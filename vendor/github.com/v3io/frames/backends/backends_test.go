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

package backends

import (
	"fmt"
	"strings"
	"testing"

	"github.com/nuclio/logger"
	"github.com/v3io/frames"
)

// Special error return from testFactory so we can see it's this function
var errorBackendsTest = fmt.Errorf("backends test")

func testFactory(logger.Logger, *frames.BackendConfig, *frames.Config) (frames.DataBackend, error) {
	return nil, errorBackendsTest
}

func TestBackends(t *testing.T) {
	typ := "testBackend"
	err := Register(typ, testFactory)
	if err != nil {
		t.Fatalf("can't register - %s", err)
	}

	err = Register(typ, testFactory)
	if err == nil {
		t.Fatalf("managed to register twice")
	}

	capsType := strings.ToUpper(typ)
	factory := GetFactory(capsType)
	if factory == nil {
		t.Fatalf("can't get %q - %s", capsType, err)
	}

	_, err = factory(nil, nil, nil)
	if err != errorBackendsTest {
		t.Fatalf("wrong factory")
	}
}
