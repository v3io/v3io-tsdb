/*
Copyright 2017 The Nuclio Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package nutest

import (
	"testing"
	"github.com/nuclio/nuclio-sdk-go"
)

func MyHandler(context *nuclio.Context, event nuclio.Event) (interface{}, error) {
	context.Logger.Info("path: %s", string(event.GetPath()))
	context.Logger.Info("body: %s", string(event.GetBody()))
	context.Logger.Info("headers: %+v", event.GetHeaders())
	context.Logger.Info("str header: %s", event.GetHeaderString("first"))
	return "test me\n" + string(event.GetBody()), nil
}


func TestNutest(t *testing.T) {
	data := DataBind{Name:"db0", Url:"<v3io address>", Container:"x"}
	tc, err := NewTestContext(MyHandler, true, &data )
	if err != nil {
		t.Fail()
	}

	testEvent := TestEvent{
		Path: "/some/path",
		Body: []byte("1234"),
		Headers:map[string]interface{}{"first": "string", "sec": "1"},
		}
	resp, err := tc.Invoke(&testEvent)
	tc.Logger.InfoWith("Run complete", "resp", resp, "err", err)
}

