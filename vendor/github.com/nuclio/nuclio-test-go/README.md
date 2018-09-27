# nuclio function wrapper

Test nuclio functions locally of as part of Go testing 

# Usage, Golang unit testing framework:

```golang
package main

import (
	"testing"
	"github.com/nuclio/nuclio-test-go"
)

func TestName(t *testing.T) {
	// data binding for V3IO data containers, optional (use nil instead of &data)
	data := nutest.DataBind{Name:"db0", Url:"<v3io address>", Container:"x"}

	// Create TestContext and specify the function name, verbose, data 
	tc, err := nutest.NewTestContext(MyHandler, true, &data )
	if err != nil {
		t.Fail()
	}

	// Optional, initialize context must have a function in the form:
	//    InitContext(context *nuclio.Context) error
	err = tc.InitContext(InitContext)
	if err != nil {
		t.Fail()
	}

	// Create a new test event 
	testEvent := nutest.TestEvent{
		Path: "/some/path",
		Body: []byte("1234"),
		Headers:map[string]interface{}{"first": "string"},
		}
	
	// invoke the tested function with the new event and log it's output 
	resp, err := tc.Invoke(&testEvent)
	tc.Logger.InfoWith("Run complete", "resp", resp, "err", err)
}
```

# Usage, called from another program:

```golang
package main

import (
	"github.com/nuclio/nuclio-test-go"
)

func main() {
	// data binding for V3IO data containers, optional (use nil instead of &data)
	data := nutest.DataBind{Name:"db0", Url:""<v3io address>", Container:"x"}

	// Create TestContext and specify the function name, verbose, data 
	tc, err := nutest.NewTestContext(MyHandler, true, &data )
	if err != nil {
		panic(err)
	}

	// Create a new test event 
	testEvent := nutest.TestEvent{
		Path: "/some/path",
		Body: []byte("1234"),
		Headers:map[string]interface{}{"first": "something"},
	}
	
	// invoke the tested function with the new event and log it's output 
	resp, err := tc.Invoke(&testEvent)
	tc.Logger.InfoWith("Run complete", "resp", resp, "err", err)
}
```
