package main

import (
	"fmt"
	"github.com/nuclio/nuclio-test-go"
	"os"
	"testing"
	"time"
)

func TestName(t *testing.T) {
	data := nutest.DataBind{Name: "db0", Url: os.Getenv("V3IO_URL"), Container: "nuclio"}
	tc, err := nutest.NewTestContext(Handler, true, &data)
	if err != nil {
		t.Fatal(err)
	}

	err = tc.InitContext(InitContext)
	if err != nil {
		t.Fatal(err)
	}
	testEvent := nutest.TestEvent{
		Path:    "/some/path",
		Body:    []byte(pushEvent),
		Headers: map[string]interface{}{"first": "string", "sec": "1"},
	}
	resp, err := tc.Invoke(&testEvent)
	tc.Logger.InfoWith("Run complete", "resp", resp, "err", err)
	fmt.Println(resp)

	time.Sleep(time.Second * 10)
}
