package query

import (
	"fmt"
	"github.com/nuclio/nuclio-test-go"
	"os"
	"testing"
)

func TestQueryIntegration(t *testing.T) {

	if testing.Short() {
		t.Skip("Skipping integration test.")
	}

	data := nutest.DataBind{
		Name: "db0", Url: os.Getenv("V3IO_SERVICE_URL"), Container: "1", User: "<TDB>", Password: "<TBD>"}
	tc, err := nutest.NewTestContext(Handler, false, &data)
	if err != nil {
		t.Fatal(err)
	}

	err = tc.InitContext(InitContext)
	if err != nil {
		t.Fatal(err)
	}
	testEvent := nutest.TestEvent{
		Body: []byte(queryEvent),
	}
	resp, err := tc.Invoke(&testEvent)
	tc.Logger.InfoWith("Run complete", "resp", resp, "err", err)
	fmt.Println(resp)

}
