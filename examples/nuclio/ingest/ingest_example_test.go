// +build integration

package ingest

import (
	"fmt"
	"github.com/nuclio/nuclio-test-go"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
	"os"
	"testing"
)

func TestIngestIntegration(t *testing.T) {
	v3ioConfig, err := tsdbtest.LoadV3ioConfig()
	defer tsdbtest.SetUp(t, v3ioConfig)()
	tsdbConfig = fmt.Sprintf(`path: "%v"`, v3ioConfig.Path)

	url := os.Getenv("V3IO_SERVICE_URL")
	if url == "" {
		url = v3ioConfig.V3ioUrl
	}

	data := nutest.DataBind{
		Name: "db0", Url: url, Container: v3ioConfig.Container, User: v3ioConfig.Username, Password: v3ioConfig.Password}
	tc, err := nutest.NewTestContext(Handler, true, &data)
	if err != nil {
		t.Fatal(err)
	}

	err = tc.InitContext(InitContext)
	if err != nil {
		t.Fatal(err)
	}
	testEvent := nutest.TestEvent{
		Body: []byte(pushEvent),
	}
	resp, err := tc.Invoke(&testEvent)
	tc.Logger.InfoWith("Run complete", "resp", resp, "err", err)
	fmt.Println(resp)
}
