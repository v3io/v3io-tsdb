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

package http_test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	nhttp "net/http"
	"reflect"
	"testing"
	"time"

	"github.com/v3io/frames"
	"github.com/v3io/frames/http"
)

func TestEnd2End(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "frames-e2e")
	if err != nil {
		t.Fatal(err)
	}

	backendName := "e2e-backend"
	cfg := &frames.Config{
		Log: frames.LogConfig{
			Level: "debug",
		},
		Backends: []*frames.BackendConfig{
			&frames.BackendConfig{
				Name:    backendName,
				Type:    "csv",
				RootDir: tmpDir,
			},
		},
	}

	port, err := freePort()
	if err != nil {
		t.Fatal(err)
	}

	srv, err := http.NewServer(cfg, fmt.Sprintf(":%d", port), nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond) // Let server start

	url := fmt.Sprintf("http://localhost:%d", port)
	client, err := http.NewClient(url, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	frame, err := makeFrame()
	if err != nil {
		t.Fatalf("can't create frame - %s", err)
	}

	tableName := "e2e"
	writeReq := &frames.WriteRequest{
		Backend: backendName,
		Table:   tableName,
	}

	appender, err := client.Write(writeReq)
	if err != nil {
		t.Fatal(err)
	}

	if err := appender.Add(frame); err != nil {
		t.Fatal(err)
	}

	if err := appender.WaitForComplete(10 * time.Second); err != nil {
		t.Fatal(err)
	}

	readReq := &frames.ReadRequest{
		Backend:      backendName,
		Table:        tableName,
		MessageLimit: 100,
	}

	it, err := client.Read(readReq)
	if err != nil {
		t.Fatal(err)
	}

	nRows := 0

	for it.Next() {
		iFrame := it.At()
		if !reflect.DeepEqual(iFrame.Names(), frame.Names()) {
			t.Fatalf("columns mismatch: %v != %v", iFrame.Names(), frame.Names())
		}
		nRows += iFrame.Len()
	}

	if err := it.Err(); err != nil {
		t.Fatal(err)
	}

	if nRows != frame.Len() {
		t.Fatalf("# of rows mismatch - %d != %d", nRows, frame.Len())
	}

	testGrafana(t, url, backendName, tableName)

	// Exec
	execReq := &frames.ExecRequest{
		Backend: backendName,
		Table:   tableName,
		Command: "ping",
	}

	if _, err := client.Exec(execReq); err != nil {
		t.Fatalf("can't exec - %s", err)
	}
}

func testGrafana(t *testing.T, baseURL string, backend string, table string) {
	// ack
	ackUrl := fmt.Sprintf("%s/", baseURL)
	resp, err := nhttp.Get(ackUrl)
	if err != nil {
		t.Fatalf("can't call simplejson API - %s", err)
	}

	if resp.StatusCode != nhttp.StatusOK {
		t.Fatalf("bad status from simplejson API - %d %s", resp.StatusCode, resp.Status)
	}
	var responseContent map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&responseContent); err != nil {
		t.Fatalf("can't decode simplejson API ACK response - %s", err)
	} else {
		if responseContent["state"].(string) != "running" {
			t.Fatalf("wrong simplejson API response on status check: %s", responseContent["state"])
		}
	}
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, err
	}

	l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func makeFrame() (frames.Frame, error) {
	size := 1027
	now := time.Now()
	idata := make([]int64, size)
	fdata := make([]float64, size)
	sdata := make([]string, size)
	tdata := make([]time.Time, size)
	bdata := make([]bool, size)

	for i := 0; i < size; i++ {
		idata[i] = int64(i)
		fdata[i] = float64(i)
		sdata[i] = fmt.Sprintf("val%d", i)
		tdata[i] = now.Add(time.Duration(i) * time.Second)
		bdata[i] = i%2 == 0
	}

	columns := map[string]interface{}{
		"ints":    idata,
		"floats":  fdata,
		"strings": sdata,
		"times":   tdata,
		"bools":   bdata,
	}
	return frames.NewFrameFromMap(columns, nil)
}
