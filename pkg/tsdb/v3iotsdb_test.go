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

package tsdb

import (
	"fmt"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/formatter"
	"github.com/v3io/v3io-tsdb/pkg/partmgr"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"math/rand"
	"os"
	"testing"
	"time"
)

//const basetime = 1524690488000
var basetime int64

func TestTsdbIntegration(t *testing.T) {

	if testing.Short() {
		t.Skip("Skipping integration test.")
	}

	basetime = time.Now().Unix()*1000 - 3600*1000*15 // now - 15hr
	time.Unix(basetime/1000, 0)

	fmt.Println("base time =", time.Unix(basetime/1000, 0).String())
	cfg, err := config.LoadConfig("../../v3io.yaml")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(cfg)

	adapter, err := NewV3ioAdapter(cfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	appender, err := adapter.Appender()
	if err != nil {
		t.Fatal(err)
	}

	lset := utils.FromStrings("__name__", "http_req", "method", "post")

	err = DoAppend(lset, appender, 350, 120)
	if err != nil {
		t.Fatal(err)
	}

	lset = utils.FromStrings("__name__", "http_req", "method", "get")

	err = DoAppend(lset, appender, 180, 240)
	if err != nil {
		t.Fatal(err)
	}

	appender.WaitForCompletion(0)
	fmt.Println("Append Done!")

	return

	qry, err := adapter.Querier(nil, basetime-3*3600*1000, basetime+14*3600*1000)
	if err != nil {
		t.Fatal(err)
	}

	set, err := qry.Select("http_req", "", 0, "")
	//set, err := qry.Select("count,avg,sum", 1000*3600, "_name=='http_req'")
	//set, err := qry.SelectOverlap("count,avg,sum,max", 1000*3600, []int{4, 2, 1}, "_name=='http_req'")
	if err != nil {
		t.Fatal(err)
	}

	f, err := formatter.NewFormatter("", nil)
	if err != nil {
		t.Fatal(err, "failed to start formatter")
	}

	err = f.Write(os.Stdout, set)
	if err != nil {
		t.Fatal(err)
	}

}

func DoAppend(lset utils.Labels, app Appender, num, interval int) error {
	// TODO: Implement this test
	//return nil

	//time.Sleep(time.Second * 1)
	curTime := int64(basetime)

	ref, err := app.Add(lset, curTime, 2)
	if err != nil {
		return err
	}

	for i := 0; i <= num; i++ {
		time.Sleep(time.Millisecond * 10)
		curTime += int64(interval * 1000)
		t := curTime + int64(rand.Intn(100)) - 50
		_, h := partmgr.TimeToDHM(t)
		v := rand.Float64()*10 + float64(h*100)
		fmt.Printf("t-%d,v%3.2f ", t, v)
		err = app.AddFast(lset, ref, t, v)
		if err != nil {
			return err
		}
	}

	return nil

}
