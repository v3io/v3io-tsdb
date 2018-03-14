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

package v3io_tsdb

import (
	"fmt"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/v3io/v3io-tsdb/config"
	"testing"
	"time"
)

const basetime = 0 //1520346654002

func TestName(t *testing.T) {

	ts1 := []int64{2000, 3050, 4100, 4950, 7000, 8200}
	arr1 := []float64{1, 2, 3, 4, 5, 6}
	cfg, err := config.LoadConfig("v3io.yaml")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(cfg)

	adapter := NewV3ioAdapter(cfg, nil, nil)
	adapter.Start()

	appender, err := adapter.Appender()
	if err != nil {
		t.Fatal(err)
	}

	lset := labels.Labels{labels.Label{Name: "__name__", Value: "http_req"},
		labels.Label{Name: "method", Value: "post"}}

	lset2 := labels.Labels{labels.Label{Name: "__name__", Value: "http_req"},
		labels.Label{Name: "method", Value: "get"}}

	ref, err := appender.Add(lset, basetime+1000, 2)
	if err != nil {
		t.Fatal(err)
	}

	//time.Sleep(time.Second * 1)
	ref2, err := appender.Add(lset2, basetime+1600, 9.3)
	if err != nil {
		t.Fatal(err)
	}

	//time.Sleep(time.Second * 2)
	for i := 0; i < len(arr1); i++ {
		err = appender.AddFast(lset, ref, basetime+ts1[i], arr1[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	err = appender.AddFast(lset2, ref2, basetime+2300, 7.7)
	if err != nil {
		t.Fatal(err)
	}

	err = appender.AddFast(lset, ref, basetime+2500, 8.3)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second * 1)

	qry, err := adapter.Querier(nil, basetime+400, basetime+9000)
	if err != nil {
		t.Fatal(err)
	}

	match := labels.Matcher{Type: labels.MatchEqual, Name: "__name__", Value: "http_req"}
	set, err := qry.Select(&match)
	if err != nil {
		t.Fatal(err)
	}

	for set.Next() {
		if set.Err() != nil {
			t.Fatal(set.Err())
		}

		series := set.At()
		fmt.Println("\nLables:", series.Labels())
		iter := series.Iterator()
		for iter.Next() {

			if iter.Err() != nil {
				t.Fatal(iter.Err())
			}

			t, v := iter.At()
			fmt.Printf("t=%d,v=%f ", t, v)
		}
		fmt.Println()
	}

}
