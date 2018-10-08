// +build integration

package promtsdb

import (
	"fmt"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/partmgr"
	"math/rand"
	"testing"
	"time"
)

const basetime = 15222481971234

func TestTsdbIntegration(t *testing.T) {
	t.Skip("Needs to be refactored - Doesnt test anything")

	d, h := partmgr.TimeToDHM(basetime)
	fmt.Println("base=", d, h)
	cfg, err := config.GetOrLoadFromFile("../v3io-tsdb-config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(cfg)

	adapter, err := NewV3ioProm(cfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	//adapter.partitionMngr.GetHead().NextPart(0)

	appender, err := adapter.Appender()
	if err != nil {
		t.Fatal(err)
	}

	lset := labels.Labels{labels.Label{Name: "__name__", Value: "http_req"},
		labels.Label{Name: "method", Value: "post"}}

	err = DoAppend(lset, appender, 50, 120)
	if err != nil {
		t.Fatal(err)
	}

	//time.Sleep(time.Second * 5)
	//return

	qry, err := adapter.Querier(nil, basetime-0*3600*1000, basetime+5*3600*1000)
	if err != nil {
		t.Fatal(err)
	}

	match := labels.Matcher{Type: labels.MatchEqual, Name: "__name__", Value: "http_req"}
	match2 := labels.Matcher{Type: labels.MatchEqual, Name: aggregate.AggregateLabel, Value: "count,avg,sum"}
	//params := storage.SelectParams{Func: "count,avg,sum", Step: 1000 * 3600}
	params := storage.SelectParams{Func: "", Step: 0}
	set, err := qry.Select(&params, &match, &match2)
	if err != nil {
		t.Fatal(err)
	}

	lasth := 0
	for set.Next() {
		if set.Err() != nil {
			t.Fatal(set.Err())
		}

		series := set.At()
		fmt.Println("\nLables:", series.Labels())
		iter := series.Iterator()
		//iter.Seek(basetime-1*3600*1000)
		for iter.Next() {

			if iter.Err() != nil {
				t.Fatal(iter.Err())
			}

			t, v := iter.At()
			d, h := partmgr.TimeToDHM(t)
			if h != lasth {
				fmt.Println()
			}
			fmt.Printf("t=%d:%d,v=%.2f ", d, h, v)
			lasth = h
		}
		fmt.Println()
	}

}

func DoAppend(lset labels.Labels, app storage.Appender, num, interval int) error {
	//return nil
	//time.Sleep(time.Second * 1)
	curTime := int64(basetime)

	ref, err := app.Add(lset, curTime, 2)
	if err != nil {
		return err
	}

	for i := 0; i <= num; i++ {
		time.Sleep(time.Millisecond * 80)
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
