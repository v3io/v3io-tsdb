package aggregates

import (
	"fmt"
	"github.com/v3io/v3io-tsdb/pkg/partmgr"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"math/rand"
	"testing"
	"time"
)

var basetime int64

type TestConfig struct {
	Interval     int64
	TimeVariance int
	TestDuration string
	NumMetrics   int
	NumLabels    int
	Values       []float64
}

type metricContext struct {
	lset utils.Labels
	ref  uint64
}

func TestTsdb(t *testing.T) {

	testConfig := TestConfig{
		Interval:     10,
		TimeVariance: 0,
		TestDuration: "200h",
		NumMetrics:   1,
		NumLabels:    1,
		Values:       []float64{1, 2, 3, 4, 5},
	}

	duration, err := utils.Str2duration(testConfig.TestDuration)
	if err != nil {
		t.Fatalf("Failed to read test duration %s - %s", testConfig.TestDuration, err)
	}
	basetime = time.Now().Unix()*1000 - duration - 60000 // now - duration - 1 min

	cfg, err := tsdbtest.LoadV3ioConfig()
	if err != nil {
		t.Fatalf("Failed to read config %s", err)
	}
	fmt.Println(cfg)

	adapter, err := tsdb.NewV3ioAdapter(cfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	//adapter.partitionMngr.GetHead().NextPart(0)

	appender, err := adapter.Appender()
	if err != nil {
		t.Fatal(err)
	}

	metrics := []*metricContext{}
	for m := 0; m < testConfig.NumMetrics; m++ {
		for l := 0; l < testConfig.NumLabels; l++ {
			metrics = append(metrics, &metricContext{
				lset: utils.FromStrings(
					"__name__", fmt.Sprintf("metric%d", m), "label", fmt.Sprintf("lbl%d", l)),
			})
		}
	}

	index := 0
	total := 0
	for curTime := basetime; curTime < basetime+duration; curTime += testConfig.Interval * 1000 {
		v := testConfig.Values[index]
		//trand := curTime + int64(rand.Intn(testConfig.TimeVariance) - testConfig.TimeVariance/2)
		err := writeNext(appender, metrics, curTime, v)
		if err != nil {
			t.Fatal(err)
		}
		index = (index + 1) % len(testConfig.Values)
		total++
	}
	fmt.Println("total:", total)

	appender.WaitForCompletion(0)

	time.Sleep(time.Second * 2)
	return

	qry, err := adapter.Querier(nil, basetime-6*3600*1000, basetime+2*3600*1000)
	if err != nil {
		t.Fatal(err)
	}

	set, err := qry.Select("http_req", "", 0, "")
	//set, err := qry.Select("count,avg,sum", 1000*3600, "_name=='http_req'")
	//set, err := qry.SelectOverlap("count,avg,sum,max", 1000*3600, []int{4, 2, 1}, "_name=='http_req'")
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

			t, v := iter.At()
			d, h := partmgr.TimeToDHM(t)
			if h != lasth {
				fmt.Println()
			}
			m := (t % (3600 * 1000)) / 60000
			fmt.Printf("t=%d:%d:%d,v=%.2f ", d, h, m, v)
			lasth = h
		}
		if iter.Err() != nil {
			t.Fatal(iter.Err())
		}

		fmt.Println()
	}

}

func DoAppend(lset utils.Labels, app tsdb.Appender, num, interval int) error {
	return nil
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

func writeNext(app tsdb.Appender, metrics []*metricContext, t int64, v float64) error {

	for _, metric := range metrics {
		if metric.ref == 0 {
			ref, err := app.Add(metric.lset, t, v)
			if err != nil {
				return err
			}
			metric.ref = ref
		} else {
			err := app.AddFast(metric.lset, metric.ref, t, v)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
