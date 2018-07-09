package formatter

import (
	"fmt"
	"github.com/v3io/v3io-tsdb/pkg/querier"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
)

func MakePlot(set querier.SeriesSet, path string) error {

	p, err := plot.New()
	if err != nil {
		return err
	}

	//p.Title.Text = "Plotutil example"
	//p.X.Label.Text = "time"
	//p.Y.Label.Text = "Y"
	p.X.Tick.Marker = plot.TimeTicks{Format: "2006-01-02\n15:04"}

	var vs []interface{}
	for set.Next() {

		series := set.At()
		plt, err := addLine(p, series)
		if err != nil {
			return err
		}
		name, labelStr := labelsToStr(series.Labels())
		vs = append(vs, fmt.Sprintf("%s{%s}", name, labelStr), plt)
	}

	if set.Err() != nil {
		return set.Err()
	}

	plotutil.AddLinePoints(p, vs...)
	// Save the plot to a PNG file.
	if err := p.Save(6*vg.Inch, 4*vg.Inch, path); err != nil {
		return err
	}

	return nil
}

// randomPoints returns some random x, y points.
func addLine(p *plot.Plot, series querier.Series) (plotter.XYs, error) {
	pts := plotter.XYs{}

	iter := series.Iterator()
	for iter.Next() {
		t, v := iter.At()
		pts = append(pts, struct{ X, Y float64 }{X: float64(t) / 1000, Y: v})
	}

	return pts, iter.Err()
}
