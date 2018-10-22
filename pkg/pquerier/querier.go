package pquerier

import (
	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/internal/pkg/performance"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/partmgr"
	"strings"
	"time"
)

// Create a new Querier interface
func NewV3ioQuerier(container *v3io.Container, logger logger.Logger, mint, maxt int64,
	cfg *config.V3ioConfig, partMngr *partmgr.PartitionManager) *V3ioQuerier {
	newQuerier := V3ioQuerier{
		container: container,
		mint:      mint, maxt: maxt,
		logger: logger.GetChild("Querier"),
		cfg:    cfg,
	}
	newQuerier.partitionMngr = partMngr
	newQuerier.performanceReporter = performance.ReporterInstanceFromConfig(cfg)
	return &newQuerier
}

type V3ioQuerier struct {
	logger              logger.Logger
	container           *v3io.Container
	cfg                 *config.V3ioConfig
	mint, maxt          int64
	partitionMngr       *partmgr.PartitionManager
	performanceReporter *performance.MetricReporter
}

type SelectParams struct {
	Name      string
	Functions string
	Step      int64
	Windows   []int
	Filter    string
}

// Base query function
func (q *V3ioQuerier) SelectQry(params *SelectParams) (set SeriesSet, err error) {

	err = q.partitionMngr.ReadAndUpdateSchema()
	if err != nil {
		return nullSeriesSet{}, errors.Wrap(err, "Failed to read/update the TSDB schema.")
	}

	// TODO: should be checked in config
	if !IsPowerOfTwo(q.cfg.QryWorkers) {
		return nullSeriesSet{}, errors.New("Query workers num must be a power of 2 and > 0 !")
	}

	set = nullSeriesSet{}
	selectContext := selectQueryContext{
		mint: q.mint, maxt: q.maxt, step: params.Step, filter: params.Filter,
		container: q.container, logger: q.logger, workers: q.cfg.QryWorkers,
		disableClientAggr: q.cfg.DisableClientAggr,
	}

	q.logger.Debug("Select query:\n\tMetric: %s\n\tStart Time: %s (%d)\n\tEnd Time: %s (%d)\n\tFunction: %s\n\t"+
		"Step: %d\n\tFilter: %s\n\tWindows: %v\n\tDisable All Aggr: %t\n\tDisable Client Aggr: %t",
		params.Name, time.Unix(q.mint/1000, 0).String(), q.mint, time.Unix(q.maxt/1000, 0).String(),
		q.maxt, params.Functions, params.Step,
		params.Filter, params.Windows, selectContext.disableAllAggr, selectContext.disableClientAggr)

	q.performanceReporter.WithTimer("QueryTimer", func() {
		params.Filter = strings.Replace(params.Filter, "__name__", "_name", -1)

		parts := q.partitionMngr.PartsForRange(q.mint, q.maxt)
		if len(parts) == 0 {
			return
		}

		set, err = selectContext.start(parts, params)
		if err != nil {
			set = nullSeriesSet{}
			return
		}

		// TODO: return a proper set
		set = nullSeriesSet{}
		return
	})

	return
}

func IsPowerOfTwo(x int) bool {
	return (x != 0) && ((x & (x - 1)) == 0)
}
