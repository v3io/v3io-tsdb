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
func NewV3ioQuerier(container *v3io.Container, logger logger.Logger,
	cfg *config.V3ioConfig, partMngr *partmgr.PartitionManager) *V3ioQuerier {
	newQuerier := V3ioQuerier{
		container: container,
		logger:    logger.GetChild("Querier"),
		cfg:       cfg,
	}
	newQuerier.partitionMngr = partMngr
	newQuerier.performanceReporter = performance.ReporterInstanceFromConfig(cfg)
	return &newQuerier
}

type V3ioQuerier struct {
	logger              logger.Logger
	container           *v3io.Container
	cfg                 *config.V3ioConfig
	partitionMngr       *partmgr.PartitionManager
	performanceReporter *performance.MetricReporter
}

type SelectParams struct {
	Name           string
	Functions      string
	From, To, Step int64
	Windows        []int
	Filter         string
	columnSpecs    []RequestedColumn
}

func (s *SelectParams) getRequestedColumns() []RequestedColumn {
	if s.columnSpecs != nil {
		return s.columnSpecs
	}
	functions := strings.Split(s.Functions, ",")
	columns := make([]RequestedColumn, len(functions))
	for i, function := range functions {
		trimmed := strings.TrimSpace(function)
		newCol := RequestedColumn{function: trimmed, metric: s.Name, interpolator: "replace me"}
		columns[i] = newCol
	}
	return columns
}

// Base query function
func (q *V3ioQuerier) SelectQry(params *SelectParams) (set SeriesSet, err error) {
	if params.To < params.From {
		return nil, errors.Errorf("End time '%d' is lower than start time '%d'.", params.To, params.From)
	}

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
		mint: params.From, maxt: params.To, step: params.Step, filter: params.Filter,
		container: q.container, logger: q.logger, workers: q.cfg.QryWorkers,
		disableClientAggr: q.cfg.DisableClientAggr,
	}

	q.logger.Debug("Select query:\n\tMetric: %s\n\tStart Time: %s (%d)\n\tEnd Time: %s (%d)\n\tFunction: %s\n\t"+
		"Step: %d\n\tFilter: %s\n\tWindows: %v\n\tDisable All Aggr: %t\n\tDisable Client Aggr: %t",
		params.Name, time.Unix(params.From/1000, 0).String(), params.From, time.Unix(params.To/1000, 0).String(),
		params.To, params.Functions, params.Step,
		params.Filter, params.Windows, selectContext.disableAllAggr, selectContext.disableClientAggr)

	q.performanceReporter.WithTimer("QueryTimer", func() {
		params.Filter = strings.Replace(params.Filter, "__name__", "_name", -1)

		parts := q.partitionMngr.PartsForRange(params.From, params.To)
		if len(parts) == 0 {
			return
		}

		set, err = selectContext.start(parts, params)
		if err != nil {
			set = nullSeriesSet{}
			return
		}

		// TODO: return a proper set
		return
	})

	return
}

func IsPowerOfTwo(x int) bool {
	return (x != 0) && ((x & (x - 1)) == 0)
}
