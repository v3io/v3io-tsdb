package pquerier

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/internal/pkg/performance"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/partmgr"
	"github.com/v3io/v3io-tsdb/pkg/utils"
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
	Name             string
	Functions        string
	From, To, Step   int64
	Windows          []int
	Filter           string
	RequestedColumns []RequestedColumn

	disableAllAggr    bool
	disableClientAggr bool
}

func (s *SelectParams) getRequestedColumns() []RequestedColumn {
	if s.RequestedColumns != nil {
		return s.RequestedColumns
	}
	functions := strings.Split(s.Functions, ",")
	columns := make([]RequestedColumn, len(functions))
	for i, function := range functions {
		trimmed := strings.TrimSpace(function)
		newCol := RequestedColumn{Function: trimmed, Metric: s.Name, Interpolator: "next"}
		columns[i] = newCol
	}
	return columns
}

func (q *V3ioQuerier) SelectProm(params *SelectParams, noAggr bool) (set SeriesSet, err error) {

	params.disableClientAggr = true
	params.disableAllAggr = noAggr

	set, err = q.baseSelectQry(params, false)
	if err != nil {
		set = nullSeriesSet{}
	}

	return set, err
}

// Base query function
func (q *V3ioQuerier) SelectQry(params *SelectParams) (set SeriesSet, err error) {
	params.disableAllAggr = false
	params.disableClientAggr = q.cfg.DisableClientAggr
	set, err = q.baseSelectQry(params, true)
	if err != nil {
		set = nullSeriesSet{}
	}

	return
}

func (q *V3ioQuerier) SelectDataFrame(params *SelectParams) (iter FrameSet, err error) {
	params.disableAllAggr = false
	params.disableClientAggr = q.cfg.DisableClientAggr
	iter, err = q.baseSelectQry(params, true)
	if err != nil {
		iter = nullFrameSet{}
	}

	return
}

func (q *V3ioQuerier) baseSelectQry(params *SelectParams, showAggregateLabel bool) (iter *frameIterator, err error) {
	if params.To < params.From {
		return nil, errors.Errorf("End time '%d' is lower than start time '%d'.", params.To, params.From)
	}

	err = q.partitionMngr.ReadAndUpdateSchema()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to read/update the TSDB schema.")
	}

	// TODO: should be checked in config
	if !isPowerOfTwo(q.cfg.QryWorkers) {
		return nil, errors.New("Query workers num must be a power of 2 and > 0 !")
	}

	selectContext := selectQueryContext{
		mint: params.From, maxt: params.To, step: params.Step, filter: params.Filter,
		container: q.container, logger: q.logger, workers: q.cfg.QryWorkers,
		disableAllAggr: params.disableAllAggr, disableClientAggr: params.disableClientAggr,
		showAggregateLabel: showAggregateLabel,
	}

	q.logger.Debug("Select query:\n\tMetric: %s\n\tStart Time: %s (%d)\n\tEnd Time: %s (%d)\n\tFunction: %s\n\t"+
		"Step: %d\n\tFilter: %s\n\tWindows: %v\n\tDisable All Aggr: %t\n\tDisable Client Aggr: %t",
		params.Name, time.Unix(params.From/1000, 0).String(), params.From, time.Unix(params.To/1000, 0).String(),
		params.To, params.Functions, params.Step,
		params.Filter, params.Windows, selectContext.disableAllAggr, selectContext.disableClientAggr)

	q.performanceReporter.WithTimer("QueryTimer", func() {
		params.Filter = strings.Replace(params.Filter, config.PrometheusMetricNameAttribute, config.MetricNameAttrName, -1)

		parts := q.partitionMngr.PartsForRange(params.From, params.To, true)
		if len(parts) == 0 {
			return
		}

		iter, err = selectContext.start(parts, params)
		return
	})

	return
}

func isPowerOfTwo(x int) bool {
	return (x != 0) && ((x & (x - 1)) == 0)
}

// Return the current metric names
func (q *V3ioQuerier) LabelValues(labelKey string) (result []string, err error) {
	q.performanceReporter.WithTimer("LabelValuesTimer", func() {
		if labelKey == config.PrometheusMetricNameAttribute {
			result, err = q.getMetricNames()
		} else {
			result, err = q.getLabelValues(labelKey)
		}
	})
	return
}

func (q *V3ioQuerier) getMetricNames() ([]string, error) {
	input := v3io.GetItemsInput{
		Path:           filepath.Join(q.cfg.TablePath, config.NamesDirectory),
		AttributeNames: []string{config.ObjectNameAttrName},
	}

	iter, err := utils.NewAsyncItemsCursor(q.container, &input, q.cfg.QryWorkers, []string{}, q.logger)
	if err != nil {
		return nil, err
	}

	var metricNames []string

	for iter.Next() {
		metricNames = append(metricNames, iter.GetField(config.ObjectNameAttrName).(string))
	}

	sort.Sort(sort.StringSlice(metricNames))

	if iter.Err() != nil {
		return nil, fmt.Errorf("failed to read metric names; err = %v", iter.Err().Error())
	}

	return metricNames, nil
}

func (q *V3ioQuerier) getLabelValues(labelKey string) ([]string, error) {

	// Sync the partition manager (hack)
	err := q.partitionMngr.ReadAndUpdateSchema()
	if err != nil {
		return nil, err
	}

	partitionPaths := q.partitionMngr.GetPartitionsPaths()

	// If there are no partitions yet, there are no labels
	if len(partitionPaths) == 0 {
		return nil, nil
	}

	labelValuesMap := map[string]struct{}{}

	// Get all label sets
	input := v3io.GetItemsInput{
		Path:           partitionPaths[0],
		AttributeNames: []string{config.LabelSetAttrName},
	}

	iter, err := utils.NewAsyncItemsCursor(q.container, &input, q.cfg.QryWorkers, []string{}, q.logger)
	if err != nil {
		return nil, err
	}

	// Iterate over the results
	for iter.Next() {
		labelSet := iter.GetField(config.LabelSetAttrName).(string)

		// For a label set of k1=v1,k2=v2, k2=v3, for labelKey "k2", for example,
		// we want to convert the set to [v2, v3]

		// Split at "," to get k=v pairs
		for _, label := range strings.Split(labelSet, ",") {

			// Split at "=" to get the label key and label value
			splitLabel := strings.SplitN(label, "=", 2)

			// If we have two elements and the first element (the key) is equal
			// to what we're looking for, save the label value in the map.
			// Use a map to prevent duplicates.
			if len(splitLabel) == 2 && splitLabel[0] == labelKey {
				labelValuesMap[splitLabel[1]] = struct{}{}
			}
		}
	}

	if iter.Err() != nil {
		return nil, fmt.Errorf("failed to read label values, err= %v", iter.Err().Error())
	}

	var labelValues []string
	for labelValue := range labelValuesMap {
		labelValues = append(labelValues, labelValue)
	}

	return labelValues, nil
}
