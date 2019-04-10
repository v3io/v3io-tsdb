package pquerier

import (
	"fmt"
	"math"
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
	Name              string
	Functions         string
	From, To, Step    int64
	Windows           []int
	Filter            string
	RequestedColumns  []RequestedColumn
	GroupBy           string
	AggregationWindow int64
	UseOnlyClientAggr bool

	disableAllAggr    bool
	disableClientAggr bool
}

func (s *SelectParams) getRequestedColumns() ([]RequestedColumn, error) {
	if err := s.validateSelectParams(); err != nil {
		return nil, err
	}
	if s.RequestedColumns != nil {
		return s.RequestedColumns, nil
	}
	functions := strings.Split(s.Functions, ",")
	metricNames := strings.Split(s.Name, ",")
	columns := make([]RequestedColumn, len(functions)*len(metricNames))
	var index int
	for _, metric := range metricNames {
		for _, function := range functions {
			trimmed := strings.TrimSpace(function)
			metricName := strings.TrimSpace(metric)
			newCol := RequestedColumn{Function: trimmed, Metric: metricName, Interpolator: defaultInterpolation.String()}
			columns[index] = newCol
			index++
		}
	}
	return columns, nil
}

func (s *SelectParams) validateSelectParams() error {
	if s.UseOnlyClientAggr && s.disableClientAggr {
		return errors.New("can not query, both `useOnlyClientAggr` and `disableClientAggr` flags are set")
	}

	if s.RequestedColumns == nil {
		functions := strings.Split(s.Functions, ",")
		functionMap := make(map[string]bool, len(functions))
		for _, function := range functions {
			trimmed := strings.TrimSpace(function)
			if functionMap[trimmed] {
				return fmt.Errorf("function '%v' was requested multiple time", trimmed)
			}
			functionMap[trimmed] = true
		}
	} else {
		functionMap := make(map[string]bool, len(s.RequestedColumns))
		for _, col := range s.RequestedColumns {
			trimmed := strings.TrimSpace(col.Function)
			key := fmt.Sprintf("%v-%v", col.Metric, trimmed)
			if functionMap[key] {
				return fmt.Errorf("function '%v' for metric '%v' was requested multiple time", trimmed, col.Metric)
			}
			functionMap[key] = true
		}
	}

	return nil
}

func (q *V3ioQuerier) SelectProm(params *SelectParams, noAggr bool) (utils.SeriesSet, error) {
	params.disableAllAggr = noAggr
	params.disableClientAggr = q.cfg.DisableClientAggr
	iter, err := q.baseSelectQry(params, false)
	if err != nil || iter == nil {
		return utils.NullSeriesSet{}, err
	}

	return iter, nil
}

// Base query function
func (q *V3ioQuerier) Select(params *SelectParams) (utils.SeriesSet, error) {
	params.disableAllAggr = false
	params.disableClientAggr = q.cfg.DisableClientAggr
	iter, err := q.baseSelectQry(params, true)
	if err != nil || iter == nil {
		return utils.NullSeriesSet{}, err
	}

	return iter, nil
}

func (q *V3ioQuerier) SelectDataFrame(params *SelectParams) (FrameSet, error) {
	params.disableAllAggr = false
	params.disableClientAggr = q.cfg.DisableClientAggr
	iter, err := q.baseSelectQry(params, true)
	if err != nil || iter == nil {
		return nullFrameSet{}, err
	}

	return iter, nil
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

	// If the config is set to use only client configuration override the query parameter.
	if q.cfg.UsePreciseAggregations {
		params.UseOnlyClientAggr = true
	}

	selectContext := selectQueryContext{
		container:          q.container,
		logger:             q.logger,
		workers:            q.cfg.QryWorkers,
		showAggregateLabel: showAggregateLabel,
		v3ioConfig:         q.cfg,
	}

	q.logger.Debug("Select query:\n\tMetric: %s\n\tStart Time: %s (%d)\n\tEnd Time: %s (%d)\n\tFunction: %s\n\t"+
		"Step: %d\n\tFilter: %s\n\tWindows: %v\n\tDisable All Aggr: %t\n\tDisable Client Aggr: %t",
		params.Name, time.Unix(params.From/1000, 0).String(), params.From, time.Unix(params.To/1000, 0).String(),
		params.To, params.Functions, params.Step,
		params.Filter, params.Windows, params.disableAllAggr, params.disableClientAggr)

	q.performanceReporter.WithTimer("QueryTimer", func() {
		params.Filter = strings.Replace(params.Filter, config.PrometheusMetricNameAttribute, config.MetricNameAttrName, -1)

		// Get all partitions containing data relevant to the query. If the Aggregation Window parameter is specified take it in account.
		parts := q.partitionMngr.PartsForRange(params.From-params.AggregationWindow, params.To, true)
		if len(parts) == 0 {
			return
		}

		minExistingTime, maxExistingTime := parts[0].GetStartTime(), parts[len(parts)-1].GetEndTime()
		if params.From < minExistingTime {
			params.From = minExistingTime
		}
		if params.To > maxExistingTime {
			params.To = maxExistingTime
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

// Stub
func (q *V3ioQuerier) LabelNames() ([]string, error) {
	return nil, nil
}

func (q *V3ioQuerier) getMetricNames() ([]string, error) {
	input := v3io.GetItemsInput{
		Path:           filepath.Join(q.cfg.TablePath, config.NamesDirectory) + "/", // Need a trailing slash
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

// Returns all unique labels sets we have in the data
func (q *V3ioQuerier) GetLabelSets(metric string, filter string) ([]utils.Labels, error) {
	err := q.partitionMngr.ReadAndUpdateSchema()
	if err != nil {
		return nil, err
	}

	partitionPaths := q.partitionMngr.GetPartitionsPaths()

	// If there are no partitions yet, there are no labels
	if len(partitionPaths) == 0 {
		return nil, nil
	}

	var shardingKeys []string
	if metric != "" {
		shardingKeys = q.partitionMngr.PartsForRange(0, math.MaxInt64, true)[0].GetShardingKeys(metric)
	}

	labelsMap := make(map[uint64]utils.Labels)

	// Get all label sets
	input := v3io.GetItemsInput{
		Path:           partitionPaths[0],
		Filter:         filter,
		AttributeNames: []string{config.LabelSetAttrName, config.MetricNameAttrName},
	}

	iter, err := utils.NewAsyncItemsCursor(q.container, &input, q.cfg.QryWorkers, shardingKeys, q.logger)
	if err != nil {
		return nil, err
	}

	// Iterate over the results
	for iter.Next() {
		labelSet := iter.GetField(config.LabelSetAttrName).(string)
		currLabels, err := utils.LabelsFromString(labelSet)
		if err != nil {
			return nil, err
		}

		currLabels = append(utils.LabelsFromStringList(config.PrometheusMetricNameAttribute,
			iter.GetField(config.MetricNameAttrName).(string)), currLabels...)

		labelsMap[currLabels.Hash()] = currLabels
	}

	if iter.Err() != nil {
		return nil, fmt.Errorf("failed to read label values, err= %v", iter.Err().Error())
	}

	labels := make([]utils.Labels, len(labelsMap))
	var counter int
	for _, lset := range labelsMap {
		labels[counter] = lset
		counter++
	}
	return labels, nil
}
