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
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	pathUtil "path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-go/pkg/dataplane"
	"github.com/v3io/v3io-go/pkg/dataplane/http"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/appender"
	"github.com/v3io/v3io-tsdb/pkg/chunkenc"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/partmgr"
	"github.com/v3io/v3io-tsdb/pkg/pquerier"
	"github.com/v3io/v3io-tsdb/pkg/querier"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/schema"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

const (
	defaultHTTPTimeout = 30 * time.Second

	errorCodeString              = "ErrorCode"
	falseConditionOuterErrorCode = "184549378" // todo: change codes
	falseConditionInnerErrorCode = "385876025"
	maxExpressionsInUpdateItem   = 1500 // max is 2000, we're taking a buffer since it doesn't work with 2000
)

type V3ioAdapter struct {
	startTimeMargin int64
	logger          logger.Logger
	container       v3io.Container
	HTTPTimeout     time.Duration
	MetricsCache    *appender.MetricsCache
	cfg             *config.V3ioConfig
	partitionMngr   *partmgr.PartitionManager
}

type DeleteParams struct {
	Metrics   []string
	Filter    string
	From, To  int64
	DeleteAll bool

	IgnoreErrors bool
}

func CreateTSDB(cfg *config.V3ioConfig, schema *config.Schema, container v3io.Container) error {

	lgr, _ := utils.NewLogger(cfg.LogLevel)
	httpTimeout := parseHTTPTimeout(cfg, lgr)
	var err error
	if container == nil {
		container, err = utils.CreateContainer(lgr, cfg, httpTimeout)
		if err != nil {
			return errors.Wrap(err, "Failed to create a data container.")
		}
	}
	data, err := json.Marshal(schema)
	if err != nil {
		return errors.Wrap(err, "Failed to marshal the TSDB schema file.")
	}

	dataPlaneInput := v3io.DataPlaneInput{Timeout: httpTimeout}

	path := pathUtil.Join(cfg.TablePath, config.SchemaConfigFileName)
	// Check whether the config file already exists, and abort if it does
	_, err = container.GetObjectSync(&v3io.GetObjectInput{Path: path, DataPlaneInput: dataPlaneInput})
	if err == nil {
		return fmt.Errorf("A TSDB table already exists at path '" + cfg.TablePath + "'.")
	}

	err = container.PutObjectSync(&v3io.PutObjectInput{Path: path, Body: data, DataPlaneInput: dataPlaneInput})
	if err != nil {
		return errors.Wrapf(err, "Failed to create a TSDB schema at path '%s/%s/%s'.", cfg.WebAPIEndpoint, cfg.Container, path)
	}
	return err
}

func parseHTTPTimeout(cfg *config.V3ioConfig, logger logger.Logger) time.Duration {
	if cfg.HTTPTimeout == "" {
		return defaultHTTPTimeout
	}
	timeout, err := time.ParseDuration(cfg.HTTPTimeout)
	if err != nil {
		logger.Warn("Failed to parse httpTimeout '%s'. Defaulting to %d millis.", cfg.HTTPTimeout, defaultHTTPTimeout/time.Millisecond)
		return defaultHTTPTimeout
	}
	return timeout
}

// Create a new TSDB adapter, similar to Prometheus TSDB adapter but with a few
// extensions. The Prometheus compliant adapter is found under /promtsdb.
func NewV3ioAdapter(cfg *config.V3ioConfig, container v3io.Container, logger logger.Logger) (*V3ioAdapter, error) {

	var err error
	newV3ioAdapter := V3ioAdapter{}
	newV3ioAdapter.cfg = cfg
	if logger != nil {
		newV3ioAdapter.logger = logger
	} else {
		newV3ioAdapter.logger, err = utils.NewLogger(cfg.LogLevel)
		if err != nil {
			return nil, err
		}
	}

	newV3ioAdapter.HTTPTimeout = parseHTTPTimeout(cfg, logger)

	if container != nil {
		newV3ioAdapter.container = container
	} else {
		newV3ioAdapter.container, err = utils.CreateContainer(newV3ioAdapter.logger, cfg, newV3ioAdapter.HTTPTimeout)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to create V3IO data container")
		}
	}

	err = newV3ioAdapter.connect()

	return &newV3ioAdapter, err
}

func NewContainer(v3ioURL string, numWorkers int, accessKey string, username string, password string, containerName string, logger logger.Logger) (v3io.Container, error) {
	newContextInput := &v3iohttp.NewContextInput{
		NumWorkers: numWorkers,
	}
	ctx, err := v3iohttp.NewContext(logger, newContextInput)
	if err != nil {
		return nil, err
	}

	session, err := ctx.NewSession(&v3io.NewSessionInput{URL: v3ioURL, Username: username, Password: password, AccessKey: accessKey})
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create session.")
	}

	container, err := session.NewContainer(&v3io.NewContainerInput{ContainerName: containerName})
	if err != nil {
		return nil, err
	}
	return container, nil
}

func (a *V3ioAdapter) GetSchema() *config.Schema {
	return a.partitionMngr.GetConfig()
}

func (a *V3ioAdapter) GetLogger(child string) logger.Logger {
	return a.logger.GetChild(child)
}

func (a *V3ioAdapter) GetContainer() (v3io.Container, string) {
	return a.container, a.cfg.TablePath
}

func (a *V3ioAdapter) connect() error {

	fullpath := fmt.Sprintf("%s/%s/%s", a.cfg.WebAPIEndpoint, a.cfg.Container, a.cfg.TablePath)
	resp, err := a.container.GetObjectSync(&v3io.GetObjectInput{Path: pathUtil.Join(a.cfg.TablePath, config.SchemaConfigFileName)})
	if err != nil {
		if utils.IsNotExistsError(err) {
			return errors.Errorf("No TSDB schema file found at '%s'.", fullpath)
		}
		return errors.Wrapf(err, "Failed to read a TSDB schema from '%s'.", fullpath)
	}

	tableSchema := config.Schema{}
	err = json.Unmarshal(resp.Body(), &tableSchema)
	if err != nil {
		return errors.Wrapf(err, "Failed to unmarshal the TSDB schema at '%s', got: %v .", fullpath, string(resp.Body()))
	}

	// in order to support backward compatibility we do not fail on version mismatch and only logging warning
	if a.cfg.LoadPartitionsFromSchemaAttr && tableSchema.TableSchemaInfo.Version != schema.Version {
		a.logger.Warn("Table Schema version mismatch - existing table schema version is %d while the tsdb library version is %d! Make sure to create the table with same library version",
			tableSchema.TableSchemaInfo.Version, schema.Version)
	}

	a.partitionMngr, err = partmgr.NewPartitionMngr(&tableSchema, a.container, a.cfg)
	if err != nil {
		return errors.Wrapf(err, "Failed to create a TSDB partition manager at '%s'.", fullpath)
	}
	err = a.partitionMngr.Init()
	if err != nil {
		return errors.Wrapf(err, "Failed to initialize the TSDB partition manager at: %s", fullpath)
	}

	a.logger.Debug("Running with the following TSDB configuration: %+v\n", a.cfg)

	return nil
}

func (a *V3ioAdapter) InitAppenderCache() error {
	if a.MetricsCache == nil {
		a.MetricsCache = appender.NewMetricsCache(a.container, a.logger, a.cfg, a.partitionMngr)
		return a.MetricsCache.Start()
	}

	return nil
}

// Create an appender interface, for writing performance
func (a *V3ioAdapter) Appender() (Appender, error) {
	err := a.InitAppenderCache()
	if err != nil {
		return nil, err
	}

	newAppender := v3ioAppender{metricsCache: a.MetricsCache}
	return newAppender, nil
}

func (a *V3ioAdapter) StartTime() (int64, error) {
	startTime := time.Now().Unix() * 1000
	return startTime - 1000*3600*24*1000, nil // TODO: from config or DB w default
}

func (a *V3ioAdapter) Close() error {
	return nil
}

// Create a Querier interface, used for time-series queries
func (a *V3ioAdapter) Querier(_ context.Context, mint, maxt int64) (*querier.V3ioQuerier, error) {
	if maxt < mint {
		return nil, errors.Errorf("End time '%d' is lower than start time '%d'.", maxt, mint)
	}
	return querier.NewV3ioQuerier(a.container, a.logger, mint, maxt, a.cfg, a.partitionMngr), nil
}

// Create a Querier interface, used for time-series queries
func (a *V3ioAdapter) QuerierV2() (*pquerier.V3ioQuerier, error) {
	return pquerier.NewV3ioQuerier(a.container, a.logger, a.cfg, a.partitionMngr), nil
}

// Delete by time range can optionally specify metrics and filter by labels
func (a *V3ioAdapter) DeleteDB(deleteParams DeleteParams) error {
	if deleteParams.DeleteAll {
		// Ignore time boundaries
		deleteParams.From = 0
		deleteParams.To = math.MaxInt64
	} else {
		if deleteParams.To == 0 {
			deleteParams.To = time.Now().Unix() * 1000
		}
	}

	// Delete Data
	err := a.DeletePartitionsData(&deleteParams)
	if err != nil {
		return err
	}

	// If no data is left, delete Names folder
	if len(a.partitionMngr.GetPartitionsPaths()) == 0 {
		path := filepath.Join(a.cfg.TablePath, config.NamesDirectory) + "/" // Need a trailing slash
		a.logger.Info("Delete metric names at path '%s'.", path)
		err := utils.DeleteTable(a.logger, a.container, path, "", a.cfg.QryWorkers)
		if err != nil && !deleteParams.IgnoreErrors {
			return errors.Wrap(err, "Failed to delete the metric-names table.")
		}
		// Delete the Directory object
		err = a.container.DeleteObjectSync(&v3io.DeleteObjectInput{Path: path})
		if err != nil && !deleteParams.IgnoreErrors {
			if !utils.IsNotExistsError(err) {
				return errors.Wrapf(err, "Failed to delete table object '%s'.", path)
			}
		}
	}

	// If need to 'deleteAll', delete schema + TSDB table folder
	if deleteParams.DeleteAll {
		// Delete Schema file
		schemaPath := pathUtil.Join(a.cfg.TablePath, config.SchemaConfigFileName)
		a.logger.Info("Delete the TSDB configuration at '%s'.", schemaPath)
		err := a.container.DeleteObjectSync(&v3io.DeleteObjectInput{Path: schemaPath})
		if err != nil && !deleteParams.IgnoreErrors {
			return errors.New("The configuration at '" + schemaPath + "' cannot be deleted or doesn't exist.")
		}

		// Delete the Directory object
		path := a.cfg.TablePath + "/"
		err = a.container.DeleteObjectSync(&v3io.DeleteObjectInput{Path: path})
		if err != nil && !deleteParams.IgnoreErrors {
			if !utils.IsNotExistsError(err) {
				return errors.Wrapf(err, "Failed to delete table object '%s'.", path)
			}
		}
	}

	return nil
}

func (a *V3ioAdapter) DeletePartitionsData(deleteParams *DeleteParams) error {
	partitions := a.partitionMngr.PartsForRange(deleteParams.From, deleteParams.To, true)
	var entirelyDeletedPartitions []*partmgr.DBPartition

	deleteWholePartition := deleteParams.DeleteAll || (deleteParams.Filter == "" && len(deleteParams.Metrics) == 0)

	fileToDeleteChan := make(chan v3io.Item, 1024)
	getItemsTerminationChan := make(chan error, len(partitions))
	deleteTerminationChan := make(chan error, a.cfg.Workers)
	numOfGetItemsRoutines := len(partitions)
	if len(deleteParams.Metrics) > 0 {
		numOfGetItemsRoutines = numOfGetItemsRoutines * len(deleteParams.Metrics)
	}
	goRoutinesNum := numOfGetItemsRoutines + a.cfg.Workers
	onErrorTerminationChannel := make(chan struct{}, goRoutinesNum)
	systemAttributesToFetch := []string{config.ObjectNameAttrName, config.MtimeSecsAttributeName, config.MtimeNSecsAttributeName, config.EncodingAttrName, config.MaxTimeAttrName}
	var getItemsWorkers, getItemsTerminated, deletesTerminated int

	var getItemsWG sync.WaitGroup
	getItemsErrorChan := make(chan error, numOfGetItemsRoutines)

	aggregates := a.GetSchema().PartitionSchemaInfo.Aggregates
	hasServerSideAggregations := len(aggregates) != 1 || aggregates[0] != ""

	var aggrMask aggregate.AggrType
	var err error
	if hasServerSideAggregations {
		aggrMask, _, err = aggregate.AggregatesFromStringListWithCount(aggregates)
		if err != nil {
			return err
		}
	}

	for i := 0; i < a.cfg.Workers; i++ {
		go deleteObjectWorker(a.container, deleteParams, a.logger,
			fileToDeleteChan, deleteTerminationChan, onErrorTerminationChannel,
			aggrMask)
	}

	for _, part := range partitions {
		partitionEntirelyInRange := deleteParams.From <= part.GetStartTime() && deleteParams.To >= part.GetEndTime()
		deleteEntirePartitionFolder := partitionEntirelyInRange && deleteWholePartition

		// Delete all files in partition folder and then delete the folder itself
		if deleteEntirePartitionFolder {
			a.logger.Info("Deleting entire partition '%s'.", part.GetTablePath())

			getItemsWG.Add(1)
			go deleteEntirePartition(a.logger, a.container, part.GetTablePath(), a.cfg.QryWorkers,
				&getItemsWG, getItemsErrorChan)

			entirelyDeletedPartitions = append(entirelyDeletedPartitions, part)
			// First get all items based on filter+metric+time range then delete what is necessary
		} else {
			a.logger.Info("Deleting partial partition '%s'.", part.GetTablePath())

			start, end := deleteParams.From, deleteParams.To

			// Round the start and end times to the nearest aggregation buckets - to later on recalculate server side aggregations
			if hasServerSideAggregations {
				start = part.GetAggregationBucketStartTime(part.Time2Bucket(deleteParams.From))
				end = part.GetAggregationBucketEndTime(part.Time2Bucket(deleteParams.To))
			}

			var chunkAttributesToFetch []string

			// If we don't want to delete the entire object, fetch also the desired chunks to delete.
			if !partitionEntirelyInRange {
				chunkAttributesToFetch, _ = part.Range2Attrs("v", start, end)
			}

			allAttributes := append(chunkAttributesToFetch, systemAttributesToFetch...)
			if len(deleteParams.Metrics) == 0 {
				getItemsWorkers++
				input := &v3io.GetItemsInput{Path: part.GetTablePath(),
					AttributeNames: allAttributes,
					Filter:         deleteParams.Filter}
				go getItemsWorker(a.logger, a.container, input, part, fileToDeleteChan, getItemsTerminationChan, onErrorTerminationChannel)
			} else {
				for _, metric := range deleteParams.Metrics {
					for _, shardingKey := range part.GetShardingKeys(metric) {
						getItemsWorkers++
						input := &v3io.GetItemsInput{Path: part.GetTablePath(),
							AttributeNames: allAttributes,
							Filter:         deleteParams.Filter,
							ShardingKey:    shardingKey}
						go getItemsWorker(a.logger, a.container, input, part, fileToDeleteChan, getItemsTerminationChan, onErrorTerminationChannel)
					}
				}
			}
		}
	}
	a.logger.Debug("issued %v getItems", getItemsWorkers)

	// Waiting fot deleting of full partitions
	getItemsWG.Wait()
	select {
	case err = <-getItemsErrorChan:
		// Signal all other goroutines to quite
		for i := 0; i < goRoutinesNum; i++ {
			onErrorTerminationChannel <- struct{}{}
		}
		return err
	default:
	}

	if getItemsWorkers != 0 {
		for deletesTerminated < a.cfg.Workers {
			select {
			case err := <-getItemsTerminationChan:
				a.logger.Debug("finished getItems worker, total finished: %v, error: %v", getItemsTerminated+1, err)
				if err != nil {
					// If requested to ignore non-existing tables do not return error.
					if !(deleteParams.IgnoreErrors && utils.IsNotExistsOrConflictError(err)) {
						for i := 0; i < goRoutinesNum; i++ {
							onErrorTerminationChannel <- struct{}{}
						}
						return errors.Wrapf(err, "GetItems failed during recursive delete.")
					}
				}
				getItemsTerminated++

				if getItemsTerminated == getItemsWorkers {
					close(fileToDeleteChan)
				}
			case err := <-deleteTerminationChan:
				a.logger.Debug("finished delete worker, total finished: %v, err: %v", deletesTerminated+1, err)
				if err != nil {
					for i := 0; i < goRoutinesNum; i++ {
						onErrorTerminationChannel <- struct{}{}
					}
					return errors.Wrapf(err, "Delete failed during recursive delete.")
				}
				deletesTerminated++
			}
		}
	} else {
		close(fileToDeleteChan)
	}

	a.logger.Debug("finished deleting data, removing partitions from schema")
	err = a.partitionMngr.DeletePartitionsFromSchema(entirelyDeletedPartitions)
	if err != nil {
		return err
	}

	return nil
}

func deleteEntirePartition(logger logger.Logger, container v3io.Container, partitionPath string, workers int,
	wg *sync.WaitGroup, errChannel chan<- error) {
	defer wg.Done()

	err := utils.DeleteTable(logger, container, partitionPath, "", workers)
	if err != nil {
		errChannel <- errors.Wrapf(err, "Failed to delete partition '%s'.", partitionPath)
		return
	}
	// Delete the Directory object
	err = container.DeleteObjectSync(&v3io.DeleteObjectInput{Path: partitionPath})
	if err != nil && !utils.IsNotExistsError(err) {
		errChannel <- errors.Wrapf(err, "Failed to delete partition folder '%s'.", partitionPath)
	}
}

func getItemsWorker(logger logger.Logger, container v3io.Container, input *v3io.GetItemsInput, partition *partmgr.DBPartition,
	filesToDeleteChan chan<- v3io.Item, terminationChan chan<- error, onErrorTerminationChannel <-chan struct{}) {
	for {
		select {
		case _ = <-onErrorTerminationChannel:
			terminationChan <- nil
			return
		default:
		}

		logger.Debug("going to getItems for partition '%v', input: %v", partition.GetTablePath(), *input)
		resp, err := container.GetItemsSync(input)
		if err != nil {
			terminationChan <- err
			return
		}
		resp.Release()
		output := resp.Output.(*v3io.GetItemsOutput)

		for _, item := range output.Items {
			item["partition"] = partition

			// In case we got error on delete while iterating getItems response
			select {
			case _ = <-onErrorTerminationChannel:
				terminationChan <- nil
				return
			default:
			}

			filesToDeleteChan <- item
		}
		if output.Last {
			terminationChan <- nil
			return
		}
		input.Marker = output.NextMarker
	}
}

func deleteObjectWorker(container v3io.Container, deleteParams *DeleteParams, logger logger.Logger,
	filesToDeleteChannel <-chan v3io.Item, terminationChan chan<- error, onErrorTerminationChannel <-chan struct{},
	aggrMask aggregate.AggrType) {
	for {
		select {
		case _ = <-onErrorTerminationChannel:
			return
		case itemToDelete, ok := <-filesToDeleteChannel:
			if !ok {
				terminationChan <- nil
				return
			}

			currentPartition := itemToDelete.GetField("partition").(*partmgr.DBPartition)
			fileName, err := itemToDelete.GetFieldString(config.ObjectNameAttrName)
			if err != nil {
				terminationChan <- err
				return
			}
			fullFileName := pathUtil.Join(currentPartition.GetTablePath(), fileName)

			// Delete whole object
			if deleteParams.From <= currentPartition.GetStartTime() &&
				deleteParams.To >= currentPartition.GetEndTime() {

				logger.Debug("delete entire item '%v' ", fullFileName)
				input := &v3io.DeleteObjectInput{Path: fullFileName}
				err = container.DeleteObjectSync(input)
				if err != nil && !utils.IsNotExistsOrConflictError(err) {
					terminationChan <- err
					return
				}
				// Delete partial object - specific chunks or sub-parts of chunks
			} else {
				mtimeSecs, err := itemToDelete.GetFieldInt(config.MtimeSecsAttributeName)
				if err != nil {
					terminationChan <- err
					return
				}
				mtimeNSecs, err := itemToDelete.GetFieldInt(config.MtimeNSecsAttributeName)
				if err != nil {
					terminationChan <- err
					return
				}

				deleteUpdateExpression := strings.Builder{}
				dataEncoding, err := getEncoding(itemToDelete)
				if err != nil {
					terminationChan <- err
					return
				}

				var aggregationsByBucket map[int]*aggregate.AggregatesList
				if aggrMask != 0 {
					aggregationsByBucket = make(map[int]*aggregate.AggregatesList)
					aggrBuckets := currentPartition.Times2BucketRange(deleteParams.From, deleteParams.To)
					for _, bucketID := range aggrBuckets {
						aggregationsByBucket[bucketID] = aggregate.NewAggregatesList(aggrMask)
					}
				}

				var newMaxTime int64 = math.MaxInt64
				var numberOfExpressionsInUpdate int
				for attributeName, value := range itemToDelete {
					if strings.HasPrefix(attributeName, "_v") {
						// Check whether the whole chunk attribute needed to be deleted or just part of it.
						if currentPartition.IsChunkInRangeByAttr(attributeName, deleteParams.From, deleteParams.To) {
							deleteUpdateExpression.WriteString("delete(")
							deleteUpdateExpression.WriteString(attributeName)
							deleteUpdateExpression.WriteString(");")
						} else {
							currentChunksMaxTime, err := generatePartialChunkDeleteExpression(logger, &deleteUpdateExpression, attributeName,
								value.([]byte), dataEncoding, deleteParams, currentPartition, aggregationsByBucket)
							if err != nil {
								terminationChan <- err
								return
							}

							// We want to save the earliest max time possible
							if currentChunksMaxTime < newMaxTime {
								newMaxTime = currentChunksMaxTime
							}
						}
						numberOfExpressionsInUpdate++
					}
				}

				dbMaxTime := int64(itemToDelete.GetField(config.MaxTimeAttrName).(int))

				// Update the partition's max time if needed.
				if deleteParams.From < dbMaxTime && deleteParams.To >= dbMaxTime {
					if deleteParams.From < newMaxTime {
						newMaxTime = deleteParams.From
					}

					deleteUpdateExpression.WriteString(fmt.Sprintf("%v=%v;", config.MaxTimeAttrName, newMaxTime))
				}

				if deleteUpdateExpression.Len() > 0 {
					// If there are server aggregates, update the needed buckets
					if aggrMask != 0 {
						for bucket, aggregations := range aggregationsByBucket {
							numberOfExpressionsInUpdate = numberOfExpressionsInUpdate + len(*aggregations)

							// Due to engine limitation, If we reached maximum number of expressions in an UpdateItem
							// we need to break the update into chunks
							// TODO: refactor in 2.8:
							// in 2.8 there is a better way of doing it by uniting multiple update expressions into
							// one expression by range in a form similar to `_v_sum[15...100]=0`
							if numberOfExpressionsInUpdate < maxExpressionsInUpdateItem {
								deleteUpdateExpression.WriteString(aggregations.SetExpr("v", bucket))
							} else {
								exprStr := deleteUpdateExpression.String()
								logger.Debug("delete item '%v' with expression '%v'", fullFileName, exprStr)
								mtimeSecs, mtimeNSecs, err = sendUpdateItem(fullFileName, exprStr, mtimeSecs, mtimeNSecs, container)
								if err != nil {
									terminationChan <- err
									return
								}

								// Reset stuff for next update iteration
								numberOfExpressionsInUpdate = 0
								deleteUpdateExpression.Reset()
							}
						}
					}

					// If any expressions are left, save them
					if deleteUpdateExpression.Len() > 0 {
						exprStr := deleteUpdateExpression.String()
						logger.Debug("delete item '%v' with expression '%v'", fullFileName, exprStr)
						_, _, err = sendUpdateItem(fullFileName, exprStr, mtimeSecs, mtimeNSecs, container)
						if err != nil {
							terminationChan <- err
							return
						}
					}
				}
			}
		}
	}
}

func sendUpdateItem(path, expr string, mtimeSecs, mtimeNSecs int, container v3io.Container) (int, int, error) {
	condition := fmt.Sprintf("%v == %v and %v == %v",
		config.MtimeSecsAttributeName, mtimeSecs,
		config.MtimeNSecsAttributeName, mtimeNSecs)

	input := &v3io.UpdateItemInput{Path: path,
		Expression: &expr,
		Condition:  condition}

	response, err := container.UpdateItemSync(input)
	if err != nil && !utils.IsNotExistsOrConflictError(err) {
		returnError := err
		if isFalseConditionError(err) {
			returnError = errors.Wrapf(err, "Item '%v' was updated while deleting occurred. Please disable any ingestion and retry.", path)
		}
		return 0, 0, returnError
	}

	output := response.Output.(*v3io.UpdateItemOutput)
	return output.MtimeSecs, output.MtimeNSecs, nil
}

func getEncoding(itemToDelete v3io.Item) (chunkenc.Encoding, error) {
	var encoding chunkenc.Encoding
	encodingStr, ok := itemToDelete.GetField(config.EncodingAttrName).(string)
	// If we don't have the encoding attribute, use XOR as default. (for backwards compatibility)
	if !ok {
		encoding = chunkenc.EncXOR
	} else {
		intEncoding, err := strconv.Atoi(encodingStr)
		if err != nil {
			return 0, fmt.Errorf("error parsing encoding type of chunk, got: %v, error: %v", encodingStr, err)
		}
		encoding = chunkenc.Encoding(intEncoding)
	}

	return encoding, nil
}

func generatePartialChunkDeleteExpression(logger logger.Logger, expr *strings.Builder,
	attributeName string, value []byte, encoding chunkenc.Encoding, deleteParams *DeleteParams,
	partition *partmgr.DBPartition, aggregationsByBucket map[int]*aggregate.AggregatesList) (int64, error) {
	chunk, err := chunkenc.FromData(logger, encoding, value, 0)
	if err != nil {
		return 0, err
	}

	newChunk := chunkenc.NewChunk(logger, encoding == chunkenc.EncVariant)
	appender, err := newChunk.Appender()
	if err != nil {
		return 0, err
	}

	var currentMaxTime int64
	var remainingItemsCount int
	iter := chunk.Iterator()
	for iter.Next() {
		var t int64
		var v interface{}
		if encoding == chunkenc.EncXOR {
			t, v = iter.At()
		} else {
			t, v = iter.AtString()
		}

		// Append back only events that are not in the delete range
		if t < deleteParams.From || t > deleteParams.To {
			remainingItemsCount++
			appender.Append(t, v)

			// Calculate server-side aggregations
			if aggregationsByBucket != nil {
				currentAgg, ok := aggregationsByBucket[partition.Time2Bucket(t)]
				// A chunk may contain more data then needed for the aggregations, if this is the case do not aggregate
				if ok {
					currentAgg.Aggregate(t, v)
				}
			}

			// Update current chunk's new max time
			if t > currentMaxTime {
				currentMaxTime = t
			}
		}
	}

	if remainingItemsCount == 0 {
		expr.WriteString("delete(")
		expr.WriteString(attributeName)
		expr.WriteString(");")
		currentMaxTime, _ = partition.GetChunkStartTimeByAttr(attributeName)
	} else {
		bytes := appender.Chunk().Bytes()
		val := base64.StdEncoding.EncodeToString(bytes)

		expr.WriteString(fmt.Sprintf("%s=blob('%s'); ", attributeName, val))
	}

	return currentMaxTime, nil

}

// Return the number of items in a TSDB table
func (a *V3ioAdapter) CountMetrics(part string) (int, error) {
	count := 0
	paths := a.partitionMngr.GetPartitionsPaths()
	for _, path := range paths {
		input := v3io.GetItemsInput{Path: path, Filter: "", AttributeNames: []string{"__size"}}
		iter, err := utils.NewAsyncItemsCursor(a.container, &input, a.cfg.QryWorkers, []string{}, a.logger)
		if err != nil {
			return 0, err
		}

		for iter.Next() {
			count++
		}
		if iter.Err() != nil {
			return count, errors.Wrap(iter.Err(), "Failed on count iterator.")
		}
	}

	return count, nil
}

type v3ioAppender struct {
	metricsCache *appender.MetricsCache
}

// Add a t/v value to a metric item and return refID (for AddFast)
func (a v3ioAppender) Add(lset utils.Labels, t int64, v interface{}) (uint64, error) {
	return a.metricsCache.Add(lset, t, v)
}

// Faster Add using refID obtained from Add (avoid some hash/lookup overhead)
func (a v3ioAppender) AddFast(lset utils.Labels, ref uint64, t int64, v interface{}) error {
	return a.metricsCache.AddFast(ref, t, v)
}

// Wait for completion of all updates
func (a v3ioAppender) WaitForCompletion(timeout time.Duration) (int, error) {
	return a.metricsCache.WaitForCompletion(timeout)
}

func (a v3ioAppender) Close() {
	a.metricsCache.Close()
}

// In V3IO, all operations are committed (no client cache)
func (a v3ioAppender) Commit() error   { return nil }
func (a v3ioAppender) Rollback() error { return nil }

// The Appender interface provides batched appends against a storage.
type Appender interface {
	Add(l utils.Labels, t int64, v interface{}) (uint64, error)
	AddFast(l utils.Labels, ref uint64, t int64, v interface{}) error
	WaitForCompletion(timeout time.Duration) (int, error)
	Commit() error
	Rollback() error
	Close()
}

// Check if the current error was caused specifically because the condition was evaluated to false.
func isFalseConditionError(err error) bool {
	errString := err.Error()

	if strings.Count(errString, errorCodeString) == 2 &&
		strings.Contains(errString, falseConditionOuterErrorCode) &&
		strings.Contains(errString, falseConditionInnerErrorCode) {
		return true
	}

	return false
}
