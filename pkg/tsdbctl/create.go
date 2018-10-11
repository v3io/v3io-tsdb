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

package tsdbctl

import (
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/schema"
)

type createCommandeer struct {
	cmd                    *cobra.Command
	rootCommandeer         *RootCommandeer
	path                   string
	storageClass           string
	defaultRollups         string
	aggregationGranularity string
	shardingBucketsCount   int
	sampleRetention        int
	samplesIngestionRate   string
}

func newCreateCommandeer(rootCommandeer *RootCommandeer) *createCommandeer {
	commandeer := &createCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a new TSDB instance",
		Long:  `Create a new TSDB instance (table) according to the provided configuration.`,
		Example: `- tsdbctl create -s 192.168.1.100:8081 -u myuser -p mypassword -c mycontainer -t my_tsdb -r 1/s
- tsdbctl create -s 192.168.204.14:8081 -u janed -p OpenSesame -c bigdata -t my_dbs/metrics_table -r 60/m -a "min,avg,stddev" -i 3h
- tsdbctl create -g ~/my_tsdb_cfg.yaml -u johnl -p "P@ssNoW!" -c admin_container -t perf_metrics -r "100/h"
  (where ~/my_tsdb_cfg.yaml sets "webApiEndpoint" to the endpoint of the web-gateway service)`,
		RunE: func(cmd *cobra.Command, args []string) error {

			return commandeer.create()

		},
	}

	cmd.Flags().StringVarP(&commandeer.defaultRollups, "aggregates", "a", "",
		"Default aggregates to calculate in real time during\nthe samples ingestion, as a comma-separated list of\nsupported aggregation functions - count | avg | sum |\nmin | max | stddev | stdvar | last | rate.\nExample: \"sum,avg,max\".")
	cmd.Flags().StringVarP(&commandeer.aggregationGranularity, "aggregation-granularity", "i", config.DefaultAggregationGranularity,
		"Aggregation granularity - a time interval for applying\nthe aggregation functions (if  configured - see the\n-a|--aggregates flag), of the format \"[0-9]+[mhd]\"\n(where 'm' = minutes, 'h' = hours, and 'd' = days).\nExamples: \"2h\"; \"90m\".")
	cmd.Flags().IntVarP(&commandeer.shardingBucketsCount, "sharding-buckets", "b", config.DefaultShardingBucketsCount,
		"Number of storage buckets across which to split the\ndata of a single metric to optimize storage of\nnon-uniform data. Example: 10.")
	// TODO: enable sample-retention when supported:
	// cmd.Flags().IntVarP(&commandeer.sampleRetention, "sample-retention", "r", config.DefaultSampleRetentionHours,
	//	"Metric-samples retention period, in hours. Example: 1 (retain samples for 1 hour).")
	cmd.Flags().StringVarP(&commandeer.samplesIngestionRate, "ingestion-rate", "r", config.DefaultIngestionRate,
		"[Required] Metric-samples ingestion rate - the maximum\ningestion rate for a single metric (calculated\naccording to the slowest expected ingestion rate) -\nof the format \"[0-9]+/[mhd]\" (where 'm' = minutes,\n'h' = hours, and 'd' = days). Examples: \"12/m\" (12\nsamples per minute); \"1s\" (one sample per second).")

	commandeer.cmd = cmd

	return commandeer
}

func (cc *createCommandeer) create() error {

	// Initialize parameters
	if err := cc.rootCommandeer.initialize(); err != nil {
		return err
	}

	dbSchema, err := schema.NewSchema(
		cc.rootCommandeer.v3iocfg,
		cc.samplesIngestionRate,
		cc.aggregationGranularity,
		cc.defaultRollups)

	if err != nil {
		return errors.Wrap(err, "Failed to create a TSDB schema.")
	}

	return tsdb.CreateTSDB(cc.rootCommandeer.v3iocfg, dbSchema)
}
