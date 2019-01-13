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
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/formatter"
	"github.com/v3io/v3io-tsdb/pkg/pquerier"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

type queryCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
	name           string
	filter         string
	to             string
	from           string
	last           string
	windows        string
	functions      string
	step           string
	output         string
	oldQuerier     bool
	groupBy        string
}

func newQueryCommandeer(rootCommandeer *RootCommandeer) *queryCommandeer {
	commandeer := &queryCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Aliases: []string{"get"},
		Use:     "query [<metrics>] [flags]",
		Short:   "Query a TSDB instance",
		Long:    `Query a TSDB instance (table).`,
		Example: `The examples assume that the endpoint of the web-gateway service, the login credentials, and
the name of the data container are configured in the default configuration file (` + config.DefaultConfigurationFileName + `)
instead of using the -s|--server, -u|--username, -p|--password, and -c|--container flags.
- tsdbctl query temperature -t mytsdb
- tsdbctl query -t performance -f "starts(__name__, 'cpu') AND os=='win'"
- tsdbctl query metric2,metric3,metric4 -t pmetrics -b 0 -e now-1h -a "sum,avg" -i 20m
- tsdbctl query -t mytsdb -f "LabelA==8.1" -l 1d -o json
- tsdbctl query metric1 -t my_tsdb -l 1d -a "count,sum,avg" --groupBy LabelA,LabelB
- tsdbctl query metric1 -t my_tsdb -l 1d -a "count_all,sum_all"

Notes:
- You must set the metric-name argument (<metrics>) and/or the query-filter flag (-f|--filter).
- Queries that set the metric-name argument (<metrics>) use range scan and are therefore faster.
- To query the full TSDB content, set the -f|--filter to a query filter that always evaluates
  to true (such as "1==1"), don't set the <metrics> argument, and set the -b|--begin flag to 0.
- You can use either over-time aggregates or cross series (*_all) aggregates, but not both in the same query.

Arguments:
  <metrics> (string) Comma-separated list of metric names to query. If you don't set this argument, you must
                    provide a query filter using the -f|--filter flag.`,
		RunE: func(cmd *cobra.Command, args []string) error {

			// Save the metric name if provided as a positional argument ($1)
			if len(args) > 0 {
				commandeer.name = args[0]
			}

			return commandeer.query()
		},
	}

	cmd.Flags().StringVarP(&commandeer.to, "end", "e", "",
		"End (maximum) time for the query, as a string containing an\nRFC 3339 time string, a Unix timestamp in milliseconds, or\na relative time of the format \"now\" or \"now-[0-9]+[mhd]\"\n(where 'm' = minutes, 'h' = hours, and 'd' = days).\nExamples: \"2018-09-26T14:10:20Z\"; \"1537971006000\";\n\"now-3h\"; \"now-7d\". (default \"now\")")
	cmd.Flags().StringVarP(&commandeer.from, "begin", "b", "",
		"Start (minimum) time for the query, as a string containing\nan RFC 3339 time, a Unix timestamp in milliseconds, a\nrelative time of the format \"now\" or \"now-[0-9]+[mhd]\"\n(where 'm' = minutes, 'h' = hours, and 'd' = days), or 0\nfor the earliest time. Examples: \"2016-01-02T15:34:26Z\";\n\"1451748866\"; \"now-90m\"; \"0\". (default = <end time> - 1h)")
	cmd.Flags().StringVarP(&commandeer.output, "output", "o", formatter.DefaultOutputFormat,
		"Output format in which to display the query results -\n\"text\" | \"csv\" | \"json\".")
	cmd.Flags().StringVarP(&commandeer.filter, "filter", "f", "",
		"Query filter, as an Iguazio Continuous Data Platform\nfilter expression. To reference a metric name from within\nthe query filter, use the \"__name__\" attribute.\nExamples: \"method=='get'\"; \"__name__='cpu' AND os=='win'\".")
	cmd.Flags().StringVarP(&commandeer.last, "last", "l", "",
		"Return data for the specified time period before the\ncurrent time, of the format \"[0-9]+[mhd]\" (where\n'm' = minutes, 'h' = hours, and 'd' = days>). When setting\nthis flag, don't set the -b|--begin or -e|--end flags.\nExamples: \"1h\"; \"15m\"; \"30d\" to return data for the last\n1 hour, 15 minutes, or 30 days.")
	cmd.Flags().StringVarP(&commandeer.windows, "windows", "w", "",
		"Overlapping windows of time to which to apply the aggregation\nfunctions (if defined - see the -a|--aggregates flag), as a\ncomma separated list of integer values (\"[0-9]+\").\nThe duration of each window is calculated by multiplying the\nvalue from the windows flag with the aggregation interval\n(see -i|--aggregation-interval). The windows' end time is\nthe query's end time (see -e|--end and -l|--last). If the\nwindow's duration extends beyond the query's start time (see\n-b|--begin and -l|--last), it will be shortened to fit the\nstart time. Example: -w \"1,2\" with -i \"2h\", -b 0, and the\ndefault end time (\"now\") defines overlapping aggregation\nwindows for the last 2 hours and 4 hours.")
	// The default aggregates list for an overlapping-windows query is "avg",
	// provided the TSDB instance has the "count" and "sum" aggregates, which
	// make up the "avg" aggregate; ("count" is added automatically when adding
	// any other aggregate). However, it was decided that documenting this
	// would over complicate the documentation.
	cmd.Flags().StringVarP(&commandeer.functions, "aggregates", "a", "",
		"Aggregation information to return, as a comma-separated\nlist of supported aggregation functions - count | avg |\nsum | min | max | stddev | stdvar | last | rate.\nFor cross series aggregations add an \"_all\" suffix for the wanted aggregate.\nNote: you can query either over time aggregates or cross series aggregate but not both in the same query.\nExample: \"sum,min,max,count\", \"sum_all,avg_all\".")
	cmd.Flags().StringVarP(&commandeer.step, "aggregation-interval", "i", "",
		"Aggregation interval for applying the aggregation functions\n(if set - see the -a|--aggregates flag), of the format\n\"[0-9]+[mhd]\" (where 'm' = minutes, 'h' = hours, and\n'd' = days). Examples: \"1h\"; \"150m\". (default =\n<end time> - <start time>)")
	cmd.Flags().StringVar(&commandeer.groupBy, "groupBy", "",
		"Comma separated list of labels to group the result by")

	cmd.Flags().BoolVarP(&commandeer.oldQuerier, "oldQuerier", "q", false, "use old querier")
	cmd.Flags().Lookup("oldQuerier").Hidden = true
	cmd.Flags().Lookup("windows").Hidden = true // hidden, because only supported in old querier.
	commandeer.cmd = cmd

	return commandeer
}

func (qc *queryCommandeer) query() error {

	if qc.name == "" && qc.filter == "" {
		return errors.New("The query command must receive either a metric-name paramter (<metrics>) or a query filter (set via the -f|--filter flag).")
	}

	if qc.last != "" && (qc.from != "" || qc.to != "") {
		return errors.New("The -l|--last flag cannot be set together with the -b|--begin and/or -e|--end flags.")
	}

	// Initialize parameters and adapter
	if err := qc.rootCommandeer.initialize(); err != nil {
		return err
	}

	if err := qc.rootCommandeer.startAdapter(); err != nil {
		return err
	}

	step, err := utils.Str2duration(qc.step)
	if err != nil {
		return err
	}

	// Set start & end times
	to := time.Now().Unix() * 1000
	if qc.to != "" {
		to, err = utils.Str2unixTime(qc.to)
		if err != nil {
			return err
		}
	}

	from := to - 1000*3600 // Default start time = one hour before the end time
	if qc.from != "" {
		from, err = utils.Str2unixTime(qc.from)
		if err != nil {
			return err
		}
	}

	if qc.last != "" {
		last, err := utils.Str2duration(qc.last)
		if err != nil {
			return err
		}
		from = to - last
	}

	qc.rootCommandeer.logger.DebugWith("Query", "from", from, "to", to, "name", qc.name,
		"filter", qc.filter, "functions", qc.functions, "step", qc.step, "groupBy", qc.groupBy)

	if !qc.oldQuerier {
		return qc.newQuery(from, to, step)
	} else {
		return qc.oldQuery(from, to, step)
	}
}

func (qc *queryCommandeer) newQuery(from, to, step int64) error {
	qry, err := qc.rootCommandeer.adapter.QuerierV2()
	if err != nil {
		return errors.Wrap(err, "Failed to initialize the Querier object.")
	}

	var selectParams *pquerier.SelectParams

	if strings.HasPrefix(qc.name, "select") {
		selectParams, _, err = pquerier.ParseQuery(qc.name)
		if err != nil {
			return errors.Wrap(err, "failed to parse sql")
		}
		selectParams.Step = step
		selectParams.From = from
		selectParams.To = to
	} else {
		selectParams = &pquerier.SelectParams{Name: qc.name, Functions: qc.functions,
			Step: step, Filter: qc.filter, From: from, To: to, GroupBy: qc.groupBy}
	}
	set, err := qry.Select(selectParams)

	if err != nil {
		return errors.Wrap(err, "The query selection failed.")
	}

	f, err := formatter.NewFormatter(qc.output, nil)
	if err != nil {
		return errors.Wrapf(err, "Failed to start formatter '%s'.", qc.output)
	}

	err = f.Write(qc.cmd.OutOrStdout(), set)

	return err
}

func (qc *queryCommandeer) oldQuery(from, to, step int64) error {

	qry, err := qc.rootCommandeer.adapter.Querier(nil, from, to)
	if err != nil {
		return errors.Wrap(err, "Failed to initialize the Querier object.")
	}

	var set utils.SeriesSet
	if qc.windows == "" {
		set, err = qry.Select(qc.name, qc.functions, step, qc.filter)
	} else {
		list := strings.Split(qc.windows, ",")
		win := []int{}
		for _, val := range list {
			i, err := strconv.Atoi(val)
			if err != nil {
				return errors.Wrap(err, "Invalid window.")
			}
			win = append(win, i)

		}

		set, err = qry.SelectOverlap(qc.name, qc.functions, step, win, qc.filter)
	}

	if err != nil {
		return errors.Wrap(err, "The query selection failed.")
	}

	f, err := formatter.NewFormatter(qc.output, nil)
	if err != nil {
		return errors.Wrapf(err, "Failed to start formatter '%s'.", qc.output)
	}

	err = f.Write(qc.cmd.OutOrStdout(), set)

	return err
}
