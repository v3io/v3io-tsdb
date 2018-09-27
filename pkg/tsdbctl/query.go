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
	"github.com/v3io/v3io-tsdb/pkg/formatter"
	"github.com/v3io/v3io-tsdb/pkg/querier"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"strconv"
	"strings"
	"time"
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
}

func newQueryCommandeer(rootCommandeer *RootCommandeer) *queryCommandeer {
	commandeer := &queryCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:     "query name [flags]",
		Aliases: []string{"get"},
		Short:   "query time series performance",
		RunE: func(cmd *cobra.Command, args []string) error {

			// if we got positional arguments
			if len(args) > 0 {
				commandeer.name = args[0]
			}

			return commandeer.query()

		},
	}

	cmd.Flags().StringVarP(&commandeer.to, "end", "e", "", "to time")
	cmd.Flags().StringVarP(&commandeer.from, "begin", "b", "", "from time")
	cmd.Flags().StringVarP(&commandeer.output, "output", "o", "", "output format: text,csv,json")
	cmd.Flags().StringVarP(&commandeer.filter, "filter", "f", "", "v3io query filter e.g. method=='get'")
	cmd.Flags().StringVarP(&commandeer.last, "last", "l", "", "last min/hours/days e.g. 15m")
	cmd.Flags().StringVarP(&commandeer.windows, "windows", "w", "", "comma separated list of overlapping windows")
	cmd.Flags().StringVarP(&commandeer.functions, "aggregates", "a", "",
		"comma separated list of aggregation functions, e.g. count,avg,sum,min,max,stddev,stdvar,last,rate")
	cmd.Flags().StringVarP(&commandeer.step, "aggregation-interval", "i", "", "interval step for aggregation functions")

	commandeer.cmd = cmd

	return commandeer
}

func (qc *queryCommandeer) query() error {

	if qc.name == "" && qc.filter == "" {
		return errors.New("query must have a metric name or filter string")
	}
	// initialize psrsmd snd adapter
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

	// TODO: start & end times
	to := time.Now().Unix() * 1000
	if qc.to != "" {
		to, err = utils.Str2unixTime(qc.to)
		if err != nil {
			return err
		}
	}

	from := to - 1000*3600 // default of last hour
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
		"filter", qc.filter, "functions", qc.functions, "step", qc.step)

	qry, err := qc.rootCommandeer.adapter.Querier(nil, from, to)
	if err != nil {
		return errors.Wrap(err, "Failed to initialize Querier")
	}

	var set querier.SeriesSet
	if qc.windows == "" {
		set, err = qry.Select(qc.name, qc.functions, step, qc.filter)
	} else {
		list := strings.Split(qc.windows, ",")
		win := []int{}
		for _, val := range list {
			i, err := strconv.Atoi(val)
			if err != nil {
				return errors.Wrap(err, "not a valid window")
			}
			win = append(win, i)

		}

		set, err = qry.SelectOverlap(qc.name, qc.functions, step, win, qc.filter)
	}

	if err != nil {
		return errors.Wrap(err, "Select Failed")
	}

	f, err := formatter.NewFormatter(qc.output, nil)
	if err != nil {
		return errors.Wrap(err, "failed to start formatter "+qc.output)
	}

	err = f.Write(qc.cmd.OutOrStdout(), set)

	return err
}
