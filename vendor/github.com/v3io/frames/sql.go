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

package frames

import (
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

// Query is query structure
type Query struct {
	Table   string
	Columns []string
	Filter  string
	GroupBy string
}

// ParseSQL parsers SQL query to a Query struct
func ParseSQL(sql string) (*Query, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}

	slct, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("not a SELECT statement")
	}

	if nTables := len(slct.From); nTables != 1 {
		return nil, fmt.Errorf("nan select from only one table (got %d)", nTables)
	}

	aliased, ok := slct.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, fmt.Errorf("not a table select")
	}

	table, ok := aliased.Expr.(sqlparser.TableName)
	if !ok {
		return nil, fmt.Errorf("not a table in FROM field")
	}

	query := &Query{
		Table: table.Name.String(),
	}

	for _, sexpr := range slct.SelectExprs {
		switch col := sexpr.(type) {
		case *sqlparser.AliasedExpr:
			if !col.As.IsEmpty() {
				return nil, fmt.Errorf("SELECT ... AS ... is not supported")
			}

			colName, ok := col.Expr.(*sqlparser.ColName)
			if !ok {
				return nil, fmt.Errorf("unknown columns type - %T", col.Expr)
			}

			query.Columns = append(query.Columns, colName.Name.String())
		default:
			return nil, fmt.Errorf("unknown SELECT column type - %T", sexpr)
		}
	}

	if len(query.Columns) == 0 {
		return nil, fmt.Errorf("no columns")
	}

	if slct.Where != nil {
		query.Filter = strings.TrimSpace(sqlparser.String(slct.Where))
	}

	if slct.GroupBy != nil {
		// TODO: Do we want to extract just the table names? Then GroupBy will be []string
		query.GroupBy = strings.TrimSpace(sqlparser.String(slct.GroupBy))
	}

	return query, nil
}
