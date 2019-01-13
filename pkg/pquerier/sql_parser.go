package pquerier

import (
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

const emptyTableName = "dual"

// ParseQuery Parses an sql query into `tsdb.selectParams`
// Currently supported syntax:
// select - selecting multiple metrics, aggregations, interpolation functions and aliasing
// from   - only one table
// where  - equality, and range operators. Not supporting regex,`IS NULL`, etc..
// group by
func ParseQuery(sql string) (*SelectParams, string, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, "", err
	}
	slct, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil, "", fmt.Errorf("not a SELECT statement")
	}

	fromTable, err := getTableName(slct)
	if err != nil {
		return nil, "", err
	}

	selectParams := &SelectParams{}
	var columns []RequestedColumn

	for _, sexpr := range slct.SelectExprs {
		currCol := RequestedColumn{}
		switch col := sexpr.(type) {
		case *sqlparser.AliasedExpr:
			if !col.As.IsEmpty() {
				currCol.Alias = col.As.String()
			}

			switch expr := col.Expr.(type) {
			case *sqlparser.FuncExpr:
				switch firstExpr := expr.Exprs[0].(type) {
				case *sqlparser.AliasedExpr:
					cc := firstExpr.Expr.(*sqlparser.ColName)
					currCol.Function = sqlparser.String(expr.Name)
					currCol.Interpolator = removeBackticks(sqlparser.String(cc.Qualifier.Name)) // Some of the interpolators are parsed with a `
					currCol.Metric = sqlparser.String(cc.Name)
				case *sqlparser.StarExpr:
					// Appending column with empty metric name, meaning a column template with the given aggregate
					currCol.Function = sqlparser.String(expr.Name)
				}
			case *sqlparser.ColName:
				currCol.Metric = sqlparser.String(expr.Name)
				currCol.Interpolator = removeBackticks(sqlparser.String(expr.Qualifier.Name)) // Some of the interpolators are parsed with a `
			default:
				return nil, "", fmt.Errorf("unknown columns type - %T", col.Expr)
			}
			columns = append(columns, currCol)
		case *sqlparser.StarExpr:
			// Appending empty column, meaning a column template for raw data
			columns = append(columns, currCol)
		default:
			return nil, "", fmt.Errorf("unknown SELECT column type - %T", sexpr)
		}
	}
	if len(columns) == 0 {
		return nil, "", fmt.Errorf("no columns")
	}
	selectParams.RequestedColumns = columns

	if slct.Where != nil {
		selectParams.Filter, _ = parseFilter(strings.TrimPrefix(sqlparser.String(slct.Where), " where "))
	}
	if slct.GroupBy != nil {
		selectParams.GroupBy = strings.TrimPrefix(sqlparser.String(slct.GroupBy), " group by ")
	}

	return selectParams, fromTable, nil
}

func getTableName(slct *sqlparser.Select) (string, error) {
	if nTables := len(slct.From); nTables != 1 {
		return "", fmt.Errorf("select from multiple tables is not supported (got %d)", nTables)
	}
	aliased, ok := slct.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return "", fmt.Errorf("not a table select")
	}
	table, ok := aliased.Expr.(sqlparser.TableName)
	if !ok {
		return "", fmt.Errorf("not a table in FROM field")
	}

	tableStr := sqlparser.String(table)
	if tableStr == emptyTableName {
		return "", nil
	}
	return tableStr, nil
}
func parseFilter(originalFilter string) (string, error) {
	return strings.Replace(originalFilter, " = ", " == ", -1), nil
}
func removeBackticks(origin string) string {
	return strings.Replace(origin, "`", "", -1)
}
