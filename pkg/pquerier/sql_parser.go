package pquerier

import (
	"fmt"
	"strings"

	"github.com/v3io/v3io-tsdb/pkg/utils"
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
				parseFuncExpr(expr, &currCol)
			case *sqlparser.ColName:
				currCol.Metric = removeBackticks(sqlparser.String(expr.Name))
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

func parseFuncExpr(expr *sqlparser.FuncExpr, destCol *RequestedColumn) error {
	possibleInterpolator := removeBackticks(sqlparser.String(expr.Name))
	if _, err := StrToInterpolateType(possibleInterpolator); err == nil {
		destCol.Interpolator = possibleInterpolator
		numOfParameters := len(expr.Exprs)
		if numOfParameters == 1 {
			collName := expr.Exprs[0].(*sqlparser.AliasedExpr).Expr.(*sqlparser.ColName)
			destCol.Metric = sqlparser.String(collName)
		} else if numOfParameters == 2 {
			collName := expr.Exprs[0].(*sqlparser.AliasedExpr).Expr.(*sqlparser.ColName)
			destCol.Metric = sqlparser.String(collName)
			toleranceVal := expr.Exprs[1].(*sqlparser.AliasedExpr).Expr.(*sqlparser.SQLVal)
			toleranceString := sqlparser.String(toleranceVal)

			// SQLVal cannot start with a number so it has to be surrounded with ticks.
			// Stripping ticks
			tolerance, err := utils.Str2duration(toleranceString[1 : len(toleranceString)-1])
			if err != nil {
				return err
			}
			destCol.InterpolationTolerance = tolerance
		} else {
			return fmt.Errorf("unssoported number of parameters for function %v", possibleInterpolator)
		}
	} else {
		destCol.Function = sqlparser.String(expr.Name)

		switch firstExpr := expr.Exprs[0].(type) {
		case *sqlparser.AliasedExpr:
			switch innerExpr := firstExpr.Expr.(type) {
			case *sqlparser.ColName:
				destCol.Metric = sqlparser.String(innerExpr.Name)
			case *sqlparser.FuncExpr:
				parseFuncExpr(innerExpr, destCol)
			}
		}
	}

	return nil
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

	tableStr := table.Name.String()
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
