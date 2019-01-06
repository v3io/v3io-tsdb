package pquerier

import (
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func ParseQuery(sql string) (*SelectParams, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}
	slct, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("not a SELECT statement")
	}
	if nTables := len(slct.From); nTables != 1 {
		return nil, fmt.Errorf("select from multiple tables is not supported (got %d)", nTables)
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
				cc := expr.Exprs[0].(*sqlparser.AliasedExpr).Expr.(*sqlparser.ColName)
				currCol.Function = sqlparser.String(expr.Name)
				currCol.Interpolator = removeComma(sqlparser.String(cc.Qualifier.Name)) // Some of the interpolators are parsed with a `
				currCol.Metric = sqlparser.String(cc.Name)
			case *sqlparser.ColName:
				currCol.Metric = sqlparser.String(expr.Name)
				currCol.Interpolator = removeComma(sqlparser.String(expr.Qualifier.Name)) // Some of the interpolators are parsed with a `
			default:
				return nil, fmt.Errorf("unknown columns type - %T", col.Expr)
			}
			columns = append(columns, currCol)
		default:
			return nil, fmt.Errorf("unknown SELECT column type - %T", sexpr)
		}
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns")
	}
	selectParams.RequestedColumns = columns

	if slct.Where != nil {
		selectParams.Filter, _ = parseFilter(strings.TrimPrefix(sqlparser.String(slct.Where), " where "))
	}
	if slct.GroupBy != nil {
		selectParams.GroupBy = strings.TrimPrefix(sqlparser.String(slct.GroupBy), " group by ")
	}

	return selectParams, nil
}

func parseFilter(originalFilter string) (string, error) {
	return strings.Replace(originalFilter, " = ", " == ", -1), nil
}

func removeComma(origin string) string {
	return strings.Replace(origin, "`", "", -1)
}
