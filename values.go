package sqrl

import (
	"bytes"
	"fmt"
	"strings"
)

// ValuesBuilder is a collection of rows
// Provided to support `SelectBuilder.FromValues`
type ValuesBuilder struct {
	StatementBuilderType

	values [][]interface{}
}

// Add another row to the ValuesBuilder
func (vb *ValuesBuilder) Values(values ...interface{}) *ValuesBuilder {
	vb.values = append(vb.values, values)
	return vb
}

func (vb *ValuesBuilder) ToSql() (sqlStr string, args []interface{}, err error) {
	sql := &bytes.Buffer{}

	sql.WriteString("VALUES ")
	if len(vb.values) == 0 {
		err = fmt.Errorf("values list is empty")
		return
	}

	valuesStrings := make([]string, len(vb.values))
	for r, row := range vb.values {
		valueStrings := make([]string, len(row))
		for v, val := range row {
			switch typedVal := val.(type) {
			case expr:
				valueStrings[v] = typedVal.sql
				args = append(args, typedVal.args...)
			case Sqlizer:
				valSql, valArgs, err := typedVal.ToSql()
				if err != nil {
					return "", args, err
				}
				valueStrings[v] = valSql
				args = append(args, valArgs...)
			default:
				valueStrings[v] = "?"
				args = append(args, val)
			}

		}
		valuesStrings[r] = fmt.Sprintf("(%s)", strings.Join(valueStrings, ","))
	}
	sql.WriteString(strings.Join(valuesStrings, ","))
	sqlStr, err = vb.placeholderFormat.ReplacePlaceholders(sql.String())
	return
}
