package sqrl

import "io"

type returning []Sqlizer

func (r *returning) Returning(columns ...string) {
	parts := make([]Sqlizer, len(columns))
	for i, column := range columns {
		parts[i] = newPart(column)
	}
	*r = append(*r, parts...)
}

func (r *returning) ReturningSelect(from *SelectBuilder, alias string) {
	*r = append(*r, Alias(from, alias))
}

func (r *returning) AppendToSql(w io.Writer, args []interface{}) ([]interface{}, error) {
	io.WriteString(w, " RETURNING ")
	return appendToSql(*r, w, ", ", args)

}
