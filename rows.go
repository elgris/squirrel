package sqrl

// RowsScanner is the interface that wraps the sql row methods.
//
// functions behave like database/sql.Rows methods
type RowsScanner interface {
	Columns() ([]string, error)
	Next() bool
	Close() error
	Err() error
	RowScanner
}
