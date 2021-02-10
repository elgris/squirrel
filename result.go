package sqrl

import (
	"database/sql"
)

// RowsAffected returns number of rows affected or error from query execution
func RowsAffected(res sql.Result, err error) (int64, error) {
	if err != nil {
		return 0, err
	}

	return res.RowsAffected()
}

// LastInsertId returns last insert id or error from query execution
func LastInsertId(res sql.Result, err error) (int64, error) {
	if err != nil {
		return 0, err
	}

	return res.LastInsertId()
}
