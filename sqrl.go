// package sqrl provides a fluent SQL generator.
//
// See https://github.com/elgris/sqrl for examples.
package sqrl

import (
	"context"
	"database/sql"
	"fmt"
)

// Sqlizer is the interface that wraps the ToSql method.
//
// ToSql returns a SQL representation of the Sqlizer, along with a slice of args
// as passed to e.g. database/sql.Exec. It can also return an error.
type Sqlizer interface {
	ToSql() (string, []interface{}, error)
}

// Execer is the interface that wraps the Exec method.
//
// Exec executes the given query as implemented by database/sql.Exec.
type Execer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

// ExecerContext is the interface that wraps the Exec method.
//
// ExecContext executes the given query using given context as implemented by database/sql.ExecContext.
type ExecerContext interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// Queryer is the interface that wraps the Query method.
//
// Query executes the given query as implemented by database/sql.Query.
type Queryer interface {
	Query(query string, args ...interface{}) (RowsScanner, error)
}

// QueryerContext is the interface that wraps the Query method.
//
// QueryerContext executes the given query using given context as implemented by database/sql.QueryContext.
type QueryerContext interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (RowsScanner, error)
}

// QueryRower is the interface that wraps the QueryRow method.
//
// QueryRow executes the given query as implemented by database/sql.QueryRow.
type QueryRower interface {
	QueryRow(query string, args ...interface{}) RowScanner
}

// QueryRowerContext is the interface that wraps the QueryRow method.
//
// QueryRowContext executes the given query using given context as implemented by database/sql.QueryRowContext.
type QueryRowerContext interface {
	QueryRowContext(ctx context.Context, query string, args ...interface{}) RowScanner
}

// BaseRunner groups the Execer and Queryer interfaces.
type BaseRunner interface {
	Execer
	ExecerContext
}

// Runner groups the Execer, Queryer, and QueryRower interfaces.
type Runner interface {
	Execer
	ExecerContext
	Queryer
	QueryerContext
	QueryRower
	QueryRowerContext
}

// ErrRunnerNotSet is returned by methods that need a Runner if it isn't set.
var ErrRunnerNotSet = fmt.Errorf("cannot run; no Runner set (RunWith)")

// ErrRunnerNotQueryRunner is returned by QueryRow if the RunWith value doesn't implement QueryRower.
var ErrRunnerNotQueryRunner = fmt.Errorf("cannot QueryRow; Runner is not a QueryRower")

// ErrRunnerNotQueryRunnerContext is returned by QueryRowContext if the RunWith value doesn't implement QueryRowerContext.
var ErrRunnerNotQueryRunnerContext = fmt.Errorf("cannot QueryRow; Runner is not a QueryRowerContext")

// ErrRunnerNotQueryer is returned by Query if the RunWith value doesn't implement Queryer.
var ErrRunnerNotQueryer = fmt.Errorf("cannot Query; Runner is not a Queryer")

// ErrRunnerNotQueryerContext is returned by QueryContext if the RunWith value doesn't implement QueryerContext.
var ErrRunnerNotQueryerContext = fmt.Errorf("cannot Query; Runner is not a QueryerContext")

// ExecWith Execs the SQL returned by s with db.
func ExecWith(db Execer, s Sqlizer) (res sql.Result, err error) {
	query, args, err := s.ToSql()
	if err != nil {
		return
	}
	return db.Exec(query, args...)
}

// ExecWithContext Execs the SQL returned by s with db.
func ExecWithContext(ctx context.Context, db ExecerContext, s Sqlizer) (res sql.Result, err error) {
	query, args, err := s.ToSql()
	if err != nil {
		return
	}
	return db.ExecContext(ctx, query, args...)
}

// QueryWith Querys the SQL returned by s with db.
func QueryWith(db Queryer, s Sqlizer) (rows RowsScanner, err error) {
	query, args, err := s.ToSql()
	if err != nil {
		return
	}
	return db.Query(query, args...)
}

// QueryWithContext Querys the SQL returned by s with db.
func QueryWithContext(ctx context.Context, db QueryerContext, s Sqlizer) (rows RowsScanner, err error) {
	query, args, err := s.ToSql()
	if err != nil {
		return
	}
	return db.QueryContext(ctx, query, args...)
}

// QueryRowWith QueryRows the SQL returned by s with db.
func QueryRowWith(db QueryRower, s Sqlizer) RowScanner {
	query, args, err := s.ToSql()
	return &Row{RowScanner: db.QueryRow(query, args...), err: err}
}

// QueryRowWithContext QueryRows the SQL returned by s with db.
func QueryRowWithContext(ctx context.Context, db QueryRowerContext, s Sqlizer) RowScanner {
	query, args, err := s.ToSql()
	return &Row{RowScanner: db.QueryRowContext(ctx, query, args...), err: err}
}

// DBRunner wraps sql.DB to implement Runner.
type dbRunner struct {
	*sql.DB
}

func (r *dbRunner) QueryRow(query string, args ...interface{}) RowScanner {
	return r.DB.QueryRow(query, args...)
}

func (r *dbRunner) QueryRowContext(ctx context.Context, query string, args ...interface{}) RowScanner {
	return r.DB.QueryRowContext(ctx, query, args...)
}

func (r *dbRunner) Query(query string, args ...interface{}) (RowsScanner, error) {
	return r.DB.Query(query, args...)
}

func (r *dbRunner) QueryContext(ctx context.Context, query string, args ...interface{}) (RowsScanner, error) {
	return r.DB.QueryContext(ctx, query, args...)
}

// TxRunner wraps sql.Tx to implement Runner.
type txRunner struct {
	*sql.Tx
}

func (r *txRunner) QueryRow(query string, args ...interface{}) RowScanner {
	return r.Tx.QueryRow(query, args...)
}

func (r *txRunner) QueryRowContext(ctx context.Context, query string, args ...interface{}) RowScanner {
	return r.Tx.QueryRowContext(ctx, query, args...)
}

func (r *txRunner) Query(query string, args ...interface{}) (RowsScanner, error) {
	return r.Tx.Query(query, args...)
}

func (r *txRunner) QueryContext(ctx context.Context, query string, args ...interface{}) (RowsScanner, error) {
	return r.Tx.QueryContext(ctx, query, args...)
}

// otherRunner wraps BaseRunner to implement Runner.
type otherRunner struct {
	BaseRunner
}

func (r *otherRunner) QueryRow(query string, args ...interface{}) RowScanner {
	queryRower, ok := r.BaseRunner.(QueryRower)
	if !ok {
		return &Row{err: ErrRunnerNotQueryRunner}
	}
	return queryRower.QueryRow(query, args)
}

func (r *otherRunner) QueryRowContext(ctx context.Context, query string, args ...interface{}) RowScanner {
	queryRower, ok := r.BaseRunner.(QueryRowerContext)
	if !ok {
		return &Row{err: ErrRunnerNotQueryRunnerContext}
	}
	return queryRower.QueryRowContext(ctx, query, args)
}

func (r *otherRunner) Query(query string, args ...interface{}) (RowsScanner, error) {
	queryer, ok := r.BaseRunner.(Queryer)
	if !ok {
		return nil, ErrRunnerNotQueryer
	}
	return queryer.Query(query, args...)
}

func (r *otherRunner) QueryContext(ctx context.Context, query string, args ...interface{}) (RowsScanner, error) {
	queryer, ok := r.BaseRunner.(QueryerContext)
	if !ok {
		return nil, ErrRunnerNotQueryerContext
	}
	return queryer.QueryContext(ctx, query, args...)
}

// WrapRunner returns Runner for sql.DB and sql.Tx, or BaseRunner otherwise.
func wrapRunner(baseRunner BaseRunner) (runner Runner) {
	switch r := baseRunner.(type) {
	case *sql.DB:
		runner = &dbRunner{r}
	case *sql.Tx:
		runner = &txRunner{r}
	case Runner:
		runner = r
	default:
		runner = &otherRunner{r}
	}
	return
}
