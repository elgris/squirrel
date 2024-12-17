package sqrl

import (
	"context"
	"database/sql"
	"sync"
	"time"
)

const maxAge = 4 * time.Hour

// Preparer is the interface that wraps the Prepare method.
//
// Prepare executes the given query as implemented by database/sql.Prepare.
// Prepare executes the given query as implemented by database/sql.PrepareContext.
type Preparer interface {
	Prepare(query string) (*sql.Stmt, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

// DBProxy groups the Execer, Queryer, QueryRower, and Preparer interfaces.
type DBProxy interface {
	Execer
	Queryer
	QueryRower
	Preparer
	ExecerContext
	QueryerContext
	QueryRowerContext
}

type savedStmt struct {
	stmt    *sql.Stmt
	lastUse time.Time
}

type stmtCacher struct {
	prep  Preparer
	cache map[string]*savedStmt
	mu    sync.Mutex
}

// NewStmtCacher returns a DBProxy wrapping prep that caches Prepared Stmts.
//
// Stmts are cached based on the string value of their queries.
func NewStmtCacher(prep Preparer) DBProxy {
	sc := &stmtCacher{prep: prep, cache: make(map[string]*savedStmt)}

	go sc.startCleanup(maxAge)

	return sc
}

func (sc *stmtCacher) startCleanup(maxAge time.Duration) {
	for range time.NewTicker(maxAge).C {
		sc.mu.Lock()

		for k, v := range sc.cache {
			if time.Since(v.lastUse) > maxAge {
				v.stmt.Close()
				delete(sc.cache, k)
			}
		}

		sc.mu.Unlock()
	}
}

func (sc *stmtCacher) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if s, ok := sc.cache[query]; ok {
		s.lastUse = time.Now()

		return s.stmt, nil
	}

	stmt, err := sc.prep.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	sc.cache[query] = &savedStmt{
		stmt:    stmt,
		lastUse: time.Now(),
	}

	return stmt, nil
}

func (sc *stmtCacher) ExecContext(ctx context.Context, query string, args ...interface{}) (res sql.Result, err error) {
	stmt, err := sc.PrepareContext(ctx, query)
	if err != nil {
		return
	}
	return stmt.ExecContext(ctx, args...)
}

func (sc *stmtCacher) QueryContext(ctx context.Context, query string, args ...interface{}) (rows RowsScanner, err error) {
	stmt, err := sc.PrepareContext(ctx, query)
	if err != nil {
		return
	}
	return stmt.QueryContext(ctx, args...)
}

func (sc *stmtCacher) QueryRowContext(ctx context.Context, query string, args ...interface{}) RowScanner {
	stmt, err := sc.PrepareContext(ctx, query)
	if err != nil {
		return &Row{err: err}
	}
	return stmt.QueryRowContext(ctx, args...)
}

func (sc *stmtCacher) Prepare(query string) (*sql.Stmt, error) {
	return sc.PrepareContext(context.Background(), query)
}

func (sc *stmtCacher) Exec(query string, args ...interface{}) (res sql.Result, err error) {
	return sc.ExecContext(context.Background(), query, args...)
}

func (sc *stmtCacher) Query(query string, args ...interface{}) (rows RowsScanner, err error) {
	return sc.QueryContext(context.Background(), query, args...)
}

func (sc *stmtCacher) QueryRow(query string, args ...interface{}) RowScanner {
	return sc.QueryRowContext(context.Background(), query, args...)
}

// DBProxyBeginner describes a DBProxy that can start transactions
type DBProxyBeginner interface {
	DBProxy
	Begin() (*sql.Tx, error)
}

type stmtCacheProxy struct {
	DBProxy
	db *sql.DB
}

// NewStmtCacheProxy creates new cache proxy for statements
func NewStmtCacheProxy(db *sql.DB) DBProxyBeginner {
	return &stmtCacheProxy{DBProxy: NewStmtCacher(db), db: db}
}

func (sp *stmtCacheProxy) Begin() (*sql.Tx, error) {
	return sp.db.Begin()
}
