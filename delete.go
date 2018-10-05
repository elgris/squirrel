package sqrl

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

// Builder

// DeleteBuilder builds SQL DELETE statements.
type DeleteBuilder struct {
	StatementBuilderType

	prefixes   exprs
	what       []string
	from       string
	joins      []string
	usingParts []Sqlizer
	whereParts []Sqlizer
	orderBys   []string

	limit       uint64
	limitValid  bool
	offset      uint64
	offsetValid bool

	suffixes exprs
}

// NewDeleteBuilder creates new instance of DeleteBuilder
func NewDeleteBuilder(b StatementBuilderType) *DeleteBuilder {
	return &DeleteBuilder{StatementBuilderType: b}
}

// RunWith sets a Runner (like database/sql.DB) to be used with e.g. Exec.
func (b *DeleteBuilder) RunWith(runner BaseRunner) *DeleteBuilder {
	b.runWith = wrapRunner(runner)
	return b
}

// Exec builds and Execs the query with the Runner set by RunWith.
func (b *DeleteBuilder) Exec() (sql.Result, error) {
	return b.ExecContext(context.Background())
}

// Exec builds and Execs the query with the Runner set by RunWith using given context.
func (b *DeleteBuilder) ExecContext(ctx context.Context) (sql.Result, error) {
	if b.runWith == nil {
		return nil, ErrRunnerNotSet
	}
	return ExecWithContext(ctx, b.runWith, b)
}

// Query builds and Querys the query with the Runner set by RunWith.
func (b *DeleteBuilder) Query() (RowsScanner, error) {
	return b.QueryContext(context.Background())
}

// QueryContext builds and runs the query using given context and Query command.
func (b *DeleteBuilder) QueryContext(ctx context.Context) (RowsScanner, error) {
	if b.runWith == nil {
		return nil, ErrRunnerNotSet
	}
	return QueryWithContext(ctx, b.runWith, b)
}

// QueryRow builds and QueryRows the query with the Runner set by RunWith.
func (b *DeleteBuilder) QueryRow() RowScanner {
	return b.QueryRowContext(context.Background())
}

// QueryRowContext builds and runs the query using given context.
func (b *DeleteBuilder) QueryRowContext(ctx context.Context) RowScanner {
	if b.runWith == nil {
		return &Row{err: ErrRunnerNotSet}
	}
	queryRower, ok := b.runWith.(QueryRowerContext)
	if !ok {
		return &Row{err: ErrRunnerNotQueryRunnerContext}
	}
	return QueryRowWithContext(ctx, queryRower, b)
}

// Scan is a shortcut for QueryRow().Scan.
func (b *DeleteBuilder) Scan(dest ...interface{}) error {
	return b.QueryRow().Scan(dest...)
}

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b *DeleteBuilder) PlaceholderFormat(f PlaceholderFormat) *DeleteBuilder {
	b.placeholderFormat = f
	return b
}

// ToSql builds the query into a SQL string and bound args.
func (b *DeleteBuilder) ToSql() (sqlStr string, args []interface{}, err error) {
	if len(b.from) == 0 {
		err = fmt.Errorf("delete statements must specify a From table")
		return
	}

	sql := &bytes.Buffer{}

	if len(b.prefixes) > 0 {
		args, _ = b.prefixes.AppendToSql(sql, " ", args)
		sql.WriteString(" ")
	}

	sql.WriteString("DELETE ")
	// following condition helps to avoid duplicate "from" value in DELETE query
	// e.g. "DELETE a FROM a ..." which is valid for MySQL but not for PostgreSQL
	if len(b.what) > 0 && (len(b.what) != 1 || b.what[0] != b.from) {
		sql.WriteString(strings.Join(b.what, ", "))
		sql.WriteString(" ")
	}

	sql.WriteString("FROM ")
	sql.WriteString(b.from)

	if len(b.joins) > 0 {
		sql.WriteString(" ")
		sql.WriteString(strings.Join(b.joins, " "))
	}

	if len(b.usingParts) > 0 {
		sql.WriteString(" USING ")
		args, err = appendToSql(b.usingParts, sql, ", ", args)
		if err != nil {
			return
		}
	}

	if len(b.whereParts) > 0 {
		sql.WriteString(" WHERE ")
		args, err = appendToSql(b.whereParts, sql, " AND ", args)
		if err != nil {
			return
		}
	}

	if len(b.orderBys) > 0 {
		sql.WriteString(" ORDER BY ")
		sql.WriteString(strings.Join(b.orderBys, ", "))
	}

	// TODO: limit == 0 and offswt == 0 are valid. Need to go dbr way and implement offsetValid and limitValid
	if b.limitValid {
		sql.WriteString(" LIMIT ")
		sql.WriteString(strconv.FormatUint(b.limit, 10))
	}

	if b.offsetValid {
		sql.WriteString(" OFFSET ")
		sql.WriteString(strconv.FormatUint(b.offset, 10))
	}

	if len(b.suffixes) > 0 {
		sql.WriteString(" ")
		args, _ = b.suffixes.AppendToSql(sql, " ", args)
	}

	sqlStr, err = b.placeholderFormat.ReplacePlaceholders(sql.String())
	return
}

// Prefix adds an expression to the beginning of the query
func (b *DeleteBuilder) Prefix(sql string, args ...interface{}) *DeleteBuilder {
	b.prefixes = append(b.prefixes, Expr(sql, args...))
	return b
}

// From sets the FROM clause of the query.
func (b *DeleteBuilder) From(from string) *DeleteBuilder {
	b.from = from
	return b
}

// What sets names of tables to be used for deleting from
func (b *DeleteBuilder) What(what ...string) *DeleteBuilder {
	filteredWhat := make([]string, 0, len(what))
	for _, item := range what {
		if len(item) > 0 {
			filteredWhat = append(filteredWhat, item)
		}
	}

	b.what = filteredWhat
	if len(filteredWhat) == 1 {
		b.From(filteredWhat[0])
	}

	return b
}

// Using sets the USING clause of the query.
//
// DELETE ... USING is an MySQL/PostgreSQL specific extension
func (b *DeleteBuilder) Using(tables ...string) *DeleteBuilder {
	parts := make([]Sqlizer, len(tables))
	for i, table := range tables {
		parts[i] = newPart(table)
	}

	b.usingParts = append(b.usingParts, parts...)
	return b
}

// UsingSelect sets a subquery into the USING clause of the query.
//
// DELETE ... USING is an MySQL/PostgreSQL specific extension
func (b *DeleteBuilder) UsingSelect(from *SelectBuilder, alias string) *DeleteBuilder {
	b.usingParts = append(b.usingParts, Alias(from, alias))
	return b
}

// Where adds WHERE expressions to the query.
func (b *DeleteBuilder) Where(pred interface{}, args ...interface{}) *DeleteBuilder {
	b.whereParts = append(b.whereParts, newWherePart(pred, args...))
	return b
}

// OrderBy adds ORDER BY expressions to the query.
func (b *DeleteBuilder) OrderBy(orderBys ...string) *DeleteBuilder {
	b.orderBys = append(b.orderBys, orderBys...)
	return b
}

// Limit sets a LIMIT clause on the query.
func (b *DeleteBuilder) Limit(limit uint64) *DeleteBuilder {
	b.limit = limit
	b.limitValid = true
	return b
}

// Offset sets a OFFSET clause on the query.
func (b *DeleteBuilder) Offset(offset uint64) *DeleteBuilder {
	b.offset = offset
	b.offsetValid = true
	return b
}

// Suffix adds an expression to the end of the query
func (b *DeleteBuilder) Suffix(sql string, args ...interface{}) *DeleteBuilder {
	b.suffixes = append(b.suffixes, Expr(sql, args...))
	return b
}

// JoinClause adds a join clause to the query.
func (b *DeleteBuilder) JoinClause(join string) *DeleteBuilder {
	b.joins = append(b.joins, join)
	return b
}

// Join adds a JOIN clause to the query.
func (b *DeleteBuilder) Join(join string) *DeleteBuilder {
	return b.JoinClause("JOIN " + join)
}

// LeftJoin adds a LEFT JOIN clause to the query.
func (b *DeleteBuilder) LeftJoin(join string) *DeleteBuilder {
	return b.JoinClause("LEFT JOIN " + join)
}

// RightJoin adds a RIGHT JOIN clause to the query.
func (b *DeleteBuilder) RightJoin(join string) *DeleteBuilder {
	return b.JoinClause("RIGHT JOIN " + join)
}
