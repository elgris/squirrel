package sqrl

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type setClause struct {
	column string
	value  interface{}
}

// Builder

// UpdateBuilder builds SQL UPDATE statements.
type UpdateBuilder struct {
	StatementBuilderType

	returning

	prefixes   exprs
	table      string
	fromParts  []Sqlizer
	setClauses []setClause
	whereParts []Sqlizer
	orderBys   []string

	limit       uint64
	limitValid  bool
	offset      uint64
	offsetValid bool

	suffixes exprs
}

// NewUpdateBuilder creates new instance of UpdateBuilder
func NewUpdateBuilder(b StatementBuilderType) *UpdateBuilder {
	return &UpdateBuilder{StatementBuilderType: b}
}

// RunWith sets a Runner (like database/sql.DB) to be used with e.g. Exec.
func (b *UpdateBuilder) RunWith(runner BaseRunner) *UpdateBuilder {
	b.runWith = wrapRunner(runner)
	return b
}

// Exec builds and Execs the query with the Runner set by RunWith.
func (b *UpdateBuilder) Exec() (sql.Result, error) {
	return b.ExecContext(context.Background())
}

// ExecContext builds and Execs the query with the Runner set by RunWith using given context.
func (b *UpdateBuilder) ExecContext(ctx context.Context) (sql.Result, error) {
	if b.runWith == nil {
		return nil, ErrRunnerNotSet
	}
	return ExecWithContext(ctx, b.runWith, b)
}

// Query builds and Querys the query with the Runner set by RunWith.
func (b *UpdateBuilder) Query() (RowsScanner, error) {
	return b.QueryContext(context.Background())
}

// QueryContext builds and runs the query using given context and Query command.
func (b *UpdateBuilder) QueryContext(ctx context.Context) (RowsScanner, error) {
	if b.runWith == nil {
		return nil, ErrRunnerNotSet
	}
	return QueryWithContext(ctx, b.runWith, b)
}

// QueryRow builds and QueryRows the query with the Runner set by RunWith.
func (b *UpdateBuilder) QueryRow() RowScanner {
	return b.QueryRowContext(context.Background())
}

// QueryRowContext builds and runs the query using given context.
func (b *UpdateBuilder) QueryRowContext(ctx context.Context) RowScanner {
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
func (b *UpdateBuilder) Scan(dest ...interface{}) error {
	return b.QueryRow().Scan(dest...)
}

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b *UpdateBuilder) PlaceholderFormat(f PlaceholderFormat) *UpdateBuilder {
	b.placeholderFormat = f
	return b
}

// ToSql builds the query into a SQL string and bound args.
func (b *UpdateBuilder) ToSql() (sqlStr string, args []interface{}, err error) {
	if len(b.table) == 0 {
		err = fmt.Errorf("update statements must specify a table")
		return
	}
	if len(b.setClauses) == 0 {
		err = fmt.Errorf("update statements must have at least one Set clause")
		return
	}

	sql := &bytes.Buffer{}

	if len(b.prefixes) > 0 {
		args, _ = b.prefixes.AppendToSql(sql, " ", args)
		sql.WriteString(" ")
	}

	sql.WriteString("UPDATE ")
	sql.WriteString(b.table)

	sql.WriteString(" SET ")
	setSqls := make([]string, len(b.setClauses))
	for i, setClause := range b.setClauses {
		var valSql string
		switch typedVal := setClause.value.(type) {
		case Sqlizer:
			var valArgs []interface{}
			valSql, valArgs, err = typedVal.ToSql()
			if err != nil {
				return
			}
			args = append(args, valArgs...)
		default:
			valSql = "?"
			args = append(args, typedVal)
		}
		setSqls[i] = fmt.Sprintf("%s = %s", setClause.column, valSql)
	}
	sql.WriteString(strings.Join(setSqls, ", "))

	if len(b.fromParts) > 0 {
		sql.WriteString(" FROM ")
		args, err = appendToSql(b.fromParts, sql, ", ", args)
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

	if len(b.returning) > 0 {
		args, err = b.returning.AppendToSql(sql, args)
		if err != nil {
			return
		}
	}

	if len(b.suffixes) > 0 {
		sql.WriteString(" ")
		args, _ = b.suffixes.AppendToSql(sql, " ", args)
	}

	sqlStr, err = b.placeholderFormat.ReplacePlaceholders(sql.String())
	return
}

// SQL methods

// Prefix adds an expression to the beginning of the query
func (b *UpdateBuilder) Prefix(sql string, args ...interface{}) *UpdateBuilder {
	b.prefixes = append(b.prefixes, Expr(sql, args...))
	return b
}

// Table sets the table to be updateb.
func (b *UpdateBuilder) Table(table string) *UpdateBuilder {
	b.table = table
	return b
}

// Set adds SET clauses to the query.
func (b *UpdateBuilder) Set(column string, value interface{}) *UpdateBuilder {
	b.setClauses = append(b.setClauses, setClause{column: column, value: value})
	return b
}

// SetMap is a convenience method which calls .Set for each key/value pair in clauses.
func (b *UpdateBuilder) SetMap(clauses map[string]interface{}) *UpdateBuilder {
	keys := make([]string, len(clauses))
	i := 0
	for key := range clauses {
		keys[i] = key
		i++
	}
	sort.Strings(keys)
	for _, key := range keys {
		val, _ := clauses[key]
		b = b.Set(key, val)
	}
	return b
}

// Where adds WHERE expressions to the query.
//
// See SelectBuilder.Where for more information.
func (b *UpdateBuilder) Where(pred interface{}, args ...interface{}) *UpdateBuilder {
	b.whereParts = append(b.whereParts, newWherePart(pred, args...))
	return b
}

// From adds tables to FROM clause of the query.
//
// UPDATE ... FROM is an PostgreSQL specific extension
func (b *UpdateBuilder) From(tables ...string) *UpdateBuilder {
	parts := make([]Sqlizer, len(tables))
	for i, table := range tables {
		parts[i] = newPart(table)
	}

	b.fromParts = append(b.fromParts, parts...)
	return b
}

// FromSelect adds subquery to FROM clause of the query.
//
// UPDATE ... FROM is an PostgreSQL specific extension
func (b *UpdateBuilder) FromSelect(from *SelectBuilder, alias string) *UpdateBuilder {
	b.fromParts = append(b.fromParts, Alias(from, alias))
	return b
}

// OrderBy adds ORDER BY expressions to the query.
func (b *UpdateBuilder) OrderBy(orderBys ...string) *UpdateBuilder {
	b.orderBys = append(b.orderBys, orderBys...)
	return b
}

// Limit sets a LIMIT clause on the query.
func (b *UpdateBuilder) Limit(limit uint64) *UpdateBuilder {
	b.limit = limit
	b.limitValid = true
	return b
}

// Offset sets a OFFSET clause on the query.
func (b *UpdateBuilder) Offset(offset uint64) *UpdateBuilder {
	b.offset = offset
	b.offsetValid = true
	return b
}

// Returning adds columns to RETURNING clause of the query
//
// UPDATE ... RETURNING is PostgreSQL specific extension
func (b *UpdateBuilder) Returning(columns ...string) *UpdateBuilder {
	b.returning.Returning(columns...)
	return b
}

// ReturningSelect adds subquery to RETURNING clause of the query
//
// UPDATE ... RETURNING is PostgreSQL specific extension
func (b *UpdateBuilder) ReturningSelect(from *SelectBuilder, alias string) *UpdateBuilder {
	b.returning.ReturningSelect(from, alias)
	return b
}

// Suffix adds an expression to the end of the query
func (b *UpdateBuilder) Suffix(sql string, args ...interface{}) *UpdateBuilder {
	b.suffixes = append(b.suffixes, Expr(sql, args...))
	return b
}
