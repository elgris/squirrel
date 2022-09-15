package sqrl

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

// SelectBuilder builds SQL SELECT statements.
type SelectBuilder struct {
	StatementBuilderType

	prefixes    exprs
	distinct    bool
	options     []string
	columns     []Sqlizer
	fromParts   []Sqlizer
	joins       []Sqlizer
	whereParts  []Sqlizer
	groupBys    []string
	havingParts []Sqlizer
	orderBys    []Sqlizer

	limit       uint64
	limitValid  bool
	offset      uint64
	offsetValid bool

	suffixes exprs
}

// NewSelectBuilder creates new instance of SelectBuilder
func NewSelectBuilder(b StatementBuilderType) *SelectBuilder {
	return &SelectBuilder{StatementBuilderType: b}
}

// RunWith sets a Runner (like database/sql.DB) to be used with e.g. Exec.
func (b *SelectBuilder) RunWith(runner BaseRunner) *SelectBuilder {
	b.runWith = wrapRunner(runner)
	return b
}

// Exec builds and Execs the query with the Runner set by RunWith.
func (b *SelectBuilder) Exec() (sql.Result, error) {
	return b.ExecContext(context.Background())
}

// ExecContext builds and Execs the query with the Runner set by RunWith using given context.
func (b *SelectBuilder) ExecContext(ctx context.Context) (sql.Result, error) {
	if b.runWith == nil {
		return nil, ErrRunnerNotSet
	}
	return ExecWithContext(ctx, b.runWith, b)
}

// Query builds and Querys the query with the Runner set by RunWith.
func (b *SelectBuilder) Query() (RowsScanner, error) {
	return b.QueryContext(context.Background())
}

// QueryContext builds and Querys the query with the Runner set by RunWith in given context.
func (b *SelectBuilder) QueryContext(ctx context.Context) (RowsScanner, error) {
	if b.runWith == nil {
		return nil, ErrRunnerNotSet
	}
	return QueryWithContext(ctx, b.runWith, b)
}

// QueryRow builds and QueryRows the query with the Runner set by RunWith.
func (b *SelectBuilder) QueryRow() RowScanner {
	return b.QueryRowContext(context.Background())
}

func (b *SelectBuilder) QueryRowContext(ctx context.Context) RowScanner {
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
func (b *SelectBuilder) Scan(dest ...interface{}) error {
	return b.QueryRow().Scan(dest...)
}

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b *SelectBuilder) PlaceholderFormat(f PlaceholderFormat) *SelectBuilder {
	b.placeholderFormat = f
	return b
}

// ToSql builds the query into a SQL string and bound args.
func (b *SelectBuilder) ToSql() (sqlStr string, args []interface{}, err error) {
	if len(b.columns) == 0 {
		err = fmt.Errorf("select statements must have at least one result column")
		return
	}

	sql := &bytes.Buffer{}

	if len(b.prefixes) > 0 {
		args, _ = b.prefixes.AppendToSql(sql, " ", args)
		sql.WriteString(" ")
	}

	sql.WriteString("SELECT ")

	if b.distinct {
		sql.WriteString("DISTINCT ")
	}

	if len(b.options) > 0 {
		sql.WriteString(strings.Join(b.options, " "))
		sql.WriteString(" ")
	}

	if len(b.columns) > 0 {
		args, err = appendToSql(b.columns, sql, ", ", args)
		if err != nil {
			return
		}
	}

	if len(b.fromParts) > 0 {
		sql.WriteString(" FROM ")
		args, err = appendToSql(b.fromParts, sql, ", ", args)
		if err != nil {
			return
		}
	}

	if len(b.joins) > 0 {
		sql.WriteString(" ")
		args, err = appendToSql(b.joins, sql, " ", args)
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

	if len(b.groupBys) > 0 {
		sql.WriteString(" GROUP BY ")
		sql.WriteString(strings.Join(b.groupBys, ", "))
	}

	if len(b.havingParts) > 0 {
		sql.WriteString(" HAVING ")
		args, err = appendToSql(b.havingParts, sql, " AND ", args)
		if err != nil {
			return
		}
	}

	if len(b.orderBys) > 0 {
		sql.WriteString(" ORDER BY ")
		args, err = appendToSql(b.orderBys, sql, ", ", args)
		if err != nil {
			return
		}
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
func (b *SelectBuilder) Prefix(sql string, args ...interface{}) *SelectBuilder {
	b.prefixes = append(b.prefixes, Expr(sql, args...))
	return b
}

// Distinct adds a DISTINCT clause to the query.
func (b *SelectBuilder) Distinct() *SelectBuilder {
	b.distinct = true

	return b
}

// Options adds select option to the query
func (b *SelectBuilder) Options(options ...string) *SelectBuilder {
	for _, str := range options {
		b.options = append(b.options, str)
	}
	return b
}

// Columns adds result columns to the query.
func (b *SelectBuilder) Columns(columns ...string) *SelectBuilder {
	for _, str := range columns {
		b.columns = append(b.columns, newPart(str))
	}

	return b
}

// Column adds a result column to the query.
// Unlike Columns, Column accepts args which will be bound to placeholders in
// the columns string, for example:
//
//	Column("IF(col IN ("+Placeholders(3)+"), 1, 0) as col", 1, 2, 3)
func (b *SelectBuilder) Column(column interface{}, args ...interface{}) *SelectBuilder {
	b.columns = append(b.columns, newPart(column, args...))

	return b
}

// From sets the FROM clause of the query.
func (b *SelectBuilder) From(tables ...string) *SelectBuilder {
	parts := make([]Sqlizer, len(tables))
	for i, table := range tables {
		parts[i] = newPart(table)
	}

	b.fromParts = append(b.fromParts, parts...)
	return b
}

// FromSelect sets a subquery into the FROM clause of the query.
func (b *SelectBuilder) FromSelect(from *SelectBuilder, alias string) *SelectBuilder {
	b.fromParts = append(b.fromParts, Alias(from, alias))
	return b
}

// JoinClause adds a join clause to the query.
func (b *SelectBuilder) JoinClause(pred interface{}, args ...interface{}) *SelectBuilder {
	b.joins = append(b.joins, newPart(pred, args...))

	return b
}

// Join adds a JOIN clause to the query.
func (b *SelectBuilder) Join(join string, rest ...interface{}) *SelectBuilder {
	return b.JoinClause("JOIN "+join, rest...)
}

// LeftJoin adds a LEFT JOIN clause to the query.
func (b *SelectBuilder) LeftJoin(join string, rest ...interface{}) *SelectBuilder {
	return b.JoinClause("LEFT JOIN "+join, rest...)
}

// RightJoin adds a RIGHT JOIN clause to the query.
func (b *SelectBuilder) RightJoin(join string, rest ...interface{}) *SelectBuilder {
	return b.JoinClause("RIGHT JOIN "+join, rest...)
}

// Where adds an expression to the WHERE clause of the query.
//
// Expressions are ANDed together in the generated SQL.
//
// Where accepts several types for its pred argument:
//
// nil OR "" - ignored.
//
// string - SQL expression.
// If the expression has SQL placeholders then a set of arguments must be passed
// as well, one for each placeholder.
//
// map[string]interface{} OR Eq - map of SQL expressions to values. Each key is
// transformed into an expression like "<key> = ?", with the corresponding value
// bound to the placeholder. If the value is nil, the expression will be "<key>
// IS NULL". If the value is an array or slice, the expression will be "<key> IN
// (?,?,...)", with one placeholder for each item in the value. These expressions
// are ANDed together.
//
// Where will panic if pred isn't any of the above types.
func (b *SelectBuilder) Where(pred interface{}, args ...interface{}) *SelectBuilder {
	b.whereParts = append(b.whereParts, newWherePart(pred, args...))
	return b
}

// GroupBy adds GROUP BY expressions to the query.
func (b *SelectBuilder) GroupBy(groupBys ...string) *SelectBuilder {
	b.groupBys = append(b.groupBys, groupBys...)
	return b
}

// Having adds an expression to the HAVING clause of the query.
//
// See Where.
func (b *SelectBuilder) Having(pred interface{}, rest ...interface{}) *SelectBuilder {
	b.havingParts = append(b.havingParts, newWherePart(pred, rest...))
	return b
}

// OrderBy adds ORDER BY expressions to the query.
func (b *SelectBuilder) OrderBy(orderBys ...Sqlizer) *SelectBuilder {
	b.orderBys = append(b.orderBys, orderBys...)
	return b
}

// Limit sets a LIMIT clause on the query.
func (b *SelectBuilder) Limit(limit uint64) *SelectBuilder {
	b.limit = limit
	b.limitValid = true
	return b
}

// Offset sets a OFFSET clause on the query.
func (b *SelectBuilder) Offset(offset uint64) *SelectBuilder {
	b.offset = offset
	b.offsetValid = true
	return b
}

// Suffix adds an expression to the end of the query
func (b *SelectBuilder) Suffix(sql string, args ...interface{}) *SelectBuilder {
	b.suffixes = append(b.suffixes, Expr(sql, args...))

	return b
}
