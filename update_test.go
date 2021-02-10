package sqrl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUpdateBuilderToSql(t *testing.T) {
	b := Update("").
		Prefix("WITH prefix AS ?", 0).
		Table("a").
		Set("b", Expr("? + 1", 1)).
		SetMap(Eq{"c": 2}).
		Where("d = ?", 3).
		OrderBy("e").
		Limit(4).
		Offset(5).
		Suffix("RETURNING ?", 6)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql :=
		"WITH prefix AS ? " +
			"UPDATE a SET b = ? + 1, c = ? WHERE d = ? " +
			"ORDER BY e LIMIT 4 OFFSET 5 " +
			"RETURNING ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{0, 1, 2, 3, 6}
	assert.Equal(t, expectedArgs, args)
}

func TestUpdateBuilderFrom(t *testing.T) {
	b := Update("a").
		Set("foo", 1).
		From("b").
		Where("id = b.id").
		Where(Eq{"b.id": 42})

	sql, args, err := b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE a SET foo = ? FROM b WHERE id = b.id AND b.id = ?", sql)
	assert.Equal(t, []interface{}{1, 42}, args)
}

func TestUpdateBuilderReturning(t *testing.T) {
	b := Update("a").
		Set("foo", 1).
		Where("id = ?", 42).
		Returning("bar")

	sql, args, err := b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE a SET foo = ? WHERE id = ? RETURNING bar", sql)
	assert.Equal(t, []interface{}{1, 42}, args)

	b = Update("a").
		Set("foo", 1).
		Where("id = ?", 42).
		ReturningSelect(Select("bar").From("b").Where("b.id = a.id"), "bar")

	sql, args, err = b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE a SET foo = ? WHERE id = ? RETURNING (SELECT bar FROM b WHERE b.id = a.id) AS bar", sql)
	assert.Equal(t, []interface{}{1, 42}, args)
}

func TestUpdateBuilderZeroOffsetLimit(t *testing.T) {
	qb := Update("a").
		Set("b", true).
		Limit(0).
		Offset(0)

	sql, args, err := qb.ToSql()
	assert.NoError(t, err)

	expectedSql := "UPDATE a SET b = ? LIMIT 0 OFFSET 0"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{true}
	assert.Equal(t, expectedArgs, args)
}

func TestUpdateBuilderToSqlErr(t *testing.T) {
	_, _, err := Update("").Set("x", 1).ToSql()
	assert.Error(t, err)

	_, _, err = Update("x").ToSql()
	assert.Error(t, err)
}

func TestUpdateBuilderPlaceholders(t *testing.T) {
	b := Update("test").SetMap(Eq{"x": 1, "y": 2})

	sql, _, _ := b.PlaceholderFormat(Question).ToSql()
	assert.Equal(t, "UPDATE test SET x = ?, y = ?", sql)

	sql, _, _ = b.PlaceholderFormat(Dollar).ToSql()
	assert.Equal(t, "UPDATE test SET x = $1, y = $2", sql)
}

func TestUpdateBuilderRunners(t *testing.T) {
	db := &DBStub{}
	b := Update("test").Set("x", 1).Suffix("RETURNING y").RunWith(db)

	expectedSql := "UPDATE test SET x = ? RETURNING y"

	b.Exec()
	assert.Equal(t, expectedSql, db.LastExecSql)

	b.Query()
	assert.Equal(t, expectedSql, db.LastQuerySql)

	b.QueryRow()
	assert.Equal(t, expectedSql, db.LastQueryRowSql)

	b.ExecContext(context.TODO())
	assert.Equal(t, expectedSql, db.LastExecSql)

	b.QueryContext(context.TODO())
	assert.Equal(t, expectedSql, db.LastQuerySql)

	b.QueryRowContext(context.TODO())
	assert.Equal(t, expectedSql, db.LastQueryRowSql)

	err := b.Scan()
	assert.NoError(t, err)
}

func TestUpdateBuilderNoRunner(t *testing.T) {
	b := Update("test").Set("x", 1)

	_, err := b.Exec()
	assert.Equal(t, ErrRunnerNotSet, err)

	_, err = b.Query()
	assert.Equal(t, ErrRunnerNotSet, err)

	_, err = b.ExecContext(context.TODO())
	assert.Equal(t, ErrRunnerNotSet, err)

	_, err = b.QueryContext(context.TODO())
	assert.Equal(t, ErrRunnerNotSet, err)

	err = b.Scan()
	assert.Equal(t, ErrRunnerNotSet, err)
}
