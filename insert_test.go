package sqrl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInsertBuilderToSql(t *testing.T) {
	b := Insert("").
		Prefix("WITH prefix AS ?", 0).
		Into("a").
		Options("DELAYED", "IGNORE").
		Columns("b", "c").
		Values(1, 2).
		Values(3, Expr("? + 1", 4)).
		Suffix("RETURNING ?", 5)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql :=
		"WITH prefix AS ? " +
			"INSERT DELAYED IGNORE INTO a (b,c) VALUES (?,?),(?,? + 1) " +
			"RETURNING ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{0, 1, 2, 3, 4, 5}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderToSqlErr(t *testing.T) {
	_, _, err := Insert("").Values(1).ToSql()
	assert.Error(t, err)

	_, _, err = Insert("x").ToSql()
	assert.Error(t, err)
}

func TestInsertBuilderPlaceholders(t *testing.T) {
	b := Insert("test").Values(1, 2)

	sql, _, _ := b.PlaceholderFormat(Question).ToSql()
	assert.Equal(t, "INSERT INTO test VALUES (?,?)", sql)

	sql, _, _ = b.PlaceholderFormat(Dollar).ToSql()
	assert.Equal(t, "INSERT INTO test VALUES ($1,$2)", sql)
}

func TestInsertBuilderReturning(t *testing.T) {
	b := Insert("a").
		Columns("foo").
		Values(1).
		Returning("bar")

	sql, args, err := b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "INSERT INTO a (foo) VALUES (?) RETURNING bar", sql)
	assert.Equal(t, []interface{}{1}, args)

	b = Insert("a").
		Columns("foo").
		Values(1).
		ReturningSelect(Select("bar").From("b").Where("b.id = a.id"), "bar")

	sql, args, err = b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "INSERT INTO a (foo) VALUES (?) RETURNING (SELECT bar FROM b WHERE b.id = a.id) AS bar", sql)
	assert.Equal(t, []interface{}{1}, args)
}

func TestInsertBuilderRunners(t *testing.T) {
	db := &DBStub{}
	b := Insert("test").Values(1).Suffix("RETURNING y").RunWith(db)

	expectedSql := "INSERT INTO test VALUES (?) RETURNING y"

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

func TestInsertBuilderNoRunner(t *testing.T) {
	b := Insert("test").Values(1)

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

func TestInsertBuilderSetMap(t *testing.T) {
	b := Insert("table").SetMap(Eq{"field1": 1})

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "INSERT INTO table (field1) VALUES (?)"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderSelect(t *testing.T) {
	sb := Select("field1").From("table1").Where(Eq{"field1": 1})
	ib := Insert("table2").Columns("field1").Select(sb)

	sql, args, err := ib.ToSql()
	assert.NoError(t, err)

	expectedSql := "INSERT INTO table2 (field1) SELECT field1 FROM table1 WHERE field1 = ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}
