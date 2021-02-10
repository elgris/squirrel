package sqrl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeleteBuilderToSql(t *testing.T) {
	b := Delete("").
		Prefix("WITH prefix AS ?", 0).
		From("a").
		Where("b = ?", 1).
		OrderBy("c").
		Limit(2).
		Offset(3).
		Suffix("RETURNING ?", 4)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql :=
		"WITH prefix AS ? " +
			"DELETE FROM a WHERE b = ? ORDER BY c LIMIT 2 OFFSET 3 " +
			"RETURNING ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{0, 1, 4}
	assert.Equal(t, expectedArgs, args)
}

func TestDeleteFromAndWhatDiffer(t *testing.T) {
	b := Delete("b").
		From("a").
		Where("b = ?", 1)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "DELETE b FROM a WHERE b = ?"
	assert.Equal(t, expectedSql, sql)
	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestDeleteFromAndWhatSame(t *testing.T) {
	b := Delete("a").
		From("a").
		Where("b = ?", 1)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "DELETE FROM a WHERE b = ?"
	assert.Equal(t, expectedSql, sql)
	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}
func TestDeleteWithoutFrom(t *testing.T) {
	b := Delete("a").
		Where("b = ?", 1)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "DELETE FROM a WHERE b = ?"
	assert.Equal(t, expectedSql, sql)
	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestDeleteSqlMultipleTables(t *testing.T) {
	b := Delete("a1", "a2").
		From("z1 AS a1").
		JoinClause("INNER JOIN a2 ON a1.id = a2.ref_id").
		Join("a3").
		Where("b = ?", 1)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql :=
		"DELETE a1, a2 " +
			"FROM z1 AS a1 " +
			"INNER JOIN a2 ON a1.id = a2.ref_id " +
			"JOIN a3 " +
			"WHERE b = ?"

	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestDeleteUsing(t *testing.T) {
	b := Delete("a1").
		Using("a2").
		Where("id = a2.ref_id AND a2.num = ?", 42)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "DELETE FROM a1 USING a2 WHERE id = a2.ref_id AND a2.num = ?", sql)
	assert.Equal(t, []interface{}{42}, args)

	b = Delete("a1").
		Using("a2", "a3").
		Where("id = a2.ref_id AND a2.num = ?", 42)

	sql, args, err = b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "DELETE FROM a1 USING a2, a3 WHERE id = a2.ref_id AND a2.num = ?", sql)
	assert.Equal(t, []interface{}{42}, args)
}

func TestDeleteBuilderReturning(t *testing.T) {
	b := Delete("a").
		Where("id = ?", 42).
		Returning("bar")

	sql, args, err := b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "DELETE FROM a WHERE id = ? RETURNING bar", sql)
	assert.Equal(t, []interface{}{42}, args)

	b = Delete("a").
		Where("id = ?", 42).
		ReturningSelect(Select("bar").From("b").Where("b.id = a.id"), "bar")

	sql, args, err = b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "DELETE FROM a WHERE id = ? RETURNING (SELECT bar FROM b WHERE b.id = a.id) AS bar", sql)
	assert.Equal(t, []interface{}{42}, args)
}

func TestDeleteBuilderZeroOffsetLimit(t *testing.T) {
	qb := Delete("").
		From("b").
		Limit(0).
		Offset(0)

	sql, _, err := qb.ToSql()
	assert.NoError(t, err)

	expectedSql := "DELETE FROM b LIMIT 0 OFFSET 0"
	assert.Equal(t, expectedSql, sql)
}

func TestDeleteBuilderToSqlErr(t *testing.T) {
	_, _, err := Delete("").ToSql()
	assert.Error(t, err)
}

func TestDeleteBuilderPlaceholders(t *testing.T) {
	b := Delete("test").Where("x = ? AND y = ?", 1, 2)

	sql, _, _ := b.PlaceholderFormat(Question).ToSql()
	assert.Equal(t, "DELETE FROM test WHERE x = ? AND y = ?", sql)

	sql, _, _ = b.PlaceholderFormat(Dollar).ToSql()
	assert.Equal(t, "DELETE FROM test WHERE x = $1 AND y = $2", sql)
}

func TestDeleteBuilderRunners(t *testing.T) {
	db := &DBStub{}
	b := Delete("test").Where("x = ?", 1).Suffix("RETURNING y").RunWith(db)

	expectedSql := "DELETE FROM test WHERE x = ? RETURNING y"

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

func TestDeleteBuilderNoRunner(t *testing.T) {
	b := Delete("test")

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

func TestIssue11(t *testing.T) {
	b := Delete("a").
		From("A a").
		Join("B b ON a.c = b.c").
		Where("b.d = ?", 1).
		Limit(2)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "DELETE a FROM A a " +
		"JOIN B b ON a.c = b.c " +
		"WHERE b.d = ? " +
		"LIMIT 2"

	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}
