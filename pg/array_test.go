package pg_test

import (
	"fmt"
	"testing"

	"github.com/SharperShape/sqrl"
	"github.com/SharperShape/sqrl/pg"
	"github.com/stretchr/testify/assert"
)

func TestValidArray(t *testing.T) {
	valid := []struct {
		op    sqrl.Sqlizer
		sql   string
		value string
	}{
		{pg.Array([]string{}), "?", "{}"},
		{pg.Array([]int{}), "?", "{}"},
		{pg.Array([]float32{}), "?", "{}"},
		{pg.Array([]float64{}), "?", "{}"},
		{pg.Array([][]int{}), "?", "{}"},
		{pg.Array([]string{"foo", "bar", "\"quoted\""}), "?", `{"foo","bar","\"quoted\""}`},
		{pg.Array([]int{6, 7, 42}), "?", `{6,7,42}`},
		{pg.Array([]uint8{6, 7, 42}), "?", `{6,7,42}`},
		{pg.Array([][]int{{1, 2}, {3, 4}}), "?", `{{1,2},{3,4}}`},
		{pg.Array([2][2]int{{1, 2}, {3, 4}}), "?", `{{1,2},{3,4}}`},
		{pg.Array([]float32{1.5, 2, 3}), "?", `{1.5,2,3}`},
		{pg.Array([]float64{1.5, 2, 3}), "?", `{1.5,2,3}`},
	}

	for _, test := range valid {
		sql, args, err := test.op.ToSql()

		assert.NoError(t, err, "Unexpected error at case %v", test.op)
		assert.Equal(t, test.sql, sql)
		assert.Equal(t, []interface{}{test.value}, args)
	}
}

func TestInvalidArray(t *testing.T) {
	invalid := []sqrl.Sqlizer{
		pg.Array([]struct{}{{}}),
		pg.Array(42),
		pg.Array("foo"),
		pg.Array([]interface{}{6, 7, "foo"}),
		pg.Array([][]interface{}{}),
		pg.Array([][]interface{}{{1}}),
	}

	for _, test := range invalid {
		_, _, err := test.ToSql()
		assert.NotNil(t, err, "Expected error at case %+v", test)
	}
}

func ExampleArray() {
	sql, args, err := sqrl.Insert("posts").
		Columns("content", "tags").
		Values("Lorem Ipsum", pg.Array([]string{"foo", "bar"})).
		PlaceholderFormat(sqrl.Dollar).
		ToSql()

	if err != nil {
		panic(err)
	}

	fmt.Println(sql)
	fmt.Println(args)

	// Output:
	// INSERT INTO posts (content,tags) VALUES ($1,$2)
	// [Lorem Ipsum {"foo","bar"}]
}
