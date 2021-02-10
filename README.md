# sqrl - fat-free version of squirrel - fluent SQL generator for Go

**Non thread safe** fork of [squirrel](http://github.com/lann/squirrel). The same handy fluffy helper, but with extra letters removed :)

```go
import "github.com/elgris/sqrl"
```

[![GoDoc](https://godoc.org/github.com/elgris/sqrl?status.svg)](https://godoc.org/github.com/elgris/sqrl)
[![Build Status](https://travis-ci.org/elgris/sqrl.svg?branch=master)](https://travis-ci.org/elgris/sqrl)

**Requires Go 1.8 and higher**

## Inspired by

- [squirrel](https://github.com/lann/squirrel)
- [dbr](https://github.com/gocraft/dbr)

## Why to make good squirrel lighter?

Ask [benchmarks](https://github.com/elgris/golang-sql-builder-benchmark) about that ;). Squirrel is good, reliable and thread-safe with it's immutable query builder. Although immutability is nice, it's resource consuming and sometimes redundant. As authors of `dbr` say: "100% of our application code was written without the need for this".

## Why not to use dbr then?

Although, `dbr`'s query builder is proven to be much [faster than squirrel](https://github.com/tyler-smith/golang-sql-benchmark) and even faster than [sqrl](https://github.com/elgris/golang-sql-builder-benchmark), it doesn't have all syntax sugar. Especially I miss support of JOINs, subqueries and aliases.

## Usage

**sqrl is not an ORM.**, it helps you build SQL queries from composable parts.
**sqrl is non thread safe**. SQL builders change their state, so using the same builder in parallel is dangerous.

It's very easy to switch between original squirrel and sqrl, because there is no change in interface:

```go
import sq "github.com/elgris/sqrl" // you can easily use github.com/lann/squirrel here

users := sq.Select("*").From("users").Join("emails USING (email_id)")

active := users.Where(sq.Eq{"deleted_at": nil})

sql, args, err := active.ToSql()

sql == "SELECT * FROM users JOIN emails USING (email_id) WHERE deleted_at IS NULL"
```

```go
sql, args, err := sq.
    Insert("users").Columns("name", "age").
    Values("moe", 13).Values("larry", sq.Expr("? + 5", 12)).
    ToSql()

sql == "INSERT INTO users (name,age) VALUES (?,?),(?,? + 5)"
```

Like [squirrel](https://github.com/lann/squirrel), sqrl can execute queries directly:

```go
stooges := users.Where(sq.Eq{"username": []string{"moe", "larry", "curly", "shemp"}})
three_stooges := stooges.Limit(3)
rows, err := three_stooges.RunWith(db).Query()

// Behaves like:
rows, err := db.Query("SELECT * FROM users WHERE username IN (?,?,?,?) LIMIT 3", "moe", "larry", "curly", "shemp")
```

Build conditional queries with ease:

```go
if len(q) > 0 {
    users = users.Where("name LIKE ?", q)
}
```

### MySQL-specific functions

#### [Multi-table delete](https://dev.mysql.com/doc/refman/5.7/en/delete.html)

```go
sql, args, err := sq.Delete("a1", "a2").
    From("z1 AS a1").
    JoinClause("INNER JOIN a2 ON a1.id = a2.ref_id").
    Where("b = ?", 1).
    ToSql()
```

```go
sql, args, err := sq.Delete("a1").
    Using("a2").
    Where("a1.id = a2.ref_id AND a2.num = ?", 42).
    ToSql()
```

### PostgreSQL-specific functions

Package [pg](https://godoc.org/github.com/elgris/sqrl/pg) contains PostgreSQL specific operators.

#### [Update from](https://www.postgresql.org/docs/current/static/sql-update.html)

```go
sql, args, err := sq.Update("a1").
    Set("foo", 1).
    From("a2").
    Where("id = a2.ref_id AND a2.num = ?", 42).
    ToSql()
```

#### [Delete using](https://www.postgresql.org/docs/current/static/sql-delete.html)
```go
sql, args, err := sq.Delete("a1").
    Using("a2").
    Where("id = a2.ref_id AND a2.num = ?", 42).
    ToSql()
```

#### [Returning clause](https://www.postgresql.org/docs/current/static/dml-returning.html)
```go
sql, args, err := Update("a").
    Set("foo", 1).
    Where("id = ?", 42).
    Returning("bar").
    ToSql()
```

#### [JSON values](https://www.postgresql.org/docs/current/static/functions-json.html)

JSON and JSONB use json.Marshal to serialize values and cast them to appropriate column type.

```go
sql, args, err := sq.Insert("posts").
    Columns("content", "tags").
    Values("Lorem Ipsum", pg.JSONB([]string{"foo", "bar"})).
    ToSql()
```

#### [Array values](https://www.postgresql.org/docs/current/static/arrays.html)

Array serializes single and multidimensional slices of string, int, float32 and float64 values.

```go
sql, args, err := sqrl.Insert("posts").
    Columns("content", "tags").
    Values("Lorem Ipsum", pg.Array([]string{"foo", "bar"})).
    ToSql()
```

## License

Sqrl is released under the
[MIT License](http://www.opensource.org/licenses/MIT).
