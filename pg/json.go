package pg

import (
	"encoding/json"
	"fmt"

	"github.com/SharperShape/sqrl"
)

// JSONB converts value into Postgres JSONB
func JSONB(value interface{}) sqrl.Sqlizer {
	return jsonOp{
		value: value,
		tpe:   "jsonb",
	}
}

// JSON converts value into Postgres JSON
func JSON(value interface{}) sqrl.Sqlizer {
	return jsonOp{
		value: value,
		tpe:   "json",
	}
}

type jsonOp struct {
	value interface{}
	tpe   string
}

// ToSql builds the query into a SQL string and bound args.
func (jo jsonOp) ToSql() (string, []interface{}, error) {
	v, err := json.Marshal(jo.value)
	if err != nil {
		return "", nil, fmt.Errorf("Failed to serialize %s value: %v", jo.tpe, err)
	}

	return fmt.Sprintf("?::%s", jo.tpe), []interface{}{string(v)}, nil
}
