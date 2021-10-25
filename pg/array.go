package pg

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/SharperShape/sqrl"
)

// Array converts value into Postgres Array
//
// Valid values are slices or arrays of arbitrary depth
// with elements of type string, int, uint and float elements of any bit size
// Example: []int, [][]uint16, [2][2]int, []string
func Array(arr interface{}) sqrl.Sqlizer {
	return array{arr}
}

// ToSql builds the query into a SQL string and bound args.
func (a array) ToSql() (string, []interface{}, error) {
	if err := checkArrayType(a.value); err != nil {
		return "", nil, err
	}

	buf := &bytes.Buffer{}
	marshalArray(reflect.ValueOf(a.value), buf)
	return "?", []interface{}{buf.String()}, nil
}

type marshaler func(reflect.Value, *bytes.Buffer)

var marshalers = map[reflect.Kind]marshaler{
	reflect.Uint:    marshalUint,
	reflect.Uint8:   marshalUint,
	reflect.Uint16:  marshalUint,
	reflect.Uint32:  marshalUint,
	reflect.Uint64:  marshalUint,
	reflect.Int:     marshalInt,
	reflect.Int8:    marshalInt,
	reflect.Int16:   marshalInt,
	reflect.Int32:   marshalInt,
	reflect.Int64:   marshalInt,
	reflect.Float32: marshalFloat,
	reflect.Float64: marshalFloat,
	reflect.String:  marshalString,
}

var validElems = makeValidElems(marshalers)

type array struct {
	value interface{}
}

func checkArrayType(src interface{}) error {
	t := reflect.TypeOf(src)
	k := t.Kind()
	if k != reflect.Slice && k != reflect.Array {
		return fmt.Errorf("Expected value of type slice or array, got %s", k)
	}

	for k == reflect.Slice || k == reflect.Array {
		t = t.Elem()
		k = t.Kind()
	}

	if _, ok := marshalers[k]; !ok {
		return fmt.Errorf("Expected element of type %s, got: %s", validElems, k)
	}
	return nil
}

func marshalArray(v reflect.Value, buf *bytes.Buffer) {
	l := v.Len()
	if l == 0 {
		buf.WriteString("{}")
		return
	}

	k := v.Type().Elem().Kind()
	marshalElem, ok := marshalers[k]
	if !ok {
		marshalElem = marshalArray
	}

	buf.WriteRune('{')
	marshalElem(v.Index(0), buf)
	for i := 1; i < v.Len(); i++ {
		buf.WriteRune(',')
		marshalElem(v.Index(i), buf)
	}
	buf.WriteRune('}')
}

func marshalInt(v reflect.Value, buf *bytes.Buffer) {
	buf.WriteString(strconv.FormatInt(v.Int(), 10))
}

func marshalUint(v reflect.Value, buf *bytes.Buffer) {
	buf.WriteString(strconv.FormatUint(v.Uint(), 10))
}

func marshalFloat(v reflect.Value, buf *bytes.Buffer) {
	buf.WriteString(strconv.FormatFloat(v.Float(), 'f', -1, 64))
}

func marshalString(v reflect.Value, buf *bytes.Buffer) {
	buf.WriteString(strconv.Quote(v.String()))
}

func makeValidElems(m map[reflect.Kind]marshaler) string {
	s := make([]string, 0, len(m))
	for k := range m {
		s = append(s, k.String())
	}

	return strings.Join(s, ", ")
}
