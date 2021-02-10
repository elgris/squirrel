package sqrl

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRowsAffected(t *testing.T) {
	testErr := errors.New("test error")

	c, err := RowsAffected(nil, testErr)
	assert.Zero(t, c)
	assert.Equal(t, testErr, err)

	c, err = RowsAffected(&resultStub{err: testErr}, nil)
	assert.Zero(t, c)
	assert.Equal(t, testErr, err)

	c, err = RowsAffected(&resultStub{rowsAffected: 42}, nil)
	assert.Equal(t, int64(42), c)
	assert.NoError(t, err)
}

func TestLastInsertId(t *testing.T) {
	testErr := errors.New("test error")

	c, err := LastInsertId(nil, testErr)
	assert.Zero(t, c)
	assert.Equal(t, testErr, err)

	c, err = LastInsertId(&resultStub{err: testErr}, nil)
	assert.Zero(t, c)
	assert.Equal(t, testErr, err)

	c, err = LastInsertId(&resultStub{lastInsertId: 42}, nil)
	assert.Equal(t, int64(42), c)
	assert.NoError(t, err)
}
