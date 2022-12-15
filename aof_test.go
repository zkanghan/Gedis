package main

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_AOF(t *testing.T) {
	err := InitServer()
	assert.Nil(t, err)

	for i := 0; i < 8; i++ {
		key := NewObject(STR, fmt.Sprintf("key%d", i))
		val := NewObject(STR, fmt.Sprintf("val%d", i))
		err = server.db.data.Add(key, val)
		assert.Nil(t, err)
	}

	testFileName := "test_aof_rewrite.aof"

	err = rewriteAppendOnlyFile(testFileName)
	assert.Nil(t, err)

	for i := 0; i < 8; i++ {
		key := NewObject(STR, fmt.Sprintf("key%d", i))
		err := server.db.data.Delete(key)
		assert.Nil(t, err)
	}
	//test load from aof
	err = loadAppendOnlyFile(testFileName)
	assert.Nil(t, err)

	for i := 0; i < 8; i++ {
		key := NewObject(STR, fmt.Sprintf("key%d", i))
		val := NewObject(STR, fmt.Sprintf("val%d", i))
		e := server.db.data.Find(key)
		assert.NotNil(t, e)
		assert.Equal(t, val.StrVal(), e.Val.StrVal())
	}
}
