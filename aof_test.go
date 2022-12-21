package main

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
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

	server.aofFileName = "test_aof_rewrite.aof"

	err = rewriteAppendOnlyFile(server.aofFileName)
	assert.Nil(t, err)

	for i := 0; i < 8; i++ {
		key := NewObject(STR, fmt.Sprintf("key%d", i))
		err := server.db.data.Delete(key)
		assert.Nil(t, err)
	}
	//test load from aof
	err = loadAppendOnlyFile(server.aofFileName)
	assert.Nil(t, err)

	var fi os.FileInfo
	fi, err = os.Stat(server.aofFileName)
	assert.Nil(t, err)

	assert.Equal(t, fi.Size(), server.aofCurrentSize)

	for i := 0; i < 8; i++ {
		key := NewObject(STR, fmt.Sprintf("key%d", i))
		val := NewObject(STR, fmt.Sprintf("val%d", i))
		e := server.db.data.Find(key)
		assert.NotNil(t, e)
		assert.Equal(t, val.StrVal(), e.Val.StrVal())
	}
}

func Test_catAppendOnlyExpireAtFile(t *testing.T) {
	cmd := cmdTable[2]

	s := catAppendOnlyExpireAtFile(&cmd, NewObject(STR, "key"), NewObject(STR, "5"))
	assert.Equal(t, "*3\r\n$9\r\npexpireat\r\n$3\r\nkey\r\n$13", s[:31])
}

func Test_catAppendOnlyGenericCommand(t *testing.T) {
	args := []*GObj{NewObject(STR, "set"), NewObject(STR, "key"), NewObject(STR, "val")}

	s := catAppendOnlyGenericCommand(args)
	assert.Equal(t, "*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$3\r\nval\r\n", s)

	args = []*GObj{NewObject(STR, "pexpireat"), NewObject(STR, "key"), NewObject(STR, "1234567890")}
	s = catAppendOnlyGenericCommand(args)
	assert.Equal(t, "*3\r\n$9\r\npexpireat\r\n$3\r\nkey\r\n$10\r\n1234567890\r\n", s)
}

func Test_autoRewriteAOF(t *testing.T) {
	err := InitServer()
	assert.Nil(t, err)

	server.aofRewriteMinSize = 1
	server.aofRewritePerc = 1
	server.aofFileName = "test_appendOnly.aof"

	ServerCron(server.aeloop, 0, 0)
}
