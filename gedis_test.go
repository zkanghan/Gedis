package main

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func fillQuery(client *GedisClient, query string) {
	byteData := []byte(query)
	for _, v := range byteData {
		client.queryBuf[client.queryLen] = v
		client.queryLen += 1
	}
}

func printArgs(client *GedisClient) {
	fmt.Printf("the result: ")
	for _, a := range client.args {
		str := a.Val_.(string)
		fmt.Printf("(%s)", str)
	}
	fmt.Println()
}

func TestHandleInlineBuf(t *testing.T) {
	client := NewClient(0)

	fillQuery(client, "set key value\r\n")
	ok, err := handleInlineBuf(client)
	assert.Nil(t, err)
	assert.Equal(t, true, ok)
	printArgs(client)

	fillQuery(client, "set key\r\n")
	ok, err = handleInlineBuf(client)
	assert.Nil(t, err)
	assert.Equal(t, true, ok)
	printArgs(client)

	fillQuery(client, "key value\r\n")
	ok, err = handleInlineBuf(client)
	assert.Nil(t, err)
	assert.Equal(t, true, ok)
	printArgs(client)

	fillQuery(client, "set value\r\n")
	ok, err = handleInlineBuf(client)
	assert.Nil(t, err)
	assert.Equal(t, true, ok)
	printArgs(client)

	fillQuery(client, "set value")
	ok, err = handleInlineBuf(client)
	assert.Nil(t, err)
	assert.Equal(t, false, ok)

	fillQuery(client, "set ")
	ok, err = handleInlineBuf(client)
	assert.Nil(t, err)
	assert.Equal(t, false, ok)
}

func TestHandleBulkBuf(t *testing.T) {
	client := NewClient(0)

	//legal command
	fillQuery(client, "*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$3\r\nval\r\n")
	ok, err := handleBulkBuf(client)
	assert.Nil(t, err)
	assert.Equal(t, true, ok)
	printArgs(client)

	// incomplete command at CRLF
	fillQuery(client, "*3\r")
	ok, err = handleBulkBuf(client)
	assert.Nil(t, err)
	assert.Equal(t, false, ok)

	fillQuery(client, "\n$3\r\nset\r\n$3\r\nkey\r\n$3\r\nval\r\n")
	ok, err = handleBulkBuf(client)
	assert.Nil(t, err)
	assert.Equal(t, true, ok)
	printArgs(client)

	// incomplete command at string
	fillQuery(client, "*3\r\n$3\r\nse")
	ok, err = handleBulkBuf(client)
	assert.Nil(t, err)
	assert.Equal(t, false, ok)

	fillQuery(client, "t\r\n$3\r\nkey\r\n$3\r\nval\r\n")
	ok, err = handleBulkBuf(client)
	assert.Nil(t, err)
	assert.Equal(t, true, ok)
	printArgs(client)

	//incomplete command at $
	fillQuery(client, "*3\r\n$")
	ok, err = handleBulkBuf(client)
	assert.Nil(t, err)
	assert.Equal(t, false, ok)

	fillQuery(client, "3\r\nset\r\n$3\r\nkey\r\n$3\r\nval\r\n")
	ok, err = handleBulkBuf(client)
	assert.Nil(t, err)
	assert.Equal(t, true, ok)
	printArgs(client)
}

func TestProcessQueryBuf(t *testing.T) {
	err := InitServer()
	assert.Nil(t, err)

	client := NewClient(0)
	fillQuery(client, "*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$3\r\nval\r\n")
	err = client.ProcessQueryBuf()
	assert.Nil(t, err)
	assert.Equal(t, 3, len(client.args))

	fillQuery(client, "set key val\r\n")
	err = client.ProcessQueryBuf()
	assert.Nil(t, err)
	assert.Equal(t, 3, len(client.args))
}
