package main

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestRioWrite(t *testing.T) {
	f, err := os.OpenFile("test_rio.aof", os.O_APPEND|os.O_WRONLY, 0)
	defer func() {
		err = f.Close()
		assert.Nil(t, err)
	}()
	assert.Nil(t, err)
	r := NewRioWithFile(f)

	text := []byte("*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$3\r\nval\r\n")
	err = r.Write(text, 31)
	assert.Nil(t, err)
}

func Test_RioRead(t *testing.T) {
	f, err := os.OpenFile("test_rio.aof", os.O_RDONLY, 0)
	defer func() {
		err = f.Close()
		assert.Nil(t, err)
	}()
	assert.Nil(t, err)

	r := NewRioWithFile(f)
	buf := make([]byte, 100)
	err = r.Read(buf, 10)

	assert.Equal(t, []byte("*3\r\n$3\r\nse"), buf[:10])
}
