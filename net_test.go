package main

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
)

func TestNet(t *testing.T) {
	err := InitServer()
	assert.Nil(t, err)
	fmt.Println("blocking...")
	tcpConn, err := server.listener.AcceptTCP()
	assert.Nil(t, err)

	fd := getConnFd(tcpConn)

	b := make([]byte, 20)
	n, err := Read(tcpConn, b)
	assert.Nil(t, err)
	assert.Equal(t, 0, n)

	n, err = Read(tcpConn, b)
	assert.Nil(t, err)
	assert.Equal(t, 0, n)

	b = []byte("hello gedis")
	n, err = Write(tcpConn, b)
	assert.Nil(t, err)
	assert.Equal(t, 11, n)

	nfd := getConnFd(tcpConn)
	assert.Equal(t, fd, nfd)
}

func TestNet2(t *testing.T) {
	_, err := net.Dial("tcp", "127.0.0.1:8888")
	assert.Nil(t, err)

}
