package main

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func serve(a, b, c chan struct{}) {
	sfd, err := TcpServer(6666)
	if err != nil {
		fmt.Println("tcpServe error:", err)
	}
	fmt.Println("server ready")
	a <- struct{}{}
	<-b
	nfd, err := Accept(sfd)
	if err != nil {
		fmt.Println("accept error: ", err)
	}
	fmt.Println("accept nfd: ", nfd)
	buf := make([]byte, 11)
	n, err := Read(nfd, buf)
	if err != nil {
		fmt.Println("read error:", err)
	}
	fmt.Printf("read %d bytes\n", n)
	n, err = Write(nfd, buf)
	if err != nil {
		fmt.Println("write error:", err)
	}
	fmt.Printf("write %d bytes\n", n)
	c <- struct{}{}
}

func TestNet(t *testing.T) {
	a, b, c := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go serve(a, b, c)
	<-a

	nfd, err := Dial([4]byte{127, 0, 0, 1}, 6666)
	assert.Nil(t, err)
	fmt.Println("dial nfd :", nfd)
	b <- struct{}{}

	n, err := Write(nfd, []byte("hello gedis"))
	assert.Nil(t, err)
	assert.Equal(t, 11, n)
	<-c //wait for server write

	buf := make([]byte, 11)
	n, err = Read(nfd, buf)
	assert.Nil(t, err)
	assert.Equal(t, 11, n)

	err = Close(nfd)
	assert.Nil(t, err)
}
