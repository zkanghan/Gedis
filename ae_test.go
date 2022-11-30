package main

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// writable event
var writeProc FileProc = func(loop *AeEventLoop, nfd int, extra any) {
	by := extra.([]byte)

	n, err := Write(nfd, by)
	if err != nil {
		fmt.Println("write error: ", err)
		return
	}
	fmt.Printf("write %v bytes\n", n)
	loop.RemoveFileEvent(nfd, AE_WRITABLE)
	return
}

var readProc FileProc = func(loop *AeEventLoop, nfd int, extra any) {
	b := make([]byte, 11)
	defer fmt.Println("readProc end")
	n, err := Read(nfd, b)
	if err != nil {
		fmt.Println(err)
		return
	}
	if n == 0 {
		return
	}
	fmt.Printf("read %v bytes\n", n)
	loop.AddFileEvent(nfd, AE_WRITABLE, writeProc, b)
	return
}

var ac FileProc = func(loop *AeEventLoop, fd int, extra any) {
	nfd, err := Accept(fd)
	if err != nil {
		fmt.Println("accept error: ", err)
		return
	}
	loop.AddFileEvent(nfd, AE_READABLE, readProc, nil)
}

var OneProc TimeProc = func(loop *AeEventLoop, id int, extra any) {
	t := extra.(*testing.T)
	assert.Equal(t, 1, id)
	fmt.Printf("once time event %v done\n", id)
}

var NormalProc TimeProc = func(loop *AeEventLoop, id int, extra any) {
	wg := extra.(chan struct{})
	fmt.Printf("normal time event %v done\n", id)
	wg <- struct{}{}
}

func TestAe(t *testing.T) {
	loop, err := NewAeEventLoop()
	assert.Nil(t, err)

	sfd, err := TcpServer(6666)
	assert.Nil(t, err)

	wg := make(chan struct{}, 3)
	loop.AddTimeEvent(AE_ONCE, 10, OneProc, t)
	loop.AddTimeEvent(AE_NORNAL, 10, NormalProc, wg)
	loop.AddFileEvent(sfd, AE_READABLE, ac, nil)
	go loop.AeMain()
	//  下面的充当客户端请求
	nfd, err := Dial([4]byte{127, 0, 0, 1}, 6666)
	assert.Nil(t, err)

	//测试写事件
	msg := "hello gedis"
	n, err := Write(nfd, []byte(msg))
	assert.Nil(t, err)
	assert.Equal(t, 11, n)
	// 测试读事件
	time.Sleep(time.Second)
	b := make([]byte, 11)
	n, err = Read(nfd, b)
	assert.Nil(t, err)
	assert.Equal(t, 11, n)

	<-wg
	<-wg
	<-wg

	loop.stopped = true
}
