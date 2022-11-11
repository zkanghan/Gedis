package main

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
	"time"
)

// 向conn写入数据
var writeProc FileProc = func(loop *AeEventLoop, conn *net.TCPConn, extra any) {
	by := extra.([]byte)

	n, err := conn.Write(by)
	if err != nil {
		fmt.Println("write error: ", err)
		return
	}
	fmt.Printf("write %v bytes\n", n)
	loop.RemoveFileEvent(conn, AE_WRITABLE)
	return
}

var readProc FileProc = func(loop *AeEventLoop, conn *net.TCPConn, extra any) {
	b := make([]byte, 11)

	err := conn.SetReadDeadline(time.Now().Add(time.Millisecond * 5))
	if err != nil {
		fmt.Println(err)
		return
	}
	n, err := conn.Read(b) //我超，就是在这里阻塞住了
	if err != nil {        //未读取到数据或者出错必需要返回
		need := err.(*net.OpError)
		if !need.Timeout() {
			fmt.Println("read error: ", err)
		}
		return
	}
	fmt.Printf("read %v bytes\n", n)
	loop.AddFileEvent(conn, AE_WRITABLE, writeProc, b)
	return
}

func ac() {
	err := server.listener.SetDeadline(time.Now().Add(time.Millisecond * 12))
	if err != nil {
		fmt.Println("set listener error: ", err)
		return
	}
	for {
		conn, err := server.listener.Accept()
		if err != nil {
			opErr := err.(*net.OpError)
			if opErr.Timeout() { //expected read time out error
				return
			}
			fmt.Printf("%+v\n", err)
			return
		}
		tcpConn, ok := conn.(*net.TCPConn)
		if !ok { //不是tcp连接
			return
		}
		client := NewClient(tcpConn)
		server.clients[getConnFd(tcpConn)] = client
		// 连接建立成功，事件可读，处理客户端请求
		server.aeloop.AddFileEvent(tcpConn, AE_READABLE, readProc, client)
	}
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
	err := InitServer()
	assert.Nil(t, err)

	wg := make(chan struct{}, 3)
	server.aeloop.AddTimeEvent(AE_ONCE, 10, OneProc, t)
	server.aeloop.AddTimeEvent(AE_NORNAL, 10, NormalProc, wg)
	go server.aeloop.AeMain(ac)
	//  下面的充当客户端请求
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1%s", PORT))
	assert.Nil(t, err)

	//测试写事件
	msg := "hello gedis"
	n, err := conn.Write([]byte(msg))
	assert.Nil(t, err)
	assert.Equal(t, 11, n)
	// 测试读事件
	b := make([]byte, 11)
	n, err = conn.Read(b)
	assert.Nil(t, err)
	assert.Equal(t, 11, n)

	<-wg
	<-wg
	<-wg
	server.aeloop.stopped = true
}
