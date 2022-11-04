package main

import (
	"errors"
	"net"
)

//  conf 文件用负责 全局变量和项目初始化

const (
	PORT = ":8888"
)

var server *GedisServer

func InitServer() error {
	l, err := net.Listen("tcp", PORT)
	if err != nil {
		return err
	}
	tcpListener, ok := l.(*net.TCPListener)
	if !ok {
		return errors.New("listener is not tcp")
	}
	server.listener = tcpListener
	server.aeloop = NewAeEventLoop()
	return nil
}
