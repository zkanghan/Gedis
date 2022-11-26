package main

import (
	"errors"
	"net"
)

const (
	PORT = ":8888"
)

// global variable
var server GedisServer

func InitServer() error {
	l, err := net.Listen("tcp", PORT)
	if err != nil {
		return err
	}
	tcpListener, ok := l.(*net.TCPListener)
	if !ok {
		return errors.New("listener is not tcp")
	}
	server = GedisServer{
		listener: tcpListener,
		port:     8888,
		db: &GedisDB{
			data:   NewDict(DictType{HashFunc: HashStr, EqualFunc: EqualStr}),
			expire: NewDict(DictType{HashFunc: HashStr, EqualFunc: EqualStr}),
		},
		clients: make(map[int]*GedisClient),
		aeloop:  NewAeEventLoop(),
	}
	return nil
}
