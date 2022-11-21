package main

import (
	"net"
	"syscall"
)

func Read(tcpConn *net.TCPConn, b []byte) (n int, err error) {
	fd := getConnFd(tcpConn)
	n, err = syscall.Read(fd, b)
	if err != nil {
		if err == syscall.EAGAIN || err == syscall.EINTR {
			return 0, nil
		}
	}
	return n, err
}

func Write(tcpConn *net.TCPConn, b []byte) (n int, err error) {
	fd := getConnFd(tcpConn)
	n, err = syscall.Write(fd, b)
	if err != nil {
		if err == syscall.EAGAIN {
			return 0, nil
		}
	}
	return n, err
}
