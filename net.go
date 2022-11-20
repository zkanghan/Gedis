package main

import (
	"net"
	"syscall"
)

func Read(tcpConn *net.TCPConn, b []byte) (n int, err error) {
	fd, err := getConnFD(tcpConn)
	if err != nil {
		return 0, err
	}
	n, err = syscall.Read(fd, b)
	if err != nil {
		if err == syscall.EAGAIN || err == syscall.EINTR {
			return 0, nil
		}
	}
	return n, err
}

func getConnFD(tcpConn *net.TCPConn) (int, error) {
	file, err := tcpConn.File()
	if err != nil {
		return 0, err
	}
	return int(file.Fd()), nil
}

func Write(tcpConn *net.TCPConn, b []byte) (n int, err error) {
	fd, err := getConnFD(tcpConn)
	if err != nil {
		return 0, err
	}
	n, err = syscall.Write(fd, b)
	if err != nil {
		if err == syscall.EAGAIN {
			return 0, nil
		}
	}
	return n, err
}
