package main

import (
	"syscall"
)

func Read(nfd int, b []byte) (n int, err error) {
	n, err = syscall.Read(nfd, b)
	if err != nil {
		if err == syscall.EAGAIN || err == syscall.EINTR {
			return 0, nil
		}
	}
	return n, err
}

func Write(nfd int, b []byte) (n int, err error) {
	n, err = syscall.Write(nfd, b)
	if err != nil {
		if err == syscall.EAGAIN {
			return 0, nil
		}
	}
	return n, err
}

//TODO: use epoll
