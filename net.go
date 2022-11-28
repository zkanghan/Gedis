package main

import (
	"log"
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

func Accept(fd int) (int, error) {
	nfd, _, err := syscall.Accept(fd)
	return nfd, err
}

func Dial(host [4]byte, port int) (int, error) {
	sfd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		log.Printf("init socket error: %#v \n", err)
		return -1, err
	}
	addr := syscall.SockaddrInet4{
		Port: port,
		Addr: host,
	}
	if err = syscall.Connect(sfd, &addr); err != nil {
		_ = syscall.Close(sfd)
		return -1, err
	}
	return sfd, nil
}

func TcpServer(port int) (int, error) {
	sfd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		log.Printf("init socket error: %#v \n", err)
		return -1, err
	}
	//set reuse port
	//err = syscall.SetsockoptInt(sfd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, port)
	//if err != nil {
	//	log.Printf("set socket reuse port error: %#v \n", err)
	//	_ = syscall.Close(sfd)
	//	return -1, err
	//}
	addr := syscall.SockaddrInet4{Port: port}
	if err = syscall.Bind(sfd, &addr); err != nil {
		log.Printf("bind port error %#v \n", err)
		_ = syscall.Close(sfd)
		return -1, err
	}

	if err = syscall.Listen(sfd, syscall.SOMAXCONN); err != nil {
		log.Printf("listen socket error %#v \n", err)
		_ = syscall.Close(sfd)
		return -1, err
	}
	return sfd, nil
}

//TODO: use epoll
