package main

import (
	"log"
	"net"
)

var gedisServer GedisServer

type CmdType int8

const (
	CMD_UNKNOWN CmdType = 1
	CMD_INLINE  CmdType = 2
	CMD_BULK    CmdType = 3

	GEDIS_IO_BUF     int = 1024 * 8
	GEDIS_MAX_BULK   int = 1024 * 4
	GEDIS_MAX_INLINE int = 1024 * 4
)

type GedisClient struct {
	conn     *net.TCPConn
	db       *GedisDB
	args     []*GObj
	reply    *List // 节点为STR类型
	queryBuf []byte
	queryLen int
	cmdType  CmdType
	bulkNum  int
	bulkLen  int
}

type GedisServer struct {
	listener *net.TCPListener //服务器的连接
	port     int
	db       *GedisDB
	clients  map[int]*GedisClient
	aeloop   *AeEventLoop
}

type GedisCommand struct {
}

type GedisDB struct {
}

func NewClient(conn *net.TCPConn) *GedisClient {
	var client GedisClient
	client.conn = conn
	client.db = server.db
	client.queryBuf = make([]byte, GEDIS_IO_BUF)
	client.reply = ListCreate(ListType{Equal: EqualStr}) //链表节点为STR类型
	return &client
}

func main() {
	err := InitServer()
	if err != nil {
		log.Println("init server error: ", err)
	}
	server.aeloop.AddTimeEvent(AE_NORNAL, 100, ServerCron, nil)
	log.Println("gedis server is up")
	server.aeloop.AeMain()
}
