package main

import (
	"errors"
	"log"
	"net"
	"strconv"
	"strings"
)

type CmdType int8

const (
	CMD_UNKNOWN CmdType = 0
	CMD_INLINE  CmdType = 1
	CMD_BULK    CmdType = 2

	GEDIS_IO_BUF      int = 1024 * 8
	GEDIS_MAX_CMD_BUF int = 1024 * 4
)

type GedisClient struct {
	conn     *net.TCPConn
	db       *GedisDB
	args     []*GObj
	reply    *List // 节点为STR类型，存储需要响应的数据
	sentLen  int
	queryBuf []byte //读取缓冲区
	queryLen int    //已读的长度
	cmdType  CmdType
	bulkCnt  int //the number of bulk strings to be read
	bulkLen  int //the length of string that need to read At present
}

func (client *GedisClient) ProcessQueryBuf() error {
	for client.queryLen > 0 {
		if client.cmdType == CMD_UNKNOWN { // 命令第一次被处理
			if client.queryBuf[0] == '*' {
				client.cmdType = CMD_BULK
			} else {
				client.cmdType = CMD_INLINE
			}
		}
		// translate query to args with RESP protocol
		var ok bool
		var err error
		if client.cmdType == CMD_INLINE {
			ok, err = handleInlineBuf(client)
		} else if client.cmdType == CMD_BULK {
			ok, err = handleBulkBuf(client)
		} else {
			return errors.New("unknown Gedis command type")
		}
		if err != nil {
			return err
		}
		if ok { // commands can be executed
			if len(client.args) == 0 { //nothing to do
				resetClient(client)
			} else {
				ProcessCommand(client)
				resetClient(client)
			}
		} else {
			break //incomplete command
		}
	}
	return nil
}

// 查找换行符 \r\n，未找到返回-1
func findCRLFInQuery(client *GedisClient) (int, error) {
	index := strings.Index(string(client.queryBuf[:client.queryLen]), "\r\n")
	if index < 0 && client.queryLen > GEDIS_MAX_CMD_BUF { //缓冲区被读满了还没出现换行符
		return index, errors.New("command is too big")
	}
	return index, nil
}

//  if the inline command is ready, it returns true
func handleInlineBuf(client *GedisClient) (bool, error) {
	index, err := findCRLFInQuery(client)
	if index < 0 { //未找到或出错，不进行处理
		return false, err
	}
	//  到这里说明找到一条完整的inline格式命令，按空格切割
	subs := strings.Split(string(client.queryBuf[:index]), " ")
	client.skipCRLF(index)
	client.args = make([]*GObj, len(subs))
	for i, v := range subs {
		client.args[i] = NewObject(STR, v)
	}
	return true, nil
}

// 将client.client.queryBuf[start:end] 的字符串转化为数字，注意截取数字后会跳过CRLF符
func getNumInQuery(client *GedisClient, start, end int) (int, error) {
	num, err := strconv.Atoi(string(client.queryBuf[start:end]))
	client.skipCRLF(end)
	return num, err
}

// 从index位置跳过 \r\n
func (client *GedisClient) skipCRLF(index int) {
	client.queryBuf = client.queryBuf[index+2:]
	client.queryLen -= index + 2
}

// if the bulk command is completely, handleBulkBuf returns true, the Rules of parsing as:
//For Simple Strings, the first byte is "+"
//For Errors, the first byte is "-"
//For Integers, the first byte is ":"
//For Bulk Strings, the first byte is "$"
//For Arrays, the first byte is "*"
// example: SET key value  --> *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
func handleBulkBuf(client *GedisClient) (bool, error) {
	if client.bulkCnt == 0 { //if first time to handle the bulk command
		index, err := findCRLFInQuery(client)
		if index < 0 {
			return false, err
		}
		// skip the beginning char '*'
		bCnt, err := getNumInQuery(client, 1, index)
		if err != nil {
			return false, err
		}
		if bCnt == 0 {
			return true, nil
		}
		client.bulkCnt = bCnt
		client.args = make([]*GObj, bCnt)
	}
	// read every string according to bNum
	for client.bulkCnt > 0 {
		if client.bulkLen == 0 {
			index, err := findCRLFInQuery(client)
			if index < 0 {
				return false, err
			}
			if client.queryBuf[0] != '$' {
				return false, errors.New("expect $ for bulk length")
			}
			// skip the char '$'
			bLen, err := getNumInQuery(client, 1, index)
			if err != nil || bLen == 0 {
				return false, err
			}
			if bLen > GEDIS_MAX_CMD_BUF {
				return false, errors.New("bulk command is too big")
			}
			client.bulkLen = bLen
		}
		// read the bulk string
		if client.queryLen < client.bulkLen+2 { //if this bulk command is incomplete
			return false, nil
		}
		index := client.bulkLen
		client.args[len(client.args)-client.bulkCnt] = NewObject(STR, string(client.queryBuf[:index]))
		client.skipCRLF(index)
		//reset client
		client.bulkLen = 0
		client.bulkCnt -= 1
	}
	return true, nil
}

type GedisServer struct {
	listener *net.TCPListener
	port     int
	db       *GedisDB
	clients  map[int]*GedisClient
	aeloop   *AeEventLoop //also global unique
}

type GedisCommand struct {
}

func ProcessCommand(client *GedisClient) {
	//TODO: finish it
}

type GedisDB struct {
}

func NewClient(conn *net.TCPConn) *GedisClient {
	var client GedisClient
	client.conn = conn
	client.db = server.db
	client.queryBuf = make([]byte, GEDIS_IO_BUF)
	client.reply = ListCreate(ListType{EqualFunc: EqualStr}) //链表节点为STR类型
	return &client
}

func freeClient(client *GedisClient) {

}

func resetClient(client *GedisClient) {
	// TODO: finish reset
	client.cmdType = CMD_UNKNOWN
}

func main() {
	err := InitServer()
	if err != nil {
		log.Println("init server error: ", err)
	}
	server.aeloop.AddTimeEvent(AE_NORNAL, 100, ServerCron, nil)
	log.Println("gedis server is up")
	server.aeloop.AeMain(AcceptHandler)
}
