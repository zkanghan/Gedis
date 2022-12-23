package main

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
)

type CmdType int8

const (
	REPLY_UNKNOWN_CMD string = "-ERR unknown command\r\n"
	REPLY_WRONG_ARITY string = "-ERR wrong number of arguments\r\n"
	REPLY_NIL         string = "+nil\r\n"
	REPLY_WRONG_TYPE  string = "-ERR invalid type\r\n"
	REPLY_OK          string = "+ok\r\n"
	REPLY_ZERO        string = ":0\r\n"
	REPLY_ONE         string = ":1\r\n"

	CMD_UNKNOWN CmdType = 0
	CMD_INLINE  CmdType = 1
	CMD_BULK    CmdType = 2

	GEDIS_IO_BUF      int = 1024 * 8
	GEDIS_MAX_CMD_BUF int = 1024 * 4
)

type GedisClient struct {
	//conn     *net.TCPConn
	nfd      int
	db       *GedisDB
	args     []*GObj
	reply    *List  // the type of node is string
	sentLen  int    //the length that has been sent
	queryBuf []byte //client buffer
	queryLen int    //the effective length of the buffer
	cmdType  CmdType
	bulkCnt  int //the number of bulk strings to be read
	bulkLen  int //the length of string that need to read At present
}

func NewClient(nfd int) *GedisClient {
	var client GedisClient
	client.nfd = nfd
	client.db = server.db
	client.queryBuf = make([]byte, GEDIS_IO_BUF)
	client.reply = ListCreate(ListType{EqualFunc: EqualStr}) //the type of node is string
	return &client
}

func freeClient(client *GedisClient) {
	delete(server.clients, client.nfd)
	server.aeloop.RemoveFileEvent(client.nfd, AE_READABLE)
	server.aeloop.RemoveFileEvent(client.nfd, AE_WRITABLE)

	if err := Close(client.nfd); err != nil {
		log.Printf("close client conn error: %v ", err)
	}
}

func resetClient(client *GedisClient) {
	client.bulkCnt = 0
	client.cmdType = CMD_UNKNOWN
}

func (client *GedisClient) AddReply(str string) {
	node := NewObject(STR, str)
	client.reply.TailPush(node)
}

func (client *GedisClient) ProcessQueryBuf() error {
	for client.queryLen > 0 {
		if client.cmdType == CMD_UNKNOWN { // the command have not processed currently
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
				server.aeloop.AddFileEvent(client.nfd, AE_WRITABLE, SendReplyToClient, client)
			}
		} else {
			break //incomplete command
		}
	}
	return nil
}

// return the index of next CRLF, -1 will be returned if didn't find
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
	//  find a complete command
	subs := strings.Split(string(client.queryBuf[:index]), " ")
	client.skipCRLF(index)
	client.args = make([]*GObj, len(subs))
	for i, v := range subs {
		client.args[i] = NewObject(STR, v)
	}
	return true, nil
}

// note that it will skip a CRLF behind the number
func getNumInQuery(client *GedisClient, start, end int) (int, error) {
	num, err := strconv.Atoi(string(client.queryBuf[start:end]))
	client.skipCRLF(end)
	return num, err
}

// skip a \r\n in query buffer
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
		client.bulkLen = 0
		client.bulkCnt -= 1
	}
	return true, nil
}

type GedisServer struct {
	sfd     int
	port    int
	db      *GedisDB
	clients map[int]*GedisClient
	aeloop  *AeEventLoop //also global unique

	//   AOF
	aofFileName        string // Name of the AOF file
	aofRewriteMinSize  int64  // the AOF file is at least N bytes
	aofCurrentSize     int64  // AOF current size
	aofRewritePerc     int64  // Rewrite AOF if growth > it
	aofRewriteBaseSize int64  // AOF size on latest startup or rewrite
	aofBuf             string // AOF buffer, written before entering the event loop
	aofRewriteChan     chan bool
	aofRewriteBuf      []byte //Hold changes during an AOF rewrite
}

type CommandProc func(client *GedisClient)
type GedisCommand struct {
	name  string
	arity int
	proc  CommandProc
}

var cmdTable = []GedisCommand{
	{"get", 2, getCommand},
	{"set", 3, setCommand},
	{"expire", 3, expireCommand},
	{"ttl", 2, ttlCommand},
	{"pexpireat", 3, pexpireatCommand},
	//TODO: list command & hash command
}

//get a string
var getCommand CommandProc = func(client *GedisClient) {
	key := client.args[1]
	expireIfNeeded(key)
	entry := server.db.data.Find(key)
	if entry == nil {
		client.AddReply(REPLY_NIL)
		return
	}
	if entry.Val.Type_ != STR {
		client.AddReply(REPLY_WRONG_TYPE)
		return
	}
	client.AddReply(fmt.Sprintf("$%d\r\n", len(entry.Val.StrVal())))
	client.AddReply(fmt.Sprintf("%s\r\n", entry.Val.StrVal()))
	return
}

// set a string value, and will remove expire time of key
var setCommand CommandProc = func(client *GedisClient) {
	key, val := client.args[1], client.args[2]
	entry := server.db.data.Find(key)
	if entry != nil && entry.Val.Type_ != STR {
		client.AddReply(REPLY_WRONG_TYPE)
		return
	}
	server.db.data.Set(key, val)
	_ = removeExpire(key)
	client.AddReply(REPLY_OK)
}

func loadDataFromDisk() error {
	return loadAppendOnlyFile(server.aofFileName)
}

func lookUpCommand(name string) *GedisCommand {
	for _, cmd := range cmdTable {
		if cmd.name == name {
			return &cmd
		}
	}
	return nil
}

func ProcessCommand(client *GedisClient) {
	cmd := lookUpCommand(client.args[0].StrVal())
	if cmd == nil {
		client.AddReply(REPLY_UNKNOWN_CMD)
		resetClient(client)
		return
	} else if cmd.arity > 0 && cmd.arity != len(client.args) {
		client.AddReply(REPLY_WRONG_ARITY)
		resetClient(client)
		return
	}
	//exec the command
	cmd.proc(client)
	//persist the command
	propagate(cmd, client.args)
	resetClient(client)
}

//propagate the specified command to AOF
func propagate(cmd *GedisCommand, args []*GObj) {
	_ = feedAppendOnlyFile(cmd, args)
}

type GedisDB struct {
	data *Dict
	//val is a unix timestamp
	expire *Dict
}

//================================= Expire =================================
func expireIfNeeded(key *GObj) {
	entry := server.db.expire.Find(key)
	// no expire
	if entry == nil {
		return
	}
	now := GetTimeMs()
	//  key haven't expired
	if now < entry.Val.IntVal() {
		return
	}
	_ = server.db.expire.Delete(key)
	_ = server.db.data.Delete(key)
}

func removeExpire(key *GObj) error {
	return server.db.expire.Delete(key)
}

func setExpire(key *GObj, timeMS string) {
	val := NewObject(STR, timeMS)
	server.db.expire.Set(key, val)
}

var expireCommand CommandProc = func(client *GedisClient) {
	key := client.args[1]
	if server.db.data.Find(key) == nil {
		client.AddReply(REPLY_ZERO)
		return
	}
	sc := client.args[2].IntVal()
	if sc <= 0 {
		client.AddReply(REPLY_ZERO)
		return
	}
	expireTime := time.Now().Add(time.Second * time.Duration(sc)).UnixMilli()
	setExpire(key, strconv.FormatInt(expireTime, 10))
	client.AddReply(REPLY_ONE)
}

//return -1 means the specified key no expire, or -2 means the specified key not exist
func getExpire(key *GObj) int64 {
	// key not exist
	if server.db.data.Find(key) == nil {
		return -2
	}
	// key no expire
	entry := server.db.expire.Find(key)
	if entry == nil {
		return -1
	}
	ttl := time.UnixMilli(entry.Val.IntVal()).Sub(time.Now()).Seconds()
	//if the key expired, we consider it was deleted in logic
	if ttl < 0 {
		return -2
	}
	return int64(ttl)
}

//return -1 if the specified key no exist or no expire is associated with this key
var ttlCommand CommandProc = func(client *GedisClient) {
	key := client.args[1]
	ttl := getExpire(key)
	client.AddReply(fmt.Sprintf(":%d\r\n", ttl))
}

var pexpireatCommand CommandProc = func(client *GedisClient) {
	key := client.args[1]
	exTime := client.args[2].IntVal()
	//invalid expire time or key
	if server.db.data.Find(key) == nil || exTime <= 0 {
		client.AddReply(REPLY_ZERO)
		return
	}
	setExpire(key, client.args[2].StrVal())
	client.AddReply(REPLY_ONE)
}

func main() {
	err := InitServer()
	if err != nil {
		panic("init server error: " + err.Error())
	}
	if err = loadDataFromDisk(); err != nil {
		panic("load data from disk error: " + err.Error())
	}
	server.aeloop.AddTimeEvent(AE_NORNAL, 1, ServerCron, nil)
	server.aeloop.AddFileEvent(server.sfd, AE_READABLE, AcceptHandler, nil)
	log.Println("gedis server is up")
	server.aeloop.AeMain()
}
