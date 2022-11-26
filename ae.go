package main

import (
	"log"
	"net"
	"time"
)

// FeType define the type of event
type FeType int8
type TeType int8

const (
	AE_READABLE FeType = 1
	AE_WRITABLE FeType = 2

	AE_NORNAL TeType = 1
	AE_ONCE   TeType = 2
)

type FileProc func(loop *AeEventLoop, nfd int, extra any)
type TimeProc func(loop *AeEventLoop, id int, extra any)

// AcceptHandler blocked to wait for connection
func AcceptHandler() {
	err := server.listener.SetDeadline(time.Now().Add(time.Millisecond * 1))
	if err != nil {
		log.Println("set listener error: ", err)
		return
	}
	for {
		tcpConn, err := server.listener.AcceptTCP()
		if err != nil {
			opErr := err.(*net.OpError)
			if opErr.Timeout() {
				return
			}
			//  if isn't time out error
			log.Println("accept tcp error: ", err)
			return
		}
		client := NewClient(tcpConn)
		server.clients[client.nfd] = client
		// the connection can be read
		server.aeloop.AddFileEvent(client.nfd, AE_READABLE, ReadQueryFromClient, client)
	}

}

// ReadQueryFromClient the 'extra' should store the client
var ReadQueryFromClient FileProc = func(loop *AeEventLoop, nfd int, extra any) {
	client := extra.(*GedisClient)
	//expand query buffer's capacity if it is less than the max command capacity，
	if len(client.queryBuf)-client.queryLen < GEDIS_MAX_CMD_BUF {
		client.queryBuf = append(client.queryBuf, make([]byte, GEDIS_MAX_CMD_BUF)...)
	}

	// no blocked read
	n, err := Read(nfd, client.queryBuf[client.queryLen:])
	if err != nil {
		log.Printf("client %v read error: %v", nfd, err)
		freeClient(client)
		return
	}
	if n == 0 {
		return
	}
	client.queryLen += n

	err = client.ProcessQueryBuf()
	if err != nil {
		log.Printf("process query buf err:%v", err)
		freeClient(client)
		return
	}
}

var ServerCron TimeProc = func(loop *AeEventLoop, id int, extra any) {
	// TODO: check the dict
}

var SendReplyToClient FileProc = func(loop *AeEventLoop, nfd int, extra any) {
	client := extra.(*GedisClient)
	for client.reply.Length() > 0 {
		rep := client.reply.First()
		buf := []byte(rep.Val.StrVal())
		bufLen := len(buf)
		if client.sentLen < bufLen {
			n, err := Write(nfd, buf[client.sentLen:])
			if err != nil {
				log.Println("sent reply error: ", err)
				freeClient(client)
				return
			}
			client.sentLen += n
			if client.sentLen == bufLen {
				client.reply.DelNode(rep)
				client.sentLen = 0
			} else {
				break
			}
		}
	}
	if client.reply.Length() == 0 { //finish write
		client.sentLen = 0
		loop.RemoveFileEvent(nfd, AE_WRITABLE)
	}
}

// AeFileEvent deal with net i/o between server and client
type AeFileEvent struct {
	connection int    //net FD
	mask       FeType //type of file event
	proc       FileProc
	extra      interface{}
}

type AeTimeEvent struct {
	id       int
	mask     TeType
	when     int64 //when the time event will happen
	interval int64
	proc     TimeProc
	extra    interface{}
	next     *AeTimeEvent
}

type AeEventLoop struct {
	FileEvents      map[int]*AeFileEvent
	TimeEventsHead  *AeTimeEvent
	nextTimeEventID int
	stopped         bool
}

func NewAeEventLoop() *AeEventLoop {
	return &AeEventLoop{
		FileEvents:      make(map[int]*AeFileEvent),
		nextTimeEventID: 1,
		stopped:         false,
	}
}

// determine the index via connection and event type
func getFeKey(nfd int, mask FeType) int {
	if mask == AE_READABLE {
		return nfd
	}
	return -1 * nfd
}

// get file descriptor by connection
func getConnFd(conn *net.TCPConn) int {
	rawConn, err := conn.SyscallConn()
	if err != nil {
		log.Println("get raw connection error: ", err)
		return 0
	}
	var FD int
	err = rawConn.Control(func(fd uintptr) {
		FD = int(fd)
	})
	if err != nil {
		log.Println("executing raw connection's custom function error: ", err)
		return 0
	}
	return FD
}

func (loop *AeEventLoop) AddFileEvent(nfd int, mask FeType, proc FileProc, extra interface{}) {
	fe := AeFileEvent{
		connection: nfd,
		mask:       mask,
		proc:       proc,
		extra:      extra,
	}
	loop.FileEvents[getFeKey(nfd, mask)] = &fe
}

func (loop *AeEventLoop) RemoveFileEvent(nfd int, mask FeType) {
	delete(loop.FileEvents, getFeKey(nfd, mask))
}

func GetTimeMs() int64 {
	return time.Now().UnixMilli()
}

// AddTimeEvent insert at the head of the linked list
func (loop *AeEventLoop) AddTimeEvent(mask TeType, interval int64, proc TimeProc, extra interface{}) int {
	nextID := loop.nextTimeEventID
	loop.nextTimeEventID++
	te := AeTimeEvent{
		id:       nextID,
		mask:     mask,
		interval: interval,
		when:     GetTimeMs() + interval,
		proc:     proc,
		extra:    extra,
		next:     loop.TimeEventsHead,
	}
	loop.TimeEventsHead = &te
	return nextID
}

func (loop *AeEventLoop) RemoveTimeEvent(id int) {
	p := loop.TimeEventsHead
	var pre *AeTimeEvent
	for p != nil {
		if p.id == id {
			if pre == nil {
				loop.TimeEventsHead = p.next
			} else {
				pre.next = p.next
			}
			p.next = nil
			break
		}
		pre = p
		p = p.next
	}
}

func (loop *AeEventLoop) AeProcess() {

	for _, fe := range loop.FileEvents {
		fe.proc(loop, fe.connection, fe.extra)
	}

	p := loop.TimeEventsHead
	now := GetTimeMs()
	for p != nil {
		if p.when <= now {
			p.proc(loop, p.id, p.extra)
			if p.mask == AE_ONCE {
				loop.RemoveTimeEvent(p.id)
			} else if p.mask == AE_NORNAL {
				p.when = GetTimeMs() + p.interval //set next trigger time
			}
		}
		p = p.next
	}
}

func (loop *AeEventLoop) AeMain(accept func()) {
	for !loop.stopped {
		accept()
		loop.AeProcess()
	}
}
