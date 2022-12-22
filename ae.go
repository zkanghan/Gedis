package main

import (
	"log"
	"syscall"
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

type FileProc func(loop *AeEventLoop, fd int, extra any)
type TimeProc func(loop *AeEventLoop, id int, extra any)

var AcceptHandler FileProc = func(loop *AeEventLoop, fd int, extra any) {
	nfd, err := Accept(server.sfd)
	if err != nil {
		log.Println("accept error: ", err)
		return
	}
	client := NewClient(nfd)
	server.clients[client.nfd] = client
	//check max clients number limit
	if len(server.clients) > MAX_CLIENTS {
		errMsg := []byte("-ERR max number of clients reached\r\n")
		// it's a best effort error message, so don't check write errors
		_, _ = Write(client.nfd, errMsg)
		freeClient(client)
	}
	// the fd can be read
	server.aeloop.AddFileEvent(client.nfd, AE_READABLE, ReadQueryFromClient, client)

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

	if server.aofRewriteChan != nil {
		select {
		case flag := <-server.aofRewriteChan:
			close(server.aofRewriteChan)
			server.aofRewriteChan = nil
			bgRewriteDoneHandler(flag)
		default:
			break
		}
	}

	//If there is not a background saving/rewrite in progress check if we have to save/rewrite now
	if server.aofRewriteChan == nil && server.aofRewritePerc > 0 && server.aofCurrentSize > server.aofRewriteMinSize {
		base := int64(1)
		if server.aofRewriteBaseSize > 0 {
			base = server.aofRewriteBaseSize
		}

		growth := (server.aofCurrentSize * 100 / base) - 100
		if growth >= server.aofRewritePerc {
			_ = rewriteAppendOnlyFileBackground()
		}
	}

	flushAppendOnlyFile()

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
	fd    int
	mask  FeType
	proc  FileProc
	extra interface{}
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
	efd             int //epoll fd
	nextTimeEventID int
	stopped         bool
}

func NewAeEventLoop() (*AeEventLoop, error) {
	epollFD, err := syscall.EpollCreate1(0)
	if err != nil {
		return nil, err
	}
	return &AeEventLoop{
		FileEvents:      make(map[int]*AeFileEvent),
		nextTimeEventID: 1,
		efd:             epollFD,
		stopped:         false,
	}, nil
}

var FeEvMap = [3]uint32{0, syscall.EPOLLIN, syscall.EPOLLOUT}

// get the index in map by nfd and file event type
func getFeKey(nfd int, mask FeType) int {
	if mask == AE_READABLE {
		return nfd
	}
	return -1 * nfd
}

//gets the registered events in epoll
func (loop *AeEventLoop) getEpollEvent(nfd int) uint32 {
	var ev uint32 = 0
	if loop.FileEvents[getFeKey(nfd, AE_READABLE)] != nil {
		ev |= FeEvMap[AE_READABLE]
	}
	if loop.FileEvents[getFeKey(nfd, AE_WRITABLE)] != nil {
		ev |= FeEvMap[AE_WRITABLE]
	}
	return ev
}

func (loop *AeEventLoop) AddFileEvent(nfd int, mask FeType, proc FileProc, extra interface{}) {
	// epoll ctl
	op := syscall.EPOLL_CTL_ADD
	ev := loop.getEpollEvent(nfd)
	if ev != 0 {
		op = syscall.EPOLL_CTL_MOD
	}
	ev |= FeEvMap[mask]
	err := syscall.EpollCtl(loop.efd, op, nfd, &syscall.EpollEvent{Events: ev, Fd: int32(nfd)})
	if err != nil {
		log.Printf("epoll ctl err: %v\n", err)
		return
	}
	// ae ctl
	fe := AeFileEvent{
		fd:    nfd,
		mask:  mask,
		proc:  proc,
		extra: extra,
	}
	loop.FileEvents[getFeKey(nfd, mask)] = &fe
}

func (loop *AeEventLoop) RemoveFileEvent(nfd int, mask FeType) {
	// epoll ctl
	op := syscall.EPOLL_CTL_DEL
	ev := loop.getEpollEvent(nfd)
	ev ^= FeEvMap[mask]
	if ev != 0 {
		op = syscall.EPOLL_CTL_MOD
	}
	err := syscall.EpollCtl(loop.efd, op, nfd, &syscall.EpollEvent{
		Events: ev,
		Fd:     int32(nfd),
	})
	if err != nil {
		log.Printf("epoll del err: %v\n", err)
		return
	}
	//ae ctl
	delete(loop.FileEvents, getFeKey(nfd, mask))
}

func GetTimeMs() int64 {
	return time.Now().UnixMilli()
}

func (loop *AeEventLoop) nearestTime() int64 {
	var nearest int64 = GetTimeMs() + 100
	p := loop.TimeEventsHead
	for p != nil {
		if p.when < nearest {
			nearest = p.when
		}
		p = p.next
	}
	return nearest
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
	timeout := loop.nearestTime() - GetTimeMs()
	// at least block 1 ms
	if timeout <= 0 {
		timeout = 1
	}
	events := [128]syscall.EpollEvent{}
	n, err := syscall.EpollWait(loop.efd, events[:], int(timeout))
	if err != nil && err != syscall.EINTR {
		log.Printf("epoll wait error: %v\n", err)
		return
	}

	// exec file event
	for i := 0; i < n; i++ {
		if events[i].Events&FeEvMap[AE_READABLE] != 0 {
			fe := loop.FileEvents[getFeKey(int(events[i].Fd), AE_READABLE)]
			if fe != nil {
				fe.proc(loop, fe.fd, fe.extra)
			}
		}
		if events[i].Events&FeEvMap[AE_WRITABLE] != 0 {
			fe := loop.FileEvents[getFeKey(int(events[i].Fd), AE_WRITABLE)]
			if fe != nil {
				fe.proc(loop, fe.fd, fe.extra)
			}
		}
	}

	// exec time event
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

func (loop *AeEventLoop) AeMain() {
	for !loop.stopped {
		loop.AeProcess()
	}
}
