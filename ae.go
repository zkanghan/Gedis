package main

import (
	"log"
	"net"
	"time"
)

// FeType 事件类型
type FeType int8
type TeType int8

const (
	AE_READABLE FeType = 1
	AE_WRITABLE FeType = 2

	AE_NORNAL TeType = 1 //普通时间事件
	AE_ONCE   TeType = 2 // 只执行一次的时间时间
)

type FileProc func(loop *AeEventLoop, conn *net.TCPConn, extra any)
type TimeProc func(loop *AeEventLoop, id int, extra any)

// AcceptHandler 阻塞处理网络连接请求
func AcceptHandler() {
	err := server.listener.SetDeadline(time.Now().Add(time.Millisecond * 12))
	if err != nil {
		log.Println("set listener error: ", err)
		return
	}
	for {
		conn, err := server.listener.Accept()
		if err != nil {
			log.Println("accept error: ", err)
			break
		}
		tcpConn, ok := conn.(*net.TCPConn)
		if !ok { //不是tcp连接
			return
		}
		client := NewClient(tcpConn)
		server.clients[getConnFd(tcpConn)] = client
		// 连接建立成功，事件可读，处理客户端请求
		server.aeloop.AddFileEvent(tcpConn, AE_READABLE, ReadQueryFromClient, client)
	}

}

// ReadQueryFromClient extra为需要读取的客户端
var ReadQueryFromClient FileProc = func(loop *AeEventLoop, conn *net.TCPConn, extra any) {
	//  TODO: 完成 resp 协议

}

var ServerCron TimeProc = func(loop *AeEventLoop, id int, extra any) {
	// TODO: 执行对键值的随机检查
}

// AeFileEvent 文件事件处理Gedis与客户端的网络IO
type AeFileEvent struct {
	connection *net.TCPConn
	mask       FeType //文件事件类型
	proc       FileProc
	extra      interface{}
}

type AeTimeEvent struct {
	id       int //时间事件标识符
	mask     TeType
	when     int64 //何时发生
	interval int64 // 时间事件间隔
	proc     TimeProc
	extra    interface{}
	next     *AeTimeEvent
}

type AeEventLoop struct {
	FileEvents      map[int]*AeFileEvent //所有的文件事件
	TimeEventsHead  *AeTimeEvent         //时间事件链表
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

// 以conn的文件描述符和类型 确定map中的编号
func getFeKey(conn *net.TCPConn, mask FeType) int {
	fd := getConnFd(conn)
	if mask == AE_READABLE {
		return fd
	}
	return -1 * fd
}

// 获取conn对应的文件描述符
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

func (loop *AeEventLoop) AddFileEvent(conn *net.TCPConn, mask FeType, proc FileProc, extra interface{}) {
	// 文件事件添加至事件库
	fe := AeFileEvent{
		connection: conn,
		mask:       mask,
		proc:       proc,
		extra:      extra,
	}
	loop.FileEvents[getFeKey(conn, mask)] = &fe
}

func (loop *AeEventLoop) RemoveFileEvent(conn *net.TCPConn, mask FeType) {
	err := conn.Close()
	if err != nil {
		log.Println("close connection error to remove file event: ", err)
	}
	loop.FileEvents[getFeKey(conn, mask)] = nil
}

func GetTimeMs() int64 {
	return time.Now().UnixMilli()
}

// AddTimeEvent 使用头插法插入时间事件
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

// RemoveTimeEvent 删除对应id的时间事件
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

// 遍历时间时间链表，返回最近的要发生的时间事件
func (loop *AeEventLoop) nearestTime() int64 {
	p := loop.TimeEventsHead
	nearest := GetTimeMs() + 1000
	for p != nil {
		if p.when < nearest {
			nearest = p.when
		}
		p = p.next
	}
	return nearest
}

// AeProcess 执行一次 Process函数 相当于一次处理循环
func (loop *AeEventLoop) AeProcess() {
	AcceptHandler() // 阻塞的监听网络连接

	for _, fe := range loop.FileEvents { //先执行可读事件，因为可读事件可能会产生可写事件
		if fe.mask == AE_READABLE {
			fe.proc(loop, fe.connection, fe.extra)
		}
	}

	for _, fe := range loop.FileEvents {
		if fe.mask == AE_WRITABLE {
			fe.proc(loop, fe.connection, fe.extra)
		}
	}

	p := loop.TimeEventsHead
	for p != nil {
		if p.when > GetTimeMs() { //时间事件已超时，触发运行
			p.proc(loop, p.id, p.extra)
			if p.mask == AE_ONCE {
				loop.RemoveTimeEvent(p.id)
			} else if p.mask == AE_NORNAL {
				p.when = GetTimeMs() + p.interval //重置下次触发时间
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
