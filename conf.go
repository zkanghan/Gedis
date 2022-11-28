package main

const (
	PORT = ":8888"
)

// global variable
var server GedisServer

func InitServer() error {
	server = GedisServer{
		port: 8888,
		db: &GedisDB{
			data:   NewDict(DictType{HashFunc: HashStr, EqualFunc: EqualStr}),
			expire: NewDict(DictType{HashFunc: HashStr, EqualFunc: EqualStr}),
		},
		clients: make(map[int]*GedisClient),
	}
	var err error
	server.aeloop, err = NewAeEventLoop()
	if err != nil {
		return err
	}
	server.sfd, err = TcpServer(server.port)
	return err
}
