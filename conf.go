package main

const (
	PORT        int = 8888
	MAX_CLIENTS int = 10000

	AOF_AUTOSYNC_BYTES   int = 1024 * 1024 * 10
	DEFULT_AOF_FILENAME      = "appendOnly.aof"
	AOF_REWRITE_MIN_SIZE     = 1024 * 1024 * 32
	AOF_REWRITE_PERC         = 80
)

// global variable
var server GedisServer

func InitServer() error {
	server = GedisServer{
		port: PORT,
		db: &GedisDB{
			data:   NewDict(DictType{HashFunc: HashStr, EqualFunc: EqualStr}),
			expire: NewDict(DictType{HashFunc: HashStr, EqualFunc: EqualStr}),
		},
		clients:           make(map[int]*GedisClient),
		aofFileName:       DEFULT_AOF_FILENAME,
		aofRewriteMinSize: AOF_REWRITE_MIN_SIZE,
		aofRewritePerc:    AOF_REWRITE_PERC,
		aofRewriteBuf:     make([]byte, 0),
	}
	var err error
	server.aeloop, err = NewAeEventLoop()
	if err != nil {
		return err
	}
	server.sfd, err = TcpServer(server.port)
	return err
}
