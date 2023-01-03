package core

import (
	"errors"
	. "goRedis/data_structure"
	"goRedis/net"
	"log"
)

// DataBase server core lib

type Server struct {
	Fd            int
	Db            *DataBase
	Clients       map[int]*Client
	Loop          *AeLoop
	Port          int
	MaxConnection int32
}

func NewServer(config *Config) (*Server, error) {
	// create listening socket
	server := &Server{
		Port:          config.Port,
		MaxConnection: config.MaxConnection,
	}
	// listening fd
	fd := net.TcpServer(config.Port)
	if fd == -1 {
		return nil, errors.New("TcpServer init error")
	}
	server.Fd = fd
	loop, err := AeLoopCreate(server)
	if err != nil {
		return nil, err
	}
	server.Loop = loop
	server.Db = &DataBase{
		// key : string
		data: NewDict(StrHash, StrEqual),
		// key : string
		expire: NewDict(StrHash, StrEqual),
	}
	server.Clients = make(map[int]*Client)
	return server, nil
}

// AcceptHandler Accept a connection request of client
// 监听socket处理连接请求的EPOLL回调函数
// 建立连接, 创建Client并加入到Server中, 并注册EPOLLIN(readQueryFromClient)事件
// fd listening fd
func AcceptHandler(loop *AeLoop, fd int, extra interface{}) {
	socket, err := net.Accept(fd)
	if err != err {
		log.Printf("[Listening Socket Accept Handler ERROR] Init client socket error, err :%s", err)
		return
	}
	client := NewClient(socket, loop.server)
	loop.server.Clients[socket] = client
	loop.AddFileEvent(socket, READABLE, readQueryFromClient, client)
}
