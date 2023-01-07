package core

import (
	"errors"
	. "goRedis/db"
	"goRedis/net"
	"goRedis/service"
	"log"
)

// DataBase server core lib

type Server struct {
	Fd             int
	Db             *Database
	Clients        map[int]*Client
	Loop           *AeLoop
	Port           int
	MaxConnection  int32
	MaxQueryLength int32
}

func NewServer(config *Config) (*Server, error) {
	// create listening socket
	server := &Server{
		Port:           config.Port,
		MaxConnection:  config.MaxConnection,
		MaxQueryLength: config.MaxQueryLength,
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
	server.Db = NewDatabase()
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
		log.Printf("[LISTENING SOCKET ACCEPT HANDLER ERROR] Accept client error, err :%s", err)
		return
	}
	client := NewClient(socket, loop.server)
	loop.server.Clients[socket] = client
	loop.AddFileEvent(socket, READABLE, readQueryFromClient, client)
	client.AddReplyStr(service.WELCOME)
	log.Printf("[LISTENING SOCKET ACCEPT HANDLER] Accept client success, connection build, fd = %d\n", client.fd)
}
