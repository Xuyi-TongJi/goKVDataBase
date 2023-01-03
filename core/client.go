package core

import (
	. "goRedis/data_structure"
	"goRedis/net"
	"log"
	"strconv"
	"strings"
)

// Database Client core library

const (
	// IoBufferSize client io buffer size
	IoBufferSize int = 1024 * 16
	MaxBulk      int = 1024 * 4
	MaxInline    int = 1024 * 4
)

type Client struct {
	// socket
	fd int
	// request object after ReadQueryFromClient
	args []*DbObject
	// response object after command process
	reply *List
	// TODO
	sentLength int
	// query bffer read from socket
	queryBuffer []byte
	// sliding window query length
	queryLength int
	// query command type
	cmdType CmdType
	// bulk string数组长度
	bulkNum int
	// bulk length string数组下一个string的长度
	bulkLength int
	// Server
	server *Server
	// isClosed
	isClosed bool
	// 是否有读到一半没有读完的请求
	isQueryProcessing bool
	// 是否可以进行下一次命令处理
	canDoNextCommandHandle bool
}

func (client *Client) expandQueryBufIfNeeded() {
	if len(client.queryBuffer)-client.queryLength < MaxQueryLength {
		client.queryBuffer = append(client.queryBuffer, make([]byte, MaxQueryLength)...)
	}
}

// findCrlfFromQueryBuffer
// CRLF: \r\n
func (client *Client) findCrlfFromQueryBuffer() int {
	return strings.Index(string(client.queryBuffer[:client.queryLength]), "\r\n")
}

func (client *Client) getNumberFromQuery(startIndex, endIndex int) (int, error) {
	return strconv.Atoi(string(client.queryBuffer[startIndex:endIndex]))
}

func NewClient(fd int, server *Server) *Client {
	return &Client{
		fd:                fd,
		args:              make([]*DbObject, 0),
		reply:             NewList(StrEqual),
		sentLength:        0,
		queryBuffer:       make([]byte, IoBufferSize),
		queryLength:       0,
		cmdType:           UNKNOWN,
		bulkNum:           0,
		bulkLength:        0,
		server:            server,
		isQueryProcessing: false,
	}
}

// FreeClient 断开连接
func FreeClient(client *Client) {
	if client.isClosed {
		return
	}
	log.Printf("[CLIENT DISCONNECTION...] Connection with client %d is disconnecting...\n", client.fd)
	// remove file event
	client.server.Loop.RemoveFileEvent(client.fd, READABLE)
	client.server.Loop.RemoveFileEvent(client.fd, WRITEABLE)
	// disconnect
	client.isClosed = true
	if err := net.Close(client.fd); err != nil {
		log.Printf("[CLIENT DISCONNECTION ERROR] Connection with client %d disconnected error, err = %s\n", client.fd, err)
	}
	// remove from server
	delete(client.server.Clients, client.fd)
}

// ResetClient 重置client参数
func ResetClient(client *Client) {
	client.args = make([]*DbObject, 0)
	client.reply = NewList(StrEqual)
	client.sentLength = 0
	client.queryBuffer = make([]byte, IoBufferSize)
	client.cmdType = UNKNOWN
	client.bulkNum = 0
	client.bulkLength = 0
	client.isQueryProcessing = false
}
