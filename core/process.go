package core

import (
	. "goRedis/data_structure"
	"goRedis/net"
	"log"
	"strings"
)

type CmdType int32

// 协议类型
// 不用多态，因为协议格式的添加是极少数的
const (
	UNKNOWN CmdType = 0x00
	INLINE  CmdType = 0x01
	BULK    CmdType = 0x02
)

// database server process requests and responses core lib
// ReadQueryFromClient and SendReplyToClient

const (
	// 一次查询命令最长字节大小
	MaxQueryLength int = 1024 * 4
)

// readQueryFromClient 从Client中读数据
// 滑动窗口法(可能一次发过来好几条命令)，必要时扩容，确保留出一个MaxQueryLength的Buffer空间
// 客户端必须限制发送的请求大小
// 遇到错误时，断开连接
func readQueryFromClient(loop *AeLoop, fd int, extra interface{}) {
	client := extra.(*Client)
	// expand if needed
	client.expandQueryBufIfNeeded()
	// unix C read
	n, err := net.Read(fd, client.queryBuffer[client.queryLength:])
	if err != nil {
		log.Printf("[READ QUERY FROM CLIENT ERROR] Read query from client %d error, err = %s\n", client.fd, err)
		FreeClient(client)
		return
	}
	client.queryLength += n
	if client.queryLength > MaxQueryLength {
		log.Printf("[READ QUERY FROM CLIENT ERROR] Client %d query length overflow error\n", client.fd)
		FreeClient(client)
		return
	}
	err = processRequest(client)
	if err != nil {
		log.Printf("[READ QUERY FROM CLIENT ERROR] Process request from client %d error, err = %s\n", client.fd, err)
		FreeClient(client)
	}
}

// sendReplyToClient 向Client中写数据[不能直接阻塞写]
func sendReplyToClient(loop *AeLoop, fd int, extra interface{}) {

}

// processRequest 处理请求 功能：将请求string转为Client对象中的args
// 1. 获取请求协议类型[INLINE/BULK]
// 2. 将请求[]byte解析道client.args
// 解析失败，则断开连接
func processRequest(client *Client) error {
	// 只要还有请求要处理，则处理请求
	for client.queryLength > 0 {
		if client.queryBuffer[0] == '*' {
			client.cmdType = BULK
		} else {
			client.cmdType = INLINE
		}
		// query -> args
		var err error
		if client.cmdType == BULK {
			err = handleBulkRequest(client)
		} else {
			err = handleInlineRequest(client)
		}
		if err != nil {
			return err
		}
		if len(client.args) > 0 {
			processCommand(client)
		} else {
			// args == 0 空请求
			ResetClient(client)
		}
	}
	return nil
}

// handleInlineRequest 解析inline请求
// query string -> client.args
// 滑动窗口
func handleInlineRequest(client *Client) error {
	crlfIndex, err := client.findCrlfFromQueryBuffer()
	if err != nil {
		log.Printf("[HANDLE INLINE REQUEST ERROR] Handle lnline request error, err = %s\n", err)
		return err
	}
	values := strings.Split(string(client.queryBuffer[:crlfIndex]), " ")
	client.queryLength -= crlfIndex + 2
	client.queryBuffer = client.queryBuffer[crlfIndex+2:]
	client.args = make([]*DbObject, len(values))
	for index, val := range values {
		client.args[index] = &DbObject{
			Type: STR,
			Val:  val,
		}
	}
	return nil
}

// handleBulkRequeset 解析Bulk请求
// query string -> client.args
// 滑动窗口
func handleBulkRequest(client *Client) error {

	return nil
}

// processCommand 根据client中的args，进行命令执行
// 如果请求无法执行，则发送错误消息
func processCommand(client *Client) {

}
