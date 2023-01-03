package core

import (
	"errors"
	"fmt"
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
	// unix C read 至多读一个MaxQueryLength
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
// 解析发生错误，则断开连接
// 未完整解析一条指令，则保留queryBuffer和queryLength，到下一次Read(readQueryFromClient)返回后再处理
// 处理一定是从queryBuffer的第一个字节开始
func processRequest(client *Client) error {
	// 只要缓冲区还有未处理的queryBuffer就进行处理
	for client.queryLength > 0 {
		// 没有处理到一半的请求
		if !client.isQueryProcessing {
			if client.queryBuffer[0] == '*' {
				client.cmdType = BULK
			} else {
				client.cmdType = INLINE
			}
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
		// 不能进行下一次processCommand(没有完整解析，即完整Read完整这一条指令),则break，等待下一次Read
		if !client.canDoNextCommandHandle {
			break
		} else {
			processCommand(client)
		}
	}
	return nil
}

// handleInlineRequest 解析inline请求
// query string -> client.args
// queryBuffer至少一个MaxQueryLength大小，如果在一个MaxQueryLength大小还未找到CRLF,则为不合法请求，直接断开连接
// 滑动窗口
// error -> 解析发生错误，则返回error，断开连接
func handleInlineRequest(client *Client) error {
	// new request
	crlfIndex := client.findCrlfFromQueryBuffer()
	if crlfIndex == -1 {
		return errors.New(fmt.Sprintf("Query Length overflows\n"))
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
	client.isQueryProcessing = false
	client.canDoNextCommandHandle = true
	return nil
}

// handleBulkRequeset 解析Bulk请求
// query string -> client.args
// 滑动窗口
// error -> 解析发生错误，则返回error，断开连接
func handleBulkRequest(client *Client) error {
	// new request -> bulkNum == 0
	if client.bulkNum == 0 {
		crlfIndex := client.findCrlfFromQueryBuffer()
		if crlfIndex == -1 {
			return errors.New(fmt.Sprintf("Query Length overflows\n"))
		}
		bNum, err := client.getNumberFromQuery(1, crlfIndex)
		if err != nil {
			return errors.New("[CLIENT HANDLE BULK REQUEST ERROR] Illegal client protocol format")
		}
		client.isQueryProcessing = true
		client.canDoNextCommandHandle = false
		client.bulkNum = bNum
		// move sliding window
		client.queryBuffer = append(client.queryBuffer[crlfIndex+2:])
		client.queryLength -= crlfIndex + 2
	}
	for client.bulkNum > 0 {
		if len(client.queryBuffer) == 0 {
			break
		}
		// find bulkLength
		if client.bulkLength == 0 {
			if client.queryBuffer[0] != '$' {
				return errors.New("[CLIENT HANDLE BULK REQUEST ERROR] Illegal client protocol format")
			}
			crlfIndex := client.findCrlfFromQueryBuffer()
			if crlfIndex == -1 {
				break
			}
			bLength, err := client.getNumberFromQuery(1, crlfIndex)
			if err != nil {
				return errors.New("[CLIENT HANDLE BULK REQUEST ERROR] Illegal client protocol format")
			}
			client.bulkLength = bLength
			// move sliding window
			client.queryBuffer = append(client.queryBuffer[crlfIndex+2:])
			client.queryLength -= crlfIndex + 2
		}
		// find next string element
		crlfIndex := client.findCrlfFromQueryBuffer()
		if crlfIndex == -1 {
			break
		}
		// build client arg
		newArg := &DbObject{
			Type: STR,
			Val:  string(client.queryBuffer[:crlfIndex]),
		}
		client.args = append(client.args, newArg)
		client.bulkLength = 0
		client.queryBuffer = append(client.queryBuffer[:crlfIndex+2])
		client.queryLength -= crlfIndex + 2
		client.bulkNum -= 1
	}
	// 下一次command可以执行
	if client.bulkNum == 0 {
		client.isQueryProcessing = false
		client.canDoNextCommandHandle = true
	}
	return nil
}

// processCommand 根据client中的args，进行命令执行
// 如果请求无法执行（非法请求），则向请求发送错误消息（不会断开连接）
func processCommand(client *Client) {
	// TODO 空请求
	/* test code */
	for _, a := range client.args {
		fmt.Println(a.StrVal())
	}
	client.args = make([]*DbObject, 0)
}
