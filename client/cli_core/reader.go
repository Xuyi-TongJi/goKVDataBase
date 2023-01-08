package cli_core

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

const (
	MaxMessageSize int    = 1024 << 16
	CRLF           string = "\r\n"
	SEPARATION     string = " "
)

// sliding window

type resolveFunction func([]byte, int, *Pack) int

type ReType int

const (
	EMPTY   ReType = 0
	STRING  ReType = 1
	ERROR   ReType = 2
	INT     ReType = 3
	BULKSTR ReType = 4
	BULKARR ReType = 5
)

var typeMap map[rune]ReType
var funcMap map[ReType]resolveFunction

func init() {
	typeMap = make(map[rune]ReType, 0)
	typeMap['+'] = STRING
	typeMap['-'] = ERROR
	typeMap[':'] = INT
	typeMap['$'] = BULKSTR
	typeMap['*'] = BULKARR
	funcMap = make(map[ReType]resolveFunction, 0)
	funcMap[STRING] = resolveStr
	funcMap[ERROR] = resolveStr
	funcMap[INT] = resolveStr
	funcMap[BULKSTR] = resolveBulkStr
	funcMap[BULKARR] = resolveBulkArr
}

type Pack struct {
	// 是否有正在处理的请求，if not then isQuerying == 0
	isQuerying    ReType
	bulkNum       int
	bulkLength    int
	output        []string
	readyToOutput bool
}

func NewPack() *Pack {
	return &Pack{
		isQuerying:    EMPTY,
		bulkNum:       -1,
		bulkLength:    -1,
		output:        make([]string, 0),
		readyToOutput: false,
	}
}

func (pack *Pack) reset() {
	pack.isQuerying = EMPTY
	pack.bulkNum = -1
	pack.bulkLength = -1
	pack.output = make([]string, 0)
	pack.readyToOutput = false
}

func (pack *Pack) doFmtOutput() {
	if pack.readyToOutput {
		if pack.isQuerying == BULKARR {
			var builder strings.Builder
			for i, s := range pack.output {
				builder.WriteString(s)
				if i != len(pack.output)-1 {
					builder.WriteString(SEPARATION)
				}
			}
			fmt.Printf("%s\n", builder.String())
			// reset
		} else {
			fmt.Printf("%s\n", pack.output[0])
		}
		pack.reset()
	}
}

func StartReader(conn net.Conn, stop chan error) {
	pack := NewPack()
	byteBuffer := make([]byte, 1024<<16)
	// 待处理的消息长度
	length := 0
	for {
		// expand
		if len(byteBuffer)-length < MaxMessageSize {
			byteBuffer = append(byteBuffer, make([]byte, MaxMessageSize)...)
		}
		n, err := conn.Read(byteBuffer[length:])
		// fmt.Printf("[CLIENT READER] Receive message of %d bytes\n", n)
		if err != nil {
			stop <- err
			break
		}
		length += n
		for length > 0 {
			x := resolve(byteBuffer, length, pack)
			if x == 0 {
				break
			}
			length -= x
			byteBuffer = append(byteBuffer[x:])
			if pack.readyToOutput {
				pack.doFmtOutput()
			}
		}
	}
}

// return the length has been resolved
func resolve(byteBuffer []byte, length int, pack *Pack) int {
	if length <= 0 {
		return 0
	}
	if pack.isQuerying == EMPTY {
		// judge the return string type
		pack.isQuerying = typeMap[rune(byteBuffer[0])]
	}
	var process = funcMap[pack.isQuerying]
	return process(byteBuffer, length, pack)
}

func resolveStr(byteBuffer []byte, length int, pack *Pack) int {
	index := findCrlf(byteBuffer, 0, length)
	if index == -1 {
		return 0
	}
	pack.output = append(pack.output, findStr(byteBuffer, 1, index))
	pack.readyToOutput = true
	// CRLF + 2
	return index + 2
}

func resolveBulkStr(byteBuffer []byte, length int, pack *Pack) int {
	left := 0
	if pack.bulkLength == -1 {
		// find bulkLength
		index := findCrlf(byteBuffer, 0, length)
		if index != -1 {
			pack.bulkLength = findNumber(byteBuffer, 1, index)
			// CRLF
			left = index + 2
		}
	}
	if pack.bulkLength != -1 {
		// CRLF
		if left+pack.bulkLength+2 <= length {
			pack.output = append(pack.output, findStr(byteBuffer, left, left+pack.bulkLength))
			pack.readyToOutput = true
			left += pack.bulkLength + 2
		}
	}
	return left
}

func resolveBulkArr(byteBuffer []byte, length int, pack *Pack) int {
	left := 0
	if pack.bulkNum == -1 {
		index := findCrlf(byteBuffer, 0, length)
		if index != -1 {
			pack.bulkNum = findNumber(byteBuffer, 1, index)
			left = index + 2
		}
	}
	for len(pack.output) < pack.bulkNum {
		if pack.bulkLength == -1 {
			index := findCrlf(byteBuffer, left, length)
			if index != -1 {
				// $
				pack.bulkLength = findNumber(byteBuffer, left+1, index)
				left = index + 2
			}
		}
		if pack.bulkLength != -1 {
			if left+pack.bulkLength+2 <= length {
				pack.output = append(pack.output, findStr(byteBuffer, left, left+pack.bulkLength))
				left += pack.bulkLength + 2
				pack.bulkLength = -1
			} else {
				break
			}
		} else {
			break
		}
	}
	if len(pack.output) == pack.bulkNum {
		pack.readyToOutput = true
	}
	return left
}

func findCrlf(byteBuffer []byte, left, right int) int {
	if left > right {
		return -1
	}
	return strings.Index(string(byteBuffer[left:right]), CRLF) + left
}

func findNumber(byteBuffer []byte, left, right int) int {
	num, _ := strconv.Atoi(string(byteBuffer[left:right]))
	return num
}

func findStr(byteBuffer []byte, left, right int) string {
	return string(byteBuffer[left:right])
}
