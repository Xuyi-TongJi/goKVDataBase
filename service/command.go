package service

import (
	. "goRedis/data_structure"
	. "goRedis/db"
	"goRedis/util"
	"log"
	"strconv"
	"strings"
	"time"
)

// database command and command process func core library
// id type(top 16 bit) : 0  system
//                       1  string
//                       2  zset

const (
	ErrorHead      string = "-ERROR: "
	StringHead     string = "+"
	IntHead        string = ":"
	BulkStringHead string = "$"
	BulkArrayHead  string = "*"
	CRLF           string = "\r\n"
	WELCOME        string = "+Welcome!\r\n"
)

type handleProcess func(args []*DbObject, db *Database) string

type DataBaseCommand struct {
	name    string
	proc    handleProcess
	id      uint32 // id: 高16位-操作的value种类, 0为功能指令， 低16位-操作种类
	minArgs int32  // args number a valid command needed
	maxArgs int32
}

// commandTable 全局CommandTable
var commandTable map[string]*DataBaseCommand

func init() {
	commandTable = make(map[string]*DataBaseCommand, 0)
	// string
	commandTable["GET"] = &DataBaseCommand{
		name:    "get",
		proc:    getCommandProcess,
		id:      1<<16 | 1,
		minArgs: 2,
		maxArgs: 2,
	}
	commandTable["SET"] = &DataBaseCommand{
		name:    "set",
		proc:    setCommandProcess,
		id:      1<<16 | 2,
		minArgs: 3,
		maxArgs: 3,
	}
	commandTable["SETEX"] = &DataBaseCommand{
		name:    "setex",
		proc:    setexCommandProcess,
		id:      1<<16 | 3,
		minArgs: 4,
		maxArgs: 4,
	}
	commandTable["SETNX"] = &DataBaseCommand{
		name:    "setnx",
		proc:    setnxCommandProcess,
		id:      1<<16 | 4,
		minArgs: 3,
		maxArgs: 3,
	}
	// zset
	commandTable["ZADD"] = &DataBaseCommand{
		name:    "zadd",
		proc:    zaddCommandProcess,
		id:      1<<17 | 1,
		minArgs: 4,
		maxArgs: 4,
	}
	commandTable["ZANGE"] = &DataBaseCommand{
		name:    "zrange",
		proc:    zrangeCommandProcess,
		id:      1<<17 | 2,
		minArgs: 4,
		maxArgs: 4,
	}
	commandTable["ZINCREBY"] = &DataBaseCommand{
		name:    "zincreby",
		proc:    zincrebyCommandProcess,
		id:      1<<17 | 3,
		minArgs: 4,
		maxArgs: 4,
	}
	commandTable["ZREM"] = &DataBaseCommand{
		name:    "zrange",
		proc:    zremCommandProcess,
		id:      1<<17 | 4,
		minArgs: 3,
		maxArgs: 3,
	}
	// system
	commandTable["QUIT"] = &DataBaseCommand{
		name:    "quit",
		proc:    quitCommandProcess,
		id:      1,
		minArgs: 1,
		maxArgs: 1,
	}
}

func Handle(args []*DbObject, db *Database) string {
	cmdType := strings.ToUpper(args[0].StrVal())
	cmd := commandTable[cmdType]
	if cmd == nil {
		return packErrorMessage("Unknown command type")
	}
	if len(args) < int(cmd.minArgs) || len(args) > int(cmd.maxArgs) {
		return packErrorMessage("Invalid parameter number")
	}
	return cmd.proc(args, db)
}

// string

// 'get' Process Function
func getCommandProcess(args []*DbObject, db *Database) string {
	val, err := db.GetStr(args[1])
	if err != nil {
		return packErrorMessage(err.Error())
	}
	log.Printf("[GET COMMAND]Success\n")
	return packString(val.StrVal())
}

// 'set' Process Function
func setCommandProcess(args []*DbObject, db *Database) string {
	if err := db.SetStr(args[1], args[2], DefaultExpireTime+getTime()); err != nil {
		return packErrorMessage(err.Error())
	}
	log.Printf("[SET COMMAND]Success\n")
	return packString("Query OK")
}

// 'setex' Process Function
func setexCommandProcess(args []*DbObject, db *Database) string {
	expire, _ := args[2].IntVal()
	expire = expire * 1000000000
	if err := db.SetStr(args[1], args[3], expire); err != nil {
		return packErrorMessage(err.Error())
	}
	log.Printf("[SETEX COMMAND]Success\n")
	return packString("Query OK")
}

// 'setnx' Process Function
func setnxCommandProcess(args []*DbObject, db *Database) string {
	if val, _ := db.GetStr(args[1]); val != nil {
		return packString("Key already exist")
	}
	if err := db.SetStr(args[1], args[2], DefaultExpireTime+getTime()); err != nil {
		return packErrorMessage(err.Error())
	}
	log.Printf("[SETNX COMMAND]Success\n")
	return packString("Query OK")
}

// zset

// 'zadd' Process Function
func zaddCommandProcess(args []*DbObject, db *Database) string {
	// judge parameter
	key := args[1]
	score, err := args[2].IntVal()
	member := args[3]
	if err != nil {
		return packErrorMessage("Illegal parameter, score must be an integer")
	}
	obj, err := db.GetKeyObject(key, ZSET)
	if err != nil {
		return packErrorMessage(err.Error())
	}
	// do exec
	zset := obj.Val.(*Zset)
	_, err = zset.GetScore(member)
	if err != nil && err != ERROR_KEY_NOT_EXIST {
		return packErrorMessage(err.Error())
	}
	if err == nil {
		zset.UpdateScore(member, score)
	} else if err == ERROR_KEY_NOT_EXIST {
		zset.AddMember(member, score)
	} else {
		return packErrorMessage(err.Error())
	}
	log.Printf("[ZADD COMMAND]Success\n")
	return packString("Query OK")
}

// 'zrange' Process Function
func zrangeCommandProcess(args []*DbObject, db *Database) string {
	// judge parameter
	left, err1 := args[2].IntVal()
	right, err2 := args[3].IntVal()
	if err1 != nil || err2 != nil {
		return packErrorMessage("Illegal parameter, score must be an integer")
	}
	obj, err := db.GetKeyObject(args[1], ZSET)
	if err != nil {
		return packErrorMessage(err.Error())
	}
	zset := obj.Val.(*Zset)
	_, members := zset.ZRange(left, right)
	n := len(members)
	result := make([]string, n)
	for i, m := range members {
		result[i] = m.StrVal()
	}
	log.Printf("[ZRANGE COMMAND]Success\n")
	return packBulkArray(result)
}

// 'zrem' Process Function
func zremCommandProcess(args []*DbObject, db *Database) string {
	obj, err := db.GetKeyObject(args[1], ZSET)
	if err != nil {
		return packErrorMessage(err.Error())
	}
	zset := obj.Val.(*Zset)
	if err = zset.Remove(args[2]); err != nil {
		return packErrorMessage(err.Error())
	}
	log.Printf("[ZREM COMMAND]Success\n")
	return packString("Query OK")
}

// 'zincreby' Process Function
func zincrebyCommandProcess(args []*DbObject, db *Database) string {
	incr, err := args[2].IntVal()
	if err != nil {
		return packErrorMessage("Illegal parameter, score must be an integer")
	}
	key := args[1]
	member := args[3]
	obj, err := db.GetKeyObject(key, ZSET)
	if err != nil {
		return packErrorMessage(err.Error())
	}
	zset := obj.Val.(*Zset)
	if err = zset.Incr(member, incr); err != nil {
		return packErrorMessage(err.Error())
	}
	log.Printf("[ZINCREBY COMMAND]Success\n")
	return packString("Query OK")
}

// set(hash)

// list

// system

func quitCommandProcess(args []*DbObject, db *Database) string {
	return util.ERROR_QUIT
}

// util

// pack

func packErrorMessage(msg string) string {
	var str []string = []string{ErrorHead, msg, CRLF}
	return strings.Join(str, "")
}

func packString(msg string) string {
	var str []string = []string{StringHead, msg, CRLF}
	return strings.Join(str, "")
}

func packInt(num int) string {
	var str []string = []string{IntHead, strconv.Itoa(num), CRLF}
	return strings.Join(str, "")
}

func packBulkString(msg string) string {
	len := strconv.Itoa(len(msg))
	var builder strings.Builder
	builder.WriteString(BulkStringHead)
	builder.WriteString(len)
	builder.WriteString(CRLF)
	builder.WriteString(msg)
	builder.WriteString(CRLF)
	return builder.String()
}

func packBulkArray(msgs []string) string {
	len := len(msgs)
	result := make([]string, len)
	for i := 0; i < len; i += 1 {
		result[i] = packBulkString(msgs[i])
	}
	// head
	var builder strings.Builder
	builder.WriteString(BulkArrayHead)
	builder.WriteString(strconv.Itoa(len))
	for i := 0; i < len; i += 1 {
		builder.WriteString(result[i])
		builder.WriteString(CRLF)
	}
	return builder.String()
}

func getTime() int64 {
	return time.Now().UnixNano()
}
