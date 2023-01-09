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
//                       3  hash
//                       4  set
//                       5  list
//                       6  keys

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

var router map[string]*DataBaseCommand

func init() {
	router = make(map[string]*DataBaseCommand, 0)
	// string
	router["GET"] = &DataBaseCommand{
		name:    "get",
		proc:    getCommandProcess,
		id:      1<<16 | 1,
		minArgs: 2,
		maxArgs: 2,
	}
	router["SET"] = &DataBaseCommand{
		name:    "set",
		proc:    setCommandProcess,
		id:      1<<16 | 2,
		minArgs: 3,
		maxArgs: 3,
	}
	router["SETEX"] = &DataBaseCommand{
		name:    "setex",
		proc:    setexCommandProcess,
		id:      1<<16 | 3,
		minArgs: 4,
		maxArgs: 4,
	}
	router["SETNX"] = &DataBaseCommand{
		name:    "setnx",
		proc:    setnxCommandProcess,
		id:      1<<16 | 4,
		minArgs: 3,
		maxArgs: 3,
	}
	router["INCRBY"] = &DataBaseCommand{
		name:    "incrby",
		proc:    incrbyCommandProcess,
		id:      1<<16 | 5,
		minArgs: 3,
		maxArgs: 3,
	}
	router["INCR"] = &DataBaseCommand{
		name:    "incr",
		proc:    incrCommandProcess,
		id:      1<<16 | 6,
		minArgs: 2,
		maxArgs: 2,
	}
	router["DECR"] = &DataBaseCommand{
		name:    "decr",
		proc:    decrCommandProcess,
		id:      1<<16 | 7,
		minArgs: 2,
		maxArgs: 2,
	}
	// zset
	router["ZADD"] = &DataBaseCommand{
		name:    "zadd",
		proc:    zaddCommandProcess,
		id:      1<<17 | 1,
		minArgs: 4,
		maxArgs: 4,
	}
	router["ZRANGE"] = &DataBaseCommand{
		name:    "zrange",
		proc:    zrangeCommandProcess,
		id:      1<<17 | 2,
		minArgs: 4,
		maxArgs: 4,
	}
	router["ZINCREBY"] = &DataBaseCommand{
		name:    "zincreby",
		proc:    zincrebyCommandProcess,
		id:      1<<17 | 3,
		minArgs: 4,
		maxArgs: 4,
	}
	router["ZREM"] = &DataBaseCommand{
		name:    "zrange",
		proc:    zremCommandProcess,
		id:      1<<17 | 4,
		minArgs: 3,
		maxArgs: 3,
	}
	router["ZSCORE"] = &DataBaseCommand{
		name:    "zscore",
		proc:    zscoreCommandProcess,
		id:      1<<17 | 5,
		minArgs: 3,
		maxArgs: 3,
	}
	// hash
	router["HSET"] = &DataBaseCommand{
		name:    "hset",
		proc:    hsetCommandProcess,
		id:      1<<18 | 1,
		minArgs: 4,
		maxArgs: 4,
	}
	router["HGET"] = &DataBaseCommand{
		name:    "hget",
		proc:    hgetCommandProcess,
		id:      1<<18 | 2,
		minArgs: 3,
		maxArgs: 3,
	}
	router["HDEL"] = &DataBaseCommand{
		name:    "hdel",
		proc:    hdelCommandProcess,
		id:      1<<18 | 3,
		minArgs: 3,
		maxArgs: 3,
	}
	// set
	router["SADD"] = &DataBaseCommand{
		name:    "sadd",
		proc:    saddCommandProcess,
		id:      1<<19 | 1,
		minArgs: 3,
		maxArgs: 3,
	}
	router["SMEMBERS"] = &DataBaseCommand{
		name:    "smembers",
		proc:    smembersCommandProcess,
		id:      1<<19 | 2,
		minArgs: 2,
		maxArgs: 2,
	}
	router["SCARD"] = &DataBaseCommand{
		name:    "smembers",
		proc:    scardCommandProcess,
		id:      1<<19 | 3,
		minArgs: 2,
		maxArgs: 2,
	}
	router["SINTER"] = &DataBaseCommand{
		name:    "sinter",
		proc:    sinterCommandProcess,
		id:      1<<19 | 4,
		minArgs: 3,
		maxArgs: 3,
	}
	router["SUNION"] = &DataBaseCommand{
		name:    "sunion",
		proc:    sunionCommandProcess,
		id:      1<<19 | 5,
		minArgs: 3,
		maxArgs: 3,
	}
	router["SREM"] = &DataBaseCommand{
		name:    "srem",
		proc:    sremCommandProcess,
		id:      1<<19 | 6,
		minArgs: 3,
		maxArgs: 3,
	}
	// list
	router["LPUSH"] = &DataBaseCommand{
		name:    "lpush",
		proc:    lpushCommandProcess,
		id:      1<<20 | 1,
		minArgs: 3,
		maxArgs: 3,
	}
	router["LPOP"] = &DataBaseCommand{
		name:    "lpop",
		proc:    lpopCommandProcess,
		id:      1<<20 | 2,
		minArgs: 2,
		maxArgs: 2,
	}
	router["RPUSH"] = &DataBaseCommand{
		name:    "rpush",
		proc:    rpushCommandProcess,
		id:      1<<20 | 3,
		minArgs: 3,
		maxArgs: 3,
	}
	router["RPOP"] = &DataBaseCommand{
		name:    "rpop",
		proc:    rpopCommandProcess,
		id:      1<<20 | 4,
		minArgs: 2,
		maxArgs: 2,
	}
	router["LLEN"] = &DataBaseCommand{
		name:    "llen",
		proc:    llenCommandProcess,
		id:      1<<20 | 5,
		minArgs: 2,
		maxArgs: 2,
	}
	// keys
	router["RENAME"] = &DataBaseCommand{
		name:    "rename",
		proc:    renameCommandProcess,
		id:      1<<21 | 1,
		minArgs: 3,
		maxArgs: 3,
	}
	router["DEL"] = &DataBaseCommand{
		name:    "del",
		proc:    delCommandProcess,
		id:      1<<21 | 2,
		minArgs: 2,
		maxArgs: 2,
	}
	// system
	router["QUIT"] = &DataBaseCommand{
		name:    "quit",
		proc:    quitCommandProcess,
		id:      1,
		minArgs: 1,
		maxArgs: 1,
	}
}

func Handle(args []*DbObject, db *Database) string {
	cmdType := strings.ToUpper(args[0].StrVal())
	cmd := router[cmdType]
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
	key := args[1]
	if !checkString(key) {
		return packErrorMessage("illegal request parameter")
	}
	val, err := db.GetStr(key)
	if err != nil {
		return packErrorMessage(err.Error())
	}
	log.Printf("[GET COMMAND]Success\n")
	return packString(val.StrVal())
}

// 'set' Process Function
func setCommandProcess(args []*DbObject, db *Database) string {
	key := args[1]
	value := args[2]
	if !checkString(key) || !checkString(value) {
		return packErrorMessage("illegal request parameter")
	}
	if err := db.SetStr(key, value, DefaultExpireTime+getTime()); err != nil {
		return packErrorMessage(err.Error())
	}
	log.Printf("[SET COMMAND]Success\n")
	return packString("Query OK")
}

// 'setex' Process Function
func setexCommandProcess(args []*DbObject, db *Database) string {
	key := args[1]
	expire, err := args[2].IntVal()
	value := args[3]
	if !checkString(key) || !checkString(value) || err != nil {
		return packErrorMessage("illegal request parameter")
	}
	expire = expire * 1000000000 // to nano seconds
	if err := db.SetStr(key, value, expire); err != nil {
		return packErrorMessage(err.Error())
	}
	log.Printf("[SETEX COMMAND]Success\n")
	return packString("Query OK")
}

// 'setnx' Process Function
func setnxCommandProcess(args []*DbObject, db *Database) string {
	key := args[1]
	value := args[2]
	if !checkString(key) || !checkString(value) {
		return packErrorMessage("illegal request parameter")
	}
	if val, _ := db.GetStr(key); val != nil {
		return packString("Key already exist")
	}
	if err := db.SetStr(key, value, DefaultExpireTime+getTime()); err != nil {
		return packErrorMessage(err.Error())
	}
	log.Printf("[SETNX COMMAND]Success\n")
	return packString("Query OK")
}

// 'incrby' Process Function
func incrbyCommandProcess(args []*DbObject, db *Database) string {
	key := args[1]
	increment, err := args[2].IntVal()
	if !checkString(key) || err != nil {
		return packErrorMessage("illegal request parameter")
	}
	if err := db.Increment(key, increment); err != nil {
		return packErrorMessage(err.Error())
	}
	log.Printf("[INCRBY COMMAND]Success\n")
	return packString("Query OK")
}

// 'incr' Process Function
func incrCommandProcess(args []*DbObject, db *Database) string {
	key := args[1]
	if !checkString(key) {
		return packErrorMessage("illegal request parameter")
	}
	if err := db.Incr(key); err != nil {
		return packErrorMessage(err.Error())
	}
	log.Printf("[INCR COMMAND]Success\n")
	return packString("Query OK")
}

// 'decr' Process Function
func decrCommandProcess(args []*DbObject, db *Database) string {
	key := args[1]
	if !checkString(key) {
		return packErrorMessage("illegal request parameter")
	}
	if err := db.Decr(key); err != nil {
		return packErrorMessage(err.Error())
	}
	log.Printf("[DECR COMMAND]Success\n")
	return packString("Query OK")
}

// zset

// 'zadd' Process Function
func zaddCommandProcess(args []*DbObject, db *Database) string {
	// judge parameter
	key := args[1]
	score, err := args[2].IntVal()
	member := args[3]
	if err != nil || !checkString(member) || !checkString(key) {
		return packErrorMessage("Illegal request parameter")
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
	key := args[1]
	left, err1 := args[2].IntVal()
	right, err2 := args[3].IntVal()
	if err1 != nil || err2 != nil || !checkString(key) {
		return packErrorMessage("Illegal parameter, score must be an integer")
	}
	obj, err := db.GetKeyIfExist(key, ZSET)
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
	key := args[1]
	member := args[2]
	if !checkString(member) || !checkString(key) {
		return packErrorMessage("Illegal request parameter")
	}
	obj, err := db.GetKeyIfExist(args[1], ZSET)
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
	key := args[1]
	incr, err := args[2].IntVal()
	member := args[3]
	if err != nil || !checkString(key) || !checkString(member) {
		return packErrorMessage("Illegal request parameter")
	}
	obj, err := db.GetKeyIfExist(key, ZSET)
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

// 'zscore' Process Function
func zscoreCommandProcess(args []*DbObject, db *Database) string {
	key := args[1]
	member := args[2]
	if !checkString(key) || !checkString(member) {
		return packErrorMessage("Illegal request parameter")
	}
	obj, err := db.GetKeyIfExist(key, ZSET)
	if err != nil {
		return packErrorMessage(err.Error())
	}
	zset := obj.Val.(*Zset)
	score, err := zset.GetScore(member)
	if err != nil {
		return packErrorMessage(err.Error())
	}
	log.Printf("[ZSCORE COMMAND]Success\n")
	return packInt(int(score))
}

// hash

// 'hset' Process Function
func hsetCommandProcess(args []*DbObject, db *Database) string {
	key := args[1]
	field := args[2]
	value := args[3]
	if !checkString(key) || !checkString(field) || !checkString(value) {
		return packErrorMessage("Illegal request parameter")
	}
	obj, err := db.GetKeyObject(key, HASH)
	if err != nil {
		return packErrorMessage(err.Error())
	}
	hash := obj.Val.(*Hash)
	if err = hash.Set(field, value); err != nil {
		return packErrorMessage(err.Error())
	}
	log.Printf("[HSET COMMAND]Success\n")
	return packString("Query OK")
}

// 'hget' Process Function
func hgetCommandProcess(args []*DbObject, db *Database) string {
	key := args[1]
	field := args[2]
	if !checkString(key) || !checkString(field) {
		return packErrorMessage("Illegal request parameter")
	}
	obj, err := db.GetKeyIfExist(key, HASH)
	if err != nil {
		return packErrorMessage(err.Error())
	}
	hash := obj.Val.(*Hash)
	obj, err = hash.Get(field)
	if err != nil {
		return packErrorMessage(err.Error())
	}
	log.Printf("[HGET COMMAND]Success\n")
	return packString(obj.StrVal())
}

// 'hdel' Process Function
func hdelCommandProcess(args []*DbObject, db *Database) string {
	key := args[1]
	field := args[2]
	if !checkString(key) || !checkString(field) {
		return packErrorMessage("Illegal request parameter")
	}
	obj, err := db.GetKeyIfExist(key, HASH)
	if err != nil {
		return packErrorMessage(err.Error())
	}
	hash := obj.Val.(*Hash)
	if err = hash.Delete(field); err != nil {
		return packErrorMessage(err.Error())
	}
	log.Printf("[HDEL COMMAND]Success\n")
	return packString("Query OK")
}

// set

// 'sadd' Process Function
func saddCommandProcess(args []*DbObject, db *Database) string {
	key := args[1]
	member := args[2]
	if !checkString(key) || !checkString(member) {
		return packErrorMessage("Illegal request parameter")
	}
	obj, err := db.GetKeyObject(key, SET)
	if err != nil {
		return packErrorMessage(err.Error())
	}
	set := obj.Val.(*Set)
	if err = set.Add(member); err != nil {
		return packErrorMessage(err.Error())
	}
	log.Printf("[SADD COMMAND]Success\n")
	return packString("Query OK")
}

// 'smembers' Process Function
func smembersCommandProcess(args []*DbObject, db *Database) string {
	key := args[1]
	if !checkString(key) {
		return packErrorMessage("Illegal request parameter")
	}
	obj, err := db.GetKeyIfExist(key, SET)
	if err != nil {
		return packErrorMessage(err.Error())
	}
	set := obj.Val.(*Set)
	members := set.Members()
	strs := make([]string, len(members))
	for i, m := range members {
		strs[i] = m.StrVal()
	}
	log.Printf("[SMEMBERS COMMAND]Success\n")
	return packBulkArray(strs)
}

// 'scard' Process Function
func scardCommandProcess(args []*DbObject, db *Database) string {
	key := args[1]
	if !checkString(key) {
		return packErrorMessage("Illegal request parameter")
	}
	obj, err := db.GetKeyIfExist(key, SET)
	if err != nil {
		return packErrorMessage(err.Error())
	}
	set := obj.Val.(*Set)
	length := set.Length()
	log.Printf("[SCARD COMMAND]Success\n")
	return packInt(length)
}

// 'srem' Process Function
func sremCommandProcess(args []*DbObject, db *Database) string {
	key := args[1]
	member := args[2]
	if !checkString(key) || !checkString(member) {
		return packErrorMessage("Illegal request parameter")
	}
	obj, err := db.GetKeyIfExist(key, SET)
	if err != nil {
		return packErrorMessage(err.Error())
	}
	set := obj.Val.(*Set)
	if err = set.Remove(member); err != nil {
		return packErrorMessage(err.Error())
	}
	log.Printf("[SREM COMMAND]Success\n")
	return packBulkString("Query OK")
}

// 'sinter' Process Function
func sinterCommandProcess(args []*DbObject, db *Database) string {
	key1 := args[1]
	key2 := args[2]
	if !checkString(key1) || !checkString(key2) {
		return packErrorMessage("Illegal request parameter")
	}
	obj1, err := db.GetKeyIfExist(key1, SET)
	if err != nil {
		return packErrorMessage(err.Error())
	}
	set1 := obj1.Val.(*Set)
	obj2, err := db.GetKeyIfExist(key2, SET)
	if err != nil {
		return packErrorMessage(err.Error())
	}
	set2 := obj2.Val.(*Set)
	members := set1.Inter(set2)
	strs := make([]string, len(members))
	for i, m := range members {
		strs[i] = m.StrVal()
	}
	log.Printf("[SINTER COMMAND]Success\n")
	return packBulkArray(strs)
}

// 'sunion' Process Function
func sunionCommandProcess(args []*DbObject, db *Database) string {
	key1 := args[1]
	key2 := args[2]
	if !checkString(key1) || !checkString(key2) {
		return packErrorMessage("Illegal request parameter")
	}
	obj1, err := db.GetKeyIfExist(key1, SET)
	if err != nil {
		return packErrorMessage(err.Error())
	}
	set1 := obj1.Val.(*Set)
	obj2, err := db.GetKeyIfExist(key2, SET)
	if err != nil {
		return packErrorMessage(err.Error())
	}
	set2 := obj2.Val.(*Set)
	members := set1.Union(set2)
	strs := make([]string, len(members))
	for i, m := range members {
		strs[i] = m.StrVal()
	}
	log.Printf("[SUNION COMMAND]Success\n")
	return packBulkArray(strs)
}

// list

func lpushCommandProcess(args []*DbObject, db *Database) string {
	key := args[1]
	value := args[2]
	if !checkString(key) || !checkString(value) {
		return packErrorMessage("Illegal request parameter")
	}
	obj, err := db.GetKeyObject(key, LINKDLIST)
	if err != nil {
		return packErrorMessage(err.Error())
	}
	list := obj.Val.(*LinkedList)
	list.Lpush(value)
	log.Printf("[LPUSH COMMAND]Success\n")
	return packString("Query OK")
}

func lpopCommandProcess(args []*DbObject, db *Database) string {
	key := args[1]
	if !checkString(key) {
		return packErrorMessage("Illegal request parameter")
	}
	obj, err := db.GetKeyIfExist(key, LINKDLIST)
	if err != nil {
		return packErrorMessage(err.Error())
	}
	list := obj.Val.(*LinkedList)
	if len := list.Len(); len == 0 {
		return packErrorMessage("List is empty")
	}
	log.Printf("[LPOP COMMAND]Success\n")
	return packString(list.Lpop().StrVal())
}

func rpushCommandProcess(args []*DbObject, db *Database) string {
	key := args[1]
	value := args[2]
	if !checkString(key) || !checkString(value) {
		return packErrorMessage("Illegal request parameter")
	}
	obj, err := db.GetKeyObject(key, LINKDLIST)
	if err != nil {
		return packErrorMessage(err.Error())
	}
	list := obj.Val.(*LinkedList)
	list.Rpush(value)
	log.Printf("[RPUSH COMMAND]Success\n")
	return packString("Query OK")
}

func rpopCommandProcess(args []*DbObject, db *Database) string {
	key := args[1]
	if !checkString(key) {
		return packErrorMessage("Illegal request parameter")
	}
	obj, err := db.GetKeyIfExist(key, LINKDLIST)
	if err != nil {
		return packErrorMessage(err.Error())
	}
	list := obj.Val.(*LinkedList)
	if len := list.Len(); len == 0 {
		return packErrorMessage("List is empty")
	}
	log.Printf("[RPOP COMMAND]Success\n")
	return packString(list.Rpop().StrVal())
}

func llenCommandProcess(args []*DbObject, db *Database) string {
	key := args[1]
	if !checkString(key) {
		return packErrorMessage("Illegal request parameter")
	}
	obj, err := db.GetKeyIfExist(key, LINKDLIST)
	if err != nil {
		return packErrorMessage(err.Error())
	}
	list := obj.Val.(*LinkedList)
	return packInt(list.Len())
}

// system

func delCommandProcess(args []*DbObject, db *Database) string {
	key := args[1]
	if !checkString(key) {
		return packErrorMessage("Illegal request parameter")
	}
	if err := db.RemoveKey(key); err != nil {
		return packErrorMessage(err.Error())
	}
	log.Printf("[DEL COMMAND]Success\n")
	return packString("Query OK")
}

func renameCommandProcess(args []*DbObject, db *Database) string {
	key := args[1]
	newKey := args[2]
	if !checkString(key) || !checkString(newKey) {
		return packErrorMessage("Illegal request parameter")
	}
	if err := db.RenameKey(key, newKey); err != nil {
		return packErrorMessage(err.Error())
	}
	log.Printf("[RENAME COMMAND]Success\n")
	return packString("Query OK")
}

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
	n := len(msgs)
	var builder strings.Builder
	// head
	builder.WriteString(BulkArrayHead)
	builder.WriteString(strconv.Itoa(n))
	builder.WriteString(CRLF)
	for i := 0; i < n; i += 1 {
		builder.WriteString(packBulkString(msgs[i]))
	}
	return builder.String()
}

// string check
func checkString(obj *DbObject) bool {
	if obj == nil || obj.Type != STR || len(obj.StrVal()) == 0 {
		return false
	}
	return true
}

func getTime() int64 {
	return time.Now().UnixNano()
}
