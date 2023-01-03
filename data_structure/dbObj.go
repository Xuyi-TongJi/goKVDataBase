package data_structure

import "strconv"

type DbObjectType uint8
type DbObjectVal interface{}
type EqualFunction func(a, b *DbObject) bool

const (
	STR  DbObjectType = 0x01
	LIST DbObjectType = 0x02
	SET  DbObjectType = 0x03
	ZSET DbObjectType = 0x04
	DICT DbObjectType = 0x05
)

type DbObject struct {
	Type DbObjectType
	Val  DbObjectVal
}

// IntVal return -1 if invalid
func (obj *DbObject) IntVal() int64 {
	if obj.Type == STR {
		val, err := strconv.ParseInt(obj.StrVal(), 10, 64)
		if err != nil {
			return val
		}
	}
	return -1
}

// StrVal return "" if invalid
func (obj *DbObject) StrVal() string {
	if obj.Type != STR {
		return ""
	} else {
		return obj.Val.(string)
	}
}

func NewObject(t DbObjectType, v DbObjectVal) *DbObject {
	return &DbObject{
		Type: t,
		Val:  v,
	}
}

func NewObjectByInt(val int64) *DbObject {
	return &DbObject{
		Type: STR,
		Val:  strconv.FormatInt(val, 10),
	}
}
