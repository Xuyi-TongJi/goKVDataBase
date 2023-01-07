package data_structure

import (
	"errors"
	"strconv"
)

type DbObjectType uint8
type DbObjectVal interface{}
type EqualFunction func(a, b *DbObject) bool
type HashFunction func(key *DbObject) int64

// return 1 if a > b -1 if a < b 0 if a == b
type CompareFunction func(a, b *DbObject) int

const (
	STR       DbObjectType = 0x01
	LIST      DbObjectType = 0x02
	SET       DbObjectType = 0x03
	ZSET      DbObjectType = 0x04
	DICT      DbObjectType = 0x05
	HASH      DbObjectType = 0x06
	LINKDLIST DbObjectType = 0x07
	NODE      DbObjectType = 0x08
)

type DbObject struct {
	Type DbObjectType
	Val  DbObjectVal
	Hash int64 // hash cache, hash cannot be negative, then hash can be initialized as -1 if not computed
}

// IntVal return -1 if invalid
func (obj *DbObject) IntVal() (ans int64, err error) {
	if obj.Type == STR {
		val, err := strconv.ParseInt(obj.StrVal(), 10, 64)
		if err != nil {
			return -1, err
		}
		return val, nil
	}
	return -1, errors.New("object type not supported")
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
		Hash: -1,
	}
}

func NewStr(val string) *DbObject {
	return NewObject(STR, val)
}

func NewObjectByInt(val int64) *DbObject {
	return NewObject(STR, strconv.FormatInt(val, 10))
}
