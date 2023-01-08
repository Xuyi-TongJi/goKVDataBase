package db

import (
	"errors"
	. "goRedis/data_structure"
	"goRedis/util"
	"time"
)

// database DB core lib

type defaultNewDataStructure func() interface{}

const (
	// default expire time : 1 hour (nano)
	DefaultExpireTime int64 = 3600 * 1000000000
)

var defaultDataStructure map[DbObjectType]defaultNewDataStructure

type Database struct {
	// 数据
	data *Dict
	// 过期
	expire *Dict
}

func init() {
	// defaultDataStructure
	defaultDataStructure = make(map[DbObjectType]defaultNewDataStructure, 0)
	defaultDataStructure[LIST] = NewDefaultLinkedListValueObj
	defaultDataStructure[ZSET] = NewDefaultZsetValueObj
	defaultDataStructure[HASH] = NewDefaultHashValueObj
	defaultDataStructure[SET] = NewDefaultSetValueObj
}

// string
func (db *Database) SetStr(key, value *DbObject, expireTime int64) error {
	if err := db.doSetStr(key, value); err != nil {
		return err
	}
	// set expire
	db.expire.Set(key, NewObjectByInt(expireTime))
	return nil
}

func (db *Database) GetStr(key *DbObject) (*DbObject, error) {
	val, err := db.doGet(key, STR)
	if err != nil {
		return nil, err
	}
	return val, nil
}

// GetKeyIfExist
// get the value of key in db only if it exists now
func (db *Database) GetKeyIfExist(key *DbObject, expectedType DbObjectType) (*DbObject, error) {
	obj, _ := db.doGet(key, expectedType)
	if obj == nil {
		return nil, ERROR_KEY_NOT_EXIST
	}
	return obj, nil
}

// GetKeyObject
// if key not exist, then add a default key (except string key)
func (db *Database) GetKeyObject(key *DbObject, expectedType DbObjectType) (*DbObject, error) {
	obj, err := db.doGet(key, expectedType)
	if err != nil || obj == nil {
		// add when not exist or expired
		if err == ERROR_KEY_NOT_EXIST || err == util.ERROR_EXPIRED {
			obj, err = db.doAddDefault(key, expectedType, DefaultExpireTime+getTime())
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	return obj, nil
}

func (db *Database) doSetStr(key, val *DbObject) error {
	oldVal, err := db.data.Get(key)
	if oldVal != nil && oldVal.Type != STR {
		return errors.New("Illegal key type")
	}
	if err != nil && err != ERROR_KEY_NOT_EXIST {
		return err
	}
	err = db.data.Set(key, val)
	if err != nil {
		return err
	}
	return nil
}

// doGet
// get a value of key after expired is judged
func (db *Database) doGet(key *DbObject, expectedType DbObjectType) (*DbObject, error) {
	val, err := db.data.Get(key)
	if val != nil && val.Type != expectedType {
		return nil, errors.New("Illegal key type")
	}
	if err != nil {
		return nil, err
	}
	if db.deleteIfExpired(key) {
		return nil, util.ERROR_EXPIRED
	}
	return val, nil
}

// add an empty data structure of key to database
// if success, return the obj added
func (db *Database) doAddDefault(key *DbObject, expectedType DbObjectType, expireTime int64) (*DbObject, error) {
	_, exist := defaultDataStructure[expectedType]
	if !exist {
		return nil, errors.New("Illegal value type")
	}
	var defaultFunc defaultNewDataStructure = defaultDataStructure[expectedType]
	ds := defaultFunc()
	obj := NewObject(expectedType, ds)
	if err := db.data.Set(key, obj); err != nil {
		return nil, err
	}
	// expired time
	db.expire.Set(key, NewObjectByInt(expireTime))
	return obj, nil
}

// delete over expired key

func (db *Database) deleteIfExpired(key *DbObject) bool {
	current := getTime()
	if key != nil {
		expire, _ := db.expire.Get(key)
		expireTime, _ := expire.IntVal()
		if current >= expireTime {
			db.data.Delete(key)
			db.expire.Delete(key)
			return true
		}
	}
	return false
}

// NewDatabase
// init database
func NewDatabase() *Database {
	return &Database{
		data:   NewDict(StrHash, StrEqual),
		expire: NewDict(StrHash, StrEqual),
	}
}

func getTime() int64 {
	return time.Now().UnixNano()
}
