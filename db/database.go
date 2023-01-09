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
	MaxIntegerNumber  int64 = 1 << 60
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
	defaultDataStructure[LINKDLIST] = NewDefaultLinkedListValueObj
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
	val, err := db.doGetByType(key, STR)
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (db *Database) Increment(key *DbObject, value int64) error {
	obj, err := db.doGetByType(key, STR)
	if err != nil {
		return err
	}
	oldVal, err := obj.IntVal()
	if err != nil {
		return errors.New("The value of this key is not a number")
	}
	oldVal += value
	if oldVal > MaxScore {
		return errors.New("The new value will be overflowed")
	}
	return db.doSetStr(key, NewObjectByInt(oldVal))
}

func (db *Database) Incr(key *DbObject) error {
	return db.Increment(key, 1)
}

func (db *Database) Decr(key *DbObject) error {
	return db.Increment(key, -1)
}

// keys

// Exist
func (db *Database) Exist(key *DbObject) (bool, error) {
	obj, err := db.doGet(key)
	if err != nil {
		return false, err
	}
	return obj != nil, nil
}

// Expired
// judge whether a key is expired or not
func (db *Database) Expired(key *DbObject, expectedType DbObjectType) (bool, error) {
	ext, err := db.data.Exist(key)
	if err != nil {
		return false, err
	}
	if ext {
		if db.deleteIfExpired(key) {
			return true, nil
		} else {
			return false, nil
		}
	} else {
		return false, ERROR_KEY_NOT_EXIST
	}
}

// RenameKey
// rename a key only if it exists in db
func (db *Database) RenameKey(key *DbObject, newName *DbObject) error {
	obj, err := db.doGet(key)
	if err != nil {
		return err
	}
	// key must in the db
	expiredTime, err := db.doGetExpired(key)
	if err != nil {
		return err
	}
	if err = db.RemoveKey(key); err != nil {
		return err
	}
	if err = db.doSet(newName, obj); err != nil {
		return err
	}
	// set expiredTime
	if err = db.expire.Set(newName, NewObjectByInt(expiredTime)); err != nil {
		return err
	}
	return nil
}

// RemoveKey
// remove a key only if it exists in db
func (db *Database) RemoveKey(key *DbObject) error {
	ext, err := db.Exist(key)
	if err != nil {
		return err
	}
	if ext {
		if err = db.doRemove(key); err != nil {
			return err
		}
		return nil
	}
	return ERROR_KEY_NOT_EXIST
}

// GetKeyIfExist
// get the value of key in db only if it exists now
func (db *Database) GetKeyIfExist(key *DbObject, expectedType DbObjectType) (*DbObject, error) {
	obj, _ := db.doGetByType(key, expectedType)
	if obj == nil {
		return nil, ERROR_KEY_NOT_EXIST
	}
	return obj, nil
}

// GetKeyObject
// if key not exist, then add a default key (except string key)
func (db *Database) GetKeyObject(key *DbObject, expectedType DbObjectType) (*DbObject, error) {
	obj, err := db.doGetByType(key, expectedType)
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

// doSetStr
// if the key exist, do update; otherwise, do add
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
// get a value of key
func (db *Database) doGet(key *DbObject) (*DbObject, error) {
	val, err := db.data.Get(key)
	if err != nil {
		return nil, err
	}
	if db.deleteIfExpired(key) {
		return nil, util.ERROR_EXPIRED
	}
	return val, err
}

// doGetByType
// get a value of key after expired is judged
func (db *Database) doGetByType(key *DbObject, expectedType DbObjectType) (*DbObject, error) {
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

// get the expired time of key if it exists in database
func (db *Database) doGetExpired(key *DbObject) (int64, error) {
	expired, err := db.expire.Get(key)
	if err != nil {
		return 0, err
	}
	return expired.IntVal()
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

// doRemove
// remove the key if it exists
func (db *Database) doRemove(key *DbObject) error {
	if err := db.data.Delete(key); err != nil {
		return err
	}
	if err := db.expire.Delete(key); err != nil {
		return err
	}
	return nil
}

// doSet
// do set key and value
// if key already exists, update it whatever
func (db *Database) doSet(key, val *DbObject) error {
	return db.data.Set(key, val)
}

//
// check whether the key is expired or not, if it is, then delete it
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
