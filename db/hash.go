package db

import (
	"errors"
	. "goRedis/data_structure"
)

// Hash Key
// Both key and value must be string

type Hash struct {
	data *Dict
}

func NewHash() *Hash {
	return &Hash{
		data: NewDict(StrHash, StrEqual),
	}
}

func NewDefaultHashValueObj() interface{} {
	return NewHash()
}

func (hash *Hash) Get(key *DbObject) (*DbObject, error) {
	obj, err := hash.data.Get(key)
	if err == ERROR_KEY_NOT_EXIST {
		return nil, errors.New("Field does not exist in the hash key")
	}
	return obj, err
}

func (hash *Hash) Set(key, val *DbObject) error {
	if val == nil || val.Type != STR {
		return errors.New("Illegal value type, the value of hash key must be STR")
	}
	return hash.data.Set(key, val)
}

func (hash *Hash) Delete(key *DbObject) error {
	err := hash.data.Delete(key)
	if err == ERROR_KEY_NOT_EXIST {
		return errors.New("Field does not exist in the hash key")
	}
	return err
}
