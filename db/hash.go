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
	return hash.data.Get(key)
}

func (hash *Hash) Set(key, val *DbObject) error {
	if val == nil || val.Type != STR {
		return errors.New("Illegal value type, the value of hash key must be STR")
	}
	return hash.data.Set(key, val)
}

func (hash *Hash) Delete(key *DbObject) error {
	return hash.Delete(key)
}
