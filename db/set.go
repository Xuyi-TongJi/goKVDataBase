package db

import (
	"errors"
	. "goRedis/data_structure"
)

// Set key
// dict key type must be STR, value must be the Node ptr in the list

type Set struct {
	dict *Dict
	list *List
}

func NewSet() *Set {
	return &Set{
		dict: NewDict(StrHash, StrEqual),
		list: NewList(StrEqual),
	}
}

func NewDefaultSetValueObj() interface{} {
	return NewSet()
}

func (set *Set) Add(key *DbObject) error {
	ext, err := set.dict.Exist(key)
	if !ext {
		// add only if not exist
		return set.doAdd(key)
	} else if err != nil {
		return err
	} else {
		return errors.New("key already exists")
	}
}

func (set *Set) Remove(key *DbObject) error {
	ext, err := set.dict.Exist(key)
	if ext {
		return set.doRemove(key)
	} else if err != nil {
		return err
	} else {
		return ERROR_KEY_NOT_EXIST
	}
}

func (set *Set) Members() []*DbObject {
	return set.list.Members()
}

func (set *Set) Length() int {
	return set.list.Length()
}

func (set *Set) Inter(other *Set) []*DbObject {
	if set.Length() > other.Length() {
		return doInter(other, set)
	}
	return doInter(set, other)
}

func (set *Set) Union(other *Set) []*DbObject {
	memA := set.Members()
	memB := other.Members()
	result := make([]*DbObject, 0)
	for _, m := range memA {
		result = append(result, m)
	}
	for _, m := range memB {
		if ext, _ := set.dict.Exist(m); !ext {
			result = append(result, m)
		}
	}
	return result
}

func (set *Set) doAdd(key *DbObject) error {
	// add the same object in list and set
	set.list.AppendLast(key)
	node := set.list.GetLastNode()
	if err := set.dict.Set(key, NewObject(NODE, node)); err != nil {
		return err
	}
	return nil
}

// list lazy delete
// O1 delete (the same object)
func (set *Set) doRemove(key *DbObject) error {
	val, err := set.dict.Get(key)
	if err != nil {
		return err
	}
	node := val.Val.(*Node)
	if err := set.dict.Delete(key); err != nil {
		return err
	}
	set.list.DeleteByNode(node)
	return nil
}

func doInter(a, b *Set) []*DbObject {
	members := a.Members()
	result := make([]*DbObject, 0)
	for _, m := range members {
		if ext, _ := b.dict.Exist(m); ext {
			result = append(result, m)
		}
	}
	return result
}
