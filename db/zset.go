package db

import (
	"errors"
	. "goRedis/data_structure"
)

const (
	MaxScore int64 = 1 << 60
)

type Zset struct {
	dict     *Dict
	skipList *SkipList
}

func NewZset() *Zset {
	return &Zset{
		dict:     NewDict(StrHash, StrEqual),
		skipList: NewSkipList(STR, StrEqual),
	}
}

func NewDefaultZsetValueObj() interface{} {
	return NewZset()
}

func (zset *Zset) GetScore(member *DbObject) (int64, error) {
	obj, err := zset.dict.Get(member)
	if err != nil {
		return 0, err
	}
	val, _ := obj.IntVal()
	return val, nil
}

func (zset *Zset) UpdateScore(member *DbObject, score int64) error {
	obj, err := zset.dict.Get(member)
	if obj == nil || err != nil {
		return errors.New("No member to update")
	}
	// delete
	if err = zset.Remove(member); err != nil {
		return err
	}
	// set new member
	zset.AddMember(member, score)
	return nil
}

// AddMember
// add a member to zset
func (zset *Zset) AddMember(member *DbObject, score int64) error {
	if score > MaxScore {
		return errors.New("Score value overflows")
	}
	obj, err := zset.dict.Get(member)
	if obj != nil {
		return errors.New("Member already exists")
	}
	if err != nil && err != ERROR_KEY_NOT_EXIST {
		return err
	}
	query := NewObjectByInt(score)
	zset.dict.Set(member, query)
	zset.skipList.Add(query, member)
	return nil
}

// ZRange
// return scores and values
func (zset *Zset) ZRange(left, right int64) ([]*DbObject, []*DbObject) {
	return zset.skipList.Range(NewObjectByInt(left), NewObjectByInt(right))
}

func (zset *Zset) Remove(member *DbObject) error {
	score, err := zset.dict.Get(member)
	if err != nil {
		return nil
	}
	zset.skipList.Delete(score, member)
	zset.dict.Delete(member)
	return nil
}

func (zset *Zset) Incr(member *DbObject, incr int64) error {
	obj, err := zset.dict.Get(member)
	if err != nil {
		return err
	}
	score, _ := obj.IntVal()
	score += incr
	if score > MaxScore {
		return errors.New("Score value overflows")
	}
	if err = zset.UpdateScore(member, score); err != nil {
		return err
	}
	return nil
}

/* TEST CODE */
func (zset *Zset) Print() {
	zset.skipList.Print()
}
