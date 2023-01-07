package data_structure

import (
	"errors"
	"fmt"
	"math/rand"
	"time"
)

const (
	// 20层跳表
	maxLevel int = 20
)

func init() {
	rand.Seed(time.Now().Unix())
}

type SkipListNode struct {
	score *DbObject // must be integer
	val   *DbObject // val can be nil
	// next[i] 第i层的下一个节点
	next [maxLevel]*SkipListNode
}

type SkipList struct {
	valType         DbObjectType
	compareFunction CompareFunction
	equalFunction   EqualFunction
	root            *SkipListNode
}

func NewSkipList(valType DbObjectType, equalFunction EqualFunction) *SkipList {
	obj := NewObject(STR, "-1")
	root := newSkipListNode(obj, nil)
	return &SkipList{
		valType:         valType,
		compareFunction: IntCompare,
		equalFunction:   equalFunction,
		root:            root,
	}
}

func (skipList *SkipList) Search(score *DbObject) bool {
	if !validScore(score) {
		return false
	}
	findResult := skipList.find(score)
	return findResult[0].next[0] != nil && skipList.compareFunction(score, findResult[0].next[0].score) == 0
}

// Add
// if exist, then do nothing
// only create a new node
func (skipList *SkipList) Add(score, value *DbObject) error {
	if !validScore(score) {
		return errors.New("Score must be an integer")
	}
	findResult := skipList.find(score)
	// add from bottom to top
	newNode := newSkipListNode(score, value)
	for i := 0; i < maxLevel; i += 1 {
		newNode.next[i] = findResult[i].next[i]
		findResult[i].next[i] = newNode
		// 50% possibility to the next level
		if rand.Intn(2) == 0 {
			break
		}
	}
	return nil
}

// Delete
// if not exist, do nothing
func (skipList *SkipList) Delete(score, value *DbObject) error {
	if !validScore(score) {
		return errors.New("Score must be an integer")
	}
	findResult := skipList.find(score)
	toDel := findResult[0].next[0]
	for toDel != nil {
		if skipList.compareFunction(toDel.score, score) == 0 && !skipList.equalFunction(toDel.val, value) {
			toDel = toDel.next[0]
		} else {
			break
		}
	}
	// should delete
	if toDel != nil && skipList.compareFunction(toDel.score, score) == 0 && skipList.equalFunction(toDel.val, value) {
		for i := 0; i < maxLevel; i += 1 {
			last := findResult[i]
			current := findResult[i].next[i]
			// move
			for current != nil && current != toDel {
				if skipList.compareFunction(toDel.score, score) < 0 {
					break
				}
				last = current
				current = current.next[i]
			}
			// this level toDel exist
			if current != nil && current == toDel {
				last.next[i] = current.next[i]
			} else {
				break
			}
		}
	}
	return nil
	// else not exist
}

// Range
// Range values whose score is from left to right
func (skipList *SkipList) Range(left, right *DbObject) ([]*DbObject, []*DbObject) {
	scores := make([]*DbObject, 0)
	values := make([]*DbObject, 0)
	if !validScore(left) || !validScore(right) {
		return scores, values
	}
	findResult := skipList.find(left)
	for current := findResult[0].next[0]; current != nil; current = current.next[0] {
		if skipList.compareFunction(right, current.score) >= 0 {
			scores = append(scores, current.score)
			values = append(values, current.val)
		} else {
			break
		}
	}
	return scores, values
}

func (zset *SkipList) find(score *DbObject) []*SkipListNode {
	result := make([]*SkipListNode, maxLevel)
	current := zset.root
	for i := maxLevel - 1; i >= 0; i -= 1 {
		for current.next[i] != nil && zset.compareFunction(score, current.next[i].score) == 1 {
			current = current.next[i]
		}
		// result[i] the last node < val on level i
		result[i] = current
	}
	return result
}

func validScore(score *DbObject) bool {
	if _, err := score.IntVal(); err != nil {
		return false
	}
	return true
}

func newSkipListNode(score, val *DbObject) *SkipListNode {
	return &SkipListNode{
		score: score,
		val:   val,
		next:  [maxLevel]*SkipListNode{},
	}
}

/* TEST CODE */
func (skipList *SkipList) Print() {
	for i := maxLevel - 1; i >= 0; i -= 1 {
		fmt.Printf("LEVEL %d\n", i)
		for current := skipList.root.next[i]; current != nil; current = current.next[i] {
			val, _ := current.score.IntVal()
			fmt.Printf("score : %d ,   value : %s\n", val, current.val.StrVal())
		}
		fmt.Println()
	}
}
