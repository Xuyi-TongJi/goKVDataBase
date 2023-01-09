package data_structure

import (
	"errors"
	"math/rand"
)

// EqualFunction 链地址，哈希冲突时判断相等
// HashFunction key -> hash value

const (
	RehashStep          int     = 1
	RehashRatio         float64 = 0.75 // load factor
	DefaultInitSize     int64   = 1 << 4
	MaxSize             int64   = 1 << 30
	MaxInitSize         int64   = 1 << 10
	MaxRandomGetAttempt int     = 100
)

var (
	ERROR_KEY_NOT_EXIST error = errors.New("key does not exist in database")
)

type Entry struct {
	key  *DbObject
	val  *DbObject
	next *Entry // 链地址
}

type HashTable struct {
	table []*Entry
	size  int64
	mask  int64 // mask = size - 1
	used  int64
}

type Dict struct {
	equalFunc EqualFunction
	hashFunc  HashFunction
	// hashTables[2] is used to rehash
	hashTables [2]*HashTable
	// rehashIndex == -1 代表当前没有进行rehash
	rehashIndex int64
}

func NewHashTable(size int64) *HashTable {
	return &HashTable{
		table: make([]*Entry, size),
		size:  size,
		mask:  size - 1,
		used:  0,
	}
}

func NewDict(hf HashFunction, ef EqualFunction) *Dict {
	dict, _ := NewDictBySize(hf, ef, DefaultInitSize)
	return dict
}

func NewDictBySize(hf HashFunction, ef EqualFunction, initSize int64) (*Dict, error) {
	if initSize > MaxInitSize {
		return nil, errors.New("Illegal init size")
	}
	hash0 := NewHashTable(nextSize(initSize))
	hash := [2]*HashTable{}
	hash[0] = hash0
	return &Dict{
		equalFunc:   ef,
		hashFunc:    hf,
		hashTables:  hash,
		rehashIndex: -1,
	}, nil
}

// hash
// hash function
// return -1 if occur error
func (dict *Dict) hash(key *DbObject) int64 {
	if key == nil {
		return -1
	}
	h := dict.hashFunc(key)
	if h == -1 {
		return -1
	}
	return (h >> 16) ^ h
}

// keyIndex
// key index of key in dict hashTable
// return -1 if occur error
func (dict *Dict) keyIndex(key *DbObject, hashTableIndex int) int64 {
	h := dict.hash(key)
	// illegal
	if h == -1 || hashTableIndex >= 2 {
		return -1
	}
	return h % dict.hashTables[hashTableIndex].mask
}

// expand -> rehash

// expand
// if ok, start rehash
func (dict *Dict) expand(nextSize int64) {
	// is already expanded or is expanding
	if dict.isRehashing() || (dict.hashTables[0].size >= nextSize) {
		return
	}
	newHashTable := NewHashTable(nextSize)
	// start rehashIndex
	dict.rehashIndex = 0
	dict.hashTables[1] = newHashTable
}

// expandIfNeeded
// judge whether this hash table needed to be expand
func (dict *Dict) expandIfNeeded() {
	size := dict.hashTables[0].size
	// cannot expand
	if dict.isRehashing() {
		return
	}
	// expand
	if float64(dict.hashTables[0].used)/float64(size) >= RehashRatio {
		nextSize := nextSize(size << 1)
		// cannot expand
		if nextSize > MaxSize {
			return
		}
		dict.expand(nextSize)
	}
}

func (dict *Dict) isRehashing() bool {
	return dict.rehashIndex != -1
}

// rehashStep
// do a rehash step
func (dict *Dict) rehashStep() {
	if !dict.isRehashing() {
		return
	}
	// the end of rehash
	dict.endRehash()
	for ; dict.hashTables[0].table[dict.rehashIndex] == nil; dict.rehashIndex += 1 {
	}
	// data remove
	current := dict.hashTables[0].table[dict.rehashIndex]
	for current != nil {
		nextEntry := current.next
		newIndex := dict.keyIndex(current.key, 1)
		// 头插法
		head := dict.hashTables[1].table[newIndex]
		dict.hashTables[1].table[newIndex] = current
		current.next = head
		current = nextEntry
		// update used
		dict.hashTables[1].used += 1
		dict.hashTables[0].used -= 1
	}
	dict.hashTables[0].table[dict.rehashIndex] = nil
	dict.endRehash()
}

func (dict *Dict) rehash(step int) {
	for i := 0; i < step && dict.isRehashing(); i += 1 {
		dict.rehashStep()
	}
}

func (dict *Dict) endRehash() {
	if dict.hashTables[0].used == 0 {
		dict.hashTables[0] = dict.hashTables[1]
		dict.hashTables[1] = nil
		dict.rehashIndex = -1
	}
}

func (dict *Dict) searchIndex() int {
	if dict.isRehashing() {
		return 1
	} else {
		return 0
	}
}

func (dict *Dict) addRaw(key *DbObject) *Entry {
	htIndexToAdd := dict.searchIndex()
	index := dict.keyIndex(key, htIndexToAdd)
	entries := dict.hashTables[htIndexToAdd].table[index]
	newEntry := &Entry{
		key:  key,
		val:  nil,
		next: entries,
	}
	// 头插法
	dict.hashTables[htIndexToAdd].table[index] = newEntry
	dict.hashTables[htIndexToAdd].used += 1
	return newEntry
}

// find
// if not exist, return nil
// return nil
// return error if param illegal
func (dict *Dict) find(key *DbObject) (*Entry, error) {
	// when rehashing search both hashTables, or search hashTable 0
	searchRange := dict.searchIndex()
	for i := 0; i <= searchRange; i += 1 {
		index := dict.keyIndex(key, i)
		if index == -1 {
			return nil, errors.New("illegal key")
		}
		current := dict.hashTables[i].table[index]
		for current != nil {
			if dict.equalFunc(current.key, key) {
				return current, nil
			}
			current = current.next
		}
	}
	return nil, ERROR_KEY_NOT_EXIST
}

// public

// Set / Add
// if key exist -> then do Set
// if key doesn't exsit -> then do Add
// val can be set nil
// an empty string key is not availble
// key type must be STR
func (dict *Dict) Set(key, val *DbObject) error {
	if key == nil || key.Type != STR || len(key.StrVal()) == 0 {
		return errors.New("illegal key")
	}
	if dict.isRehashing() {
		dict.rehash(RehashStep)
	}
	entry, _ := dict.find(key)
	if entry == nil {
		// add (must be valid key)
		entry = dict.addRaw(key)
		dict.expandIfNeeded()
	}
	entry.val = val
	return nil
}

// Delete
// if not exist, do nothing and return an error
func (dict *Dict) Delete(key *DbObject) error {
	if key == nil || key.Type != STR {
		return errors.New("illegal key")
	}
	if dict.isRehashing() {
		dict.rehash(RehashStep)
	}
	searchIndex := dict.searchIndex()
	for i := 0; i <= searchIndex; i += 1 {
		index := dict.keyIndex(key, i)
		var last *Entry = nil
		for current := dict.hashTables[i].table[index]; current != nil; current = current.next {
			if dict.equalFunc(current.key, key) {
				if last == nil {
					// head node
					dict.hashTables[i].table[index] = current.next
				} else {
					last.next = current.next
				}
				// update
				dict.hashTables[i].used -= 1
				return nil
			}
			last = current
		}
	}
	return ERROR_KEY_NOT_EXIST
}

// Get
// if not exist, return an error
// obj != nil only if it exists
func (dict *Dict) Get(key *DbObject) (*DbObject, error) {
	if key == nil || key.Type != STR {
		return nil, errors.New("illegal key")
	}
	if dict.isRehashing() {
		dict.rehash(RehashStep)
	}
	entry, err := dict.find(key)
	if err != nil {
		return nil, err
	}
	// entry != nil
	return entry.val, nil
}

// RandomGet
// get an entry randomly, failed(return nil) if attempts over max attempts
// do only if load factor > 0.5
func (dict *Dict) RandomGet() *Entry {
	if dict.isRehashing() {
		dict.rehash(RehashStep)
	}
	var result *Entry
	getIndex := 0
	if dict.isRehashing() && dict.hashTables[0].used < dict.hashTables[1].used {
		getIndex = 1
	}
	size := dict.hashTables[getIndex].size
	for i := 0; i < MaxRandomGetAttempt; i += 1 {
		index := rand.Int63n(size)
		result = dict.hashTables[getIndex].table[index]
		if result != nil {
			break
		}
	}
	if result != nil {
		current := result
		count := 0
		for current != nil {
			count += 1
			current = current.next
		}
		index := rand.Int63n(int64(count))
		current = result
		var num int64 = 0
		for num < index {
			current = current.next
			num += 1
		}
		return current
	}
	return nil
}

// Exist
// judge whether a key exists
func (dict *Dict) Exist(key *DbObject) (bool, error) {
	if key == nil || key.Type != STR {
		return false, errors.New("illegal key")
	}
	if dict.isRehashing() {
		dict.rehash(RehashStep)
	}
	entry, err := dict.find(key)
	if err != nil && err != ERROR_KEY_NOT_EXIST {
		return false, err
	}
	// entry != nil
	return entry != nil, nil
}

// nextSize
// the first power of 2 that >= size O(logN)
func nextSize(size int64) int64 {
	var nextSize int64 = 1
	for nextSize < size {
		nextSize <<= 1
	}
	return nextSize
}
