package data_structure

// EqualFunction 链地址，哈希冲突时判断相等
// HashFunction key -> hash value

type HashFunction func(key *DbObject) int64

type Entry struct {
	key *DbObject
	val *DbObject
	// 链地址
	next *Entry
}

type HashTable struct {
	table []*Entry
	size  int64
	mask  int64
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

func NewDict(hf HashFunction, ef EqualFunction) *Dict {
	return &Dict{
		equalFunc:   ef,
		hashFunc:    hf,
		rehashIndex: -1,
	}
}

func (dict *Dict) isRehashing() bool {
	return dict.rehashIndex != -1
}

func (dict *Dict) rehashStep() {

}

func (dict *Dict) rehash(step int) {

}

func nextPower(size int64) int64 {
	return 0
}

func (dict *Dict) expand(size int64) error {
	return nil
}

func (dict *Dict) expandIfNeeded() error {
	return nil
}

func (dict *Dict) keyIndex(key *DbObject) int64 {
	return 0
}

func (dict *Dict) AddRaw(key *DbObject) *Entry {
	return nil
}

func (dict *Dict) Add(key, val *DbObject) error {
	return nil
}

func (dict *Dict) Set(key, val *DbObject) error {
	return nil
}

func (dict *Dict) Delete(key *DbObject) error {
	return nil
}

func (dict *Dict) Find(key *DbObject) *Entry {
	return nil
}

func (dict *Dict) Get(key *DbObject) *DbObject {
	if entry := dict.Find(key); entry != nil {
		return entry.val
	}
	return nil
}

func (dict *Dict) RandomGet() *Entry {
	return nil
}
