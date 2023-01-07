package data_structure

import "hash/fnv"

// database string lib
// **** string(SDS) is implemented by Golang String

// StrHash String Hash
// return -1 to handle error
func StrHash(o *DbObject) int64 {
	if o.Hash != -1 {
		return o.Hash
	}
	// 字符串哈希值
	if o.Type == STR {
		hash := fnv.New64()
		if _, err := hash.Write([]byte(o.StrVal())); err != nil {
			return -1
		}
		o.Hash = int64(hash.Sum64())
		return o.Hash
	}
	return -1
}

// StrEqual
// Judge whether two STRING object is equals or not
// When hash conflict, use to find the key
func StrEqual(o1, o2 *DbObject) bool {
	if o1 == o2 {
		return true
	}
	if o1.Type == STR && o2.Type == STR {
		return o1.StrVal() == o2.StrVal()
	}
	return false
}

// IntCompare
// return -1 if  o1 < o2 0 if o1 == o2 1 if o1 > o2
// return -2 if invalid
func IntCompare(o1, o2 *DbObject) int {
	i1, err1 := o1.IntVal()
	i2, err2 := o2.IntVal()
	if err1 != nil || err2 != nil {
		return -2
	}
	if i1 > i2 {
		return 1
	} else if i1 == i2 {
		return 0
	}
	return -1
}
