package data_structure

import "hash/fnv"

// database string lib
// string is implemented by Golang String

// StrHash String Hash
// return -1 to handle error
func StrHash(o *DbObject) int64 {
	// 字符串哈希值
	if o.Type == STR {
		hash := fnv.New64()
		if _, err := hash.Write([]byte(o.StrVal())); err != nil {
			return -1
		}
		return int64(hash.Sum64())
	}
	return -1
}

// StrEqual
// Judge whether two STRING object is equals or not
// When hash conflict, use to find the key
func StrEqual(o1, o2 *DbObject) bool {
	if o1.Type == STR && o2.Type == STR {
		return o1.StrVal() == o2.StrVal()
	}
	return false
}
