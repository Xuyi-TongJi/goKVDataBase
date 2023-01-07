package test

import (
	. "goRedis/data_structure"
	. "goRedis/db"
	"testing"
)

func TestZset(t *testing.T) {
	zset := NewZset()
	var i int64 = 0
	zset.AddMember(NewObject(STR, "test"), i)
	for ; i <= 10000; i += 1 {
		zset.Incr(NewObject(STR, "test"), 1000)
	}
	zset.Print()
}
