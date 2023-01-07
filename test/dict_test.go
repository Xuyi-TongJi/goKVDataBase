package test

import (
	"fmt"
	. "goRedis/data_structure"
	"strconv"
	"testing"
)

func TestDict(t *testing.T) {
	dict := NewDict(StrHash, StrEqual)
	for i := 0; i < 100000; i += 1 {
		dict.Set(NewStr(strconv.Itoa(i)), NewStr(strconv.Itoa(i)))
		/*if dict.GetRehashIndex() == -1 {
			fmt.Printf("HASH CONFLICT %d\n", dict.HashConflict())
		}*/
	}
	for i := 0; i < 100000; i += 1 {
		res, _ := dict.Get(NewStr(strconv.Itoa(i)))
		fmt.Printf("%s\n", res.StrVal())
	}
	/*dict.Set(NewStr("KEY"), NewStr("VAL"))
	s, err := dict.Get(NewStr("KEY"))
	if err != nil {
		log.Printf("%s\n", err)
	}
	if s != nil {
		fmt.Printf("%s\n", s.StrVal())
	}
	dict.Set(NewStr("KEY"), NewStr("DFSDFSDF"))
	dict.Set(NewStr("KEEEEEEEE"), NewStr("DFSDFSDF"))
	s, err = dict.Get(NewStr("KEY"))
	if s != nil {
		fmt.Printf("%s\n", s.StrVal())
	}*/
	//fmt.Printf("HASH CONFLICT %d\n", dict.HashConflict())
	//fmt.Printf("%d\n", dict.Used())
}
