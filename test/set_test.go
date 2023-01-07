package test

import (
	"fmt"
	. "goRedis/data_structure"
	. "goRedis/db"
	"testing"
)

func TestSet(t *testing.T) {
	set := NewSet()
	var i int64 = 0
	for ; i < 100; i += 1 {
		// 0~99
		set.Add(NewObjectByInt(i))
	}
	/*i = 50
	for ; i < 100; i += 1 {
		if err := set.Remove(NewObjectByInt(i)); err != nil {
			log.Printf("[ERROR %d] \n", i)
		}
	}
	set.Print()
	fmt.Println(set.Used())*/
	set2 := NewSet()
	i = 55
	for ; i < 200; i += 1 {
		// 55~200
		set2.Add(NewObjectByInt(i))
	}
	for _, m := range set.Union(set2) {
		fmt.Printf("%s  ", m.StrVal())
	}
	fmt.Println()
	for _, m := range set.Inter(set2) {
		fmt.Printf("%s  ", m.StrVal())
	}
	fmt.Println()
}
