package test

import (
	"fmt"
	. "goRedis/data_structure"
	"testing"
)

func TestSkipList(t *testing.T) {
	skipList := NewSkipList(STR, StrEqual)
	var i int64
	for i = 100; i >= 0; i -= 1 {
		skipList.Add(NewObjectByInt(i), NewObjectByInt(i))
	}
	skipList.Print()
	fmt.Println()
	// test delete key score
	/*for i = 0; i <= 100; i += 1 {
		if i%2 == 1 {
			skipList.Delete(NewObjectByInt(i), NewObjectByInt(i+10))
		} else {
			skipList.Delete(NewObjectByInt(i), NewObjectByInt(i+10))
		}
	}*/
	for i = 100; i >= 0; i -= 1 {
		if i%2 == 1 {
			skipList.Delete(NewObjectByInt(i), NewObjectByInt(i))
		}
	}
	skipList.Print()
	a, b := skipList.Range(NewObjectByInt(20), NewObjectByInt(40))
	fmt.Println(len(a), len(b))
	for _, aa := range a {
		fmt.Printf("%s  ", aa.StrVal())
	}
	fmt.Println()
	for _, bb := range b {
		fmt.Printf("%s  ", bb.StrVal())
	}
	fmt.Println()
}
