package db

import . "goRedis/data_structure"

type LinkedList struct {
	data *List
}

func NewLinkedList() *LinkedList {
	return &LinkedList{
		data: NewList(StrEqual),
	}
}

func NewDefaultLinkedListValueObj() interface{} {
	return NewLinkedList()
}

func (list *LinkedList) Lpush(value *DbObject) {
	list.data.AppendFirst(value)
}

func (list *LinkedList) Lpop() *DbObject {
	return list.data.RemoveFirst()
}

func (list *LinkedList) Rpush(value *DbObject) {
	list.data.AppendLast(value)
}

func (list *LinkedList) Rpop() *DbObject {
	return list.data.RemoveLast()
}

func (list *LinkedList) Len() int {
	return list.data.Length()
}
