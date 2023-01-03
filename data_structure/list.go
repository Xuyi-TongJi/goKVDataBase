package data_structure

// List 双向链表
// Node 链表节点 val为DbObject类型
// EqualFunc 节点判等函数

type Node struct {
	next *Node
	prev *Node
	val  *DbObject
}

type List struct {
	equalFunc EqualFunction
	head      *Node
	tail      *Node
	length    int
}

func NewList(equalFunction EqualFunction) *List {
	headDummy := &Node{
		val:  nil,
		next: nil,
		prev: nil,
	}
	tailDummy := &Node{
		val:  nil,
		next: nil,
		prev: nil,
	}
	headDummy.next = tailDummy
	tailDummy.prev = headDummy
	return &List{
		equalFunc: equalFunction,
		head:      headDummy,
		tail:      tailDummy,
		length:    0,
	}
}

func (list *List) Length() int {
	return list.length
}

func (list *List) First() *DbObject {
	if list.head.next != list.tail {
		return list.head.next.val
	}
	return nil
}

func (list *List) Last() *DbObject {
	if list.tail.prev != list.head {
		return list.tail.prev.val
	}
	return nil
}

func (list *List) AppendLast(toAdd *DbObject) {
	current := &Node{
		val:  toAdd,
		next: nil,
		prev: nil,
	}
	last := list.tail.prev
	last.next = current
	list.tail.prev = current
	current.next = list.tail
	current.prev = last
}

func (list *List) AppendFirst(toAdd *DbObject) {
	current := &Node{
		val:  toAdd,
		next: nil,
		prev: nil,
	}
	next := list.head.next
	next.prev = current
	list.head.next = current
	current.next = next
	current.prev = list.head
}

// Delete if not exist, do nothing
func (list *List) Delete(val *DbObject) {
	if toDel := list.find(val); toDel != nil {
		last := list.head
		current := list.head.next
		for current != toDel {
			last = current
			current = current.next
		}
		// delete
		last.next = current.next
		current.next.prev = last
	}
}

// find O(N) return nil if not exist
func (list *List) find(toFind *DbObject) *Node {
	for current := list.head.next; current != list.tail; current = current.next {
		if list.equalFunc(toFind, current.val) {
			return current
		}
	}
	return nil
}
