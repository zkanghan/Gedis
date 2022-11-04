package main

//  list 双向链表数据结构

type Node struct {
	val  *GObj
	next *Node
	pre  *Node
}

type ListType struct {
	Equal func(i *GObj, j *GObj) bool //定义判断2个元素是否相等
}

type List struct {
	ListType
	head *Node
	tail *Node
}

func ListCreate(listType ListType) *List {
	var list List
	list.ListType = listType
	return &list
}

// Append 尾插法插入对象
func (list *List) Append(val *GObj) {
	var n Node
	n.val = val
	if list.head == nil {
		list.head = &n
		list.tail = &n
	} else {
		n.pre = list.tail
		list.tail.next = &n
		list.tail = list.tail.next
	}
}

// Remove 删除第一个匹配到的对象
func (list *List) Remove(val *GObj) {
	p := list.head
	for p != nil {
		if list.Equal(p.val, val) {
			break
		}
		p = p.next
	}
	if p != nil {
		p.pre = p.next
		if p.next != nil {
			p.next.pre = p.pre
		}
		p.pre = nil
		p.next = nil
	}
}
