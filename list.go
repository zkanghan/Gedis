package main

//  doubly linked list

const (
	LIST_HEAD = 1
	LIST_TAIL = 2
)

type lNode struct {
	Val  *GObj
	next *lNode
	pre  *lNode
}

type ListType struct {
	EqualFunc func(i *GObj, j *GObj) bool //定义判断2个元素是否相等
}

type List struct {
	ListType
	head   *lNode
	tail   *lNode
	length int
}

type ListIterator struct {
	direction int
	ln        *lNode
}

type ListEntry struct {
	li *ListIterator
	ln *lNode
}

// Next Stores pointer to current the entry in the provided entry structure
// and advances the position of the iterator.
/* Returns 1 when the current entry is in fact an entry, 0 otherwise. */
func (li *ListIterator) Next(entry *ListEntry) int {
	entry.li = li
	entry.ln = li.ln
	if entry.ln != nil {
		if li.direction == LIST_HEAD {
			li.ln = li.ln.next
		} else {
			li.ln = li.ln.pre
		}
		return 1
	}
	return 0
}

func ListCreate(listType ListType) *List {
	return &List{
		ListType: listType,
		length:   0,
		head:     nil,
		tail:     nil,
	}
}

// Find if not found return nil
func (list *List) Find(val *GObj) *lNode {
	p := list.head
	for p != nil {
		if list.EqualFunc(p.Val, val) {
			break
		}
		p = p.next
	}
	return p
}

// TailPush insert node at the tail
func (list *List) TailPush(val *GObj) {
	var n lNode
	n.Val = val
	if list.head == nil {
		list.head = &n
		list.tail = &n
	} else {
		n.pre = list.tail
		list.tail.next = &n
		list.tail = list.tail.next
	}
	list.length += 1
}

// HeadPush insert node at the head
func (list *List) HeadPush(val *GObj) {
	var n lNode
	n.Val = val
	if list.head == nil {
		list.head = &n
		list.tail = &n
	} else {
		n.next = list.head
		list.head.pre = &n
		list.head = &n
	}
	list.length += 1
}

func (list *List) DelNode(n *lNode) {
	if n == nil {
		return
	}
	if list.Length() == 1 && list.head == n {
		list.head = nil
		list.tail = nil
		list.length = 0
		return
	}

	if list.head == n {
		list.head = n.next
		n.next.pre = nil
		n.next = nil
	} else if list.tail == n {
		list.tail = n.pre
		n.pre.next = nil
		n.pre = nil
	} else {
		n.pre.next = n.next
		n.next.pre = n.pre
		n.pre = nil
		n.next = nil
	}
	list.length -= 1
}

// Index Return the element at the specified zero-based index
// where 0 is the head, 1 is the element next to head and so on.
// Negative integers are used in order to count from the tail,
// -1 is the last element, -2 the penultimate and so on.
// If the index is out of range nil is returned
func (list *List) Index(index int64) *lNode {
	var n *lNode
	if index < 0 {
		index = -index - 1
		n = list.tail
		for index > 0 && n != nil {
			n = n.pre
		}
	} else {
		n = list.tail
		for index > 0 && n != nil {
			n = n.next
		}
	}
	return n
}

func (list *List) Delete(val *GObj) {
	list.DelNode(list.Find(val))
}

func (list *List) First() *lNode {
	return list.head
}

func (list *List) Last() *lNode {
	return list.tail
}

func (list *List) Length() int {
	return list.length
}

func (list *List) TypePush(obj *GObj, where int) {
	if where == LIST_HEAD {
		list.HeadPush(obj)
	} else if where == LIST_TAIL {
		list.TailPush(obj)
	}
}

func (list *List) TypePop(where int) *GObj {
	var ln *lNode
	if where == LIST_HEAD {
		ln = list.First()
	} else if where == LIST_TAIL {
		ln = list.Last()
	}
	list.DelNode(ln)
	return ln.Val
}

func (list *List) TypeInitIterator(index int64, direction int) *ListIterator {
	li := &ListIterator{direction: direction}
	li.ln = list.Index(index)
	return li
}
