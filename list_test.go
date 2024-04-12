package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestList(t *testing.T) {
	list := ListCreate(ListType{EqualFunc: EqualStr})
	assert.Equal(t, 0, list.Length())

	list.TailPush(NewObject(STR, "0"))
	np := list.Find(NewObject(STR, "0"))
	list.DelNode(np)

	list.TailPush(NewObject(STR, "1"))
	list.TailPush(NewObject(STR, "2"))
	list.TailPush(NewObject(STR, "3"))
	assert.Equal(t, list.Length(), 3)
	assert.Equal(t, list.First().Val.Val_.(string), "1")
	assert.Equal(t, list.Last().Val.Val_.(string), "3")

	// test Index()
	ln := list.Index(0)
	assert.Equal(t, ln.Val.StrVal(), "1")
	ln = list.Index(1)
	assert.Equal(t, ln.Val.StrVal(), "2")
	ln = list.Index(2)
	assert.Equal(t, ln.Val.StrVal(), "3")
	ln = list.Index(-1)
	assert.Equal(t, ln.Val.StrVal(), "3")
	ln = list.Index(-2)
	assert.Equal(t, ln.Val.StrVal(), "2")
	ln = list.Index(-3)
	assert.Equal(t, ln.Val.StrVal(), "1")
	ln = list.Index(-879)
	assert.Nil(t, ln)
	ln = list.Index(90)
	assert.Nil(t, ln)

	a := NewObject(STR, "0")
	list.HeadPush(a)
	assert.Equal(t, list.Length(), 4)
	assert.Equal(t, list.First().Val.Val_.(string), "0")

	list.HeadPush(NewObject(STR, "-1"))
	assert.Equal(t, list.Length(), 5)

	b := list.Find(a)
	assert.Equal(t, b.Val, a)

	list.Delete(a)
	assert.Equal(t, list.Length(), 4)

	b = list.Find(a)
	assert.Nil(t, b)

	list.DelNode(list.First())
	assert.Equal(t, list.Length(), 3)
	assert.Equal(t, list.First().Val.Val_.(string), "1")

	list.DelNode(list.Last())
	assert.Equal(t, list.Length(), 2)
	assert.Equal(t, list.Last().Val.Val_.(string), "2")

}
