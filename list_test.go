package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestList(t *testing.T) {
	list := ListCreate(ListType{EqualFunc: EqualStr})
	assert.Equal(t, 0, list.Length())

	list.TailPush(NewObject(STR, "1"))
	list.TailPush(NewObject(STR, "2"))
	list.TailPush(NewObject(STR, "3"))

	// TODO: finish test case
}
