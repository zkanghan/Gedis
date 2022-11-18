package main

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDict(t *testing.T) {
	dict := NewDict(DictType{HashFunc: HashStr, EqualFunc: EqualStr})

	k1 := NewObject(STR, "k1")
	v1 := NewObject(STR, "v1")
	v2 := NewObject(STR, "v2")

	err := dict.Add(k1, v1)
	assert.Nil(t, err)

	e := dict.Find(k1)
	assert.Equal(t, k1, e.Key)
	assert.Equal(t, v1, e.Val)

	err = dict.Delete(k1)
	assert.Nil(t, err)
	e = dict.Find(k1)
	assert.Nil(t, e)

	err = dict.Add(k1, v1)
	assert.Nil(t, err)
	v := dict.Get(k1)
	assert.Equal(t, v1, v)

	dict.Set(k1, v2)
	v = dict.Get(k1)
	assert.Equal(t, v2, v)
}

func TestRehash(t *testing.T) {
	dict := NewDict(DictType{HashStr, EqualStr})

	cnt := int(INIT_SIZE * (FORCE_REHASH_RATION + 1))
	for i := 0; i < cnt; i++ {
		key := NewObject(STR, fmt.Sprintf("k%d", i))
		val := NewObject(STR, fmt.Sprintf("v%d", i))
		err := dict.Add(key, val)
		assert.Nil(t, err)
	}
	assert.Equal(t, false, dict.isRehashing())

	key := NewObject(STR, fmt.Sprintf("k%d", cnt))
	val := NewObject(STR, fmt.Sprintf("v%d", cnt))
	err := dict.Add(key, val)
	assert.Nil(t, err)
	assert.Equal(t, true, dict.isRehashing())
	assert.Equal(t, int64(0), dict.rehashIndex)
	assert.Equal(t, INIT_SIZE, dict.HTables[0].size)
	assert.Equal(t, nextPower(dict.HTables[0].used*EXPAND_RATION), dict.HTables[1].size)

	for i := 0; i < cnt; i++ {
		key = NewObject(STR, fmt.Sprintf("k%d", i))
		val = dict.Get(key)
		assert.Equal(t, val.StrVal(), fmt.Sprintf("v%d", i))
	}
	assert.Equal(t, false, dict.isRehashing())
	assert.Equal(t, nextPower(dict.HTables[0].used*EXPAND_RATION), dict.HTables[0].size)
	assert.Nil(t, dict.HTables[1])
}
