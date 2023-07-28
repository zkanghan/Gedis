package main

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestZslInsert(t *testing.T) {
	zsl := newSkipList()

	cnt := 10

	for i := 1; i <= cnt; i++ {
		zsl.insert(NewObject(STR, fmt.Sprintf("z_%d", i)), float64(i))
	}

	for i := 1; i <= cnt; i++ {
		node := zsl.getElementByRank(int64(i))
		assert.Equal(t, fmt.Sprintf("z_%d", i), node.Member.StrVal())
		assert.Equal(t, i, int(node.Score))
	}
}
