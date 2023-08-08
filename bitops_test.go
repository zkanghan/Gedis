package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_BitMap(t *testing.T) {
	b := growIfNeedBitmap(nil, 32)

	assert.Equal(t, 0, b.GetBit(32))
	assert.Equal(t, 0, b.GetBit(33))

	for i := int64(1); i <= 100000; i++ {
		b.SetBit(i, 1)
		assert.Equal(t, 1, b.GetBit(i))
	}

	assert.Equal(t, 0, b.GetBit(9999999))
}
