package main

import (
	"testing"
)

func TestZslInsert(t *testing.T) {
	zsl := newSkipList()

	zsl.insert("o1", 1)
	zsl.insert("o2", 2)
	zsl.insert("o3", 3)

}
