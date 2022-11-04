package main

// dict  字典数据结构  渐进式rehash
//  条目
type entry struct {
	key  *GObj
	val  *GObj
	next *entry
}

// 哈希表
type hTable struct {
	table []*entry
	used  int64 //已有节点的数量
}

type DictType struct {
	HashFunc  func(key *GObj) int
	EqualFunc func(a *GObj, b *GObj) bool
}

type Dict struct {
	rehashFlag int //标记是否在进行rehash
	HTable     [2]hTable
	DictType
}
