package main

import (
	"errors"
	"math"
)

const (
	INIT_SIZE           int64 = 8
	FORCE_REHASH_RATION int64 = 2
	EXPAND_RATION       int64 = 2
	DEFAULT_STEP        int   = 1
)

var (
	ERR_EXPAND        = errors.New("expand error")
	ERR_KEY_EXIST     = errors.New("the key already exists")
	ERR_KEY_NOT_EXIST = errors.New("key does not exist")
)

type Entry struct {
	key  *GObj
	val  *GObj
	next *Entry
}

// zipper structure
type hTable struct {
	table []*Entry
	size  int64

	//the value is always size-1，ensure that the index doesn't overstep the bounds
	sizeMask int64
	used     int64 //number of the Entry nodes
}

type DictType struct {
	HashFunc  func(key *GObj) int64
	EqualFunc func(a *GObj, b *GObj) bool
}

type Dict struct {
	//if not -1 indicate that the dict is rehashing, or it's the index that rehashing of HTable[0]
	rehashIndex int64
	HTables     [2]*hTable
	DictType
}

func NewDict(dictType DictType) *Dict {
	return &Dict{
		DictType:    dictType,
		rehashIndex: -1,
	}
}

func (dict *Dict) isRehashing() bool {
	return dict.rehashIndex != -1
}

func (dict *Dict) rehashStep() {
	dict.rehash(DEFAULT_STEP)
}

//performs n steps of incremental rehashing
func (dict *Dict) rehash(n int) {
	for n > 0 {
		// rehash is completed
		if dict.HTables[0].used == 0 {
			dict.HTables[0] = dict.HTables[1]
			dict.HTables[1] = nil
			dict.rehashIndex = -1
			return
		}
		// find a not null bucket
		for dict.HTables[0].table[dict.rehashIndex] == nil {
			dict.rehashIndex += 1
		}
		// Move all the keys in this bucket from the old to the new hash hTable
		entry := dict.HTables[0].table[dict.rehashIndex]
		for entry != nil {
			nx := entry.next
			//get the index in the new hash table
			index := dict.HashFunc(entry.key) & dict.HTables[1].sizeMask
			//insert nodes by header interpolation
			entry.next = dict.HTables[1].table[index]
			dict.HTables[1].table[index] = entry

			dict.HTables[0].used -= 1
			dict.HTables[1].used += 1
			entry = nx
		}
		dict.HTables[0].table[dict.rehashIndex] = nil
		dict.rehashIndex += 1
		n -= 1
	}
}

func nextPower(size int64) int64 {
	for i := INIT_SIZE; i < math.MaxInt64; i *= 2 {
		if i >= size {
			return i
		}
	}
	return -1
}

//expand or create the hash table
func (dict *Dict) expand(size int64) error {
	realSize := nextPower(size)
	// the size is invalid if it is smaller than the number of elements already inside the hash table
	//  or the dict is rehashing
	// TODO:check use 'size' or 'used' to judge
	if dict.isRehashing() || (dict.HTables[0] != nil && dict.HTables[0].used >= realSize) {
		return ERR_EXPAND
	}
	//the new hash table
	ht := hTable{
		table:    make([]*Entry, realSize),
		size:     realSize,
		sizeMask: realSize - 1,
		used:     0,
	}
	//check for init
	if dict.HTables[0] == nil {
		dict.HTables[0] = &ht
		return nil
	}
	dict.HTables[1] = &ht
	dict.rehashIndex = 0
	return nil
}

func (dict *Dict) expandIfNeeded() error {
	//incremental rehashing already in progress, return
	if dict.isRehashing() {
		return nil
	}
	// if the hash table is empty expand it to the initial size.
	if dict.HTables[0] == nil {
		return dict.expand(INIT_SIZE)
	}
	if dict.HTables[0].used >= dict.HTables[0].size &&
		dict.HTables[0].used/dict.HTables[1].size > FORCE_REHASH_RATION {
		return dict.expand(dict.HTables[0].used * EXPAND_RATION)
	}
	return nil
}

// returns the index of a free slot that can be populated with
// a hash entry for the given 'key', if the key already exists return -1.
// note that if the dict is doing rehashing, the returned index is always in the second hash table
func (dict *Dict) getKeyIndex(key *GObj) int64 {
	// expand the hash table if needed
	if err := dict.expandIfNeeded(); err != nil {
		return -1
	}
	h := dict.HashFunc(key)
	var idx int64
	for i := 0; i <= 1; i++ {
		idx = h & dict.HTables[i].sizeMask
		// check whether the 'key' is already exists
		e := dict.HTables[i].table[idx]
		for e != nil {
			if dict.EqualFunc(e.key, key) {
				return -1
			}
			e = e.next
		}
		// if it is not doing rehashing, the second hash table is empty, just break
		if !dict.isRehashing() {
			break
		}
	}
	return idx
}

// AddRaw Low level add.
// this method adds the entry but instead of setting a value returns the dictEntry structure to the user
// Return values:
// if key already exists NULL is returned.
// if key was added, the hash entry is returned to be manipulated by the caller.
func (dict *Dict) AddRaw(key *GObj) *Entry {
	if dict.isRehashing() {
		dict.rehashStep()
	}
	idx := dict.getKeyIndex(key)
	if idx == -1 {
		return nil
	}
	var ht *hTable
	if dict.isRehashing() {
		ht = dict.HTables[1]
	} else {
		ht = dict.HTables[0]
	}
	//insert the new entry into the header of the linked list
	var e Entry
	e.key = key
	e.next = ht.table[idx]
	ht.table[idx] = &e
	ht.used += 1
	return &e
}

//Add insert a key-value pair to the dict, return error if key exists
func (dict *Dict) Add(key, val *GObj) error {
	entry := dict.AddRaw(key)
	if entry == nil {
		return ERR_KEY_EXIST
	}
	entry.val = val
	return nil
}

// Find if key not exists, return nil
func (dict *Dict) Find(key *GObj) *Entry {
	if dict.HTables[0] == nil {
		return nil
	}
	if dict.isRehashing() {
		dict.rehashStep()
	}
	// find key in both hash table
	h := dict.HashFunc(key)
	for i := 0; i <= 1; i++ {
		idx := h & dict.HTables[i].sizeMask
		e := dict.HTables[i].table[idx]
		for e != nil {
			if dict.EqualFunc(e.key, key) {
				return e
			}
			e = e.next
		}
		//if the dict is rehashing, continue to search the second hash table
		if !dict.isRehashing() {
			return nil
		}
	}
	return nil
}

// Set add a key-value pair, discarding the old if the key already exists.
func (dict *Dict) Set(key, val *GObj) {
	// if key not exist
	if err := dict.Add(key, val); err == nil {
		return
	}
	entry := dict.Find(key)
	entry.val = val
	return
}

func (dict *Dict) Get(key *GObj) *GObj {
	entry := dict.Find(key)
	if entry == nil {
		return nil
	}
	return entry.val
}

func (dict *Dict) Delete(key *GObj) error {
	if dict.HTables[0] == nil {
		return ERR_KEY_NOT_EXIST
	}
	if dict.isRehashing() {
		dict.rehashStep()
	}
	h := dict.HashFunc(key)
	for i := 0; i <= 1; i++ {
		idx := h & dict.HTables[i].sizeMask
		e := dict.HTables[i].table[idx]
		var pre *Entry
		for e != nil {
			if dict.EqualFunc(e.key, key) {
				if pre == nil {
					dict.HTables[i].table[idx] = e.next
				} else {
					pre.next = e.next
				}
				return nil
			}
			pre = e
			e = e.next
		}
		if !dict.isRehashing() {
			return ERR_KEY_NOT_EXIST
		}
	}
	return ERR_KEY_NOT_EXIST
}
