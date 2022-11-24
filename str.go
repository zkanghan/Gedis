package main

import (
	"hash/fnv"
	"strconv"
)

func (o *GObj) IntVal() int {
	if o.Type_ != STR {
		return 0
	}
	val, _ := strconv.Atoi(o.StrVal())
	return val
}

func (o *GObj) StrVal() string {
	if o.Type_ != STR {
		return ""
	}
	return o.Val_.(string)
}

func EqualStr(a, b *GObj) bool {
	if a.Type_ != STR || b.Type_ != STR {
		return false
	}
	return a.StrVal() == b.StrVal()
}

func HashStr(o *GObj) int64 {
	if o.Type_ != STR {
		return 0
	}
	h := fnv.New64()
	h.Write([]byte(o.StrVal()))
	return int64(h.Sum64())
}
