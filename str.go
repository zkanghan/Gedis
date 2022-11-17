package main

import (
	"hash/fnv"
	"strconv"
)

// IntVal 返回STR类型的int值，非STR类型返回0
func (o *GObj) IntVal() int {
	if o.Type_ != STR {
		return 0
	}
	val, _ := strconv.Atoi(o.StrVal()) //转换失败同样返回0
	return val
}

func (o *GObj) StrVal() string {
	if o.Type_ != STR {
		return ""
	}
	return o.Val_.(string)
}

// EqualStr STR类型的比较
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
