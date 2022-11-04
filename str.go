package main

import "strconv"

// IntVal 返回STR类型的int值，非STR类型返回0
func (o *GObj) IntVal() int {
	if o.Type_ != STR {
		return 0
	}
	val, _ := strconv.Atoi(o.Val_.(string)) //转换失败同样返回0
	return val
}

// EqualStr STR类型的比较
func EqualStr(a, b *GObj) bool {
	if a.Type_ != STR || b.Type_ != STR {
		return false
	}
	return a.Val_.(string) == b.Val_.(string)
}
