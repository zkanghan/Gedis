package main

type GType int8
type GVal any

const (
	STR  GType = 1
	LIST GType = 2
	SET  GType = 3
	ZSET GType = 4
	DICT GType = 5
)

type GObj struct {
	Type_ GType
	Val_  GVal
}

func NewObject(tp GType, val any) *GObj {
	return &GObj{
		Type_: tp,
		Val_:  val,
	}
}
