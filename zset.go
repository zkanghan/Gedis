package main

const MAX_LEVEL = 32

type Element struct {
	Member string
	Score  float64
}

type Level struct {
	forward *node
	span    int64
}

type node struct {
	Element
	next  *node
	level []*Level
}

type skipList struct {
	header *node
	tail   *node
	length int64
	level  int16
}

type ZSet struct {
	//TODO: implement it
}
