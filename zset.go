package main

import (
	"math/bits"
	"math/rand"
)

const MAX_LEVEL = 32

// ZElement of skip list
type ZElement struct {
	Member *GObj
	Score  float64
}

type ZLevel struct {
	forward *zNode //前进指针
	span    int64  //与前进指针指向节点的跨度
}

type zNode struct {
	ZElement
	backward *zNode //后退指针
	level    []*ZLevel
}

type zSkipList struct {
	header *zNode
	tail   *zNode
	length int64
	level  int
}

type ZSet struct {
	SkipList zSkipList
	Dict     Dict
}

func newZNode(member *GObj, score float64, level int) *zNode {
	return &zNode{
		ZElement: ZElement{Member: member, Score: score},
		backward: nil,
		level:    make([]*ZLevel, level),
	}
}

func newSkipList() *zSkipList {
	return &zSkipList{
		header: newZNode(NewObject(STR, ""), 0, MAX_LEVEL),
		tail:   nil,
		level:  1,
		length: 0,
	}
}

func randomLevel() int {
	total := uint64(1)<<uint64(MAX_LEVEL) - 1
	k := rand.Uint64() % total
	return MAX_LEVEL - bits.Len64(k+1) + 1
}

// make sure the element not already inside before call of the method
func (zsl *zSkipList) insert(member *GObj, score float64) *zNode {
	update, rank := make([]*zNode, MAX_LEVEL), make([]int64, MAX_LEVEL)

	node := zsl.header
	for i := MAX_LEVEL - 1; i >= 0; i-- {
		rank[i] = 0
		if i != zsl.level-1 {
			rank[i] = rank[i+1]
		}
		for node.level[i].forward != nil && (node.level[i].forward.Score < score ||
			(node.level[i].forward.Score < score && CompareStr(node.level[i].forward.Member, member))) {
			rank[i] += node.level[i].span
			node = node.level[i].forward
		}
		update[i] = node
	}

	level := randomLevel()
	if level > zsl.level {
		for i := zsl.level; i < level; i++ {
			rank[i] = 0
			update[i] = zsl.header
			update[i].level[i].span = zsl.length
		}
		zsl.level = level
	}

	node = newZNode(member, score, level)
	for i := 0; i < level; i++ {
		node.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = node

		node.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = rank[0] - rank[i] + 1
	}

	// increment span for untouched levels
	for i := level; i < zsl.level; i++ {
		update[i].level[i].span++
	}

	if update[0] != zsl.header {
		node.backward = update[0]
	}

	if node.level[0].forward != nil {
		node.level[0].forward.backward = node
	} else {
		zsl.tail = node
	}
	zsl.length++
	return node
}

func (zsl *zSkipList) deleteNode(node *zNode, update []*zNode) {
	for i := 0; i < zsl.level; i++ {
		if update[i].level[i].forward == node {
			update[i].level[i].span += node.level[i].span - 1
			update[i].level[i].forward = node.level[i].forward
		} else {
			update[i].level[i].span -= 1
		}
	}
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node.backward
	} else {
		zsl.tail = node.backward
	}
	for zsl.level > 1 && zsl.header.level[zsl.level-1].forward == nil {
		zsl.level--
	}
	zsl.length--
}

// Delete an element with matching score/object from the skip list.
func (zsl *zSkipList) delete(score float64, member *GObj) {
	node := zsl.header
	update := make([]*zNode, MAX_LEVEL)
	for i := zsl.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil && (node.level[i].forward.Score < score ||
			(node.level[i].forward.Score == score && CompareStr(node.level[i].forward.Member, member))) {
			node = node.level[i].forward
		}
		update[i] = node
	}

	/* We may have multiple elements with the same score, what we need
	 * is to find the element with both the right score and object.
	 */

	node = node.level[0].forward
	if node != nil && EqualStr(node.Member, member) {
		zsl.deleteNode(node, update)
	}
}

func (zsl *zSkipList) updateScore(member *GObj, curScore, newScore float64) *zNode {
	node := zsl.header
	update := make([]*zNode, MAX_LEVEL)
	for i := zsl.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil && (node.level[i].forward.Score < curScore ||
			(node.level[i].forward.Score == curScore && CompareStr(node.level[i].forward.Member, member))) {
			node = node.level[i].forward
		}
		update[i] = node
	}

	node = node.level[0].forward

	/* If the node, after the score update, would be still exactly
	 * at the same position, we can just update the score without
	 * actually removing and re-inserting the element in the skiplist. */
	if (node.backward == nil || node.backward.Score < newScore) &&
		(node.level[0].forward == nil || node.level[0].forward.Score > newScore) {
		node.Score = newScore
		return node
	}

	zsl.deleteNode(node, update)
	newNode := zsl.insert(node.Member, newScore)

	return newNode
}

func (zsl *zSkipList) getElementByRank(rank int64) *zNode {
	node := zsl.header
	traversed := int64(0)
	for i := zsl.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil && traversed+node.level[i].span <= rank {
			traversed += node.level[i].span
			node = node.level[i].forward
		}

		if traversed == rank {
			return node
		}
	}
	return nil
}
