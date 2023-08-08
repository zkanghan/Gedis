package main

import (
	"fmt"
	"math/bits"
	"math/rand"
	"strconv"
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
	SkipList *zSkipList
	Dict     *Dict
}

func NewZSet() *ZSet {
	return &ZSet{
		SkipList: newSkipList(),
		Dict:     NewDict(DictType{HashFunc: HashStr, EqualFunc: EqualStr}),
	}
}

func (z *ZSet) Length() int64 {
	return z.SkipList.length
}

func newZNode(member *GObj, score float64, level int) *zNode {
	node := &zNode{
		ZElement: ZElement{Member: member, Score: score},
		backward: nil,
		level:    make([]*ZLevel, level),
	}

	for i := 0; i < level; i++ {
		node.level[i] = &ZLevel{}
	}
	return node
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
	for i := zsl.level - 1; i >= 0; i-- {
		if i == zsl.level-1 {
			rank[i] = 0
		} else {
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

/* zset command implement */

var zaddCommand CommandProc = func(c *GedisClient) {
	zaddGenericCommand(c, 0)
}

var zincrbyCommand CommandProc = func(c *GedisClient) {
	zaddGenericCommand(c, 1)
}

var zremCommand CommandProc = func(c *GedisClient) {
	key := c.args[1]
	zobj := LookupKey(key)

	if zobj == nil || zobj.Type_ != ZSET {
		c.AddReply(REPLY_NIL)
		return
	}

	if zobj.Type_ != ZSET {
		c.AddReply(REPLY_WRONG_TYPE)
		return
	}
	zset := zobj.Val_.(*ZSet)
	deleted := 0
	for i := 2; i < len(c.args); i++ {
		entry := zset.Dict.Find(c.args[i])
		if entry != nil {
			deleted++
			zset.SkipList.delete(entry.Val.FloatVal(), c.args[i])
			_ = zset.Dict.Delete(c.args[i])
		}
	}

	c.AddReply(fmt.Sprintf("%d", deleted))
}

var zrangeCommand CommandProc = func(c *GedisClient) {
	zrangeGenericCommand(c, 0)
}

var zrevrangeCommand CommandProc = func(c *GedisClient) {
	zrangeGenericCommand(c, 1)
}

func zrangeGenericCommand(c *GedisClient, reverse int) {
	if len(c.args) <= 3 || len(c.args) >= 6 {
		c.AddReply(REPLY_WRONG_ARITY)
		return
	}

	withScores := false
	if len(c.args) == 5 && c.args[4].StrVal() == "withscores" {
		withScores = true
	}

	zobj := LookupKey(c.args[1])
	if zobj == nil {
		c.AddReply(REPLY_NIL)
		return
	}

	if zobj.Type_ != ZSET {
		c.AddReply(REPLY_WRONG_TYPE)
		return
	}

	var start, end int64
	if GetNumber(c.args[2].StrVal(), &start) != nil || GetNumber(c.args[3].StrVal(), &end) != nil {
		c.AddReply(REPLY_INVALID_VALUE)
		return
	}
	zset := zobj.Val_.(*ZSet)
	llen := zset.Length()

	if start < 0 {
		start = llen + start
	}
	if end < 0 {
		end = end + llen
	}
	if start < 0 {
		start = 0
	}

	if start > end || start >= llen {
		c.AddReply(REPLY_INVALID_VALUE)
		return
	}

	if end >= llen {
		end = llen - 1
	}
	rangeLen := start - end + 1
	var ln *zNode
	if reverse == 1 {
		ln = zset.SkipList.tail
		if start > 0 {
			ln = zset.SkipList.getElementByRank(llen - start)
		}
	} else {
		ln = zset.SkipList.header.level[0].forward
		if start > 0 {
			ln = zset.SkipList.getElementByRank(start + 1)
		}
	}

	for rangeLen > 0 {
		ele := ln.Member
		c.AddReplyStr(ele)
		if withScores {
			c.AddReplyFloat(ln.Score)
		}
		if reverse == 1 {
			ln = ln.backward
		} else {
			ln = ln.level[0].forward
		}
	}
}

func zaddGenericCommand(c *GedisClient, incr int) {
	// The score-member parameter must be in pairs
	if len(c.args)%2 == 0 {
		c.AddReply(REPLY_WRONG_ARITY)
		return
	}

	/* Start parsing all the scores, we need to emit any syntax error
	 * before executing additions to the sorted set, as the command should
	 * either execute fully or nothing at all. */
	elements := len(c.args) / 2
	scores := make([]float64, elements)
	members := make([]*GObj, elements)
	for i := 0; i < elements; i++ {
		score, err := strconv.ParseFloat(c.args[3+i*2].StrVal(), 64)
		if err != nil {
			c.AddReply(REPLY_INVALID_VALUE)
			return
		}
		scores[i] = score
		members[i] = c.args[2+i*2]
	}

	key := c.args[1]
	zobj := LookupKey(key)
	if zobj == nil {
		zobj = NewObject(ZSET, NewZSet())
		_ = server.db.data.Add(key, zobj)
	} else {
		// check the type
		if zobj.Type_ != ZSET {
			c.AddReply(REPLY_WRONG_TYPE)
			return
		}
	}

	added, score := 0, float64(0)
	for i := 0; i < elements; i++ {
		zSet := zobj.Val_.(*ZSet)
		entry := zSet.Dict.Find(members[i])
		if entry != nil {
			curObj := zSet.Dict.Get(members[i])
			curScore := curObj.FloatVal()

			score = scores[i]
			if incr != 0 {
				score += curScore
			}
			if curScore != score {
				/* Re-inserted in skiplist. */
				zSet.SkipList.delete(curScore, curObj)
				zSet.SkipList.insert(curObj, score)
				/* Update score */
				zSet.Dict.Set(curObj, NewObject(STR, score))
			}
		} else {
			zSet.SkipList.insert(members[i], scores[i])
			_ = zSet.Dict.Add(members[i], NewObject(STR, scores[i]))
			added++
		}
	}

	if incr != 0 { /* ZINCRBY */
		c.AddReply(fmt.Sprintf("%f", score))
	} else { /* ZADD */
		c.AddReply(fmt.Sprintf("%d", added))
	}
}
