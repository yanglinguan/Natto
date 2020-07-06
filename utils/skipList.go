package utils

import (
	"math"
	"math/rand"
)

const (
	MaxLevel = 16
)

type skipListNode struct {
	// value stored in skipList
	V interface{}
	// for sorting
	Score    int64
	level    int
	Forwards []*skipListNode
}

func newSkipListNode(v interface{}, score int64, level int) *skipListNode {
	return &skipListNode{
		V:        v,
		Score:    score,
		level:    level,
		Forwards: make([]*skipListNode, level, level),
	}
}

type SkipList struct {
	head *skipListNode
	// current level
	level int
	// length
	length int
}

func NewSkipList() *SkipList {
	head := newSkipListNode(nil, math.MinInt64, MaxLevel)
	return &SkipList{head, 1, 0}
}

func (sl *SkipList) Length() int {
	return sl.length
}

func (sl *SkipList) Level() int {
	return sl.level
}

func (sl *SkipList) Insert(v interface{}, score int64) int {
	if v == nil {
		return 1
	}

	cur := sl.head
	update := [MaxLevel]*skipListNode{}
	i := MaxLevel - 1
	for ; i >= 0; i-- {
		for cur.Forwards[i] != nil {
			if cur.Forwards[i].V == v {
				return 2
			}
			if cur.Forwards[i].Score > score {
				update[i] = cur
				break
			}
			cur = cur.Forwards[i]
		}

		if cur.Forwards[i] == nil {
			update[i] = cur
		}
	}

	level := 1
	for i := 1; i < MaxLevel; i++ {
		if rand.Int31()%7 == 1 {
			level++
		}
	}

	newNode := newSkipListNode(v, score, level)

	for i := 0; i <= level-1; i++ {
		next := update[i].Forwards[i]
		update[i].Forwards[i] = newNode
		newNode.Forwards[i] = next
	}

	for level > sl.level {
		sl.level = level
	}

	sl.length++

	return 0
}

func (sl *SkipList) Delete(v interface{}, score int64) int {
	if v == nil {
		return 1
	}

	cur := sl.head
	update := [MaxLevel]*skipListNode{}
	for i := sl.level - 1; i >= 0; i-- {
		update[i] = sl.head
		for cur.Forwards[i] != nil {
			if cur.Forwards[i].Score == score && cur.Forwards[i].V == v {
				update[i] = cur
				break
			}
			cur = cur.Forwards[i]
		}
	}

	cur = update[0].Forwards[0]
	for i := cur.level - 1; i >= 0; i-- {
		if update[i] == sl.head && cur.Forwards[i] == nil {
			sl.level = i
		}

		if update[i].Forwards[i] == nil {
			update[i].Forwards[i] = nil
		} else {
			update[i].Forwards[i] = update[i].Forwards[i].Forwards[i]
		}
	}

	sl.length--

	return 0
}

func (sl *SkipList) Search(v interface{}, score int64) *skipListNode {
	if v == nil || sl.length == 0 {
		return nil
	}

	cur := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for cur.Forwards[i] != nil {
			if cur.Forwards[i].Score == score && cur.Forwards[i].V == v {
				return cur.Forwards[i]
			} else if cur.Forwards[i].Score > score {
				break
			}
			cur = cur.Forwards[i]
		}
	}

	return cur
}
