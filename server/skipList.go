package server

import (
	"math"
	"math/rand"
)

const (
	MaxLevel = 16
)

type skipListNode struct {
	// value stored in skipList
	v interface{}
	// for sorting
	score    int64
	level    int
	forwards []*skipListNode
}

func newSkipListNode(v interface{}, score int64, level int) *skipListNode {
	return &skipListNode{
		v:        v,
		score:    score,
		level:    level,
		forwards: make([]*skipListNode, level, level),
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
		for cur.forwards[i] != nil {
			if cur.forwards[i].v == v {
				return 2
			}
			if cur.forwards[i].score > score {
				update[i] = cur
				break
			}
			cur = cur.forwards[i]
		}

		if cur.forwards[i] == nil {
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
		next := update[i].forwards[i]
		update[i].forwards[i] = newNode
		newNode.forwards[i] = next
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
		for cur.forwards[i] != nil {
			if cur.forwards[i].score == score && cur.forwards[i].v == v {
				update[i] = cur
				break
			}
			cur = cur.forwards[i]
		}
	}

	cur = update[0].forwards[0]
	for i := cur.level - 1; i >= 0; i-- {
		if update[i] == sl.head && cur.forwards[i] == nil {
			sl.level = i
		}

		if update[i].forwards[i] == nil {
			update[i].forwards[i] = nil
		} else {
			update[i].forwards[i] = update[i].forwards[i].forwards[i]
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
		for cur.forwards[i] != nil {
			if cur.forwards[i].score == score && cur.forwards[i].v == v {
				return cur.forwards[i]
			} else if cur.forwards[i].score > score {
				break
			}
			cur = cur.forwards[i]
		}
	}

	return cur
}
