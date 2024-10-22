package utils

import (
	"testing"
)

func TestSkipList(t *testing.T) {
	sl := NewSkipList()

	t.Log(sl.Search("jack", 87))
	t.Log("-----------------------------")
	sl.Insert("leo", 95)
	t.Log(sl.head.Forwards[0])
	t.Log(sl.head.Forwards[0].Forwards[0])
	t.Log(sl)
	t.Log("-----------------------------")

	sl.Insert("jack", 88)
	t.Log(sl.head.Forwards[0])
	t.Log(sl.head.Forwards[0].Forwards[0])
	t.Log(sl.head.Forwards[0].Forwards[0].Forwards[0])
	t.Log(sl)
	t.Log("-----------------------------")

	sl.Insert("lily", 100)
	t.Log(sl.head.Forwards[0])
	t.Log(sl.head.Forwards[0].Forwards[0])
	t.Log(sl.head.Forwards[0].Forwards[0].Forwards[0])
	t.Log(sl.head.Forwards[0].Forwards[0].Forwards[0].Forwards[0])
	t.Log(sl)
	t.Log("-----------------------------")

	t.Log(sl.Search("jack", 87))
	t.Log("-----------------------------")

	sl.Delete("leo", 95)
	t.Log(sl.head.Forwards[0])
	t.Log(sl.head.Forwards[0].Forwards[0])
	t.Log(sl.head.Forwards[0].Forwards[0].Forwards[0])
	t.Log(sl)
	t.Log("-----------------------------")
}
