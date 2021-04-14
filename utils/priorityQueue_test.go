package utils

import (
	"fmt"
	"testing"
)

func TestPriorityQueue_Remove(t *testing.T) {
	pq := NewPriorityQueue()
	pq.Push(&Item{
		TxnId:     "9-4964-0",
		ClientId:  "9",
		Timestamp: 159544573786675952,
		index:     0,
	})

	pq.Push(&Item{
		TxnId:     "8-6686-0",
		ClientId:  "8",
		Timestamp: 159544574136858990,
		index:     0,
	})

	pq.Push(&Item{
		TxnId:     "4-10826-0",
		ClientId:  "4",
		Timestamp: 159544574833123988,
		index:     0,
	})

	//for pq.Peek() != nil {
	//	fmt.Printf("value %v\n", pq.Peek().value)
	//	pq.Pop()
	//}

	//pq.Remove(&Item{
	//	value: 3,
	//	index: 0,
	//})

	//pq.Remove(&Item{
	//	value: 4,
	//	index: 0,
	//})
	//
	//pq.Push(&Item{
	//	value: 6,
	//	index: 0,
	//})
	//
	//pq.Push(&Item{
	//	value: 2,
	//	index: 0,
	//})

	for pq.Peek() != nil {
		fmt.Printf("value %v\n", pq.Peek().TxnId)
		pq.Pop()
	}

}
