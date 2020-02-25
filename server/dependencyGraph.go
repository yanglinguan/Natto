package server

import (
	"container/list"
)

type Graph struct {
	// t1 -> (t2, t3) : t1 should complete before t2 and t3
	adjList map[string]map[string]bool
	// t1 -> (t2, t3): t1 should complete after t2 and t3
	revAdjList map[string]map[string]bool
	queue      *list.List
	inDegree   map[string]int
}

func NewDependencyGraph() *Graph {
	g := &Graph{
		adjList:    make(map[string]map[string]bool),
		revAdjList: make(map[string]map[string]bool),
		queue:      list.New(),
		inDegree:   make(map[string]int),
	}

	return g
}

func (g *Graph) AddNode(txn string) {
	if _, exist := g.inDegree[txn]; !exist {
		g.inDegree[txn] = 0
	}
	if _, exist := g.adjList[txn]; !exist {
		g.adjList[txn] = make(map[string]bool)
	}
}

// txn1 should commit before txn2 t1->t2
func (g *Graph) AddEdge(txn1 string, txn2 string) {
	if _, exist := g.adjList[txn1]; !exist {
		g.adjList[txn1] = make(map[string]bool)
	}
	if _, exist := g.adjList[txn1][txn2]; exist {
		return
	}

	if _, exist := g.revAdjList[txn2]; !exist {
		g.revAdjList[txn2] = make(map[string]bool)
	}

	g.revAdjList[txn2][txn1] = true
	g.adjList[txn1][txn2] = true

	if _, exist := g.inDegree[txn2]; !exist {
		g.inDegree[txn2] = 0
	}
	if _, exist := g.inDegree[txn1]; !exist {
		g.inDegree[txn1] = 0
	}

	g.inDegree[txn2]++
}

func (g *Graph) GetNext() []string {
	if g.queue.Len() == 0 {
		for txnId, inDegree := range g.inDegree {
			if inDegree == 0 {
				g.queue.PushBack(txnId)
				delete(g.inDegree, txnId)
			}
		}
	}

	result := make([]string, g.queue.Len())
	i := 0
	for g.queue.Len() != 0 {
		e := g.queue.Front()
		result[i] = e.Value.(string)
		g.queue.Remove(e)
	}

	return result
}

func (g *Graph) Remove(txnId string) {
	for child := range g.adjList[txnId] {
		g.inDegree[child]--
		for parent := range g.revAdjList[txnId] {
			g.adjList[parent][child] = true
			g.inDegree[child]++
		}
		if g.inDegree[child] == 0 {
			g.queue.PushBack(child)
			delete(g.inDegree, child)
		}
	}

	delete(g.inDegree, txnId)
}
