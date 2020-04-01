package server

import (
	"github.com/sirupsen/logrus"
)

type Graph struct {
	// t1 -> (t2, t3) : t1 should complete before t2 and t3
	adjList map[string]map[string]bool
	// t1 -> (t2, t3): t1 should complete after t2 and t3
	revAdjList map[string]map[string]bool
	inDegree   map[string]int

	noDependency []string

	keyToTxn map[string]map[string]bool
}

func NewDependencyGraph() *Graph {
	g := &Graph{
		adjList:      make(map[string]map[string]bool),
		revAdjList:   make(map[string]map[string]bool),
		inDegree:     make(map[string]int),
		noDependency: make([]string, 0),
		keyToTxn:     make(map[string]map[string]bool),
	}

	return g
}

func (g *Graph) AddNode(txnId string, keys map[string]bool) bool {
	for key := range keys {
		if _, exist := g.inDegree[txnId]; !exist {
			g.inDegree[txnId] = 0
		}
		if _, exist := g.adjList[txnId]; !exist {
			g.adjList[txnId] = make(map[string]bool)
		}
		if _, exist := g.revAdjList[txnId]; !exist {
			g.revAdjList[txnId] = make(map[string]bool)
		}

		if _, exist := g.keyToTxn[key]; !exist {
			g.keyToTxn[key] = make(map[string]bool)
		}

		for txn := range g.keyToTxn[key] {
			g.addEdge(txn, txnId)
		}
		g.keyToTxn[key][txnId] = true
	}

	return g.inDegree[txnId] == 0
}

func (g *Graph) RemoveNode(txnId string, keys map[string]bool) {
	for key := range keys {
		delete(g.keyToTxn[key], txnId)
	}

	g.remove(txnId)
}

// txn1 should commit before txn2 t1->t2
func (g *Graph) addEdge(txn1 string, txn2 string) {
	logrus.Debugf("add edge %v -> %v", txn1, txn2)
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
	if len(g.noDependency) == 0 {
		for txnId, inDegree := range g.inDegree {
			if inDegree == 0 {
				g.noDependency = append(g.noDependency, txnId)
				delete(g.inDegree, txnId)
			}
		}
	}

	result := make([]string, len(g.noDependency))
	for i, txnId := range g.noDependency {
		result[i] = txnId
	}

	g.noDependency = g.noDependency[:0]

	return result
}

func (g *Graph) remove(txnId string) {
	for child := range g.adjList[txnId] {
		if g.inDegree[child] > 0 {
			g.inDegree[child]--
			logrus.Debugf("txn %v inDegree-- %v", child, g.inDegree[child])
		}
		delete(g.revAdjList[child], txnId)
		for parent := range g.revAdjList[txnId] {
			if !g.adjList[parent][child] {
				g.inDegree[child]++
			}
			g.adjList[parent][child] = true
			g.revAdjList[child][parent] = true
		}
		if g.inDegree[child] == 0 {
			logrus.Debugf("txn %v can commit add to queue", child)
			g.noDependency = append(g.noDependency, child)
			delete(g.inDegree, child)
		}
	}

	for parent := range g.revAdjList[txnId] {
		delete(g.adjList[parent], txnId)
	}

	delete(g.adjList, txnId)
	delete(g.revAdjList, txnId)
	delete(g.inDegree, txnId)
}

func (g *Graph) GetConflictTxn(txnId string) []string {
	path := make([]string, 0)

	g.dfs(txnId, make(map[string]bool), &path)

	return path
}

func (g *Graph) dfs(cur string, visited map[string]bool, stack *[]string) {
	for c := range g.revAdjList[cur] {
		if visited[c] {
			continue
		}
		visited[c] = true
		g.dfs(c, visited, stack)
	}

	*stack = append(*stack, cur)
}
