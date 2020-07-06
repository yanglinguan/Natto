package utils

import "container/list"

type DepGraph struct {
	adjList [][]int
	v       int
}

func NewDepGraph(v int) *DepGraph {
	g := &DepGraph{
		adjList: make([][]int, v),
		v:       v,
	}

	for i := 0; i < v; i++ {
		g.adjList[i] = make([]int, 0)
	}

	return g
}

func (g *DepGraph) AddEdge(s int, t int) {
	if s == t {
		return
	}
	g.adjList[s] = append(g.adjList[s], t)
}

func (g *DepGraph) isCyclicUtil(i int, visited []bool, recStack []bool) bool {
	if recStack[i] {
		return true
	}

	if visited[i] {
		return false
	}

	visited[i] = true
	recStack[i] = true

	children := g.adjList[i]

	for c := range children {
		if g.isCyclicUtil(c, visited, recStack) {
			return true
		}
	}

	recStack[i] = false
	return false
}

func (g *DepGraph) IsCyclic() bool {
	visited := make([]bool, g.v)
	recStack := make([]bool, g.v)

	for i := 0; i < g.v; i++ {
		if g.isCyclicUtil(i, visited, recStack) {
			return true
		}
	}

	return false
}

func (g *DepGraph) topoSort() []int {
	result := make([]int, 0)

	inDegree := make([]int, g.v)

	for i := 0; i < g.v; i++ {
		for j := 0; j < len(g.adjList[i]); j++ {
			w := g.adjList[i][j]
			inDegree[w]++
		}
	}

	queue := list.New()

	for i := 0; i < g.v; i++ {
		if inDegree[i] == 0 {
			queue.PushBack(i)
		}
	}

	for queue.Len() != 0 {
		e := queue.Front()
		i := e.Value.(int)
		queue.Remove(e)

		result = append(result, i)
		for j := 0; j < len(g.adjList[i]); j++ {
			k := g.adjList[i][j]
			inDegree[k]--
			if inDegree[k] == 0 {
				queue.PushBack(k)
			}
		}
	}

	return result
}
