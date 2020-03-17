package server

import (
	"testing"
)

func TestGraph_AddEdge(t *testing.T) {
	graph := NewDependencyGraph()

	graph.AddEdge("a", "b")
	graph.AddEdge("a", "c")
	graph.AddEdge("c", "d")
	graph.AddEdge("e", "b")

	available := graph.GetNext()
	expect := map[string]bool{"a": true, "e": true}

	if len(expect) != len(available) {
		t.Errorf("error")
	} else {
		for _, txnId := range available {
			if _, exist := expect[txnId]; !exist {
				t.Errorf("txnId %v should not be available", txnId)
			}
		}
	}
}

func TestGraph_Remove(t *testing.T) {
	graph := NewDependencyGraph()

	graph.AddEdge("a", "b")
	graph.AddEdge("a", "c")
	graph.AddEdge("c", "d")
	graph.AddEdge("e", "b")

	path1 := graph.txnBefore("d")

	graph.Remove("c")

	path2 := graph.txnBefore("d")

	available := graph.GetNext()
	expect := map[string]bool{"a": true, "e": true}

	if len(expect) != len(available) {
		t.Errorf("error")
	} else {
		for _, txnId := range available {
			if _, exist := expect[txnId]; !exist {
				t.Errorf("txnId %v should not be available", txnId)
			}
		}
	}

	expectPath1 := []string{"a", "c", "d"}
	expectPath2 := []string{"a", "d"}

	if len(expectPath1) != len(path1) || len(expectPath2) != len(path2) {
		t.Errorf("error")
	} else {
		for i, txnId := range path1 {
			if expectPath1[i] != txnId {
				t.Errorf("txnId %v not in order", txnId)
			}
		}

		for i, txnId := range path2 {
			if expectPath2[i] != txnId {
				t.Errorf("txnId %v not in order", txnId)
			}
		}
	}
}

func TestGraph_GetNext(t *testing.T) {
	graph := NewDependencyGraph()

	graph.AddEdge("a", "b")
	graph.AddEdge("a", "c")
	graph.AddEdge("c", "d")
	graph.AddEdge("a", "d")
	graph.AddEdge("b", "c")
	graph.AddEdge("b", "d")

	path := graph.txnBefore("d")
	expectPath := []string{"a", "b", "c", "d"}
	if len(path) != len(expectPath) {
		t.Errorf("error")
	} else {
		for i, txnId := range expectPath {
			if path[i] != txnId {
				t.Errorf("txn %v not in order", txnId)
			}
		}
	}

	graph.Remove("a")
	graph.Remove("b")

	path = graph.txnBefore("d")
	expectPath = []string{"c", "d"}
	if len(path) != len(expectPath) {
		t.Errorf("error")
	} else {
		for i, txnId := range expectPath {
			if path[i] != txnId {
				t.Errorf("txn %v not in order", txnId)
			}
		}
	}

	available := graph.GetNext()
	expect := map[string]bool{"c": true, "b": true}

	if len(expect) != len(available) {
		t.Errorf("error")
	} else {
		for _, txnId := range available {
			if _, exist := expect[txnId]; !exist {
				t.Errorf("txnId %v should not be available", txnId)
			}
		}
	}
}

func TestGraph_Remove2(t *testing.T) {
	graph := NewDependencyGraph()

	graph.AddEdge("a", "b")
	graph.AddEdge("a", "c")
	graph.AddEdge("c", "d")
	graph.AddEdge("a", "d")
	graph.AddEdge("b", "c")
	graph.AddEdge("b", "d")

	path := graph.txnBefore("d")
	expectPath := []string{"a", "b", "c", "d"}
	if len(path) != len(expectPath) {
		t.Errorf("error")
	} else {
		for i, txnId := range expectPath {
			if path[i] != txnId {
				t.Errorf("txn %v not in order", txnId)
			}
		}
	}

	graph.Remove("c")
	if graph.inDegree["d"] != 2 {
		t.Errorf("error")
	}
}

func TestGraph_AddNodeWithKeys(t *testing.T) {
	graph := NewDependencyGraph()

	graph.AddNodeWithKeys("a", map[string]bool{"1": true, "2": true, "3": true})

	graph.AddNodeWithKeys("b", map[string]bool{"2": true, "4": true})

	graph.AddNodeWithKeys("c", map[string]bool{"4": true, "3": true})

	path := graph.txnBefore("c")

	expectPath := []string{"a", "b", "c"}

	if len(path) != len(expectPath) {
		t.Errorf("error")
	} else {
		for i, txnId := range expectPath {
			if path[i] != txnId {
				t.Errorf("txn %v not in order", txnId)
			}
		}
	}

}

func TestGraph_RemoveNodeWithKeys(t *testing.T) {
	graph := NewDependencyGraph()

	graph.AddNodeWithKeys("a", map[string]bool{"1": true, "2": true, "3": true})

	graph.AddNodeWithKeys("b", map[string]bool{"2": true, "4": true})

	graph.AddNodeWithKeys("c", map[string]bool{"4": true, "3": true})

	path := graph.txnBefore("c")

	expectPath := []string{"a", "b", "c"}

	if len(path) != len(expectPath) {
		t.Errorf("error")
	} else {
		for i, txnId := range expectPath {
			if path[i] != txnId {
				t.Errorf("txn %v not in order", txnId)
			}
		}
	}

	graph.RemoveNodeWithKeys("b", map[string]bool{"2": true, "4": true})
	path = graph.txnBefore("c")

	expectPath = []string{"a", "c"}

	if len(path) != len(expectPath) {
		t.Errorf("error")
	} else {
		for i, txnId := range expectPath {
			if path[i] != txnId {
				t.Errorf("txn %v not in order", txnId)
			}
		}
	}

	graph.AddNodeWithKeys("b", map[string]bool{"2": true, "4": true})

	path = graph.txnBefore("c")

	expectPath = []string{"a", "c"}

	if len(path) != len(expectPath) {
		t.Errorf("error")
	} else {
		for i, txnId := range expectPath {
			if path[i] != txnId {
				t.Errorf("txn %v not in order", txnId)
			}
		}
	}

	path = graph.txnBefore("b")

	expectPath = []string{"a", "c", "b"}

	if len(path) != len(expectPath) {
		t.Errorf("error")
	} else {
		for i, txnId := range expectPath {
			if path[i] != txnId {
				t.Errorf("txn %v not in order", txnId)
			}
		}
	}

}
