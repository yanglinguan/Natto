package utils

import (
	"testing"
)

func TestGraph_AddEdge(t *testing.T) {
	graph := NewDependencyGraph()

	graph.addEdge("a", "b")
	graph.addEdge("a", "c")
	graph.addEdge("c", "d")
	graph.addEdge("e", "b")

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

	graph.addEdge("a", "b")
	graph.addEdge("a", "c")
	graph.addEdge("c", "d")
	graph.addEdge("e", "b")

	path1 := graph.GetConflictTxn("d")

	graph.remove("c")

	path2 := graph.GetConflictTxn("d")

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

	graph.addEdge("a", "b")
	graph.addEdge("a", "c")
	graph.addEdge("c", "d")
	graph.addEdge("a", "d")
	graph.addEdge("b", "c")
	graph.addEdge("b", "d")

	path := graph.GetConflictTxn("d")
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

	graph.remove("a")
	graph.remove("b")

	path = graph.GetConflictTxn("d")
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

	graph.addEdge("a", "b")
	graph.addEdge("a", "c")
	graph.addEdge("c", "d")
	graph.addEdge("a", "d")
	graph.addEdge("b", "c")
	graph.addEdge("b", "d")

	path := graph.GetConflictTxn("d")
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

	graph.remove("c")
	if graph.inDegree["d"] != 2 {
		t.Errorf("error")
	}
}

func TestGraph_AddNodeWithKeys(t *testing.T) {
	graph := NewDependencyGraph()

	graph.AddNode("a", map[string]bool{"1": true, "2": true, "3": true})

	graph.AddNode("b", map[string]bool{"2": true, "4": true})

	graph.AddNode("c", map[string]bool{"1": true, "3": true})

	graph.AddNode("d", map[string]bool{"2": true, "3": true})

	path := graph.GetConflictTxn("d")
	parent := graph.GetParent("d")

	expectPath := []string{"a", "c", "b", "d"}

	expectParent := map[string]bool{"b": true, "c": true}
	if len(expectParent) != len(parent) {
		t.Errorf("error expect: %v, result: %v", expectParent, parent)
	} else {
		for p := range parent {
			if _, exist := expectParent[p]; !exist {
				t.Errorf("txn %v not match", p)
			}
		}
	}

	if len(path) != len(expectPath) {
		t.Errorf("error %v", path)
	} else {
		for i, txnId := range expectPath {
			if path[i] != txnId {
				t.Errorf("txn %v not in order, expect %v, result %v",
					txnId, expectPath, path)
			}
		}
	}

}

func TestGraph_RemoveNodeWithKeys(t *testing.T) {
	graph := NewDependencyGraph()

	graph.AddNode("a", map[string]bool{"1": true, "2": true, "3": true})

	graph.AddNode("b", map[string]bool{"2": true, "4": true})

	graph.AddNode("c", map[string]bool{"4": true, "3": true})

	path := graph.GetConflictTxn("c")

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

	graph.RemoveNode("b", map[string]bool{"2": true, "4": true})
	path = graph.GetConflictTxn("c")

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

	graph.AddNode("b", map[string]bool{"2": true, "4": true})

	path = graph.GetConflictTxn("c")

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

	path = graph.GetConflictTxn("b")

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

func TestGraph_RemoveNodeWithKeys2(t *testing.T) {
	graph := NewDependencyGraph()

	graph.AddNode("a", map[string]bool{"1": true})

	graph.AddNode("b", map[string]bool{"1": true})

	graph.RemoveNode("b", map[string]bool{"1": true})

	graph.AddNode("c", map[string]bool{"1": true})
	path := graph.GetConflictTxn("c")

	expectPath := []string{"a", "c"}

	if len(path) != len(expectPath) {
		t.Errorf("error: path %v", path)
	} else {
		for i, txnId := range expectPath {
			if path[i] != txnId {
				t.Errorf("txn %v not in order", txnId)
			}
		}
	}
}
