package server

import (
	"github.com/sirupsen/logrus"
	"time"
)

type Node struct {
	op    *ReadAndPrepareOp
	left  *Node
	right *Node
}

type BinarySearchTree struct {
	root       *Node
	timeWindow time.Duration
}

func NewBinarySearchTree(timeWindow time.Duration) *BinarySearchTree {
	bst := &BinarySearchTree{
		root:       nil,
		timeWindow: timeWindow,
	}
	return bst
}

func (bst *BinarySearchTree) Insert(op *ReadAndPrepareOp) {
	n := &Node{
		op:    op,
		left:  nil,
		right: nil,
	}

	if bst.root == nil {
		bst.root = n
	} else {
		insertNode(bst.root, n)
	}
}

func insertNode(node, newNode *Node) {
	if newNode.op.request.Timestamp < node.op.request.Timestamp {
		if node.left == nil {
			node.left = newNode
		} else {
			insertNode(node.left, newNode)
		}
	} else {
		if node.right == nil {
			node.right = newNode
		} else {
			insertNode(node.right, newNode)
		}
	}
}

func conflict(low *ReadAndPrepareOp, high *ReadAndPrepareOp) bool {
	for rk := range low.allReadKeys {
		if _, exist := high.allReadKeys[rk]; exist {
			logrus.Debugf("key %v : txn (low) %v read and txn (high) %v write", rk, low.txnId, high.txnId)
			return true
		}
	}

	for wk := range low.allWriteKeys {
		if _, exist := high.allWriteKeys[wk]; exist {
			return true
		}
		if _, exist := high.allReadKeys[wk]; exist {
			return true
		}
	}

	return false
}

func (bst *BinarySearchTree) SearchConflictTxnWithinTimeWindow(op *ReadAndPrepareOp) bool {
	return search(bst.root, op, bst.timeWindow)
}

func search(n *Node, op *ReadAndPrepareOp, timeWindow time.Duration) bool {
	if n == nil {
		return false
	}

	hTm := time.Unix(n.op.request.Timestamp, 0)
	lTm := time.Unix(op.request.Timestamp, 0)
	duration := lTm.Sub(hTm)
	if duration < 0 {
		duration = hTm.Sub(lTm)
	}
	if duration < timeWindow {
		if conflict(op, n.op) {
			return true
		}
		return search(n.left, op, timeWindow) && search(n.right, op, timeWindow)
	} else {
		return search(n.left, op, timeWindow)
	}
}

func (bst *BinarySearchTree) Remove(op *ReadAndPrepareOp) {
	p := bst.root
	var pp *Node = nil
	for p != nil && p.op.request.Timestamp != op.request.Timestamp {
		pp = p
		if op.request.Timestamp > p.op.request.Timestamp {
			p = p.right
		} else {
			p = p.left
		}
	}

	if p == nil {
		return
	}

	if p.left != nil && p.right != nil {
		minP := p.right
		minPP := p
		for minP.left != nil {
			minPP = minP
			minP = minP.left
		}
		p.op = minP.op
		p = minP
		pp = minPP
	}

	var child *Node
	if p.left != nil {
		child = p.left
	} else if p.right != nil {
		child = p.right
	} else {
		child = nil
	}

	if pp == nil {
		bst.root = child
	} else if pp.left == p {
		pp.left = child
	} else {
		pp.right = child
	}
}
