package tapir

import (
	"bytes"
	"encoding/gob"
	"strconv"
	"strings"
)

// NOTE: all fields to be encoded in bytes must be first-letter capitalized
// Read operation
type ReadOp struct {
	Id  string // TAPIR txnId
	Key []string
}

type VerVal struct {
	Ver int64  // version
	Val string // value
}

type ReadOpRet struct {
	Ret map[string]*VerVal // Key --> Read Result
}

// Commit / abort operation
type CommitOp struct {
	IsC bool  // true to commit, false to abort
	T   int64 // txn timestamp
	// txn
	Id    string            // TAPIR txnId
	Write map[string]string // write set, key --> value
	Read  map[string]int64  // read set, key --> version
}

// Prepare operation
type PrepareOp struct {
	T int64 // txn timestamp
	// txn
	Id    string            // TAPIR txnId
	Write map[string]string // write set, key --> value
	Read  map[string]int64  // read set, key --> version
}

type PrepareOpRet struct {
	State int   // TAPIR txn prepare result
	T     int64 // timestamp when the prepare result is retry
}

func Encode(a interface{}) []byte {
	var bf bytes.Buffer
	enc := gob.NewEncoder(&bf)
	enc.Encode(a)
	return bf.Bytes()
}

func Decode(b []byte, a interface{}) {
	bf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(bf)
	switch a.(type) {
	case *ReadOp:
		dec.Decode(a.(*ReadOp))
	case *ReadOpRet:
		dec.Decode(a.(*ReadOpRet))
	case *CommitOp:
		dec.Decode(a.(*CommitOp))
	case *PrepareOp:
		dec.Decode(a.(*PrepareOp))
	default:
		logger.Fatalf("Decoding unknown type %v", a)
	}
}

func EncodeToStr(a interface{}) string {
	switch a.(type) {
	case *PrepareOpRet:
		ret := a.(*PrepareOpRet)
		return strconv.Itoa(ret.State) + TAPIR_OP_CODING_REGEX + strconv.FormatInt(ret.T, 10)
	default:
		logger.Fatalf("Econding unsupported type %v", a)
		return ""
	}
}

func DecodeFromStr(s string, a interface{}) {
	switch a.(type) {
	case *PrepareOpRet:
		ret := a.(*PrepareOpRet)
		eList := strings.Split(s, TAPIR_OP_CODING_REGEX)
		var err error
		ret.State, err = strconv.Atoi(eList[0])
		if err != nil {
			logger.Fatalf("Fails to decode %s to *PrepareOpRet", s)
		}
		ret.T, err = strconv.ParseInt(eList[1], 10, 64)
		if err != nil {
			logger.Fatalf("Fails to decode %s to *PrepareOpRet", s)
		}
	default:
		logger.Fatalf("Econding unsupported type %v", a)
	}
}
