package workload

import (
	"Carousel-GTS/utils"
	"math"
	"math/rand"
)

type Workload interface {
	GenTxn() Txn
}

type AbstractWorkload struct {
	Workload // interface

	//KeyList   []string  // all of the keys
	KeyNum    int64     // total number of the keys
	alpha     float64   // zipfian alpha value
	zipf      []float64 // zipfian values
	zipfReady bool      // If zipfian distribution has been initialized
	txnCount  int64

	keySize            int // the size of value in Bytes
	priorityPercentage int
	//val     string
	partition int
}

func NewAbstractWorkload(
	keyNum int64,
	zipfAlpha float64,
	keySize int,
	priorityPercentage int,
	partition int,
) *AbstractWorkload {
	workload := &AbstractWorkload{
		KeyNum:             keyNum / int64(partition),
		alpha:              zipfAlpha,
		keySize:            keySize,
		priorityPercentage: priorityPercentage,
		partition:          partition,
	}
	workload.zipf = nil
	workload.zipfReady = false
	workload.txnCount = 0

	return workload
}

// Currently, the read and write keys overlap, that is, one set is a subset of the other.
// TODO Generates transactions that have read and write keys not fully overlapped.
func (workload *AbstractWorkload) buildTxn(
	txnId string,
	rN, wN int,
) Txn {
	txn := &BaseTxn{
		txnId:     txnId,
		readKeys:  make([]string, 0),
		writeKeys: make([]string, 0),
		writeData: make(map[string]string),
	}

	max := wN
	if rN > max {
		max = rN
	}
	// Generates keys
	keyList := workload.genKeyList(max)

	// Read keys
	for i := 0; i < rN; i++ {
		txn.readKeys = append(txn.readKeys, keyList[i])
	}
	// Write keys
	for i := 0; i < wN; i++ {
		//txn.writeData[keyList[i]] = keyList[i] // uses the key as the data, like in TAPIR's benchmark
		txn.writeData[keyList[i]] = ""
		txn.writeKeys = append(txn.writeKeys, keyList[i])
	}

	p := rand.Intn(100)
	txn.priority = p < workload.priorityPercentage

	return txn
}

func (workload *AbstractWorkload) genKeyList(num int) []string {
	kList := make([]string, num)
	kMap := make(map[string]bool)
	for i := 0; i < len(kList); i++ {
		for {
			k := utils.ConvertToString(workload.keySize, workload.randKey())
			if _, exist := kMap[k]; exist {
				continue
			}
			kMap[k] = true
			kList[i] = k
			break
		}
	}
	return kList
}

// Acknowledgement: this implementation is based on TAPIR's retwis benchmark.
func (workload *AbstractWorkload) randKey() int64 {
	if workload.alpha < 0 {
		// Uniform selection of keys.
		return rand.Int63n(workload.KeyNum)
	} else {
		// Zipf-like selection of keys.
		if !workload.zipfReady {
			workload.zipf = make([]float64, workload.KeyNum)

			var c = 0.0
			var i int64
			for i = 1; i <= workload.KeyNum; i++ {
				c = c + (1.0 / math.Pow(float64(i), workload.alpha))
			}
			c = 1.0 / c

			var sum = 0.0
			for i = 1; i <= workload.KeyNum; i++ {
				sum += c / math.Pow(float64(i), workload.alpha)
				workload.zipf[i-1] = sum
			}
			workload.zipfReady = true
		}

		var rndNum = 0.0
		for rndNum == 0.0 {
			rndNum = rand.Float64() //[0.0,1.0)
		}

		// Uses binary search to find the key's index
		var l, r, mid int64 = 0, workload.KeyNum, 0
		for l < r {
			if rndNum > workload.zipf[mid] {
				l = mid + 1
			} else if rndNum < workload.zipf[mid] {
				r = mid - 1
			} else {
				break
			}
			// Updates the mid for the last round
			mid = (l + r) / 2
		}

		if mid >= int64(len(workload.zipf)) {
			mid = int64(len(workload.zipf)) - 1
		}

		if workload.zipf[mid] < rndNum {
			// Takes the right one
			if mid+1 < int64(len(workload.zipf)) {
				return mid + 1
			}
		}

		p := int64(rand.Intn(workload.partition))
		return p + int64(workload.partition)*mid
	}
}

func (workload *AbstractWorkload) String() string {
	return "AbstractWorkload"
}
