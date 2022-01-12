package workload

import (
	"Carousel-GTS/utils"
	"github.com/sirupsen/logrus"
)

type Txn interface {
	GetTxnId() string
	GetReadKeys() []string
	GetWriteKeys() []string
	GetWriteData() map[string]string
	GetPriority() bool
	SetPriority(p bool)

	GenWriteData(readData map[string]string)
	GetTxnType() string
	SetTxnType(string)
}

type BaseTxn struct {
	txnId     string
	readKeys  []string
	writeKeys []string
	writeData map[string]string
	priority  bool
	txnType   string
}

func (t *BaseTxn) GetTxnId() string {
	return t.txnId
}

func (t *BaseTxn) GetReadKeys() []string {
	return t.readKeys
}

func (t *BaseTxn) GetWriteKeys() []string {
	return t.writeKeys
}

func (t *BaseTxn) GetWriteData() map[string]string {
	return t.writeData
}

func (t *BaseTxn) GetPriority() bool {
	return t.priority
}

func (t *BaseTxn) GetTxnType() string {
	return t.txnType
}

func (t *BaseTxn) SetTxnType(tp string) {
	t.txnType = tp
}

func (t *BaseTxn) SetPriority(p bool) {
	t.priority = p
}

func (t *BaseTxn) GenWriteData(readData map[string]string) {
	logrus.Debugf("gen write data for txn %v", t.txnId)
	for key := range t.writeData {
		if value, exist := readData[key]; exist {
			t.writeData[key] = utils.ConvertToString(len(value), utils.ConvertToInt(value)+1)
		} else {
			t.writeData[key] = key
		}
	}
}
