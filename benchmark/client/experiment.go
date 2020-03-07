package main

import (
	"Carousel-GTS/benchmark/workload"
	"Carousel-GTS/client"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

type Experiment interface {
	Execute()
}

type OpenLoopExperiment struct {
	client   *client.Client
	workload workload.Workload
	wg       sync.WaitGroup
}

func NewOpenLoopExperiment(client *client.Client, workload workload.Workload) *OpenLoopExperiment {
	e := &OpenLoopExperiment{
		client:   client,
		workload: workload,
		wg:       sync.WaitGroup{},
	}
	return e
}

func (o *OpenLoopExperiment) Execute() {
	txnRate := o.client.Config.GetTxnRate()
	interval := time.Duration(int64(time.Second) / int64(txnRate))
	expDuration := o.client.Config.GetExpDuration()
	totalTxn := o.client.Config.GetTotalTxn()
	s := time.Now()
	d := time.Since(s)
	c := 0
	for d < expDuration || (expDuration <= 0 && c < totalTxn) {
		txn := o.workload.GenTxn()

		o.wg.Add(1)
		go o.execTxn(txn)

		time.Sleep(interval)
		d = time.Since(s)
		c++
	}

	o.wg.Wait()
}

func (o *OpenLoopExperiment) execTxn(txn *workload.Txn) {
	execTxn(o.client, txn)
	o.wg.Done()
}

func execTxn(client *client.Client, txn *workload.Txn) (bool, bool, time.Duration) {
	readResult := client.ReadAndPrepare(txn.ReadKeys, txn.ReadKeys, txn.TxnId)
	txn.GenWriteData(readResult)

	for k, v := range txn.WriteData {
		logrus.Infof("write key %v: %v", k, v)
	}

	return client.Commit(txn.WriteData, txn.TxnId)
}

type CloseLoopExperiment struct {
	client   *client.Client
	workload workload.Workload
}

func NewCloseLoopExperiment(client *client.Client, workload workload.Workload) *CloseLoopExperiment {
	e := &CloseLoopExperiment{
		client:   client,
		workload: workload,
	}
	return e
}

func (e *CloseLoopExperiment) Execute() {
	expDuration := e.client.Config.GetExpDuration()
	totalTxn := e.client.Config.GetTotalTxn()
	s := time.Now()
	d := time.Since(s)
	c := 0
	txn := e.workload.GenTxn()
	for d < expDuration || (expDuration <= 0 && c < totalTxn) {
		commit, retry, waitTime := execTxn(e.client, txn)
		d = time.Since(s)
		if !commit && retry {
			logrus.Infof("RETRY txn %v wait time %v", txn.TxnId, waitTime)
			time.Sleep(waitTime)
			continue
		}
		txn = e.workload.GenTxn()
		c++
	}
}
