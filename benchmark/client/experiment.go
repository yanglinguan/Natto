package main

import (
	"Carousel-GTS/benchmark/workload"
	"Carousel-GTS/client"
	"Carousel-GTS/configuration"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type Experiment interface {
	Execute()
}

type OpenLoopExperiment struct {
	client client.IFClient
	config configuration.Configuration
	//client      *client.Client
	workload    workload.Workload
	wg          sync.WaitGroup
	txnChan     chan *workload.Txn
	highTxnOnly bool
}

func NewOpenLoopExperiment(
	client client.IFClient,
	config configuration.Configuration,
	wl workload.Workload,
	highTxnOnly bool) *OpenLoopExperiment {
	e := &OpenLoopExperiment{
		client:      client,
		config:      config,
		workload:    wl,
		wg:          sync.WaitGroup{},
		highTxnOnly: highTxnOnly,
		txnChan:     make(chan *workload.Txn, 10240),
	}
	return e
}

func nextTxnWaitTime(config configuration.Configuration) time.Duration {
	// transaction sending rate (txn/s)
	txnRate := config.GetTxnRate()
	interval := float64(time.Second) / float64(txnRate)
	if config.UsePoissonProcessBetweenArrivals() {
		// poisson process between arrivals
		u := rand.Float64()
		w := -interval * math.Log(1-u)
		return time.Duration(w)
	} else {
		return time.Duration(interval)
	}
}

// Open loop experiment: client sends txn at the rate (txn/s) specified in the config file ("txnRate")
// client keep sending txn within the experiment duration specified in config file ("duration")
// or when the number of txn reaches the total txn specified in config file ("totalTxn")
func (o *OpenLoopExperiment) Execute() {
	//o.client.Start()
	// transaction sending rate (txn/s)
	//txnRate := o.client.Config.GetTxnRate()
	// waiting time between sending two transactions
	//interval := time.Duration(int64(time.Second) / int64(txnRate))
	// experiment duration (s)
	expDuration := o.config.GetExpDuration()
	totalTxn := o.config.GetTotalTxn()
	s := time.Now()
	d := time.Since(s)
	c := 0
	// sending txn
	for d < expDuration || (expDuration <= 0 && c < totalTxn) {
		//var txn *workload.Txn
		//if len(o.txnChan) > 0 {
		//	txn = <-o.txnChan
		//} else {
		//	txn = o.workload.GenTxn()
		//}
		txn := o.workload.GenTxn()
		if !o.highTxnOnly || txn.GetPriority() {
			o.wg.Add(1)
			go o.execTxn(txn)
			c++
		}

		waitTime := nextTxnWaitTime(o.config)
		time.Sleep(waitTime)
		d = time.Since(s)
	}

	logrus.Debugf("waiting for all txn commit total %v", c)
	o.wg.Wait()
	logrus.Debugf("all txn commit %v", c)
}

func (o *OpenLoopExperiment) execTxn(txn workload.Txn) {
	logrus.Debugf("exec txn %v ", txn.GetTxnId())
	o.retry(txn)
	o.wg.Done()
}

// run txn
// txn finishes when it commits or does not require retry
func (o *OpenLoopExperiment) retry(txn workload.Txn) {
	commit, retry, waitTime, _ := o.client.ExecTxn(txn)
	// txn finishes when it commits or does not require retry
	if commit || !retry {
		logrus.Debugf("txn %v commit result %v retry %v", txn.GetTxnId(), commit, retry)
		return
	}
	//o.txnChan <- txn
	//// when retry the transaction, wait time depends on the retry policy (exponential back-off or constant time)
	logrus.Debugf("RETRY txn %v wait time %v", txn.GetTxnId(), waitTime)
	time.Sleep(waitTime)
	o.retry(txn)
}

type CloseLoopExperiment struct {
	client   client.IFClient
	config   configuration.Configuration
	workload workload.Workload
}

func NewCloseLoopExperiment(client client.IFClient, config configuration.Configuration, workload workload.Workload) *CloseLoopExperiment {
	e := &CloseLoopExperiment{
		client:   client,
		config:   config,
		workload: workload,
	}
	return e
}

func (e *CloseLoopExperiment) Execute() {
	//e.client.Start()
	expDuration := e.config.GetExpDuration()
	totalTxn := e.config.GetTotalTxn()
	s := time.Now()
	d := time.Since(s)
	c := 0
	txn := e.workload.GenTxn()
	for d < expDuration || (expDuration <= 0 && c < totalTxn) {
		commit, retry, waitTime, expWait := e.client.ExecTxn(txn)
		if !commit && retry {
			logrus.Infof("RETRY txn %v wait time %v", txn.GetTxnId(), waitTime)
			time.Sleep(waitTime)
			continue
		}
		time.Sleep(expWait)
		d = time.Since(s)
		txn = e.workload.GenTxn()
		c++
	}
}
