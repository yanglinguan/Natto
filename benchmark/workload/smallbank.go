package workload

import (
	"strconv"

	"Carousel-GTS/utils"
)

/*
 * This smallbank workload is based on the code in OLTPBench.
 * The original data scheme (i.e., tables in a relational database) is mapped
 * to a key-value data scheme as follows:
 *			accountID --> account balance
 * An accountID is a tuple of customerID and accountType.
 * A transaction accepts custormerID as a parameter and (determinstically)
 * constructs the accountID according to the tranaction type.
 * Customer names are omitted because a customer name is mapped to a unique
 * customerID and is not changed in the OLTPBench. Thus, a customer name can be
 * used as the customerID.
 * NOTE: When a database inits, there are only customerIDs. The database should
 * create two accountIDs for each cusotmer, and store these two accountIDs in
 * the same datacenter, better within the same data partition.
 */
const (
	// ACCOUNT TYPES
	SB_CHECKING = "c"
	SB_SAVINGS  = "s"

	// ----------------------------------------------------------------
	// ACCOUNT INFORMATION
	// ----------------------------------------------------------------
	// Default number of customers in bank
	SB_NUM_ACCOUNTS           = 1000000
	SB_HOTSPOT_USE_FIXED_SIZE = false
	SB_HOTSPOT_PERCENTAGE     = 25  // [0% - 100%]
	SB_HOTSPOT_FIXED_SIZE     = 100 // fixed number of tuples

	// ----------------------------------------------------------------
	// ADDITIONAL CONFIGURATION SETTINGS
	// ----------------------------------------------------------------
	// Initial balance amount
	// We'll just make it really big so that they never run out of money
	SB_MIN_BALANCE = 10000
	SB_MAX_BALANCE = 50000

	// ----------------------------------------------------------------
	// PROCEDURE PARAMETERS
	// These amounts are from the original code
	// ----------------------------------------------------------------
	SB_PARAM_SEND_PAYMENT_AMOUNT     = 5.0
	SB_PARAM_DEPOSIT_CHECKING_AMOUNT = 1.3
	SB_PARAM_TRANSACT_SAVINGS_AMOUNT = 20.20
	SB_PARAM_WRITE_CHECK_AMOUNT      = 5.0

	// TXN TYPES
	SB_TXN_AMALGAMATE       = 0
	SB_TXN_BALANCE          = 1
	SB_TXN_DEPOSIT_CHECKING = 2
	SB_TXN_SEND_PAYMENT     = 3
	SB_TXN_TRANSACT_SAVINGS = 4
	SB_TXN_WRITE_CHECK      = 5
)

type TxnAmalgamate struct {
	*BaseTxn
}

func (t *TxnAmalgamate) GenWriteData(readData map[string]string) {
	if len(readData) != 3 {
		// TODO Error
	}
	ccId1, csId1, ccId2 := t.readKeys[0], t.readKeys[1], t.readKeys[2]
	cB1, sB1 := utils.DecodeFloat64(readData[ccId1]), utils.DecodeFloat64(readData[csId1])
	cB2 := utils.DecodeFloat64(readData[ccId2])
	cB2 += cB1 + sB1
	cB1, sB1 = 0, 0
	t.writeData[ccId1] = utils.EncodeFloat64(cB1)
	t.writeData[csId1] = utils.EncodeFloat64(sB1)
	t.writeData[ccId2] = utils.EncodeFloat64(cB2)
	if len(t.writeData) != 3 {
		// TODO Error
	}
}

// Balance Txn
type TxnBalance struct {
	*BaseTxn
}

func (t *TxnBalance) GenWriteData(readData map[string]string) {
	if len(readData) != 2 {
		// TODO Error
	}
	if len(t.writeData) != 0 {
		// TODO Error
	}
}

// Deposit Checking Txn
type TxnDepositChecking struct {
	*BaseTxn
}

func (t *TxnDepositChecking) GenWriteData(readData map[string]string) {
	if len(readData) != 1 {
		// TODO Error
	}
	ccId := t.readKeys[0]
	balance := utils.DecodeFloat64(readData[ccId])
	balance += SB_PARAM_DEPOSIT_CHECKING_AMOUNT
	t.writeData[ccId] = utils.EncodeFloat64(balance)
	if len(t.writeData) != 1 {
		// TODO Error
	}
}

// Send Payment Txn
type TxnSendPayment struct {
	*BaseTxn
}

func (t *TxnSendPayment) GenWriteData(readData map[string]string) {
	if len(readData) != 2 {
		// TODO Error
	}
	ccId1, ccId2 := t.readKeys[0], t.readKeys[1]
	b1, b2 := utils.DecodeFloat64(readData[ccId1]), utils.DecodeFloat64(readData[ccId2])
	b1 -= SB_PARAM_SEND_PAYMENT_AMOUNT // Assuming there are sufficient money
	b2 += SB_PARAM_SEND_PAYMENT_AMOUNT
	t.writeData[ccId1], t.writeData[ccId2] = utils.EncodeFloat64(b1), utils.EncodeFloat64(b2)
	if len(t.writeData) != 2 {
		// TODO Error
	}
}

// Transact Savings Txn
type TxnTransactSavings struct {
	*BaseTxn
}

func (t *TxnTransactSavings) GenWriteData(readData map[string]string) {
	if len(readData) != 1 {
		// TODO Error
	}
	csId := t.readKeys[0]
	balance := utils.DecodeFloat64(readData[csId])
	balance -= SB_PARAM_TRANSACT_SAVINGS_AMOUNT
	t.writeData[csId] = utils.EncodeFloat64(balance)
	if len(t.writeData) != 1 {
		// TODO Error
	}
}

// Write Check Txn
type TxnWriteCheck struct {
	*BaseTxn
}

func (t *TxnWriteCheck) GenWriteData(readData map[string]string) {
	if len(readData) != 2 {
		// TODO Error
	}
	ccId, csId := t.readKeys[0], t.readKeys[1]
	cB, sB := utils.DecodeFloat64(readData[ccId]), utils.DecodeFloat64(readData[csId])
	if cB+sB < SB_PARAM_WRITE_CHECK_AMOUNT {
		cB -= SB_PARAM_WRITE_CHECK_AMOUNT + 1
	} else {
		cB -= SB_PARAM_WRITE_CHECK_AMOUNT
	}
	t.writeData[ccId] = utils.EncodeFloat64(cB)
	if len(t.writeData) != 1 {
		// TODO Error
	}
}

// SmallBank workload from OLTPBench
type SmallBankWorkload struct {
	*AbstractWorkload
}

func (w *SmallBankWorkload) GenTxn() Txn {
	w.txnCount++
	txnId := strconv.FormatInt(w.txnCount, 10)

	baseTxn := &BaseTxn{
		txnId:     txnId,
		readKeys:  make([]string, 0),
		writeKeys: make([]string, 0),
		writeData: make(map[string]string),
	}

	txnType := w.genTxnType()
	switch txnType {
	case SB_TXN_AMALGAMATE:
		cId1 := w.genCustomerID() // sender customerID
		cId2 := w.genCustomerID() // receiver customerID
		for cId1 == cId2 {
			cId2 = w.genCustomerID()
		}
		ccId1, csId1, ccId2 := w.getCheckingID(cId1), w.getSavingsID(cId1), w.getCheckingID(cId2)
		// The order of keys in the read set is important
		baseTxn.readKeys = append(baseTxn.readKeys, ccId1)
		baseTxn.readKeys = append(baseTxn.readKeys, csId1)
		baseTxn.readKeys = append(baseTxn.readKeys, ccId2)
		baseTxn.writeKeys = append(baseTxn.writeKeys, ccId1)
		baseTxn.writeKeys = append(baseTxn.writeKeys, csId1)
		baseTxn.writeKeys = append(baseTxn.writeKeys, ccId2)
		baseTxn.writeData[ccId1] = ""
		baseTxn.writeData[csId1] = ""
		baseTxn.writeData[ccId2] = ""
		return &TxnAmalgamate{BaseTxn: baseTxn}

	case SB_TXN_BALANCE:
		// Balance
		cId := w.genCustomerID()
		ccId, csId := w.getCheckingID(cId), w.getSavingsID(cId)
		baseTxn.readKeys = append(baseTxn.readKeys, ccId)
		baseTxn.readKeys = append(baseTxn.readKeys, csId)
		return &TxnBalance{BaseTxn: baseTxn}

	case SB_TXN_DEPOSIT_CHECKING:
		// Deposit Checking
		cId := w.genCustomerID()
		ccId := w.getCheckingID(cId)
		baseTxn.readKeys = append(baseTxn.readKeys, ccId)
		baseTxn.writeKeys = append(baseTxn.writeKeys, ccId)
		baseTxn.writeData[ccId] = ""
		return &TxnDepositChecking{BaseTxn: baseTxn}

	case SB_TXN_SEND_PAYMENT:
		// Send Payment
		cId1 := w.genCustomerID() // sender customerID
		cId2 := w.genCustomerID() // receiver customerID
		for cId1 == cId2 {
			cId2 = w.genCustomerID()
		}
		ccId1, ccId2 := w.getCheckingID(cId1), w.getCheckingID(cId2)
		// The order of keys in the read set is important
		baseTxn.readKeys = append(baseTxn.readKeys, ccId1)
		baseTxn.readKeys = append(baseTxn.readKeys, ccId2)
		baseTxn.writeKeys = append(baseTxn.writeKeys, ccId1)
		baseTxn.writeKeys = append(baseTxn.writeKeys, ccId2)
		baseTxn.writeData[ccId1] = ""
		baseTxn.writeData[ccId2] = ""
		return &TxnSendPayment{BaseTxn: baseTxn}

	case SB_TXN_TRANSACT_SAVINGS:
		// Transaction Savings
		cId := w.genCustomerID()
		csId := w.getSavingsID(cId)
		baseTxn.readKeys = append(baseTxn.readKeys, csId)
		baseTxn.writeKeys = append(baseTxn.writeKeys, csId)
		baseTxn.writeData[csId] = ""
		return &TxnTransactSavings{BaseTxn: baseTxn}

	case SB_TXN_WRITE_CHECK:
		// Write Check
		cId := w.genCustomerID()
		ccId := w.getCheckingID(cId)
		csId := w.getSavingsID(cId)
		// The order of keys in the read set is important
		baseTxn.readKeys = append(baseTxn.readKeys, ccId)
		baseTxn.readKeys = append(baseTxn.readKeys, csId)
		baseTxn.writeKeys = append(baseTxn.writeKeys, ccId)
		baseTxn.writeData[ccId] = ""
		return &TxnWriteCheck{BaseTxn: baseTxn}

	default:
		// TODO Error
	}
	return nil
}

func (w *SmallBankWorkload) genTxnType() int {
	// TODO implementation
	return -1
}

func (w *SmallBankWorkload) genCustomerID() string {
	// TODO implementation
	return ""
}

func (w *SmallBankWorkload) getCheckingID(cId string) string {
	return cId + SB_CHECKING
}

func (w *SmallBankWorkload) getSavingsID(cId string) string {
	return cId + SB_SAVINGS
}
