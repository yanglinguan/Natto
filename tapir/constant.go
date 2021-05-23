package tapir

const (
	IR_OP_TENTATIVE = 1
	IR_OP_FINALIZED = 2
)

const (
	TAPIR_TXN_INIT       = 0
	TAPIR_TXN_PREPARE_OK = 1
	TAPIR_TXN_ABORT      = 2
	TAPIR_TXN_ABSTAIN    = 3
	TAPIR_TXN_RETRY      = 4
	TAPIR_TXN_COMMITTED  = 5
)

const (
	TAPIR_VERSION_INVALID = -1
	TAPIR_VERSION_INIT    = 0
)

const (
	TAPIR_TIME_TO_FIND_CLOSEST_REPLICAS = 3 // seconds
)

const (
	TAPIR_OP_CODING_REGEX = ":"
)