package workload

import (
	log "github.com/sirupsen/logrus"
	"math/rand"
	"strconv"
)

// Retwis workload
const RetwisDefaultAddUserRatio = 5
const RetwisDefaultFollowUnfollowRatio = 15
const RetwisDefaultPostTweetRatio = 30
const RetwisDefaultLoadTimelineRatio = 50

// Acknowledgement: this implementation is based on TAPIR's retwis benchmark.
type RetwisWorkload struct {
	*AbstractWorkload

	addUserRatio        int
	followUnfollowRatio int
	postTweetRatio      int
	loadTimelineRatio   int
}

func NewRetwisWorkload(
	workload *AbstractWorkload,
	retwisAddUserRatio int,
	retwisFollowUnfollowRatio int,
	retwisPostTweetRatio int,
	retwisLoadTimelineRatio int,
) *RetwisWorkload {
	retwis := &RetwisWorkload{
		AbstractWorkload:    workload,
		addUserRatio:        retwisAddUserRatio,
		followUnfollowRatio: retwisFollowUnfollowRatio,
		postTweetRatio:      retwisPostTweetRatio,
		loadTimelineRatio:   retwisLoadTimelineRatio,
	}

	return retwis
}

// Generates a retwis txn. This function is currently not thread-safe
func (retwis *RetwisWorkload) GenTxn() *Txn {
	retwis.txnCount++
	txnId := strconv.FormatInt(retwis.txnCount, 10)

	txnType := rand.Intn(100) //[0,100)
	if txnType < retwis.addUserRatio {
		// Add user txn. read 1, write 3 keys
		return retwis.buildTxn(txnId, 1, 3)
	} else if txnType <
		(retwis.addUserRatio + retwis.followUnfollowRatio) {
		// Follow/Unfollow txn. read 2, write 2 keys
		return retwis.buildTxn(txnId, 2, 2)
	} else if txnType <
		(retwis.addUserRatio + retwis.followUnfollowRatio + retwis.postTweetRatio) {
		// Post tweet txn. read 3, write 5 keys
		return retwis.buildTxn(txnId, 3, 5)
	} else if txnType <
		(retwis.addUserRatio + retwis.followUnfollowRatio + retwis.postTweetRatio +
			retwis.loadTimelineRatio) {
		// Load timeline txn. read [1, 10] keys
		rN := rand.Intn(10) + 1 // [1,10]
		return retwis.buildTxn(txnId, rN, 0)
	} else {
		log.Fatal("Txn generation error: uncovered percentage to generate a txn")
		return nil
	}
}

func (retwis *RetwisWorkload) String() string {
	return "RetwisWorkload"
}
