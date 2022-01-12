package workload

import (
	"math/rand"
	"strconv"

	log "github.com/sirupsen/logrus"
)

// Retwis workload
const RetwisDefaultAddUserRatio = 5
const RetwisDefaultFollowUnfollowRatio = 15
const RetwisDefaultPostTweetRatio = 30
const RetwisDefaultLoadTimelineRatio = 50

const ADDUSE = "addUser"
const FollowUnFollow = "followUnfollow"
const PostTweet = "postTweet"
const LoadTimeline = "loadTimeline"

// Acknowledgement: this implementation is based on TAPIR's retwis benchmark.
type RetwisWorkload struct {
	*AbstractWorkload

	addUserRatio        int
	followUnfollowRatio int
	postTweetRatio      int
	loadTimelineRatio   int
	highPriorityTxn     string
	hasHPTxn            bool
}

func NewRetwisWorkload(
	workload *AbstractWorkload,
	retwisAddUserRatio int,
	retwisFollowUnfollowRatio int,
	retwisPostTweetRatio int,
	retwisLoadTimelineRatio int,
	highPriorityTxn string,
) *RetwisWorkload {
	retwis := &RetwisWorkload{
		AbstractWorkload:    workload,
		addUserRatio:        retwisAddUserRatio,
		followUnfollowRatio: retwisFollowUnfollowRatio,
		postTweetRatio:      retwisPostTweetRatio,
		loadTimelineRatio:   retwisLoadTimelineRatio,
		highPriorityTxn:     highPriorityTxn,
		hasHPTxn:            highPriorityTxn != "",
	}

	return retwis
}

// Generates a retwis txn. This function is currently not thread-safe
func (retwis *RetwisWorkload) GenTxn() Txn {
	retwis.txnCount++
	txnId := strconv.FormatInt(retwis.txnCount, 10)

	txnType := rand.Intn(100) //[0,100)
	var txn Txn
	var txnTypeStr string
	if txnType < retwis.addUserRatio {
		// Add user txn. read 1, write 3 keys
		txn = retwis.buildTxn(txnId, 1, 3)
		txnTypeStr = ADDUSE
	} else if txnType <
		(retwis.addUserRatio + retwis.followUnfollowRatio) {
		// Follow/Unfollow txn. read 2, write 2 keys
		txn = retwis.buildTxn(txnId, 2, 2)
		txnTypeStr = FollowUnFollow
	} else if txnType <
		(retwis.addUserRatio + retwis.followUnfollowRatio + retwis.postTweetRatio) {
		// Post tweet txn. read 3, write 5 keys
		txn = retwis.buildTxn(txnId, 3, 5)
		txnTypeStr = PostTweet
	} else if txnType <
		(retwis.addUserRatio + retwis.followUnfollowRatio + retwis.postTweetRatio +
			retwis.loadTimelineRatio) {
		// Load timeline txn. read [1, 10] keys
		rN := rand.Intn(10) + 1 // [1,10]
		txn = retwis.buildTxn(txnId, rN, 0)
		txnTypeStr = LoadTimeline
	} else {
		log.Fatal("Txn generation error: uncovered percentage to generate a txn")
		return nil
	}

	txn.SetTxnType(txnTypeStr)
	if retwis.hasHPTxn {
		txn.SetPriority(txnTypeStr == retwis.highPriorityTxn)
	}
	return txn
}

func (retwis *RetwisWorkload) String() string {
	return "RetwisWorkload"
}
