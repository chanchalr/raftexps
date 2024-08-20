package byor

type ConsensusModule interface {
	Report() (id int, term int, isLeader bool)
	Start(ready <-chan interface{})
	Stop()
	RequestVote(args interface{}, reply interface{}) error
	AppendEntries(args interface{}, reply interface{}) error
}
