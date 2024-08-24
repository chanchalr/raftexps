package byor

type ConsensusModule interface {
	Report() (id int, term int, isLeader bool)
	Start(ready <-chan interface{})
	Stop()
	Submit(command interface{}) bool
	RequestVote(args interface{}, reply interface{}) error
	AppendEntries(args interface{}, reply interface{}) error
}
