package byor

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

const DebugCM = 1

type LogEntry struct {
	Command interface{}
	Term    int
}
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLongTerm int
}
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type GPCMState int

const (
	GPFollower GPCMState = iota
	GPCandidate
	GPLeader
	GPDead
)

func (s GPCMState) String() string {
	switch s {
	case GPFollower:
		return "Follower"
	case GPCandidate:
		return "Candidate"
	case GPLeader:
		return "Leader"
	case GPDead:
		return "Dead"
	default:
		return "Unknown"
	}
}

type GPConsensusModule struct {
	mu                 sync.Mutex
	id                 int
	peerIds            []int
	server             Server
	currentTerm        int
	votedFor           int
	log                []LogEntry
	state              GPCMState
	electionResetEvent time.Time
}

/*
	Report() (id int, term int, isLeader bool)
	Start(chan<- interface{})
	Stop()
	RequestVote(args interface{}, reply interface{}) error
	AppendEntries(args interface{}, reply interface{}) error

*/

func NewGPConsensusModule(id int, peerIds []int, server Server) *GPConsensusModule {
	return &GPConsensusModule{
		id:          id,
		peerIds:     peerIds,
		server:      server,
		currentTerm: 0,
		votedFor:    -1,
		log:         make([]LogEntry, 1),
		state:       GPFollower,
	}
}

func (cm *GPConsensusModule) RequestVote(argsInt interface{}, replyInt interface{}) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	args := argsInt.(RequestVoteArgs)
	reply := replyInt.(*RequestVoteReply)
	if cm.state == GPDead {
		return nil
	}
	cm.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d]", args, cm.currentTerm, cm.votedFor)
	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}
	if cm.currentTerm == args.Term &&
		(cm.votedFor == -1 || cm.votedFor == args.CandidateId) {
		reply.VoteGranted = true
		cm.votedFor = args.CandidateId
		cm.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = cm.currentTerm
	cm.dlog("...RequestVote reply: %+v", reply)
	return nil
}

func (cm *GPConsensusModule) AppendEntries(argsInt interface{}, replyInt interface{}) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	args := argsInt.(AppendEntriesArgs)
	reply := replyInt.(*AppendEntriesReply)

	if cm.state == GPDead {
		return nil
	}
	cm.dlog("AppendEntries: ni=%d, args=%+v [currentTerm=%d, log=%v]", 0, args, cm.currentTerm, cm.log)
	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in AppendEntries")
		cm.becomeFollower(args.Term)
	}
	reply.Success = false
	if args.Term == cm.currentTerm {
		if cm.state != GPFollower {
			cm.becomeFollower(args.Term)
		}
		cm.electionResetEvent = time.Now()
		reply.Success = true
	}
	reply.Term = cm.currentTerm
	cm.dlog("...AppendEntries reply: %+v", reply)
	return nil
}

func (cm *GPConsensusModule) dlog(format string, args ...interface{}) {
	if DebugCM > 0 {
		f_data := fmt.Sprintf("[%d]", cm.id) + format
		log.Printf(f_data, args...)
	}
}

func (cm *GPConsensusModule) Report() (id int, term int, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.id, cm.currentTerm, cm.state == GPLeader
}

func (cm *GPConsensusModule) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.state = GPDead
	cm.dlog("becomes dead")
}

func (cm *GPConsensusModule) Start(ready <-chan interface{}) {
	go func() {
		<-ready
		cm.mu.Lock()
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()
}

func (cm *GPConsensusModule) electionTimeOut() time.Duration {
	if len(os.Getenv("RAFT_FORCE_MORE_ELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(150) * time.Millisecond
	} else {
		return time.Duration(150+rand.Intn(150)) * time.Millisecond
	}
}

func (cm *GPConsensusModule) runElectionTimer() {
	timeoutDuration := cm.electionTimeOut()
	cm.mu.Lock()
	termStarted := cm.currentTerm
	cm.mu.Unlock()
	cm.dlog("election timer started (%v) term=%d", timeoutDuration, termStarted)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C
		cm.mu.Lock()
		if cm.state != GPCandidate && cm.state != GPFollower {
			cm.dlog("in election timer state = %s, bailing out", cm.state)
			cm.mu.Unlock()
			return
		}
		if termStarted != cm.currentTerm {
			cm.dlog("in election timer term changed from %d to %d bailing out", termStarted, cm.currentTerm)
			cm.mu.Unlock()
			return
		}
		if elapsed := time.Since(cm.electionResetEvent); elapsed >= timeoutDuration {
			cm.startElection()
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
}

func (cm *GPConsensusModule) startElection() {
	cm.state = GPCandidate
	cm.currentTerm += 1
	savedCurrentTerm := cm.currentTerm
	cm.electionResetEvent = time.Now()
	cm.votedFor = cm.id
	cm.dlog("become candidate (currentTerm=%d) log=%v", savedCurrentTerm, cm.log)
	votesReceived := 1
	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			args := RequestVoteArgs{
				Term:        savedCurrentTerm,
				CandidateId: cm.id,
			}
			var reply RequestVoteReply
			cm.dlog("sending RequestVote to %d: %+v", peerId, args)
			err := cm.server.Call(peerId, "ConsensusModule.RequestVote", args, &reply)
			if err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				cm.dlog("received reply Request vote to %d: %+v", peerId, reply)
				if cm.state != GPCandidate {
					cm.dlog("while waiting for reply, state = %s", cm.state)
					return
				}
				if reply.Term > savedCurrentTerm {
					cm.dlog("term out of date in RequestVote reply")
					cm.becomeFollower(reply.Term)
					return
				} else if (reply.Term == savedCurrentTerm) && (reply.VoteGranted) {
					votesReceived += 1
					if votesReceived*2 > len(cm.peerIds)+1 {
						cm.dlog("wins election with %d votes", votesReceived)
						cm.startLeader()
						return
					}
				}
			}

		}(peerId)
	}
}
func (cm *GPConsensusModule) becomeFollower(term int) {
	cm.dlog("becomes follower with term %d; log=%v", term, cm.log)
	cm.state = GPFollower
	cm.currentTerm = term
	cm.votedFor = -1
	cm.electionResetEvent = time.Now()
	go cm.runElectionTimer()
}

func (cm *GPConsensusModule) startLeader() {
	cm.state = GPLeader
	cm.dlog("becomes leader; term=%d, log=%v", cm.currentTerm, cm.log)
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			cm.leaderSendHeartbeat()
			<-ticker.C
			cm.mu.Lock()
			if cm.state != GPLeader {
				cm.mu.Unlock()
				return
			}
			cm.mu.Unlock()
		}
	}()
}

func (cm *GPConsensusModule) leaderSendHeartbeat() {
	cm.mu.Lock()
	if cm.state != GPLeader {
		return
	}
	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()
	for _, peerId := range cm.peerIds {
		args := AppendEntriesArgs{
			Term:     savedCurrentTerm,
			LeaderId: cm.id,
		}
		go func(peerId int) {
			cm.dlog("sending AppendEntries to %v: ni=%d, args=%+v", peerId, 0, args)
			var reply AppendEntriesReply
			if err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				if reply.Term > savedCurrentTerm {
					cm.dlog("term out of date in heartbeat reply")
					cm.becomeFollower(reply.Term)
					return
				}
			}
		}(peerId)
	}
}
