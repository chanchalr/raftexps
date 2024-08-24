package byor

import (
	"log"
	"sync"
	"testing"
	"time"
)

func init() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
}

type Harness struct {
	mu          sync.Mutex
	cluster     []Server
	connected   []bool
	n           int
	t           *testing.T
	commitChans []chan CommitEntry
	commits     [][]CommitEntry
}

func NewHarness(t *testing.T, n int) *Harness {
	ns := make([]Server, n)
	connected := make([]bool, n)
	ready := make(chan interface{})
	commitChans := make([]chan CommitEntry, n)
	commits := make([][]CommitEntry, n)
	for i := 0; i < n; i++ {
		peerIds := make([]int, 0)
		for p := 0; p < n; p++ {
			if p != i {
				peerIds = append(peerIds, p)
			}
		}
		commitChans[i] = make(chan CommitEntry)
		ns[i] = NewRPCServer(i, peerIds, ready, commitChans[i])
		ns[i].Serve()
	}
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				ns[i].ConnectToPeer(j, ns[j].GetListenAddr())
			}
			connected[i] = true
		}
	}
	//close(ready)
	ready <- "sample"
	h := &Harness{
		cluster:     ns,
		connected:   connected,
		n:           n,
		t:           t,
		commitChans: commitChans,
		commits:     commits,
	}
	for i := 0; i < n; i++ {
		go h.collectCommits(i)
	}
	return h
}

func (h *Harness) collectCommits(i int) {
	for c := range h.commitChans[i] {
		h.mu.Lock()
		tlog("collectCommits(%d) got +%v", i, c)
		h.commits[i] = append(h.commits[i], c)
		h.mu.Unlock()
	}
}

func (h *Harness) CheckCommitted(cmd int) (nc int, index int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	commitsLen := -1
	for i := 0; i < h.n; i++ {
		if h.connected[i] {
			if commitsLen >= 0 {
				if len(h.commits[i]) != commitsLen {
					h.t.Fatalf("commits[%d] = %v, want %v", i, len(h.commits[i]), commitsLen)
				}
			} else {
				commitsLen = len(h.commits[i])
			}
		}
	}
	for c := 0; c < commitsLen; c++ {
		cmdAtC := -1
		for i := 0; i < h.n; i++ {
			if h.connected[i] {
				tlog("peerID:%d, and commitIndex: %d, commands h.commits[i]:%+v,commandAtC:%v", i, c, h.commits[i], cmdAtC)
				cmdOfN := h.commits[i][c].Command.(int)
				if cmdAtC >= 0 {
					if cmdOfN != cmdAtC {
						h.t.Errorf("got %d, want %d at h.commits[%d][%d]", cmdOfN, cmdAtC, i, c)
					}
				} else {
					cmdAtC = cmdOfN
				}
			}
		}
		if cmdAtC == cmd {
			index := -1
			nc := 0
			for i := 0; i < h.n; i++ {
				if h.connected[i] {
					if index >= 0 && h.commits[i][c].Index != index {
						h.t.Errorf("got Index=%d want %d at h.commits[%d][%d]", h.commits[i][c].Index, index, i, c)
					} else {
						index = h.commits[i][c].Index
					}
					nc++
				}
			}
			return nc, index
		}
	}
	h.t.Errorf("cmd=%d not found in commits", cmd)
	return -1, -1
}

func (h *Harness) CheckCommittedN(cmd int, n int) {
	nc, _ := h.CheckCommitted(cmd)
	if nc != n {
		h.t.Errorf("CheckCommmittedN got nc=%d want %d", nc, n)
	}
}

func (h *Harness) CheckNotCommitted(cmd int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := 0; i < h.n; i++ {
		if h.connected[i] {
			for c := 0; c < len(h.commits[i]); c++ {
				gotCmd := h.commits[i][c].Command.(int)
				if gotCmd == cmd {
					h.t.Errorf("Found %d at commits [%d][%d], expected none", cmd, i, c)
				}
			}
		}
	}
}

func (h *Harness) SubmitToServer(serverId int, cmd interface{}) bool {
	return h.cluster[serverId].Submit(cmd)
}

func (h *Harness) Shutdown() {
	for i := 0; i < h.n; i++ {
		h.cluster[i].DisconnectAll()
		h.connected[i] = false
	}
	for i := 0; i < h.n; i++ {
		h.cluster[i].Shutdown()
	}
}

func (h *Harness) DisconnectPeer(id int) {
	tlog("Disconnect %d", id)
	h.cluster[id].DisconnectAll()
	for j := 0; j < h.n; j++ {
		if j != id {
			h.cluster[j].DisconnectPeer(id)
		}
	}
	h.connected[id] = false
}

func (h *Harness) ReconnectPeer(id int) {
	tlog("Reconnect %d", id)
	for j := 0; j < h.n; j++ {
		if j != id {
			if err := h.cluster[id].ConnectToPeer(j, h.cluster[j].GetListenAddr()); err != nil {
				h.t.Fatal(err)
			}
			if err := h.cluster[j].ConnectToPeer(id, h.cluster[id].GetListenAddr()); err != nil {
				h.t.Fatal(err)
			}
		}
	}
	h.connected[id] = true
}

func (h *Harness) CheckSingleLeader() (int, int) {
	for r := 0; r < 5; r++ {
		leaderId := -1
		leaderTerm := -1
		for i := 0; i < h.n; i++ {
			if h.connected[i] {
				_, term, isLeader := h.cluster[i].Report()
				if isLeader {
					if leaderId < 0 {
						leaderId = i
						leaderTerm = term
					} else {
						h.t.Fatalf("multiple leaders: %d and %d", leaderId, i)
					}
				}
			}
		}
		if leaderId >= 0 {
			return leaderId, leaderTerm
		}
		time.Sleep(150 * time.Millisecond)
	}
	h.t.Fatalf("leader not found")
	return -1, -1
}

func (h *Harness) CheckNoLeader() {
	for i := 0; i < h.n; i++ {
		if h.connected[i] {
			_, _, isLeader := h.cluster[i].Report()
			if isLeader {
				h.t.Fatalf("Server %d leader;want none", i)
			}
		}
	}
}

func tlog(format string, a ...interface{}) {
	format = "[TEST] " + format
	log.Printf(format, a...)
}

func sleepMs(n int) {
	time.Sleep(time.Duration(n) * time.Millisecond)
}
