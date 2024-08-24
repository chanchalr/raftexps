package byor

import (
	"testing"
)

func TestElectionBasic(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()
}

func TestCommitOneCommand(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	origLeaderId, _ := h.CheckSingleLeader()
	isLeader := h.SubmitToServer(origLeaderId, 42)
	if !isLeader {
		t.Errorf("want id=%d leader, but its not", origLeaderId)
	}
	sleepMs(150)
	h.CheckCommittedN(42, 3)

}
func TestElectionLeaderDisconnect(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, origTerm := h.CheckSingleLeader()

	h.DisconnectPeer(origLeaderId)
	sleepMs(350)

	newLeaderId, newTerm := h.CheckSingleLeader()
	if newLeaderId == origLeaderId {
		t.Errorf("want new leader to be different from orig leader")
	}
	if newTerm <= origTerm {
		t.Errorf("want newTerm <= origTerm, got %d and %d", newTerm, origTerm)
	}
}
