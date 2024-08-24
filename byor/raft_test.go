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
