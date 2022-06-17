package raft

func (rf *Raft) getLastLogIndex() int {
	return len(rf.logs) - 1 + rf.last_snapshot_index
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.logs)-1 <= 0 {
		return rf.last_snapshot_term
	}
	return rf.logs[len(rf.logs)-1].Term
}

func (rf *Raft) getLogTerm(index int) int {
	if index <= rf.last_snapshot_index {
		return rf.last_snapshot_term
	}
	return rf.logs[index-rf.last_snapshot_index].Term
}
