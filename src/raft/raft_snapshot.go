package raft

type InstallSnapshotargs struct {
	Term              int //leader’s term
	LeaderId          int //so follower can redirect clients
	LastIncludedIndex int /*the snapshot replaces all entries up through
	and including this index*/
	LastIncludedTerm int // term of lastIncludedIndex
	//offset //byte offset where chunk is positioned in the snapshot file

	Data []byte //raw bytes of the snapshot chunk, starting at offset
	//done bool //if this is the last chunk
}
type InstallSnapshotreply struct {
	Term int //currentTerm, for leader to update itself
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	// if lastIncludedIndex <= rf.commit_index {
	// 	DPrintf("CondInstallSnapshot refused\n")
	// 	return false
	// }

	// defer func() {
	// 	rf.last_snapshot_index = lastIncludedIndex
	// 	rf.last_snapshot_term = lastIncludedTerm
	// 	rf.commit_index = lastIncludedIndex //IMPORTANT
	// 	rf.last_applied = lastIncludedIndex //IMPORTANT
	// 	rf.persister.SaveStateAndSnapshot(rf.persistdata(), snapshot)

	// }()
	// if lastIncludedIndex <= rf.getLastLogIndex() && rf.getLogTerm(lastIncludedIndex) == lastIncludedTerm {
	// 	temp := []LogEntries{}
	// 	rf.logs = append(temp, rf.logs[lastIncludedIndex-rf.last_snapshot_index:]...)
	// 	return true
	// }

	// //discard the entire log
	// rf.logs = make([]LogEntries, 0)
	// rf.logs = append(rf.logs, LogEntries{})
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.last_snapshot_index >= index || index > rf.commit_index {
		DPrintf("id %d have already snapshot", rf.me)
		return
	}

	temp_logs := make([]LogEntries, 0)
	temp_logs = append(temp_logs, LogEntries{})
	temp_logs = append(temp_logs, rf.logs[index+1-rf.last_snapshot_index:]...)

	rf.last_snapshot_term = rf.getLogTerm(index)
	rf.last_snapshot_index = index
	rf.logs = temp_logs
	if index > rf.commit_index {
		rf.commit_index = index
	}
	if index > rf.last_applied {
		rf.last_applied = index
	}
	DPrintf("[Snapshot]id %d last include index %d term %d", rf.me, index, rf.last_snapshot_term)
	rf.persister.SaveStateAndSnapshot(rf.persistdata(), snapshot)

}
func (rf *Raft) InstallSnapshot(args *InstallSnapshotargs, reply *InstallSnapshotreply) {

	rf.mu.Lock()
	if rf.current_Term > args.Term {
		reply.Term = rf.current_Term
		DPrintf("[InstallSnapshot] id %d term %d while arg term %d", rf.me, rf.current_Term, args.Term)
		rf.mu.Unlock()
		return
	}
	rf.resetTimer()
	rf.ChangeState(Follower)
	if rf.current_Term < args.Term {
		rf.voted_for = -1
	}
	rf.current_Term = args.Term
	rf.persist()
	reply.Term = rf.current_Term
	if rf.last_snapshot_index >= args.LastIncludedIndex {
		DPrintf("[InstallSnapshot] id %d term %d last snapshot index %d >= %d(arg)", rf.me, rf.current_Term,
			rf.last_snapshot_index, args.LastIncludedIndex)
		rf.mu.Unlock()
		return
	}
	DPrintf("[InstallSnapshot] id %d term %d last include %drecieve args last include index %d term %d from %d",
		rf.me, rf.current_Term, rf.last_snapshot_index, args.LastIncludedIndex, args.LastIncludedTerm, args.LeaderId)

	if args.LastIncludedIndex <= rf.getLastLogIndex() &&
		rf.getLogTerm(args.LastIncludedIndex) == args.LastIncludedIndex {
		temp := []LogEntries{}
		rf.logs = append(temp, rf.logs[args.LastIncludedIndex-rf.last_snapshot_index:]...)
	}
	rf.last_snapshot_index = args.LastIncludedIndex
	rf.last_snapshot_term = args.LastIncludedTerm
	rf.commit_index = args.LastIncludedIndex //IMPORTANT
	rf.last_applied = args.LastIncludedIndex //IMPORTANT
	//discard the entire log
	rf.logs = make([]LogEntries, 0)
	rf.logs = append(rf.logs, LogEntries{})
	rf.persister.SaveStateAndSnapshot(rf.persistdata(), args.Data)
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	rf.mu.Unlock()
	rf.applyCh <- msg

}
func (rf *Raft) sendSnapshot(server int, args *InstallSnapshotargs, reply *InstallSnapshotreply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
func (rf *Raft) leaderSendSnapshot(server int) {
	rf.mu.Lock()
	args := &InstallSnapshotargs{
		Term:              rf.current_Term,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.last_snapshot_index,
		LastIncludedTerm:  rf.last_snapshot_term,
		Data:              rf.persister.ReadSnapshot(),
	}
	if rf.current_state != Leader {
		rf.mu.Unlock()
		return
	}
	DPrintf("leader %d term %d send installsnapshot (last include index %d term %d) to"+
		"%d ", rf.me, rf.current_Term, args.LastIncludedIndex, args.LastIncludedTerm, server)
	rf.mu.Unlock()
	reply := &InstallSnapshotreply{}
	ok := rf.sendSnapshot(server, args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	if rf.current_state != Leader {
		rf.mu.Unlock()
		return
	}
	if reply.Term > rf.current_Term {
		//rf.resetTimer()
		rf.ChangeState(Follower)
		rf.voted_for = -1
		rf.persist()
		rf.mu.Unlock()
		return
	}
	rf.next_index[server] = args.LastIncludedIndex + 1
	rf.match_ndex[server] = args.LastIncludedIndex
	DPrintf("leader %d recieve installsnapshot reply from %d get next index %d",
		rf.me, server, rf.next_index[server])
	rf.mu.Unlock()
}
