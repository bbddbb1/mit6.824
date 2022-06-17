package raft

type AppendEntriesArgs struct {
	Term         int          //leader’s term
	Leaderid     int          //so follower can redirect clients
	PrevLogIndex int          //index of log entry immediately precedingnew ones
	PrevLogTerm  int          //term of prevLogIndex entry
	Entries      []LogEntries //log entries to store (empty for heartbeat;may send more than one for efficiency)
	LeaderCommit int          //leader’s commitIndex
}
type AppendEntriesReply struct {
	Term                    int  //currentTerm, for leader to update itself
	Success                 bool //true if follower contained entry matchingprevLogIndex and prevLogTerm
	Conflicting_entry_index int
	Conflicting_entry_term  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("id %d log %v last include index %d", rf.me, rf.logs, rf.last_snapshot_index)
	if args.Term < rf.current_Term {
		reply.Success = false
		reply.Term = rf.current_Term
		DPrintf("%d state %d term %d refuse to Ae due to leader %d term %d smaller",
			rf.me, rf.current_state, rf.current_Term, args.Leaderid, args.Term)
		return
	}
	rf.current_Term = args.Term
	reply.Term = args.Term
	rf.resetTimer()
	rf.ChangeState(Follower)
	if args.Term > rf.current_Term {
		rf.voted_for = -1
	}

	rf.persist()

	if rf.last_snapshot_index > args.PrevLogIndex {
		reply.Success = false
		reply.Conflicting_entry_index = rf.last_snapshot_index + 1
		reply.Conflicting_entry_term = 0
		DPrintf("id %d term %d last snapshot index %d while args prev log index %d smaller",
			rf.me, rf.current_Term, rf.last_snapshot_index, args.PrevLogIndex)
		return
	}
	//fmt.Println(len(args.Entries), args.PrevLogIndex)
	// if len(args.Entries) != 0 {
	// 	DPrintf("id %d term %d prev log index %d term %d receive AE "+
	// 		"with args prev_log index %d term %d from %d term %d logs len %d ",
	// 		rf.me, rf.current_Term, rf.getLastLogIndex(), rf.getLastLogTerm(),
	// 		args.PrevLogIndex, args.PrevLogTerm, args.Leaderid, args.Term, len(args.Entries))
	// }
	DPrintf("id %d term %d prev log index %d term %d receive AE "+
		"with args prev_log index %d term %d from %d term %d logs len %d ",
		rf.me, rf.current_Term, rf.getLastLogIndex(), rf.getLastLogTerm(),
		args.PrevLogIndex, args.PrevLogTerm, args.Leaderid, args.Term, len(args.Entries))

	//Reply false if log doesn’t contain an entry at prevLogIndex
	//whose term matches prevLogTerm
	if rf.getLastLogIndex() >= args.PrevLogIndex &&
		args.PrevLogTerm == rf.getLogTerm(args.PrevLogIndex) {
		//DPrintf("%d term %d state %d consistence passed", rf.me, rf.current_Term, rf.current_state)
		next_index := args.PrevLogIndex + 1
		match := true
		end := rf.getLastLogIndex()

		for i := 0; match && i < len(args.Entries); i++ {
			if end < next_index+i {
				match = false
			} else if rf.getLogTerm(next_index+i) != args.Entries[i].Term {
				match = false
			}
		}

		if !match {
			entries := make([]LogEntries, len(args.Entries))
			copy(entries, args.Entries)
			rf.logs = append(rf.logs[:next_index-rf.last_snapshot_index], entries...)
		}
		//rf.logs = append(rf.logs[:next_index], args.Entries...)

		// If leaderCommit > commitIndex, set commitIndex =
		//min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commit_index {
			last_new_entry_index := args.PrevLogIndex + len(args.Entries)
			rf.commit_index = args.LeaderCommit
			if rf.commit_index > last_new_entry_index {
				rf.commit_index = last_new_entry_index
			}
			DPrintf("Follower %d commite %d with leader committed %d last new entry %d",
				rf.me, rf.commit_index, args.LeaderCommit, last_new_entry_index)
			rf.apply_cond.Broadcast()
		}
		//fmt.Println("id", rf.me, "log len", rf.logs, "ct", rf.commit_index)
		rf.persist()
		reply.Term = rf.current_Term
		reply.Success = true
		return
	} else {
		if rf.getLastLogIndex() < args.PrevLogIndex {
			reply.Conflicting_entry_term = 0
			reply.Conflicting_entry_index = rf.getLastLogIndex() + 1
		} else {
			reply.Conflicting_entry_term = rf.getLogTerm(args.PrevLogIndex)
			reply.Conflicting_entry_index = args.PrevLogIndex
			for i := args.PrevLogIndex - 1; i >= rf.last_snapshot_index; i-- {
				if rf.getLogTerm(i) != reply.Conflicting_entry_term {
					break
				} else {
					reply.Conflicting_entry_index -= 1
				}

			}
			DPrintf("[AppendEntries]: Id %d Term %d State %v consistency check failed"+
				" with args's prevLogIndex %d args's prevLogTerm %d while it's prevLogIndex %d in"+
				" prevLogTerm %d reply conficted index %d term %d", rf.me, rf.current_Term, rf.current_state,
				args.PrevLogIndex, args.PrevLogTerm, rf.getLastLogIndex(),
				rf.getLastLogTerm(), reply.Conflicting_entry_index, reply.Conflicting_entry_term)

		}
		reply.Success = false
		reply.Term = rf.current_Term
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) getNextIndex(reply AppendEntriesReply, nextIndex int) int {
	if reply.Conflicting_entry_term == 0 {
		nextIndex = reply.Conflicting_entry_index
	} else {
		conflict_index := reply.Conflicting_entry_index
		conflict_term := rf.getLogTerm(reply.Conflicting_entry_index)
		DPrintf("[getNextIndex] conficted index %d term %d", conflict_index, conflict_term)
		if conflict_term >= reply.Conflicting_entry_term {
			for i := conflict_index; i > rf.last_snapshot_index; i-- {
				if reply.Conflicting_entry_term == rf.getLogTerm(i) {
					conflict_index = i
					break
				}
				conflict_index--
			}

			if conflict_index != rf.last_snapshot_index {
				for i := conflict_index + 1; i < nextIndex; i++ {
					if reply.Conflicting_entry_term != rf.getLogTerm(i) {
						break
					}
					conflict_index++
				}
				nextIndex = conflict_index + 1
			} else {
				nextIndex = reply.Conflicting_entry_index
			}
		} else {
			nextIndex = reply.Conflicting_entry_index
		}
	}
	return nextIndex
}
func (rf *Raft) RequestAppendEntries() {
	//DPrintf("id %d log %v last include index %d", rf.me, rf.logs, rf.last_snapshot_index)
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(i int) {
			//retry:
			rf.mu.Lock()
			if rf.current_state != Leader {
				DPrintf("leader %d state changed during handling rpc", rf.me)
				rf.mu.Unlock()
				return
			}
			if rf.next_index[i]-1 < rf.last_snapshot_index {
				DPrintf("leader %d last snapshot index %d prev index %d for %d ", rf.me, rf.last_snapshot_index,
					rf.next_index[i]-1, i)
				rf.mu.Unlock()
				go rf.leaderSendSnapshot(i)

				return
			}
			next_log_index := rf.next_index[i]
			prev_log_index := next_log_index - 1
			if prev_log_index-rf.last_snapshot_index < 0 {
				DPrintf("[AE]id %d index %d error for %d", rf.me, prev_log_index, i)
			}
			prev_log_term := rf.getLogTerm(prev_log_index)

			entries := make([]LogEntries, 0)
			if next_log_index < rf.getLastLogIndex()+1 {
				entries = rf.logs[next_log_index-rf.last_snapshot_index:]
			}

			args := AppendEntriesArgs{
				Term:         rf.current_Term,
				Leaderid:     rf.me,
				PrevLogIndex: prev_log_index,
				PrevLogTerm:  prev_log_term,
				Entries:      entries,
				LeaderCommit: rf.commit_index,
			}

			var reply AppendEntriesReply
			// if len(args.Entries) != 0 {
			// 	DPrintf("leader %d term %d current log %v and prev include index %d AE log %v to id %d"+
			// 		" with prev_log index %d term %d",
			// 		args.Leaderid, args.Term, len(rf.logs), rf.last_snapshot_index, len(args.Entries), i,
			// 		prev_log_index, prev_log_term)
			// }
			DPrintf("leader %d term %d current log %v and prev include index %d AE log %v to id %d"+
				" with prev_log index %d term %d",
				args.Leaderid, args.Term, len(rf.logs), rf.last_snapshot_index, len(args.Entries), i,
				prev_log_index, prev_log_term)

			rf.mu.Unlock()
			ok := rf.sendAppendEntries(i, &args, &reply)
			rf.mu.Lock()
			if !ok {
				// DPrintf("leader %d Term %d cannot concat %d", rf.me,
				// 	rf.current_Term, i)
				rf.mu.Unlock()
				return
			}

			if rf.current_state != Leader {
				DPrintf("leader %d state changed during handling rpc", rf.me)
				rf.mu.Unlock()
				return
			}
			if !reply.Success {
				DPrintf("%d term %d failed to Ae to %d term %d with conficted index %d term %d",
					rf.me, rf.current_Term, i, reply.Term, reply.Conflicting_entry_index, reply.Conflicting_entry_term)

				if rf.current_Term < reply.Term {
					rf.current_Term = reply.Term
					DPrintf("leader %d Term %d convert to Term %d", rf.me, rf.current_Term, reply.Term)
					//rf.resetTimer()
					rf.ChangeState(Follower)
					rf.voted_for = -1
					rf.persist()
					rf.mu.Unlock()
					return
				} else {
					rf.next_index[i] = rf.getNextIndex(reply, next_log_index)
					// next_log_index := reply.Conflicting_entry_index
					DPrintf("leader %d term %d find new next index %d for %d ",
						rf.me, rf.current_Term, rf.next_index[i], i)
					rf.mu.Unlock()
					//DPrintf("retry")
					//goto retry
				}
			} else {
				DPrintf("%d succeed to Ae to %d", rf.me, i)

				rf.match_ndex[i] = args.PrevLogIndex + len(args.Entries)
				rf.next_index[i] = rf.match_ndex[i] + 1
				for i := args.PrevLogIndex + len(args.Entries); i > rf.last_snapshot_index; i-- {
					success_append := 1
					for j := 0; j < len(rf.peers); j++ {
						if j == rf.me {
							continue
						}
						if rf.match_ndex[j] >= i {
							success_append += 1
						}
					}
					if success_append >= len(rf.peers)/2+1 && rf.getLogTerm(i) == rf.current_Term &&
						rf.commit_index < i {
						rf.commit_index = i
						DPrintf("%d committed %d self log %d prevss %d(succeed to AE %d/%d)",
							rf.me, rf.commit_index, len(rf.logs), rf.last_snapshot_index,
							success_append, len(rf.peers))
						rf.apply_cond.Broadcast()
						//go rf.HandleHeratBeat()
						break
					}
				}
				// if len(args.Entries) != 0 &&
				// 	success_append >= len(rf.peers)/2+1 && rf.current_state == Leader {
				// 	if rf.getLastLogTerm() == rf.current_Term && rf.commit_index < rf.getLastLogIndex() {
				// 		rf.commit_index = rf.getLastLogIndex()
				// 		DPrintf("%d committed %d (succeed to AE %d/%d)",
				// 			rf.me, rf.commit_index, success_append, len(rf.peers))
				// 		rf.mu.Unlock()
				// 		go rf.HandleHeratBeat()
				// 		rf.apply_cond.Broadcast()
				// 		return
				// 	}
				// 	// DPrintf("%d term %d last committed index %d failed to comitted %d  term %d",
				// 	// 	rf.me, rf.current_Term, rf.commit_index, index, rf.logs[index].Term)
				// }
				rf.mu.Unlock()
				return
			}
		}(server)
	}
}
