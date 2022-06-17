package raft

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //候选人的任期号
	CandidateId  int // 请求选票的候选人ID
	LastLogIndex int // 候选人的最后日志条目的索引值
	LastLogTerm  int // 候选人的最后日志条目的任期号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前任期号,便于返回后更新自己的任期号
	VoteGranted bool // 候选人赢得了此张选票时为真
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	vote := true

	if rf.getLastLogTerm() > args.LastLogTerm ||
		(rf.getLastLogTerm() == args.LastLogTerm &&
			rf.getLastLogIndex() > args.LastLogIndex) {
		vote = false
	}

	if rf.current_Term > args.Term {
		reply.Term = rf.current_Term
		reply.VoteGranted = false
	} else {
		if rf.current_Term < args.Term {
			rf.current_Term = args.Term
			rf.voted_for = -1
			rf.ChangeState(Follower)
			rf.persist()
			//rf.resetTimer()
		}
		if vote && (rf.voted_for == -1 || rf.voted_for == args.CandidateId) {
			rf.voted_for = args.CandidateId
			rf.ChangeState(Follower)
			reply.Term = rf.current_Term
			reply.VoteGranted = true
			rf.persist()
			rf.resetTimer()
		}
	}
}
func (rf *Raft) HandleTimeout() {
	rf.mu.Lock()
	rf.ChangeState(Candidate)
	rf.resetTimer()
	rf.current_Term += 1
	rf.voted_for = rf.me
	rf.persist()
	rf.votes_counts = 1
	DPrintf("%d state %d start election term %d last log index %d term %d",
		rf.me, rf.current_state, rf.current_Term, rf.getLastLogIndex(), rf.getLastLogTerm())
	rf.mu.Unlock()
	for serve := range rf.peers {
		if serve == rf.me {
			continue
		}
		go func(s int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term:         rf.current_Term,
				CandidateId:  rf.me,
				LastLogIndex: rf.getLastLogIndex(),
				LastLogTerm:  rf.getLastLogTerm(),
			}
			reply := RequestVoteReply{}
			rf.mu.Unlock()
			ok := rf.sendRequestVote(s, &args, &reply)
			rf.mu.Lock()
			if rf.current_Term != args.Term || rf.current_state != Candidate {
				//DPrintf("state %d id %d state changed during handling rpc", rf.current_state, rf.me)
				rf.mu.Unlock()
				return
			}
			if ok {
				if !reply.VoteGranted {
					if rf.current_Term < reply.Term {
						rf.current_Term = reply.Term
						rf.voted_for = -1
						rf.ChangeState(Follower)
						rf.persist()
					}
				} else if reply.VoteGranted && rf.current_state == Candidate {
					rf.votes_counts += 1
					if rf.votes_counts >= len(rf.peers)/2+1 {
						rf.ChangeState(Leader)
						DPrintf("%d become leader get %d/%d at term %d", rf.me,
							rf.votes_counts, len(rf.peers), rf.current_Term)
						for i := 0; i < len(rf.peers); i++ {
							rf.next_index[i] = rf.getLastLogIndex() + 1
							rf.match_ndex[i] = 0
						}
						go rf.HandleHeratBeat()
						rf.persist()
					}

				}
			}
			rf.mu.Unlock()
		}(serve)
	}
	//rf.resetTimer()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
