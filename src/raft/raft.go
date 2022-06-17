package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"

	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"

	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
const (
	Follower = iota
	Candidate
	Leader
)

type LogEntries struct {
	Term    int         //收到指令时的任期号
	Command interface{} //指令
}
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a RacurrentTerm int

	current_Term int // 服务器最后一次知道的任期号（初始化为 0，持续递增）
	voted_for    int // 在当前获得选票的候选人的 Id
	logs         []LogEntries
	// 日志条目集；每一个条目包含一个用户状态机执行的指令，和收到时的任期号

	// Volatile state on all servers

	commit_index int // 已知的最大的已经被提交的日志条目的索引值
	last_applied int // 最后被应用到状态机的日志条目索引值（初始化为 0，持续递增）

	// Volatile state on leaders

	next_index []int // 对于每一个服务器，需要发送给他的下一个日志条目的索引值
	match_ndex []int // 对于每一个服务器，已经复制给他的日志的最高索引值

	votes_counts int // 记录此次投票中获取的票数 2A

	current_state        int // 0:follower, 1:Candidate, 2:leader
	election_timeout     int
	heartbeatPeriod      int
	last_heartbeat_sent  int64 //
	last_heartbeat_heard int64

	applyCh chan ApplyMsg //

	chan_election_timeout chan bool
	chan_heartbeat        chan bool

	last_snapshot_index int
	last_snapshot_term  int

	apply_cond     *sync.Cond
	leader_cond    *sync.Cond
	nonleader_cond *sync.Cond
}

func (rf *Raft) RaftStateSize() int {
	return rf.persister.RaftStateSize()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var Term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Term = rf.current_Term
	isleader = rf.current_state == Leader

	return Term, isleader
}

//
// example RequestVote RPC handler.
//
// func (rf *Raft) HandleVoteReply(reply *RequestVoteReply) {
// }

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	Term := -1
	isLeader := true
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2B).
	isLeader = rf.current_state == Leader
	if !isLeader {
		return index, Term, isLeader
	}
	Term = rf.current_Term

	log := LogEntries{Term: rf.current_Term, Command: command}
	rf.logs = append(rf.logs, log)
	index = rf.getLastLogIndex()
	rf.last_heartbeat_sent = time.Now().UnixNano()
	rf.persist()
	DPrintf("leader %d get command %v index %d", rf.me, command, index)

	go rf.RequestAppendEntries()
	return index, Term, isLeader
}

func (rf *Raft) ChangeState(new_state int) {
	old_state := rf.current_state
	rf.current_state = new_state
	if old_state == Leader && new_state == Follower {
		rf.nonleader_cond.Broadcast()
		DPrintf("%d become Follower", rf.me)
	} else if old_state == Candidate && new_state == Leader {
		rf.leader_cond.Broadcast()
	}
}
func (rf *Raft) HandleHeratBeat() {
	rf.mu.Lock()
	rf.last_heartbeat_sent = time.Now().UnixNano()
	rf.mu.Unlock()
	rf.RequestAppendEntries()

}

func (rf *Raft) resetTimer() {
	rand.Seed(time.Now().UnixNano() + int64(rf.me))
	rf.election_timeout = rf.heartbeatPeriod + rand.Intn(500) + 100
	rf.last_heartbeat_heard = time.Now().UnixNano()
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// func (rf *Raft) eventloop() {
// 	for !rf.killed() {
// 		select {
// 		case <-rf.chan_election_timeout:
// 			go rf.HandleTimeout()
// 		case <-rf.chan_heartbeat:
// 			go rf.HandleHeratBeat()
// 		}
// 	}
// }
func (rf *Raft) ApplyEntries() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.commit_index <= rf.last_applied {
			rf.apply_cond.Wait()
			rf.mu.Unlock()
		} else {
			rf.last_applied++
			if rf.last_applied-rf.last_snapshot_index < 0 {
				DPrintf("id %d last applied %d last snapshot index %d", rf.me, rf.last_applied,
					rf.last_snapshot_index)
			}
			apply := ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.last_applied-rf.last_snapshot_index].Command,
				CommandIndex: rf.last_applied,
			}

			rf.mu.Unlock()
			rf.applyCh <- apply
			//DPrintf("id %d apply index %d commit_index %d", rf.me, rf.last_applied, rf.commit_index)
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		if rf.current_state == Leader {
			rf.nonleader_cond.Wait()
			rf.mu.Unlock()
		} else {
			t := time.Now().UnixNano() - rf.last_heartbeat_heard
			if int(t/int64(time.Millisecond)) >= rf.election_timeout {
				//rf.chan_election_timeout <- true
				go rf.HandleTimeout()
			}
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}
func (rf *Raft) tickerHeartbeat() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.current_state != Leader {
			rf.leader_cond.Wait()
			rf.mu.Unlock()
		} else {
			t := time.Now().UnixNano() - rf.last_heartbeat_sent
			if int(t/int64(time.Millisecond)) >= rf.heartbeatPeriod {
				//	rf.chan_heartbeat <- true
				go rf.HandleHeratBeat()
			}
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.current_Term = 0
	rf.voted_for = -1
	rf.votes_counts = 0
	rf.dead = 0
	rf.logs = make([]LogEntries, 0)
	rf.logs = append(rf.logs, LogEntries{Term: 0})

	rf.chan_election_timeout = make(chan bool)
	rf.chan_heartbeat = make(chan bool)

	rf.commit_index = 0
	rf.last_applied = 0

	rf.next_index = make([]int, len(peers))
	rf.match_ndex = make([]int, len(peers))

	rf.apply_cond = sync.NewCond(&rf.mu)
	rf.leader_cond = sync.NewCond(&rf.mu)
	rf.nonleader_cond = sync.NewCond(&rf.mu)

	rf.current_state = Follower
	rf.applyCh = applyCh

	rf.last_snapshot_index = 0
	rf.last_snapshot_term = 0

	// 为了降低出现选举失败的概率，选举超时时间随机在[200,400]ms
	//rand.Seed(time.Now().UnixNano())
	rf.heartbeatPeriod = 150

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.resetTimer()
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.tickerHeartbeat()
	//go rf.eventloop()
	go rf.ApplyEntries()
	return rf
}
