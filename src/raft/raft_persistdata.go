package raft

import (
	"bytes"
	"log"

	"6.824/labgob"
)

func (rf *Raft) persistdata() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.current_Term)
	e.Encode(rf.voted_for)
	e.Encode(rf.logs)
	e.Encode(rf.last_snapshot_index)
	e.Encode(rf.last_snapshot_term)
	data := w.Bytes()
	return data
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	data := rf.persistdata()
	rf.persister.SaveRaftState(data)

}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var current_Term int
	var voted_for int
	var logs []LogEntries
	var last_snapshot_index int
	var last_snapshot_term int
	if d.Decode(&current_Term) != nil ||
		d.Decode(&voted_for) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&last_snapshot_index) != nil ||
		d.Decode(&last_snapshot_term) != nil {
		log.Fatalln("Decode err")
	} else {
		rf.current_Term = current_Term
		rf.voted_for = voted_for
		rf.logs = logs
		rf.last_snapshot_index = last_snapshot_index
		rf.last_snapshot_term = last_snapshot_term
		rf.last_applied = last_snapshot_index
		rf.commit_index = last_snapshot_index
	}
	DPrintf("[readPersist]: Id %d Term %d State %d log len %d last sanpshot index %d restore persistent state from Persister\n",
		rf.me, rf.current_Term, rf.current_state, len(rf.logs), rf.last_snapshot_index)
}
