package kvraft

import (
	"bytes"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	f, err := os.OpenFile("log1.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if err != nil {
		return
	}
	defer func() {
		f.Close()
	}()

	log.SetOutput(f)
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const ApplyTimeout = time.Duration(100000000)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	*CommandArgs
}
type LastCommand struct {
	Commandid int
	Reply     *CommandReply
}
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	DataBase    map[string]string
	lastApplied int
	notify_ch   map[int]chan CommandReply
	lastcommand map[int64]LastCommand
}

func (kv *KVServer) Command(args *CommandArgs, reply *CommandReply) {
	kv.mu.Lock()
	if args.Op != "Get" && kv.DuplicateDetecte(args.Clientid, args.Commandid) {
		R := kv.lastcommand[args.Clientid].Reply
		reply.Err = R.Err
		kv.mu.Unlock()
		return
	}
	index, _, isLeader := kv.rf.Start(Op{args})
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	ch := kv.getnotifychannel(index)
	kv.mu.Unlock()
	select {
	case R := <-ch:
		reply.Err, reply.Value = R.Err, R.Value
	case <-time.After(ApplyTimeout):
		reply.Err = ErrTimeout
	}
	go func() {
		kv.mu.Lock()
		delete(kv.notify_ch, index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

}
func (kv *KVServer) DuplicateDetecte(clientid int64, commandid int) bool {
	lastcommand, ok := kv.lastcommand[clientid]
	return ok && int(lastcommand.Commandid) >= commandid
}
func (kv *KVServer) applier() {
	for m := range kv.applyCh {
		if kv.killed() {
			break
		}
		//DPrintf("[applier] id %d apply index %d", kv.me, m.CommandIndex)
		if m.CommandValid {
			kv.mu.Lock()
			if m.CommandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = m.CommandIndex
			command := m.Command.(Op)
			var reply CommandReply
			if command.Op != "Get" && kv.DuplicateDetecte(command.Clientid, command.Commandid) {
				R := kv.lastcommand[command.Clientid].Reply
				reply.Err = R.Err
			} else {
				if command.Op == "Put" {
					kv.DataBase[command.Key] = command.Value
					reply.Err = OK
				} else if command.Op == "Append" {
					kv.DataBase[command.Key] += command.Value
					reply.Err = OK
				} else if command.Op == "Get" {
					value, ok := kv.DataBase[command.Key]
					reply.Value, reply.Err = value, OK
					if !ok {
						reply.Err = ErrNoKey
					}

				}
				if command.Op != "Get" {
					kv.lastcommand[command.Clientid] = LastCommand{command.Commandid, &reply}
				}

			}
			if _, isLeader := kv.rf.GetState(); isLeader {
				ch := kv.getnotifychannel(m.CommandIndex)
				ch <- reply
			}
			if kv.maxraftstate != -1 && kv.rf.RaftStateSize() >= kv.maxraftstate {
				DPrintf("[TakeSnapshot]id %d RaftStateSize %d commandindex %d", kv.me, kv.rf.RaftStateSize(),
					m.CommandIndex)
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.DataBase)
				e.Encode(kv.lastcommand)
				kv.rf.Snapshot(m.CommandIndex, w.Bytes())
			}
			kv.mu.Unlock()
		} else if m.SnapshotValid {
			kv.mu.Lock()
			if kv.rf.CondInstallSnapshot(m.SnapshotTerm, m.SnapshotIndex, m.Snapshot) {
				kv.readPersist(m.Snapshot)
				DPrintf("[InstallSnapshot]id %d snapshot index %d",
					kv.me, m.SnapshotIndex)

				kv.lastApplied = m.SnapshotIndex
			}
			kv.mu.Unlock()

		} else {
			DPrintf("Command type error")
		}
	}
}
func (kv *KVServer) readPersist(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}
	DPrintf("[InstallSnapshot]id %d ", kv.me)
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var DataBase map[string]string
	var lastcommand map[int64]LastCommand
	if d.Decode(&DataBase) != nil ||
		d.Decode(&lastcommand) != nil {
		log.Fatalln("Decode err")
	}
	kv.DataBase = DataBase
	kv.lastcommand = lastcommand
}
func (kv *KVServer) getnotifychannel(index int) chan CommandReply {
	if _, ok := kv.notify_ch[index]; !ok {
		kv.notify_ch[index] = make(chan CommandReply, 1)
	}
	return kv.notify_ch[index]
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.dead = 0
	kv.lastApplied = 0
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.lastcommand = make(map[int64]LastCommand)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.DataBase = make(map[string]string)
	kv.notify_ch = make(map[int]chan CommandReply)
	// You may need initialization code here.
	kv.readPersist(persister.ReadSnapshot())
	go kv.applier()
	return kv
}
