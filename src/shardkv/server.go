package shardkv

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
	"6.824/shardctrler"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	f, err := os.OpenFile("log.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
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

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	*CommandArgs
}
type LastCommand struct {
	Commandid int

	Reply *CommandReply
}
type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	wait_del      chan []map[string]string
	mck           *shardctrler.Clerk
	DataBase      []map[string]string
	lastApplied   int
	notify_ch     map[int]chan CommandReply
	lastcommand   map[int64]LastCommand
	dead          int32 // set by Kill()
	lastconfig    shardctrler.Config
	currentconfig shardctrler.Config
	state         []string
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}
func (kv *ShardKV) Command(args *CommandArgs, reply *CommandReply) {
	kv.mu.Lock()
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	config := kv.currentconfig
	if config.Shards[key2shard(args.Key)] != kv.gid {
		DPrintf("key %s gid %d != kv gid%d", args.Key, config.Shards[key2shard(args.Key)], kv.gid)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if kv.state[key2shard(args.Key)] != Serving {
		reply.Err = ErrWrongGroup
		DPrintf("%d-%d shard %d is %s", kv.gid, kv.me, key2shard(args.Key), kv.state[key2shard(args.Key)])
		kv.mu.Unlock()
		return
	}
	if args.Op != "Get" && kv.DuplicateDetecte(args.Clientid, args.Commandid) {
		R := kv.lastcommand[args.Clientid].Reply
		reply.Err = R.Err
		DPrintf("%d-%d detected duplication from client %d command %d\n%v",
			kv.gid, kv.me, args.Clientid, args.Commandid, kv.lastcommand)
		kv.mu.Unlock()
		return
	}
	index, _, _ := kv.rf.Start(Op{CommandArgs: args})
	ch := kv.getnotifychannel(index)
	kv.mu.Unlock()
	select {
	case R := <-ch:
		reply.Err, reply.Value = R.Err, R.Value
		var value string
		if args.Op == "Get" {
			value = reply.Value
		} else {
			value = args.Value
		}
		kv.mu.Lock()
		DPrintf("[%s]%d-%d index %d key %s value %s", args.Op, kv.gid, kv.me, args.Commandid,
			args.Key, value)
		kv.mu.Unlock()
	case <-time.After(ApplyTimeout):
		reply.Err = ErrTimeout
	}
	go func() {
		kv.mu.Lock()
		delete(kv.notify_ch, index)
		kv.mu.Unlock()
	}()
}
func (kv *ShardKV) DuplicateDetecte(clientid int64, commandid int) bool {
	lastcommand, ok := kv.lastcommand[clientid]
	return ok && int(lastcommand.Commandid) >= commandid
}
func (kv *ShardKV) applier() {
	for m := range kv.applyCh {
		if kv.killed() {
			break
		}
		//DPrintf("[applier] id %d apply index %d", kv.me, m.CommandIndex)
		if m.CommandValid {
			kv.mu.Lock()
			var reply CommandReply
			command := m.Command.(Op)
			// config := kv.mck.Query(-1)
			// if config.Shards[key2shard(command.Key)] != kv.gid {
			// 	DPrintf("[ap]key gid %d != kv gid%d", config.Shards[key2shard(command.Key)], kv.gid)
			// 	reply.Err = ErrWrongGroup
			// 	kv.mu.Unlock()
			// 	continue
			// }
			if m.CommandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = m.CommandIndex
			if command.Op != CongifChange && command.Op != Migration && command.Op != Gc && kv.state[key2shard(command.Key)] != Serving {
				reply.Err = ErrWrongGroup
				if _, isLeader := kv.rf.GetState(); isLeader {
					ch := kv.getnotifychannel(m.CommandIndex)
					ch <- reply
					DPrintf("%d-%d refuse op %s", kv.gid, kv.me, command.Op)
				}
				kv.mu.Unlock()
				continue
			}

			if (command.Op == "Append" || command.Op == "Put") &&
				kv.DuplicateDetecte(command.Clientid, command.Commandid) {
				R := kv.lastcommand[command.Clientid].Reply
				reply.Err = R.Err
			} else {
				if command.Op == "Put" {
					// DPrintf("%d-%d index %d Put value %s to key %s", kv.gid, kv.me, command.Commandid,
					// 	command.Value, command.Key)
					kv.DataBase[command.Shard][command.Key] = command.Value
					reply.Err = OK
				} else if command.Op == "Append" {
					// DPrintf("%d-%d index %d Append value %s to key %s", kv.gid, kv.me, command.Commandid,
					// 	command.Value, command.Key)
					kv.DataBase[command.Shard][command.Key] += command.Value
					//DPrintf("%d-%d current value %s", kv.gid, kv.me, kv.DataBase[command.Shard][command.Key])
					reply.Err = OK
				} else if command.Op == "Get" {
					value, ok := kv.DataBase[command.Shard][command.Key]
					// DPrintf("%d-%d index %d Get value %s with key %s", kv.gid, kv.me, command.Commandid,
					// 	value, command.Key)
					reply.Value, reply.Err = value, OK
					if !ok {
						reply.Err = ErrNoKey
					}

				} else if command.Op == Migration {
					//DPrintf("ap%v", command.DataBase)
					if command.DataBase == nil {
						for i := 0; i < 10; i++ {
							kv.state[i] = Serving
						}
					}
					for shard, k_v := range command.DataBase {
						if k_v == nil {
							continue
						}
						for key, value := range k_v {
							kv.DataBase[shard][key] = value

						}
						kv.state[shard] = Serving
					}
					for client, cmd := range command.Lastcommand {
						if lastcommand, ok := kv.lastcommand[client]; !ok || lastcommand.Commandid < cmd.Commandid {
							cmd_copy := LastCommand{Reply: cmd.Reply, Commandid: cmd.Commandid}
							kv.lastcommand[client] = cmd_copy
						}
					}
					reply.Err = OK
				} else if command.Op == Gc {
					for _, shard := range command.GcShard {
						kv.DataBase[shard] = make(map[string]string)
						kv.state[shard] = Serving
					}
					reply.Err = OK
				} else if command.Op == CongifChange {
					if command.Config.Num == kv.currentconfig.Num+1 {
						kv.lastconfig = kv.currentconfig
						kv.currentconfig = command.Config
						for shard, gid := range kv.currentconfig.Shards {
							gid_ := kv.lastconfig.Shards[shard]
							if gid == kv.gid && gid_ != kv.gid {
								kv.state[shard] = Pulling
							}
							if gid != kv.gid && gid_ == kv.gid {
								kv.state[shard] = Gc
							}

						}
					}
				}
				if command.Op == "Append" || command.Op == "Put" {
					kv.lastcommand[command.Clientid] = LastCommand{command.Commandid, &reply}
					DPrintf("%d-%d update lastcommand for %d from %d to %d", kv.gid, kv.me, command.Clientid,
						kv.lastcommand[command.Clientid].Commandid, command.Commandid)
				}

			}
			if _, isLeader := kv.rf.GetState(); isLeader && command.Op != Gc && command.Op != Migration && command.Op != CongifChange {
				ch := kv.getnotifychannel(m.CommandIndex)
				ch <- reply
			}
			if kv.maxraftstate != -1 && kv.rf.RaftStateSize() >= kv.maxraftstate {
				// DPrintf("[TakeSnapshot]%d-%d RaftStateSize %d commandindex %d",
				// 	kv.gid, kv.me, kv.rf.RaftStateSize(),
				// 	m.CommandIndex)
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.DataBase)
				e.Encode(kv.lastcommand)
				e.Encode(kv.lastApplied)
				e.Encode(kv.lastconfig)
				e.Encode(kv.currentconfig)
				e.Encode(kv.state)
				kv.rf.Snapshot(m.CommandIndex, w.Bytes())
			}
			kv.mu.Unlock()
		} else if m.SnapshotValid {
			kv.mu.Lock()
			if kv.rf.CondInstallSnapshot(m.SnapshotTerm, m.SnapshotIndex, m.Snapshot) {
				DPrintf("%d-%d recieve installsnapshot", kv.gid, kv.me)
				kv.readPersist(m.Snapshot)
				// DPrintf("[InstallSnapshot]%d-%d snapshot index %d",
				// 	kv.gid, kv.me, m.SnapshotIndex)
				kv.lastApplied = m.SnapshotIndex
			}
			kv.mu.Unlock()

		} else {
			DPrintf("Command type error")
		}
	}
}
func (kv *ShardKV) readPersist(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var DataBase []map[string]string
	var lastcommand map[int64]LastCommand
	var lastApplied int
	var lastconfig shardctrler.Config
	var currentconfig shardctrler.Config
	var state []string
	if d.Decode(&DataBase) != nil ||
		d.Decode(&lastcommand) != nil ||
		d.Decode(&lastApplied) != nil ||
		d.Decode(&lastconfig) != nil ||
		d.Decode(&currentconfig) != nil ||
		d.Decode(&state) != nil {
		log.Fatalln("Decode err")
	}
	kv.DataBase = DataBase
	kv.lastcommand = lastcommand
	kv.lastApplied = lastApplied
	kv.lastconfig = lastconfig
	kv.currentconfig = currentconfig
	kv.state = state
	DPrintf("[InstallSnapshot]%d-%d lastcommand%v", kv.gid, kv.me, lastcommand)
}
func (kv *ShardKV) getnotifychannel(index int) chan CommandReply {
	if _, ok := kv.notify_ch[index]; !ok {
		kv.notify_ch[index] = make(chan CommandReply, 1)
	}
	return kv.notify_ch[index]
}
func (kv *ShardKV) detecteConfigChage() {
	for !kv.killed() {
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); !isLeader {
			kv.mu.Unlock()
			time.Sleep(500 * time.Millisecond)
			continue
		}
		ischanging := false
		for _, state := range kv.state {
			if state != Serving {
				ischanging = true
				break
			}
		}
		if ischanging {
			DPrintf("%d-%d refuse to pull config while\n %v", kv.gid, kv.me, kv.state)
			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}

		config := kv.mck.Query(kv.currentconfig.Num + 1)
		if config.Num == kv.currentconfig.Num+1 {
			DPrintf("%d-%d detectied config change %d -> %d\n%v -> \n %v",
				kv.gid, kv.me, kv.currentconfig.Num, config.Num, kv.currentconfig.Shards, config.Shards)
			args := CommandArgs{Op: CongifChange, Config: config}
			kv.rf.Start(Op{&args})
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}
func (kv *ShardKV) detecteMigration() {
	for !kv.killed() {
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); !isLeader {
			kv.mu.Unlock()
			time.Sleep(500 * time.Millisecond)
			continue
		}
		migration_shard := make(map[int][]int)
		for shard := range kv.currentconfig.Shards {
			gid_ := kv.lastconfig.Shards[shard]
			if kv.state[shard] == Pulling {
				migration_shard[gid_] = append(migration_shard[gid_], shard)
			}
		}
		var wg sync.WaitGroup
		kv.mu.Unlock()
		for gid, shards := range migration_shard {
			if gid == 0 {
				args := CommandArgs{Op: Migration}
				kv.rf.Start(Op{&args})
				continue
			}
			wg.Add(1)
			go func(gid int, shards []int, group map[int][]string) {
				defer wg.Done()
				kv.mu.Lock()
				args := PullShardDataArgs{Shard: shards, ConfigNum: kv.currentconfig.Num}
				kv.mu.Unlock()
				if servers, ok := group[gid]; ok {
					for si := 0; si < len(servers); si++ {
						srv := kv.make_end(servers[si])
						var reply PullShardDatareply
						DPrintf("%d-%d try to pull shards %v from %d-%d", kv.gid, kv.me, shards, gid, si)
						ok := srv.Call("ShardKV.PullShardData", &args, &reply)
						// ... not ok, or ErrWrongLeader
						if ok && reply.Err == OK {
							args := CommandArgs{Op: Migration, DataBase: reply.MigrationData, Lastcommand: reply.Lastcommand}
							DPrintf("%d-%d Pull shard %v from %d-%d", kv.gid, kv.me, shards, gid, si)
							//DPrintf("%v", reply.MigrationData)
							kv.rf.Start(Op{&args})
							return
						}
					}
				}

			}(gid, shards, kv.lastconfig.Groups)
			wg.Wait()
		}
		time.Sleep(200 * time.Millisecond)
	}
}
func (kv *ShardKV) PullShardData(args *PullShardDataArgs, reply *PullShardDatareply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.ConfigNum = kv.lastconfig.Num
	reply.Shard = args.Shard
	reply.MigrationData = make(map[int]map[string]string)
	reply.Lastcommand = make(map[int64]LastCommand)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if args.ConfigNum > kv.currentconfig.Num {
		reply.Err = "NotReady"
		DPrintf("%d-%d[pull] config Num %d while args Num %d", kv.gid, kv.me, kv.currentconfig.Num, args.ConfigNum)
		return
	}
	for _, shard := range args.Shard {
		if kv.DataBase[shard] == nil {
			reply.Err = ErrNoKey
		} else {
			reply.MigrationData[shard] = make(map[string]string)
			//reply.MigrationData[shard] = deepcopy_mapn(kv.DataBase[shard])
			for k, v := range kv.DataBase[shard] {
				reply.MigrationData[shard][k] = v
			}
		}
	}
	for client, command := range kv.lastcommand {
		command_copy := LastCommand{Commandid: command.Commandid, Reply: command.Reply}
		reply.Lastcommand[client] = command_copy
	}
	DPrintf("%d-%d Push shard %v", kv.gid, kv.me, args.Shard)
	reply.Err = OK
}
func (kv *ShardKV) detecteGc() {
	for !kv.killed() {
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); !isLeader {
			kv.mu.Unlock()
			time.Sleep(500 * time.Millisecond)
			continue
		}
		migration_shard := make(map[int][]int)
		for shard, gid := range kv.currentconfig.Shards {
			if kv.state[shard] == Gc {
				migration_shard[gid] = append(migration_shard[gid], shard)
			}
		}
		var wg sync.WaitGroup
		kv.mu.Unlock()
		for gid, shards := range migration_shard {
			if gid == 0 {
				continue
			}
			wg.Add(1)
			go func(gid int, shards []int, group map[int][]string) {
				defer wg.Done()
				kv.mu.Lock()
				args := GcArgs{Data2del: shards, CfgNum: kv.currentconfig.Num}
				kv.mu.Unlock()
				if servers, ok := group[gid]; ok {
					for si := 0; si < len(servers); si++ {
						srv := kv.make_end(servers[si])
						var reply GcReply

						ok := srv.Call("ShardKV.RequestGc", &args, &reply)
						if ok && reply.Err == OK {
							args := CommandArgs{Op: Gc, GcShard: shards}
							DPrintf("%d-%d GC shard %v", kv.gid, kv.me, shards)
							//DPrintf("%v", reply.MigrationData)
							kv.rf.Start(Op{&args})
							return
						}
					}
				}

			}(gid, shards, kv.currentconfig.Groups)
			wg.Wait()
		}
		time.Sleep(200 * time.Millisecond)
	}
}
func (kv *ShardKV) RequestGc(args *GcArgs, reply *GcReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if kv.currentconfig.Num > args.CfgNum {
		reply.Err = OK
		DPrintf("[Gc]%d-%d config Num %d while args Num %d", kv.gid, kv.me, kv.currentconfig.Num, args.CfgNum)
		return
	}
	if kv.currentconfig.Num < args.CfgNum {
		reply.Err = ErrRequest
		DPrintf("[Gc]%d-%d config Num %d while args Num %d", kv.gid, kv.me, kv.currentconfig.Num, args.CfgNum)
		return
	}
	for _, shard := range args.Data2del {
		if kv.state[shard] != Serving {
			return
		}
	}
	reply.Err = OK
}
func deepcopy_mapn(mapn map[string]string) map[string]string {
	mapn_copy := make(map[string]string)
	for key, value := range mapn {
		mapn_copy[key] = value
	}
	return mapn_copy
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.DataBase = make([]map[string]string, 10)
	for i := 0; i < 10; i++ {
		kv.DataBase[i] = make(map[string]string)
	}
	kv.notify_ch = make(map[int]chan CommandReply)
	kv.lastcommand = make(map[int64]LastCommand)
	kv.dead = 0
	kv.lastApplied = 0
	kv.wait_del = make(chan []map[string]string)
	kv.state = make([]string, 10)
	for i := 0; i < 10; i++ {
		kv.state[i] = Serving
	}
	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.readPersist(persister.ReadSnapshot())
	go kv.applier()
	go kv.detecteConfigChage()
	go kv.detecteMigration()
	go kv.detecteGc()
	return kv
}
