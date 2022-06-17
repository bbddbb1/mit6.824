package shardctrler

import (
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug1 = false

func DPrint(format string, a ...interface{}) (n int, err error) {
	// f, err := os.OpenFile("log.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	// if err != nil {
	// 	return
	// }
	// defer func() {
	// 	f.Close()
	// }()

	// log.SetOutput(f)

	if Debug1 {
		log.Printf(format, a...)
	}
	return
}

type LastCommand struct {
	Commandid int
	Reply     *CommandReply
}
type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	// Your data here.
	lastApplied int
	notify_ch   map[int]chan CommandReply
	configs     []Config // indexed by config num
	lastcommand map[int64]LastCommand
}

type Op struct {
	// Your data here.
	*CommandArgs
}

func (sc *ShardCtrler) getnotifychannel(index int) chan CommandReply {
	if _, ok := sc.notify_ch[index]; !ok {
		sc.notify_ch[index] = make(chan CommandReply, 1)
	}
	return sc.notify_ch[index]
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
}
func (sc *ShardCtrler) DuplicateDetecte(clientid int64, commandid int) bool {
	lastcommand, ok := sc.lastcommand[clientid]
	return ok && int(lastcommand.Commandid) >= commandid
}
func (sc *ShardCtrler) Command(args *CommandArgs, reply *CommandReply) {
	sc.mu.Lock()
	if args.Op != Query && sc.DuplicateDetecte(args.Clientid, args.Commandid) {
		R := sc.lastcommand[args.Clientid].Reply
		reply.Err = R.Err
		sc.mu.Unlock()
		return
	}
	index, _, isLeader := sc.rf.Start(Op{args})
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	ch := sc.getnotifychannel(index)
	sc.mu.Unlock()
	select {
	case R := <-ch:
		reply.Err = R.Err
		reply.Config = R.Config
	case <-time.After(ApplyTimeout):
		reply.Err = ErrTimeout
	}
	go func() {
		sc.mu.Lock()
		delete(sc.notify_ch, index)
		sc.mu.Unlock()
	}()
}
func (sc *ShardCtrler) applier() {
	for m := range sc.applyCh {
		if sc.killed() {
			break
		}
		if m.CommandValid {
			sc.mu.Lock()
			if sc.lastApplied >= m.CommandIndex {
				sc.mu.Unlock()
				continue
			}
			sc.lastApplied = m.CommandIndex
			command := m.Command.(Op)
			var reply CommandReply
			if command.Op != Query && sc.DuplicateDetecte(command.Clientid, command.Commandid) {
				R := sc.lastcommand[command.Clientid].Reply
				reply.Err = R.Err
			} else {
				if command.Op == Join {
					DPrint("Join serve %v", command.Servers)
					var config Config
					config_last := sc.configs[len(sc.configs)-1]
					config.Num = len(sc.configs)
					groups := deepcopy_map(config_last.Groups)
					for key, value := range command.Servers {
						groups[key] = value
					}
					config.Groups = groups

					config.Shards = config_last.Shards
					gid_2_shard := make(map[int][]int)
					for gid := range config.Groups {
						gid_2_shard[gid] = make([]int, 0)
					}
					for shard, gid := range config_last.Shards {
						gid_2_shard[gid] = append(gid_2_shard[gid], shard)
					}
					for {
						max_len, gid_max, min_len, gid_min := sc.getMaxMinShardLen(&gid_2_shard)
						if gid_max != 0 && max_len-min_len <= 1 {
							break
						}
						config.Shards[gid_2_shard[gid_max][0]] = gid_min
						gid_2_shard[gid_min] = append(gid_2_shard[gid_min], gid_2_shard[gid_max][0])
						gid_2_shard[gid_max] = gid_2_shard[gid_max][1:]
					}
					DPrint("Num %d after join shard %v\n count %v", config.Num, config.Shards, gid_2_shard)
					sc.configs = append(sc.configs, config)
					reply.Err = OK
				} else if command.Op == Move {
					var config Config
					DPrint("move shard %d to gid %d", command.Shard, command.GID)
					config_last := sc.configs[len(sc.configs)-1]
					config.Num = len(sc.configs)
					config.Shards = config_last.Shards
					config.Shards[command.Shard] = command.GID
					config.Groups = deepcopy_map(config_last.Groups)
					sc.configs = append(sc.configs, config)
					reply.Err = OK
				} else if command.Op == Query {
					config_len := len(sc.configs)
					DPrint("Query Num %d config len %d", command.Num, config_len)

					if command.Num == -1 || command.Num > config_len-1 {
						reply.Config = sc.configs[config_len-1]

					} else {
						reply.Config = sc.configs[command.Num]
					}
					DPrint("client id %d op %v reply num %d group %v", command.Clientid,
						command.Op, reply.Config.Num, reply.Config.Groups)

					reply.Err = OK
				} else if command.Op == Leave {
					DPrint("Leave Gids %v", command.GIDs)
					var config Config
					config_last := sc.configs[len(sc.configs)-1]
					config.Num = len(sc.configs)
					groups := deepcopy_map(config_last.Groups)
					gid_2_shard := make(map[int][]int)
					for _, gid := range command.GIDs {
						delete(groups, gid)
					}

					config.Groups = groups
					config.Shards = config_last.Shards
					iso_shard := make([]int, 0)
					for gid := range config.Groups {
						gid_2_shard[gid] = make([]int, 0)
					}
					for shard, gid := range config_last.Shards {
						if _, ok := groups[gid]; ok {
							gid_2_shard[gid] = append(gid_2_shard[gid], shard)
						} else {
							iso_shard = append(iso_shard, shard)
						}

					}
					if len(groups) != 0 {
						for _, shard := range iso_shard {
							_, _, _, gid_min := sc.getMaxMinShardLen(&gid_2_shard)
							gid_2_shard[gid_min] = append(gid_2_shard[gid_min], shard)

						}
						for gid, shards := range gid_2_shard {
							for _, shard := range shards {
								config.Shards[shard] = gid
							}
						}
					} else {
						var newShards [NShards]int
						config.Shards = newShards
					}

					DPrint("Num %d after leave shards %v\n count %v", config.Num, config.Shards, gid_2_shard)
					sc.configs = append(sc.configs, config)
					reply.Err = OK
				}
				if command.Op != Query {
					sc.lastcommand[command.Clientid] = LastCommand{command.Commandid, &reply}
				}

			}
			if _, isLeader := sc.rf.GetState(); isLeader {
				ch := sc.getnotifychannel(m.CommandIndex)
				ch <- reply
			}
			sc.mu.Unlock()
		}
	}
}
func (sc *ShardCtrler) getMaxMinShardLen(gid_2_shard *map[int][]int) (int, int, int, int) {
	max_len := 0
	min_len := NShards + 1
	gid_max := 0
	gid_min := 0
	var keys []int
	for key := range *gid_2_shard {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	for _, gid := range keys {
		shard_len := len((*gid_2_shard)[gid])
		if shard_len > max_len {
			max_len, gid_max = shard_len, gid
		}
		if gid != 0 && shard_len < min_len {
			min_len, gid_min = shard_len, gid
		}
	}
	if shard, ok := (*gid_2_shard)[0]; ok && len(shard) != 0 {
		gid_max = 0
		max_len = len(shard)
	}
	return max_len, gid_max, min_len, gid_min
}

func deepcopy_map(mapn map[int][]string) map[int][]string {
	mapn_copy := make(map[int][]string)
	for key, value := range mapn {
		mapn_copy[key] = value
	}
	return mapn_copy
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.dead = 0
	sc.lastcommand = make(map[int64]LastCommand)
	sc.notify_ch = make(map[int]chan CommandReply)
	go sc.applier()
	return sc
}
