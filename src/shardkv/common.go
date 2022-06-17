package shardkv

import (
	"time"

	"6.824/shardctrler"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ApplyTimeout   = time.Duration(100000000)
	ErrTimeout     = "ErrTimeout"
	ErrRequest     = "ErrRequest"
	Serving        = "Serving"
	Pulling        = "Pulling"
	Bepulling      = "Bepulling"
	CongifChange   = "CongifChange"
	Gc             = "Gc"
	Migration      = "Migration"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
type CommandArgs struct {
	Key   string
	Value string
	Op    string
	Shard int

	Clientid  int64
	Commandid int

	Config shardctrler.Config

	DataBase    map[int]map[string]string
	GcShard     []int
	Lastcommand map[int64]LastCommand
}
type CommandReply struct {
	Err   Err
	Value string
}
type PullShardDataArgs struct {
	Shard     []int
	ConfigNum int
}
type PullShardDatareply struct {
	Err           Err
	MigrationData map[int]map[string]string
	Shard         []int
	ConfigNum     int
	Lastcommand   map[int64]LastCommand
}
type GcArgs struct {
	Data2del []int
	CfgNum   int
}
type GcReply struct {
	Err Err
}
