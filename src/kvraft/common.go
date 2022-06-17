package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Commandid int
	Clientid  int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Commandid int
	Clientid  int64
}

type GetReply struct {
	Err   Err
	Value string
}

type CommandArgs struct {
	Key       string
	Value     string
	Clientid  int64
	Op        string
	Commandid int
}
type CommandReply struct {
	Err   Err
	Value string
}
