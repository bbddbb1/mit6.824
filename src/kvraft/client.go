package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderid  int
	commandid int
	clientid  int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderid = 0
	ck.clientid = nrand()
	ck.commandid = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	return ck.Command(CommandArgs{Key: key, Op: "Get"})
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
}
func (ck *Clerk) Command(Args CommandArgs) string {
	for {
		var reply CommandReply
		Args.Clientid, Args.Commandid = ck.clientid, ck.commandid
		leaderid := ck.leaderid
		ok := ck.servers[ck.leaderid].Call("KVServer.Command", &Args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderid = (ck.leaderid + 1) % len(ck.servers)
			continue
		}
		if Args.Op == "Get" {
			DPrintf("[client leaderid %d]id %d Op %s command id %d key %s value %d",
				leaderid, Args.Clientid, Args.Op, ck.commandid, Args.Key, len(reply.Value))
		} else {
			DPrintf("[client leaderid %d]id %d Op %s command id %d key %s value %d",
				leaderid, Args.Clientid, Args.Op, ck.commandid, Args.Key, len(Args.Value))
		}

		ck.commandid++
		return reply.Value
	}
}
func (ck *Clerk) Put(key string, value string) {
	// ck.PutAppend(key, value, "Put")
	ck.Command(CommandArgs{Key: key, Value: value, Op: "Put"})

}
func (ck *Clerk) Append(key string, value string) {
	// ck.PutAppend(key, value, "Append")
	ck.Command(CommandArgs{Key: key, Value: value, Op: "Append"})
}
