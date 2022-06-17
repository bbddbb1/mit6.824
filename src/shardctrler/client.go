package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
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
	// Your code here.
	ck.clientid = nrand()
	ck.commandid = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &CommandArgs{}
	// Your code here.
	args.Num = num
	args.Op = Query
	args.Clientid, args.Commandid = ck.clientid, ck.commandid
	for {
		// try each known server.

		for _, srv := range ck.servers {
			var reply CommandReply
			ok := srv.Call("ShardCtrler.Command", args, &reply)
			if ok && !reply.WrongLeader {
				ck.commandid++
				//DPrint("client id %d reply group %v", args.Clientid, reply.Config.Groups)
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &CommandArgs{}
	// Your code here.
	args.Servers = servers
	args.Op = Join
	args.Clientid, args.Commandid = ck.clientid, ck.commandid
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply CommandReply
			ok := srv.Call("ShardCtrler.Command", args, &reply)
			if ok && !reply.WrongLeader {
				ck.commandid++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &CommandArgs{}
	// Your code here.
	args.GIDs = gids
	args.Op = Leave
	args.Clientid, args.Commandid = ck.clientid, ck.commandid
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply CommandReply
			ok := srv.Call("ShardCtrler.Command", args, &reply)
			if ok && !reply.WrongLeader {
				ck.commandid++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &CommandArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.Op = Move
	args.Clientid, args.Commandid = ck.clientid, ck.commandid
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply CommandReply
			ok := srv.Call("ShardCtrler.Command", args, &reply)
			if ok && !reply.WrongLeader {
				ck.commandid++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
