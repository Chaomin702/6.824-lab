package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu       sync.Mutex
	clerkId  int64
	queryId  int
	leaderId int
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
	ck.clerkId = nrand()
	ck.queryId = 0
	ck.leaderId = 0
	DPrintf("Clerk %d ready", ck.clerkId)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ret := ""
	args := GetArgs{Key: key, ClerkId: ck.clerkId, QueryId: ck.queryId}
	DPrintf("Clerk %d Get, queryId: %d, Key: %s LeaderId: %d", ck.clerkId, ck.queryId, key, ck.leaderId)
	ck.queryId++
	for i := ck.leaderId; true; i = (i + 1) % len(ck.servers) {
		reply := GetReply{WrongLeader: false}
		if ck.servers[i].Call("RaftKV.Get", &args, &reply) {
			if !reply.WrongLeader {
				ck.leaderId = i
				if reply.Err == OK {
					ret = reply.Value
					break
				} else if reply.Err == ErrNoKey {
					ret = ""
					break
				}
			}
		}
	}
	DPrintf("%d received Get reply with %d leaderId: %d, Value: %s", ck.clerkId, args.QueryId, ck.leaderId, ret)
	return ret
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := PutAppendArgs{Key: key, Value: value, Op: op, QueryId: ck.queryId, ClerkId: ck.clerkId}
	DPrintf("Clerk %d PutAppend op: %s, queryId: %d, Key: %s Value %s LeaderId: %d", ck.clerkId, op, ck.queryId, key, value, ck.leaderId)
	ck.queryId++
	for i := ck.leaderId; true; i = (i + 1) % len(ck.servers) {
		reply := PutAppendReply{WrongLeader: false}
		if ck.servers[i].Call("RaftKV.PutAppend", &args, &reply) {
			if !reply.WrongLeader && reply.Err == OK {
				DPrintf("%d received PutAppend reply with %d , LeaderId: %d", ck.clerkId, args.QueryId, ck.leaderId)
				ck.leaderId = i
				break
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
