package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Description string
	Args        interface{}
}
type Result struct {
	Args  interface{}
	Reply interface{}
}
type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	database         map[string]string
	chanResponsibity map[int]chan Result //index->chan
	ack              map[int64]int       //clerkId->queryId
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	index, _, isLeader := kv.rf.Start(Op{Description: "Get", Args: *args})
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	kv.mu.Lock()
	if _, ok := kv.chanResponsibity[index]; !ok {
		kv.chanResponsibity[index] = make(chan Result, 1)
	}
	chanMsg := kv.chanResponsibity[index]
	kv.mu.Unlock()
	select {
	case result := <-chanMsg:
		if resArgs, ok := result.Args.(GetArgs); !ok {
			reply.WrongLeader = true
		} else {
			if resArgs.ClerkId == args.ClerkId && resArgs.QueryId == args.QueryId {
				*reply = result.Reply.(GetReply)
				reply.WrongLeader = false
			} else {
				reply.WrongLeader = true
			}
		}
		DPrintf("%d reply Get to %d with queryid %d, reply.WrongLeader: %d reply.Value: %s", kv.me, args.ClerkId, args.QueryId, reply.WrongLeader, reply.Value)
	case <-time.After(1000 * time.Millisecond):
		reply.WrongLeader = true
	}

}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	index, _, isLeader := kv.rf.Start(Op{Description: "PutAppend", Args: *args})
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	kv.mu.Lock()
	if _, ok := kv.chanResponsibity[index]; !ok {
		kv.chanResponsibity[index] = make(chan Result, 1)
	}
	chanMsg := kv.chanResponsibity[index]
	kv.mu.Unlock()
	select {
	case result := <-chanMsg:
		if resArgs, ok := result.Args.(PutAppendArgs); !ok {
			reply.WrongLeader = true
		} else {
			if resArgs.ClerkId == args.ClerkId && resArgs.QueryId == args.QueryId {
				reply.WrongLeader = false
				reply.Err = result.Reply.(PutAppendReply).Err
			} else {
				reply.WrongLeader = true
			}
		}
	case <-time.After(1000 * time.Millisecond):
		reply.WrongLeader = true
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	gob.Register(PutAppendArgs{})
	gob.Register(PutAppendReply{})
	gob.Register(GetArgs{})
	gob.Register(GetReply{})
	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.ack = make(map[int64]int)
	kv.database = make(map[string]string)
	kv.chanResponsibity = make(map[int]chan Result)
	go kv.Update()
	DPrintf("KV Server %d started", kv.me)
	return kv
}

func (kv *RaftKV) Update() {
	for true {
		msg := <-kv.applyCh
		operation := msg.Command.(Op)

		if operation.Description == "Get" {
			var reply GetReply
			args := operation.Args.(GetArgs)
			kv.mu.Lock()
			if id, ok := kv.ack[args.ClerkId]; !ok || id < args.QueryId {
				kv.ack[args.ClerkId] = args.QueryId
			}
			if value, ok := kv.database[operation.Args.(GetArgs).Key]; ok {
				reply.Value = value
				reply.Err = OK
			} else {
				reply.Err = ErrNoKey
			}
			kv.mu.Unlock()
			DPrintf("%d apply Get from %d with queryId %d", kv.me, args.ClerkId, args.QueryId)
			kv.sendReply(msg.Index, Result{Reply: reply, Args: args})
		} else if operation.Description == "PutAppend" {
			var reply PutAppendReply
			var isDuplicate bool
			args := operation.Args.(PutAppendArgs)
			kv.mu.Lock()
			if id, ok := kv.ack[args.ClerkId]; ok && id >= args.QueryId {
				isDuplicate = true
			} else {
				isDuplicate = false
				kv.ack[args.ClerkId] = args.QueryId
			}
			if !isDuplicate {
				if operation.Args.(PutAppendArgs).Op == "Put" {
					kv.database[args.Key] = args.Value
				} else {
					kv.database[args.Key] += args.Value
				}
			}
			DPrintf("%d apply PutAppend from %d with queryId %d, Value: %s KV ack=%d isDuplicate %d", kv.me, args.ClerkId, args.QueryId, kv.database[args.Key], kv.ack[args.ClerkId], isDuplicate)
			reply.Err = OK
			kv.mu.Unlock()
			kv.sendReply(msg.Index, Result{Reply: reply, Args: args})
		}
	}

}

func (kv *RaftKV) sendReply(index int, reply Result) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.chanResponsibity[index]; !ok {
		kv.chanResponsibity[index] = make(chan Result, 1)
	} else {
		select {
		case <-kv.chanResponsibity[index]:
		default:
		}
	}
	kv.chanResponsibity[index] <- reply
}
