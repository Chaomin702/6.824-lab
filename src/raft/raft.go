package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"

import "time"
import "math/rand"
import "fmt"

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// state
const (
	FOLLOWER  = 1
	CANDIDATE = 2
	LEADER    = 3
)

// Debugging enabled?
const debugEnabled = false

// DPrintf will only print if the debugEnabled const has been set to true
func debug(a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Println(a...)
	}
	return
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	voteGranted []int
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	state       int
	log         []Entry
	//electionTimeCh chan time.Time
	voteForCh     chan int //vote request chan
	appendEntryCh chan int //appendEntry request chan
}

func (rf *Raft) String() string {
	return fmt.Sprintf("me=%v,term=%v,votedFor=%v,commitIndex=%v,state=%v -----", rf.me, rf.currentTerm, rf.votedFor, rf.commitIndex, rf.state)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here.
	return rf.currentTerm, rf.state == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

func (req RequestVoteArgs) String() string {
	return fmt.Sprintf("\nterm=%v,CandidateID=%v,LastLogIndex=%v,LastLogTerm=%v\n", req.Term, req.CandidateID, req.LastLogIndex, req.LastLogTerm)
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.

	debug(args, rf, rf.me, " get requestVote from", args.CandidateID)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	if rf.votedFor < 0 || rf.votedFor == args.CandidateID {
		if args.LastLogTerm >= rf.log[rf.commitIndex].Term && args.LastLogIndex >= rf.commitIndex {
			rf.votedFor = args.CandidateID
			reply.VoteGranted = true
			rf.voteForCh <- 1
		} else {
			reply.VoteGranted = false
		}
	} else {
		reply.VoteGranted = false
	}
	debug(rf, "success? ", reply.VoteGranted)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	debug(rf, rf.me, " send RequestVote to ", server, " term=", args.Term)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.state == FOLLOWER {
			return ok
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.appendEntryCh <- 1
		} else if reply.VoteGranted {
			rf.voteGranted = append(rf.voteGranted, server)
			if rf.state != LEADER && len(rf.voteGranted) > len(rf.peers)/2 {
				debug(rf, rf.me, " be a leader!")
				rf.state = LEADER
				rf.appendEntryCh <- 1
			}
		}
	} else {
		debug("sendRequestVote failed, server ", server, " may be crashed")
	}
	return ok
}

type Entry struct {
	Term    int
	Command interface{}
}
type AppendEntriesArgs struct {
	Term         int //leader's term
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int //leader's commitIndex
}

func (args AppendEntriesArgs) String() string {
	return fmt.Sprintf("\nterm=%v,LeaderID=%v,PrevLogIndex=%v,PrevLogTerm=%v,LeaderCommit=%v\n", args.Term, args.LeaderID, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
}

type AppendEntriesReply struct {
	Term    int //currentTerm, for leader to update itself
	Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	//	debug("Append Entries:\n", rf, args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		debug("AE: args.term ", args.LeaderID, " ", args.Term, "<", "rf.term ", rf.me, " ", rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		debug("AE: args.term ", args.LeaderID, " ", args.Term, ">", "rf.term ", rf.me, " ", rf.currentTerm)
		rf.currentTerm = args.Term
	}
	reply.Term = rf.currentTerm
	if len(rf.log) <= args.PrevLogIndex {
		debug("fuck1 ", len(rf.log), " ", args.PrevLogIndex)
		reply.Success = false
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		debug("fuck2 ", rf.log[args.PrevLogIndex].Term, " ", args.PrevLogTerm)
		rf.log = rf.log[:args.PrevLogIndex]
		reply.Success = false
	} else {
		reply.Success = true
		if args.Entries != nil {
			rf.log = append(rf.log, args.Entries...)
		}
	}
	//	debug("After Append Entries:\n", rf, args, reply.Term, " ", reply.Success)
	if rf.state != FOLLOWER {
		rf.state = FOLLOWER
	}
	rf.appendEntryCh <- 1
}
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.state != LEADER {
			debug(rf, rf.me, " not be a leader")
			return ok
		}
		if reply.Term > rf.currentTerm {
			debug("SAE: rf.term ", rf.me, " ", rf.currentTerm, "<", "server.term ", server, " ", reply.Term)
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.appendEntryCh <- 1
		} else if reply.Success {
			if args.Entries != nil {
				rf.nextIndex[server] += len(args.Entries)
			}
		} else {
			debug("sendAppendEntries success ", reply.Success, "sender ", rf, "receiver")
			rf.nextIndex[server]--
		}
	} else {
		debug("sendAppendEntries failed, ", rf.me, "->", server, " may be crashed")
	}
	return ok
}
func (rf *Raft) sendHeartbeats(server int) bool {
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderID = rf.me
	args.PrevLogIndex = rf.nextIndex[server] - 1
	if args.PrevLogIndex >= len(rf.log) || args.PrevLogIndex < 0 {
		debug("maybe an error", rf, "nextindex ", args.PrevLogIndex, "len ", len(rf.log))
	}
	args.PrevLogTerm = rf.log[rf.nextIndex[server]-1].Term

	args.Entries = nil
	args.LeaderCommit = rf.commitIndex
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, &reply)
	//	debug("Heartbeats:  term=", reply.Term, "success? ", reply.Success)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	/*	_, ok := rf.GetState()
		if ok {
			go func(){
				rf.mu.Lock()
				rf.log = append(rf.log,{rf.currentTerm,command})
				rf.mu.Unlock()

				args := AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderID = rf.me
				args.E
			}()
		} else {
			isLeader = false
		}*/
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here.
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votedFor = -1
	rf.state = FOLLOWER
	rf.log = make([]Entry, 1)
	rf.voteForCh = make(chan int)
	rf.appendEntryCh = make(chan int)
	rf.log[0] = Entry{0, -1}
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go func() {
		for {
			switch rf.state {
			case FOLLOWER:
				select {
				case <-rf.voteForCh:
					debug(rf, rf.me, " vote for ", rf.votedFor, " term=", rf.currentTerm)
				case <-rf.appendEntryCh:
					//debug(rf, " get appendEntry term=", rf.currentTerm)
				case <-time.After(time.Duration(rand.Int63n(150)+150) * time.Millisecond):
					debug(rf, rf.me, " time out term = ", rf.currentTerm)
					rf.state = CANDIDATE
				}
			case CANDIDATE:
				rf.mu.Lock()
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.voteGranted = rf.voteGranted[:0]
				rf.voteGranted = append(rf.voteGranted, rf.me)
				rf.mu.Unlock()
				go func() {
					args := RequestVoteArgs{}
					args.Term = rf.currentTerm
					args.CandidateID = rf.me
					args.LastLogIndex = rf.commitIndex
					args.LastLogTerm = rf.log[rf.commitIndex].Term
					for i := range rf.peers {
						if i != rf.me {
							reply := RequestVoteReply{}
							go rf.sendRequestVote(i, args, &reply)
						}
					}
				}()
				select {
				case <-rf.appendEntryCh:
					debug(rf.me, " candidate encounter appendEntryCh")
				case <-rf.voteForCh:
					debug(rf.me, " candidate encouter a higer term")
					//be a leader
				case <-time.After(time.Duration(rand.Int63n(150)+150) * time.Millisecond):
					debug("timeout reelection")
				}
			case LEADER:
				select {
				case <-rf.appendEntryCh:
					debug(rf.me, " leader loss")
				case <-rf.voteForCh:
					debug("leader encounter a higher candidate~~~")
				case <-time.After(time.Millisecond * 50):
					//heartbeats
					for i := range rf.peers {
						if i != rf.me {
							go rf.sendHeartbeats(i)
						}
					}
				}
			}
		}
	}()
	return rf
}
