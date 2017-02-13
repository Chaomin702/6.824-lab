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

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"
const ElectionTimeoutLow = 150 * time.Millisecond
const ElectionTimeoutHigh = 300 * time.Millisecond
const HeartbeatPeriod = 100 * time.Millisecond

type Role int

const (
	Follower  Role = 0
	Candidate Role = 1
	Leader    Role = 2
)

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

// log entry
type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
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

	// Persistent state on all servers
	currentTerm int
	voteFor     int
	logTable    []LogEntry
	// optimization
	leaderId int
	// Volatile state on all servers
	commitIndex int
	lastApplied int
	role        Role
	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int
	// chan for sync
	chanRole      chan Role
	chanCommitted chan ApplyMsg

	chanHeartbeat chan bool
	chanGrantVote chan bool
	chanAppend    []chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logTable)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.voteFor)
	d.Decode(&rf.logTable)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.role = Follower
		rf.chanRole <- Follower
	}
	reply.Term = args.Term

	//has vote to others
	if rf.voteFor != -1 && rf.voteFor != args.CandidateId {
		reply.VoteGranted = false
	} else if rf.logTable[len(rf.logTable)-1].Term > args.LastLogTerm { //last log term
		reply.VoteGranted = false
	} else if rf.logTable[len(rf.logTable)-1].Index > args.LastLogIndex &&
		rf.logTable[len(rf.logTable)-1].Term == args.LastLogTerm { //last index
		reply.VoteGranted = false
	} else {
		rf.voteFor = args.CandidateId
		rf.chanGrantVote <- true
		reply.VoteGranted = true
	}
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
	var ret bool
	c := make(chan bool)
	go func() {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		c <- ok
	}()
	select {
	case ret = <-c:
	case <-time.After(HeartbeatPeriod):
		ret = false
	}
	return ret
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entry        []LogEntry
	LeaderCommit int
}

func (args *AppendEntriesArgs) print() {
	DPrintf("args term=%d LeaderId=%d PrevLogIndex=%d PrevLogTerm=%d len= %d leaderCommit=%d",
		args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entry), args.LeaderCommit)
}
func (reply *AppendEntriesReply) print() {
	DPrintf("reply term=%d Success=%d NextIndex=%d", reply.Term, reply.Success, reply.NextIndex)
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	//optimized
	NextIndex int
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	var ret bool
	c := make(chan bool)
	go func() {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		c <- ok
	}()
	select {
	case ret = <-c:
	case <-time.After(HeartbeatPeriod):
		ret = false
	}
	return ret
}
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	rf.chanHeartbeat <- true

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.role = Follower
		rf.leaderId = args.LeaderId
		rf.chanRole <- Follower
	}
	reply.Term = args.Term
	baseLogIndex := rf.logTable[0].Index

	if args.PrevLogIndex < baseLogIndex {
		reply.Success = false
		//LogTable start from 1
		reply.NextIndex = baseLogIndex + 1
		return
	}

	if baseLogIndex+len(rf.logTable) <= args.PrevLogIndex {
		reply.Success = false
		reply.NextIndex = baseLogIndex + len(rf.logTable)
		return
	}

	if rf.logTable[args.PrevLogIndex-baseLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		nextIndex := args.PrevLogIndex
		failTerm := rf.logTable[nextIndex-baseLogIndex].Term
		for nextIndex >= baseLogIndex && rf.logTable[nextIndex-baseLogIndex].Term == failTerm {
			nextIndex--
		}
		reply.NextIndex = nextIndex + 1
		return
	}
	//now append entry or Heartbeat
	var updateLogIndex int
	if len(args.Entry) != 0 {
		basicIndex := args.Entry[0].Index - baseLogIndex
		rf.logTable = append(rf.logTable[:basicIndex], args.Entry[:]...)
		reply.NextIndex = rf.logTable[len(rf.logTable)-1].Index + 1
		updateLogIndex = reply.NextIndex - 1
		DPrintf("AppendEntries: %d accept leader(%d) entry, index = %d", rf.me, args.LeaderId, args.PrevLogIndex+1)
	} else {
		reply.NextIndex = args.PrevLogIndex + 1
		updateLogIndex = args.PrevLogIndex
	}
	reply.Success = true
	rf.updateFollowCommit(args.LeaderCommit, updateLogIndex)
}
func (rf *Raft) updateFollowCommit(leaderCommit int, lastIndex int) {
	oldVal := rf.commitIndex
	if leaderCommit > rf.commitIndex {
		if leaderCommit < lastIndex {
			rf.commitIndex = leaderCommit
		} else {
			rf.commitIndex = lastIndex
		}
	}
	baseIndex := rf.logTable[0].Term
	for oldVal++; oldVal <= rf.commitIndex; oldVal++ {
		DPrintf("%d applymsg index=%d", rf.me, oldVal)
		rf.chanCommitted <- ApplyMsg{Index: oldVal, Command: rf.logTable[oldVal-baseIndex].Command}
		rf.lastApplied = oldVal
	}
}
func (rf *Raft) updateLeaderCommit() {
	defer rf.persist()
	//rf.commitIndex don't need lock
	lastIndex := rf.commitIndex
	baseIndex := rf.logTable[0].Index
	newIndex := lastIndex
	for i := rf.logTable[len(rf.logTable)-1].Index; i > lastIndex && rf.logTable[i-baseIndex].Term == rf.currentTerm; i-- {
		counter := 1
		for server := range rf.peers {
			if server != rf.me && rf.matchIndex[server] >= i {
				counter++
			}
		}
		if counter*2 > len(rf.peers) {
			newIndex = i
			break
		}
	}
	if newIndex == lastIndex {
		return
	}
	rf.commitIndex = newIndex
	rf.persist()
	for i := lastIndex + 1; i <= newIndex; i++ {
		rf.chanCommitted <- ApplyMsg{Index: i, Command: rf.logTable[i-baseIndex].Command}
		rf.lastApplied = i
	}
}
func (rf *Raft) doAppendEntries(server int) {
	select {
	case <-rf.chanAppend[server]:
	default:
		//fix me: why use chanAppend
		return
	}
	defer func() { rf.chanAppend[server] <- true }()
	//fix me: rf.role read without lock
	for ok := false; !ok && rf.role == Leader; {
		var args AppendEntriesArgs
		rf.mu.Lock()
		baseIndex := rf.logTable[0].Term
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.LeaderCommit = rf.commitIndex
		args.PrevLogIndex = rf.nextIndex[server] - 1
		args.PrevLogTerm = rf.logTable[args.PrevLogIndex-baseIndex].Term
		if rf.nextIndex[server]-baseIndex < len(rf.logTable) {
			args.Entry = rf.logTable[rf.nextIndex[server]-baseIndex:]
		}
		rf.mu.Unlock()
		var reply AppendEntriesReply
		if len(args.Entry) > 0 {
			DPrintf("%d do appendEntries to %d", rf.me, server)
			args.print()
		}
		if rf.sendAppendEntries(server, args, &reply) {
			if len(args.Entry) > 0 {
				DPrintf("%d reply to %d", server, rf.me)
				reply.print()
			}
			//maybe long time, so check it again
			if rf.role != Leader || rf.currentTerm != args.Term {
				break
			}
			if reply.Term > args.Term {
				rf.mu.Lock()
				rf.currentTerm = reply.Term
				rf.voteFor = -1
				rf.role = Follower
				rf.chanRole <- Follower
				rf.persist()
				rf.mu.Unlock()
				break
			}
			//rf.matchIndex nextIndex do not need lock
			if reply.Success {
				ok = true
				if len(args.Entry) > 0 {
					rf.matchIndex[server] = reply.NextIndex - 1
					rf.nextIndex[server] = reply.NextIndex
				}
			} else {
				if reply.NextIndex > rf.matchIndex[server]+1 {
					rf.nextIndex[server] = reply.NextIndex
				} else {
					rf.nextIndex[server] = rf.matchIndex[server] + 1
				}
			}
		}
	}
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
	isLeader := false
	if rf.role == Leader {
		DPrintf("new entry insert to leader-> %d, index = %d", rf.me, len(rf.logTable))
		rf.mu.Lock()
		defer rf.mu.Unlock()
		defer rf.persist()
		index = len(rf.logTable) + rf.logTable[0].Index
		term = rf.currentTerm
		isLeader = true
		entry := LogEntry{Command: command, Term: term, Index: index}
		rf.logTable = append(rf.logTable, entry)
	}
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
	rf.currentTerm = 1
	rf.voteFor = -1
	rf.leaderId = -1
	rf.role = Follower
	rf.logTable = append(rf.logTable, LogEntry{Term: 0})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers), len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers), len(rf.peers))

	rf.chanAppend = make([]chan bool, len(peers))
	for i := range rf.chanAppend {
		rf.chanAppend[i] = make(chan bool, 1)
		rf.chanAppend[i] <- true
	}
	rf.chanCommitted = applyCh
	rf.chanRole = make(chan Role)
	rf.chanGrantVote = make(chan bool)
	rf.chanHeartbeat = make(chan bool)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rand.Seed(time.Now().UnixNano())
	go rf.changeRole()
	go rf.startElectionTimer()
	return rf
}

func (rf *Raft) startElectionTimer() {
	interval := int(ElectionTimeoutHigh - ElectionTimeoutLow)
	timeout := time.Duration(rand.Intn(interval)) + ElectionTimeoutLow
	timer := time.NewTimer(timeout)
	for {
		select {
		case <-rf.chanHeartbeat:
			rf.resetElectionTimer(timer)
		case <-rf.chanGrantVote:
			rf.resetElectionTimer(timer)
		case <-timer.C:
			rf.role = Candidate
			rf.chanRole <- Candidate
			rf.resetElectionTimer(timer)
		}
	}
}

func (rf *Raft) resetElectionTimer(timer *time.Timer) {
	interval := int(ElectionTimeoutHigh - ElectionTimeoutLow)
	timeout := time.Duration(rand.Intn(interval)) + ElectionTimeoutLow
	timer.Reset(timeout)
}

func (rf *Raft) changeRole() {
	role := rf.role
	for {
		switch role {
		case Leader:
			DPrintf("%d be leader", rf.me)
			for i := range rf.peers {
				rf.nextIndex[i] = len(rf.logTable)
				rf.matchIndex[i] = 0
			}
			go rf.doHeartbeat()
			role = <-rf.chanRole
		case Candidate:
			DPrintf("%d be Candidate", rf.me)
			chanQuitElection := make(chan bool)
			go rf.startElection(chanQuitElection)
			role = <-rf.chanRole
			close(chanQuitElection)
		case Follower:
			role = <-rf.chanRole
		}
	}
}

func (rf *Raft) doHeartbeat() {
	for index := range rf.peers {
		if index == rf.me {
			go func() {
				heartbeatTimer := time.NewTimer(HeartbeatPeriod)
				for rf.role == Leader {
					rf.chanHeartbeat <- true
					rf.updateLeaderCommit()
					heartbeatTimer.Reset(HeartbeatPeriod)
					<-heartbeatTimer.C
				}
			}()
		} else {
			go func(server int) {
				heartbeatTimer := time.NewTimer(HeartbeatPeriod)
				for rf.role == Leader {
					rf.doAppendEntries(server)
					heartbeatTimer.Reset(HeartbeatPeriod)
					<-heartbeatTimer.C
				}
			}(index)
		}
	}
}

// Candidate do it.
// when require vote from server which has higher term,
// we should quit election immediately using chanQuitElection
func (rf *Raft) startElection(chanQuitElection chan bool) {
	rf.mu.Lock()
	rf.voteFor = rf.me //vote for self
	rf.currentTerm++   //inctement term
	var args RequestVoteArgs
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	lastLog := rf.logTable[len(rf.logTable)-1]
	args.LastLogIndex = lastLog.Index
	args.LastLogTerm = lastLog.Term
	rf.persist()
	rf.mu.Unlock()
	//send RequestVote RPC to all other servers
	chanGetVote := make(chan bool, len(rf.peers))
	chanGetVote <- true //self vote
	for index := range rf.peers {
		if index != rf.me {
			go func(index int) {
				var reply RequestVoteReply
				if rf.sendRequestVote(index, args, &reply) {
					if args.Term < reply.Term { //meet higher term server
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.voteFor = -1
						rf.role = Follower
						rf.chanRole <- Follower
						rf.persist()
						rf.mu.Unlock()
					} else if reply.VoteGranted {
						chanGetVote <- true
					}
				} else {
					chanGetVote <- false
				}
			}(index)
		}
	}
	yes, no := 0, 0
	isLoop := true
	for isLoop {
		select {
		case ok := <-chanGetVote:
			if ok {
				yes++
			} else {
				no++
			}
			if yes*2 > len(rf.peers) { // become leader
				rf.role = Leader
				rf.chanRole <- Leader
				isLoop = false
			} else if no*2 > len(rf.peers) { // wait timeout
				isLoop = false
			}
		case <-chanQuitElection:
			isLoop = false
		}
	}
}
