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
	"log"
	"os"

	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const (
	Leader    int8 = 0
	Candidate int8 = 1
	Follower  int8 = 2
)

var logFile, _ = os.OpenFile("../logs/raft.log", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0777)
var cLog = log.New(logFile, "[Candidate]", log.Ldate|log.Ltime)
var lLog = log.New(logFile, "[Leader]", log.Ldate|log.Ltime)
var fLog = log.New(logFile, "[Follower]", log.Ldate|log.Ltime)
var aLog = log.New(logFile, "[All]", log.Ldate|log.Ltime)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []raftLogs

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	role          int8  //节省空间
	voteCount     int64 //后续原子性操作
	selectTime    int64 //时间戳 每当接收到心跳了 就在当前时间上增加随机时间
	heartbeatChan chan struct{}
}

type raftLogs struct {
	index int
	term  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.RLock()
	term = rf.currentTerm
	isleader = rf.role == Leader
	rf.mu.RUnlock()
	return term, isleader
}

func (rf *Raft) GetRole() int8 {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.role
}

func (rf *Raft) GetTerm() int {
	var term int
	// Your code here (3A).
	rf.mu.RLock()
	term = rf.currentTerm
	rf.mu.RUnlock()
	return term
}

func (rf *Raft) GetVoteFor() int {
	var voteFor int
	// Your code here (3A).
	rf.mu.RLock()
	voteFor = rf.votedFor
	rf.mu.RUnlock()
	return voteFor
}

func (rf *Raft) GetLogTermAndIndex() (int, int) {
	var logTerm, logIndex int
	// Your code here (3A).
	rf.mu.RLock()
	logTerm = rf.log[len(rf.log)-1].term
	logIndex = rf.log[len(rf.log)-1].index
	rf.mu.RUnlock()
	return logTerm, logIndex
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	term         int
	candidateId  int
	lastLogTerm  int
	lastLogIndex int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	term        int
	voteGranted bool
}
type AppendEntriesArgs struct {
	term         int
	leaderId     int
	prevLogIndex int
	prevLogTerm  int
	entries      []raftLogs
	leaderCommit int
}
type AppendEntriesReply struct {
	term    int
	success bool
}

func (rf *Raft) MonitorHeartBeat() {
	for {
		select {
		case <-rf.heartbeatChan:
			rf.SendHeartBeat()
		}
	}
}
func (rf *Raft) SendHeartBeat() {
	for rf.GetRole() == Leader {
		args := &AppendEntriesArgs{}
		reply := &AppendEntriesReply{}
		rf.mu.Lock()
		args.term = rf.currentTerm
		args.leaderId = rf.me
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go rf.peers[i].Call("Raft.AppendEntries", args, reply)
			if reply.term > args.term {
				rf.mu.Lock()
				if reply.term > rf.currentTerm {
					rf.role = Follower
					lLog.Printf("leader %d(term:%d) receive bigger term from point %d(term%d) ,then become follower", rf.me, rf.currentTerm, i, reply.term)
					rf.currentTerm = reply.term
					rf.selectTime = time.Now().UnixMilli() + 50 + (rand.Int63() % 300)
					rf.votedFor = -1
					atomic.StoreInt64(&rf.voteCount, 0)
				} else {
					lLog.Printf("leader %d recieve bigger term but leader term has change", rf.me)
				}
				rf.mu.Unlock()
			}
		}
	}
}

// 无论执行什么操作 在执行操作之前都要确认自己的身份
func (rf *Raft) startSelect() {
	args := &RequestVoteArgs{}
	reply := &RequestVoteReply{}
	rf.mu.Lock()
	args.term = rf.currentTerm
	args.candidateId = rf.me
	args.lastLogTerm = rf.log[len(rf.log)-1].term
	args.lastLogIndex = rf.log[len(rf.log)-1].index
	rf.role = Candidate
	rf.votedFor = rf.me
	atomic.StoreInt64(&rf.voteCount, 1)
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers) && rf.GetRole() == Candidate; i++ {
		if i == rf.me {
			continue
		}
		go rf.collectVoteData(i, args, reply)
	}
}

func (rf *Raft) collectVoteData(index int, args *RequestVoteArgs, reply *RequestVoteReply) {
	cLog.Printf("point %d request vote for %d", rf.me, index)
	rf.sendRequestVote(index, args, reply)

	if reply.voteGranted && rf.GetRole() == Candidate {
		atomic.AddInt64(&rf.voteCount, 1)
		if int(atomic.LoadInt64(&rf.voteCount)) > len(rf.peers)/2 {
			rf.mu.Lock()
			if rf.role == Candidate {
				rf.role = Leader
				rf.heartbeatChan <- struct{}{}
				cLog.Printf("point %d begin leader", rf.me)
			}
			rf.mu.Unlock()
		}
	} else {
		//如果当发现有节点的任期大于自己 无论原本身份是什么都修改身份并且重置票数
		if reply.term > args.term {
			rf.mu.Lock()
			if reply.term > rf.currentTerm {
				rf.currentTerm = reply.term
				cLog.Printf("point %d update term to %d", rf.me, rf.currentTerm)
			}
			rf.role = Follower
			rf.votedFor = -1
			atomic.StoreInt64(&rf.voteCount, 0)
			rf.selectTime = time.Now().UnixMilli() + 50 + (rand.Int63() % 300)
			cLog.Printf("point %d begin follower", rf.me)
			rf.mu.Unlock()
		}
		//如果任期并没有大于当前任期 表示投票给其他人
		//或者 当前term与arg时的term不一样 表示发现了其他term大的节点
	}

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	if args.term < rf.GetTerm() {
		reply.term = rf.GetTerm()
		reply.voteGranted = false
		aLog.Printf("point %d reject vote for %d", rf.me, args.candidateId)
		return
	}
	if args.term > rf.GetTerm() {
		rf.mu.Lock()
		if args.term > rf.currentTerm {
			rf.currentTerm = args.term
			rf.votedFor = args.candidateId
			rf.role = Follower
			aLog.Printf("point %d begin follower", rf.me)
			aLog.Printf("point %d approval vote for %d", rf.me, args.candidateId)
			aLog.Printf("point %d update term to %d", rf.me, args.term)
			rf.selectTime = time.Now().UnixMilli() + 50 + (rand.Int63() % 300)
			atomic.StoreInt64(&rf.voteCount, 0)
			reply.term = args.term
			reply.voteGranted = true
		} else {
			aLog.Printf("point %d not approval vote for %d", rf.me, args.candidateId)
			reply.term = rf.currentTerm
			reply.voteGranted = false
		}
		rf.mu.Unlock()

		return
	}
	if args.term == rf.GetTerm() {
		if rf.GetVoteFor() != -1 {
			reply.term = args.term
			reply.voteGranted = false
			aLog.Printf("point %d reject vote for %d", rf.me, args.candidateId)
			return
		} else {
			term, index := rf.GetLogTermAndIndex()
			if term > args.lastLogTerm || (term == args.lastLogTerm && index > args.lastLogIndex) {
				reply.term = args.term
				reply.voteGranted = false
				aLog.Printf("point %d reject vote for %d", rf.me, args.candidateId)
				return
			} else {
				rf.mu.Lock()
				rf.votedFor = args.candidateId
				rf.role = Follower
				rf.selectTime = time.Now().UnixMilli() + 50 + (rand.Int63() % 300)
				aLog.Printf("point %d begin follower", rf.me)
				aLog.Printf("point %d approval vote for %d", rf.me, args.candidateId)
				rf.mu.Unlock()
				reply.term = args.term
				reply.voteGranted = true
			}
		}
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		if time.Now().UnixMilli() > atomic.LoadInt64(&rf.selectTime) && rf.GetRole() == Follower {
			rf.startSelect() //开启选举
			fLog.Printf("point %d start select", rf.me)
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.mu = sync.RWMutex{}
	rf.role = Follower
	rf.votedFor = -1
	rf.heartbeatChan = make(chan struct{})
	rf.selectTime = time.Now().UnixMilli() + 50 + (rand.Int63() % 300)
	rf.log = make([]raftLogs, 0)
	rf.log = append(rf.log, raftLogs{term: 0, index: 0})
	rf.voteCount = 0
	rf.currentTerm = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
