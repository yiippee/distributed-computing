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
	//"fmt"
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

const (
	LEADER = iota
	CANDIDATE
	FLLOWER

	HBINTERVAL = 50 * time.Millisecond // 50ms
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

// 1, 如果两个entry在不同的服务器中，拥有同样的index和term，则保证他们保存了相同的命令
// 2, 如果两个entry在不同的服务器中，拥有同样的index和term，则保证在两个entry之前的所有entry都相等。
// 第一个属性要保证很简单，leader每次创建entry时，都只会使用新的index，而不会去改写之前index的内容。
// 这也就是在给定index和term的情况下，只会创建一个entry。那么也就保证了相同index和term，相同命令。

// 第二个属性需要在每次AppendEntries的时候进行检查。Leader每次发送entry给follower时，会带上前一个entry的index和term。
// 当Follower收到RPC时，会检查最后一个entry的index和term是不是和AppendEntries中的prevIndex和prevTerm相同，不相同则拒绝。
// 若不相同则说明该entry保存的命令和Leader上保存的命令不同，则自然要拒绝。这个也就是所谓的一致性检查（the consistency check）。

// Raft简单的地方在于，Leader从来不会修改自己log，而是让Follower自己去修改log。
// 方法就是Leader维护一个nextIndex[i]数组，用来保存下个发给Follower[i]的Index，
// 如果consistency check通过，则nextIndex[i]++，不然的话则nextIndex[i]--，直到双方同步log位置。
// 这个感觉有点类似于tcp传数据时的ack，总是返回下一个期望收到的数据包。
// 也就是说这里除了nextIndex的更新之外，Leader不需要做什么额外的动作。这就是Raft可理解的地方了。

/*
 日志由序号与条目组成，每个条目又由任期与指令组成，总体上呈三段式结构：
 1   1   1   2   2   3   3   任期
 a   a   b   c   d   e   f   命令
 1   2   3   4   5   6   7   index，单调递增
*/
type LogEntry struct {
	LogIndex   int // log的索引
	LogTerm    int // log的任期。   这说明 Log实体 里面是含有索引和任期属性的，如果保证index和term相同，则保证leader和follower日志都完全相同
	LogCommand interface{}
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

	//channel
	state         int
	voteCount     int
	chanCommit    chan bool
	chanHeartbeat chan bool
	chanGrantVote chan bool
	chanLeader    chan bool
	chanApply     chan ApplyMsg

	//persistent state on all server
	currentTerm int
	votedFor    int
	log         []LogEntry

	//volatile state on all servers
	commitIndex int
	lastApplied int

	//volatile state on leader
	/*
		领导者通过在每一个追随者维护了一个 nextIndex，表示下一个需要发送给跟随者的日志条目索引地址，
		领导者刚获得选举时，初始化所有 nextIndex 值为自己的最后一条日志的index加1；
		当追随者的日志和领导者不一致，那在下一次的AppendEntries时的一致性检查会失败，
		被追随者拒绝后，领导者就会减小 nextIndex 值进行重试，nextIndex 会在某位置使领导者和追随者日志达成一致。
	*/
	nextIndex  []int // leader期待的需要发送的下一条日志。 这是一个数组，每一个元素对应着一个follower，维护着所有的follower
	matchIndex []int // 也是一个数组，维护所有的follower已经匹配的日志，也就是已经replicated的日志，已经复制了的日志
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.state == LEADER
}

func (rf *Raft) getLastIndex() int {
	return rf.log[len(rf.log)-1].LogIndex
}
func (rf *Raft) getLastTerm() int {
	return rf.log[len(rf.log)-1].LogTerm
}
func (rf *Raft) IsLeader() bool {
	return rf.state == LEADER
}

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
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readSnapshot(data []byte) {

	rf.readPersist(rf.persister.ReadRaftState())

	if len(data) == 0 {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	var LastIncludedIndex int
	var LastIncludedTerm int

	d.Decode(&LastIncludedIndex)
	d.Decode(&LastIncludedTerm)

	rf.commitIndex = LastIncludedIndex
	rf.lastApplied = LastIncludedIndex

	rf.log = truncateLog(LastIncludedIndex, LastIncludedTerm, rf.log)

	msg := ApplyMsg{UseSnapshot: true, Snapshot: data}

	go func() {
		rf.chanApply <- msg
	}()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogTerm  int
	LastLogIndex int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

// 复制 log 的 RPC
// 用于领导人调用来复制log实体. 也用于维持心跳.  leader广播的复制日志信息
type AppendEntriesArgs struct {
	// Your data here.
	Term         int        // 领导人的任期号
	LeaderId     int        // 领导人 ID, 用于跟随者可以转发请求
	PrevLogTerm  int        // 上一个 log 的任期号. 发送信息需要带上一个索引和任期，是因为需要保证上一个是对上号的，这样递归下去，肯定会保证所有的都正确。
	PrevLogIndex int        // 上一个 log 的索引号.
	Entries      []LogEntry // 需要存储的 log (心跳的这个字段为空; 为了提高效率可能会发送多个)
	LeaderCommit int        // leader已经提交了的日志 commitIndex
}

type AppendEntriesReply struct {
	// Your data here.
	Term      int
	Success   bool
	NextIndex int // follower期待的下一条日志
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		// 如果拉票方的任期比我自己的任期小，则回复我的任期，告诉对方我任期更大
		reply.Term = rf.currentTerm
		return
	}
	//If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		// 如果拉票的任期更大，我就给他投票，并把自己置为follower
		rf.currentTerm = args.Term
		rf.state = FLLOWER
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm

	term := rf.getLastTerm()
	index := rf.getLastIndex()
	uptoDate := false

	//If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	//Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs.
	// If the logs have last entries with different terms,then the log with the later term is more up-to-date.
	// If the logs end with the same term, then whichever log is longer is more up-to-date
	if args.LastLogTerm > term {
		uptoDate = true
	}

	if args.LastLogTerm == term && args.LastLogIndex >= index {
		// at least up to date
		uptoDate = true
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && uptoDate {
		rf.chanGrantVote <- true
		rf.state = FLLOWER
		reply.VoteGranted = true // 确认投票
		rf.votedFor = args.CandidateId
	}
}

// 这个方法是follower接受leader发送的日志复制信息的处理方法
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false
	//Reply false if term < currentTerm
	// 说明这是旧领导发送给自己的心跳包,什么也不处理，返回自己的term给旧领导，让旧领导知道他的term过期了
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.getLastIndex() + 1
		return
	}
	// 向状态机通过管道发送消息"接收到来自新的领导人的附加日志RPC",其实也是一种心跳信息
	rf.chanHeartbeat <- true
	//If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		// 如果收到的日志复制信息任期大于当前的任期，则修改为最新的任期
		rf.currentTerm = args.Term
		rf.state = FLLOWER
		rf.votedFor = -1
	}
	reply.Term = args.Term

	// 检查对于发送方来说的上一个索引。就是说，如果leader发送过来的上一个索引都大于我自己的最后一条索引，则说明我落后leader太多了啊
	// 需要发送我自己期待的下一条索引，好让leader知道同步的位置在哪里。
	if args.PrevLogIndex > rf.getLastIndex() {
		// 如果不对，则返回我期待的索引号，即我当前的索引加一
		reply.NextIndex = rf.getLastIndex() + 1
		return
	}

	baseIndex := rf.log[0].LogIndex // 从编号 0 开始获取日志，就是从第一个日志开始。因为编号不一定是从0开始，也可能是从8888开始，但是肯定是在log[0]中

	// If a follower’s log is inconsistent with the leader’s, the AppendEntries consis- tency check will fail in the next AppendEntries RPC.
	// After a rejection, the leader decrements nextIndex and retries the AppendEntries RPC
	// Eventually nextIndex will reach a point where the leader and follower logs match
	// which removes any conflicting entries in the follower’s log and appends entries from the leader’s log (if any).
	if args.PrevLogIndex > baseIndex {
		// 如果leader发过来的前序索引大于我的基础索引，（一般来说肯定会大于的）
		term := rf.log[args.PrevLogIndex-baseIndex].LogTerm // 获取我自己的日志任期
		// 判断leader发送过来的上一个任期是否与我现在的任期是否相同
		if args.PrevLogTerm != term {
			// 如果不相同，则需要找到我自己所有的索引中，第一次与leader
			for i := args.PrevLogIndex - 1; i >= baseIndex; i-- {
				if rf.log[i-baseIndex].LogTerm != term {
					reply.NextIndex = i + 1
					break
				}
			}
			return
		}
	}
	if args.PrevLogIndex < baseIndex {

	} else {
		// Append any new entries not already in the log
		rf.log = rf.log[:args.PrevLogIndex+1-baseIndex]
		rf.log = append(rf.log, args.Entries...) // 把leader发送过来的日志复制到自己的log中
		reply.Success = true
		reply.NextIndex = rf.getLastIndex() + 1
	}
	//If leaderCommit > commitIndex, set commitIndex =min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		// 如果leader提交的日志数大于我本身的日志提交数，则对于我来说，我已经提交了的日志索引就取min(leaderCommit, index of last new entry)最小的值
		last := rf.getLastIndex()
		if args.LeaderCommit > last {
			rf.commitIndex = last
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.chanCommit <- true
	}
	return
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		term := rf.currentTerm
		// 在异步/阻塞调用前后，可能身份角色（自身）和选举周期（外界）早已天翻地覆，因此需要进行自检。
		if rf.state != CANDIDATE {
			return ok
		}
		// 异步调用后，有可能任期改变了
		if args.Term != term {
			return ok
		}
		if reply.Term > term {
			// 回复的任期比我自己的任期更大，我就将自己置为follower
			rf.currentTerm = reply.Term
			rf.state = FLLOWER
			rf.votedFor = -1
			rf.persist()
		}
		if reply.VoteGranted {
			// 如果对方确认给我投票，则将票数加1，并判断是否已经获取一半以上的票数
			rf.voteCount++
			if rf.state == CANDIDATE && rf.voteCount > len(rf.peers)/2 {
				rf.state = FLLOWER
				rf.chanLeader <- true // 将自己置为leader
			}
		}
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		// Leader进行心跳检测时候，由于是异步调用，所以需要先检测自己还是不是leader，
		// 以及是否还在自己任期（由args保存了当时的任期）；
		// 当rpc返回后，再一次进行该检查，无误，才能按照自己是leader来进行下一步动作。
		if rf.state /*新的状态还是leader吗？*/ != LEADER {
			return ok
		}
		if args.Term /*args保存了阻塞调用Call之前的任期*/ != rf.currentTerm /*新的任期还是之前的任期吗*/ {
			return ok
		}

		if reply.Term > rf.currentTerm {
			// follower返回的任期大于我自己的任期，说明环境有变，我不能再做leader了，改为follower
			rf.currentTerm = reply.Term
			rf.state = FLLOWER
			rf.votedFor = -1
			rf.persist()
			return ok
		}
		if reply.Success {
			if len(args.Entries) > 0 {
				// 记录回复成功的那个follower的下一个需要发送给跟随者的日志index加1
				rf.nextIndex[server] = args.Entries[len(args.Entries)-1].LogIndex + 1
				//reply.NextIndex
				//rf.nextIndex[server] = reply.NextIndex
				// 记录回复成功的那个follower已经匹配了的日志
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
		} else {
			// 否则将下一条需要发送的日志修改为follower回复期待的索引
			rf.nextIndex[server] = reply.NextIndex
		}
	}
	return ok
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) GetPerisistSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) StartSnapshot(snapshot []byte, index int) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex := rf.log[0].LogIndex
	lastIndex := rf.getLastIndex()

	if index <= baseIndex || index > lastIndex {
		// in case having installed a snapshot from leader before snapshotting
		// second condition is a hack
		return
	}

	var newLogEntries []LogEntry

	newLogEntries = append(newLogEntries, LogEntry{LogIndex: index, LogTerm: rf.log[index-baseIndex].LogTerm})

	for i := index + 1; i <= lastIndex; i++ {
		newLogEntries = append(newLogEntries, rf.log[i-baseIndex])
	}

	rf.log = newLogEntries

	rf.persist()

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(newLogEntries[0].LogIndex)
	e.Encode(newLogEntries[0].LogTerm)

	data := w.Bytes()
	data = append(data, snapshot...)
	rf.persister.SaveSnapshot(data)
}

func truncateLog(lastIncludedIndex int, lastIncludedTerm int, log []LogEntry) []LogEntry {

	var newLogEntries []LogEntry
	newLogEntries = append(newLogEntries, LogEntry{LogIndex: lastIncludedIndex, LogTerm: lastIncludedTerm})

	for index := len(log) - 1; index >= 0; index-- {
		if log[index].LogIndex == lastIncludedIndex && log[index].LogTerm == lastIncludedTerm {
			newLogEntries = append(newLogEntries, log[index+1:]...)
			break
		}
	}

	return newLogEntries
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.chanHeartbeat <- true
	rf.state = FLLOWER
	rf.currentTerm = rf.currentTerm

	rf.persister.SaveSnapshot(args.Data)

	rf.log = truncateLog(args.LastIncludedIndex, args.LastIncludedTerm, rf.log)

	msg := ApplyMsg{UseSnapshot: true, Snapshot: args.Data}

	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex

	rf.persist()

	rf.chanApply <- msg
}

func (rf *Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if ok {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FLLOWER
			rf.votedFor = -1
			return ok
		}

		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.state == LEADER
	if isLeader {
		index = rf.getLastIndex() + 1
		//fmt.Printf("raft:%d start\n",rf.me)
		rf.log = append(rf.log, LogEntry{LogTerm: term, LogCommand: command, LogIndex: index}) // append new entry from client
		rf.persist()
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

func (rf *Raft) broadcastRequestVote() {
	var args RequestVoteArgs
	rf.mu.Lock()
	args.Term = rf.currentTerm // 我自己要竞选的任期
	args.CandidateId = rf.me
	args.LastLogTerm = rf.getLastTerm()   // 最后一条日志信息的任期
	args.LastLogIndex = rf.getLastIndex() // 最后一条日志的index
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me && rf.state == CANDIDATE {
			go func(i int) {
				var reply RequestVoteReply
				rf.sendRequestVote(i, args, &reply)
			}(i)
		}
	}
}

/**
* Log replication
 Leader接收到指令后写入到本地日志，在随后的心跳中（AppendEntries）往其他追随者发送该条目，
 等待收到过半追随者响应后将该条目标志位已提交状态，
 并发往状态机执行，完成后返回结果给客户端；
 在后续心跳包（AppendEntries）中通知所有追随者哪些条目为已提交状态，
 以便追随者更新在自己状态机中执行该指令；
 只有Leader能够接受客户端的指令，追随者只能够接收领导者的AppendEntries请求

*/
// 广播日志复制. Leader不删除任何日志、Follower只接收Leader所发送的日志信息
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	N := rf.commitIndex
	last := rf.getLastIndex()
	baseIndex := rf.log[0].LogIndex
	//If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
	// 计算从已经提交了的index开始，到最后的index，
	// 计算可以提交的index有多少
	for i := rf.commitIndex + 1; i <= last; i++ {
		num := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i && rf.log[i-baseIndex].LogTerm == rf.currentTerm {
				num++ // 记录已经复制了index的follower个数
			}
		}
		if 2*num > len(rf.peers) {
			N = i
		}
	}
	// 检测大多数 logEntry match Index，
	// 以决定是否要进行 commit，即前移 Leader 的 commitIndex ，
	// 并且在以后的 AppendEntries 的 RPC 中将其同步给各个 Follower。
	if N != rf.commitIndex {
		rf.commitIndex = N
		rf.chanCommit <- true
	}

	for i := range rf.peers {
		if i != rf.me && rf.state == LEADER {

			//copy(args.Entries, rf.log[args.PrevLogIndex + 1:])
			// 同步过程，我将其分为对 match 位置的 试探阶段 和 match 后的传送阶段；
			// 即首先通过每次前移 prevLogIndex+prevLogTerm 匹配上 Follower 中的某个 logEntry，
			// 然后 Leader 将匹配到的 logEntry 之后的 entries 一次性送给 Follower。
			if rf.nextIndex[i] > baseIndex {
				var args AppendEntriesArgs
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[i] - 1
				//	fmt.Printf("baseIndex:%d PrevLogIndex:%d\n",baseIndex,args.PrevLogIndex )
				args.PrevLogTerm = rf.log[args.PrevLogIndex-baseIndex].LogTerm
				//args.Entries = make([]LogEntry, len(rf.log[args.PrevLogIndex + 1:]))
				args.Entries = make([]LogEntry, len(rf.log[args.PrevLogIndex+1-baseIndex:]))
				copy(args.Entries, rf.log[args.PrevLogIndex+1-baseIndex:])
				args.LeaderCommit = rf.commitIndex
				go func(i int, args AppendEntriesArgs) {
					var reply AppendEntriesReply
					rf.sendAppendEntries(i, args, &reply)
				}(i, args)
			} else {
				var args InstallSnapshotArgs
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LastIncludedIndex = rf.log[0].LogIndex
				args.LastIncludedTerm = rf.log[0].LogTerm
				args.Data = rf.persister.snapshot
				go func(server int, args InstallSnapshotArgs) {
					reply := &InstallSnapshotReply{}
					rf.sendInstallSnapshot(server, args, reply)
				}(i, args)
			}
		}
	}
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
	rf.state = FLLOWER
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{LogTerm: 0})
	rf.currentTerm = 0
	rf.chanCommit = make(chan bool, 100)
	rf.chanHeartbeat = make(chan bool, 100)
	rf.chanGrantVote = make(chan bool, 100)
	rf.chanLeader = make(chan bool, 100)
	rf.chanApply = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	// 死循环，运行raft的状态机
	go func() {
		for {
			switch rf.state {
			case FLLOWER:
				select {
				case <-rf.chanHeartbeat: // 心跳
				case <-rf.chanGrantVote: // 批准投票
				case <-time.After(time.Duration(rand.Int63()%333+550) * time.Millisecond):
					// 没有心跳、没有投票信息后就将自己置为候选者
					rf.state = CANDIDATE
				}
			case LEADER:
				//fmt.Printf("Leader:%v %v\n",rf.me,"boatcastAppendEntries	")
				// 成为leader以后就广播日志信息, 定期心跳，有时携带日志同步
				rf.broadcastAppendEntries()
				time.Sleep(HBINTERVAL)
			case CANDIDATE:
				rf.mu.Lock()
				//To begin an election, a follower increments its current term and transitions to candidate state
				// 开始选举，将任期新增1
				rf.currentTerm++
				//It then votes for itself and issues RequestVote RPCs in parallel to each of the other servers in the cluster.
				// 先给自己投一票
				rf.votedFor = rf.me
				rf.voteCount = 1
				// 进行持久化 // 防止机器故障，将节点状态持久化
				rf.persist()
				rf.mu.Unlock()
				//(a) it wins the election, (b) another server establishes itself as leader, or (c) a period of time goes by with no winner
				go rf.broadcastRequestVote() // 广播拉票
				select {
				case <-time.After(time.Duration(rand.Int63()%300+510) * time.Millisecond):
				case <-rf.chanHeartbeat:
					rf.state = FLLOWER
				//	fmt.Printf("CANDIDATE %v reveive chanHeartbeat\n",rf.me)
				case <-rf.chanLeader:
					rf.mu.Lock()
					rf.state = LEADER
					//fmt.Printf("%v is Leader\n",rf.me)
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := range rf.peers {
						//The leader maintains a nextIndex for each follower, which is the index of the next log entry the leader will send to that follower.
						// When a leader first comes to power, it initializes all nextIndex values to the index just after the last one in its log
						// 领导者刚获得选举时，初始化所有 nextIndex 值为自己的最后一条日志的index加1
						rf.nextIndex[i] = rf.getLastIndex() + 1
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
					//rf.boatcastAppendEntries()
				}
			}
		}
	}()

	// 死循环，应用committed日志到raft状态机
	go func() {
		for {
			select {
			// 已经超过半数的节点日志复制成功，则可以提交
			case <-rf.chanCommit:
				rf.mu.Lock()
				commitIndex := rf.commitIndex
				baseIndex := rf.log[0].LogIndex
				for i := rf.lastApplied + 1; i <= commitIndex; i++ {
					msg := ApplyMsg{Index: i, Command: rf.log[i-baseIndex].LogCommand}
					applyCh <- msg
					//fmt.Printf("me:%d %v\n",rf.me,msg)
					rf.lastApplied = i
				}
				rf.mu.Unlock()
			}
		}
	}()
	return rf
}
