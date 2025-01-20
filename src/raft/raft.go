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
// 以下是Raft必须向服务（或测试器）暴露的API的大纲，每个函数的详细信息见其注释。
//
// rf = Make(...)
//   创建一个新的Raft服务器实例。
// rf.Start(command interface{}) (index, term, isleader)
//   针对新的日志条目启动一致性协议。
// rf.GetState() (term, isLeader)
//   获取Raft节点当前任期，并判断它是否认为自己是领导者。
// ApplyMsg
//   每当有新的日志条目被提交到日志中时，每个Raft节点都应通过Make函数传入的applyCh向同一服务器上的服务（或测试器）发送ApplyMsg消息。

import (
	//	"bytes"

	"sync"
	"sync/atomic"
	"time"

	//	"course/labgob"

	"course/labrpc"
)

// 设置随机时间的上下界，用于控制选举超时时间和日志复制间隔时间等相关机制。
const (
	electionTimeoutMin time.Duration = 250 * time.Millisecond /* 选举超时时间的最小值 */
	electionTimeoutMax time.Duration = 400 * time.Millisecond /* 选举超时时间的最大值 */

	replicateInterval time.Duration = 70 * time.Millisecond /* 日志复制（心跳包）的时间间隔 */
)

// 设置异常值
const (
	InvalidTerm  int = 0
	InvalidIndex int = 0
)

type Role string

// 设置节点的常量状态
const (
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
	Leader    Role = "Leader"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part PartD you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For PartD:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (PartA, PartB, PartC).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	role        Role
	currentTerm int /* 当前节点对应期数 */
	votedFor    int /* -1 表示没有投给任何人 */

	/* 日志同步相关 */
	log *RaftLog // 每个 Raft 节点本地的日志

	nextIndex  []int /* 存储 peers 中每个节点对应的日志的最后的下一个位置，例如: nextIndex[peer] 表示：第 peers[peer] 节点下次写入日志的位置 */
	matchIndex []int /* 领导者节点（Leader）记录已经复制到每个跟随者节点（Follower）上的日志条目的最大索引位置（也就是已经匹配上的最大日志索引） */

	/* 日志与应用层交互相关 */
	commitIndex int           /* 已经被确认提交的日志条目的最大索引值 */
	lastApplied int           /* 已经应用到状态机的日志条目的最大索引值 */
	applyCh     chan ApplyMsg /* 通道，用于传递ApplyMsg类型的消息，协调日志提交与应用的流程 */
	snapPending bool          /* 用于表示当前 Raft 节点是否有一个快照安装操作正在等待后续的处理（例如应用到状态机等操作） */
	applyCond   *sync.Cond    /* 条件变量（sync.Cond） */

	electionStart   time.Time     /* 心跳定时器的初始时间 */
	electionTimeout time.Duration /* 心跳定时器的触发时间 */
}

/* 定义函数，解决状态机角色转换问题 */
func (rf *Raft) becomeFollowerLocked(term int) {
	// 如果任期小于当前任期则无需处理
	if term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DError, "Can`t become Follower, lower term: T%d", term)
		return
	}

	// 转化角色
	LOG(rf.me, rf.currentTerm, DLog, "%s->Follower, For T%s->T%s", rf.role, rf.currentTerm, term)
	rf.role = Follower

	shouldPersit := rf.currentTerm != term

	if term > rf.currentTerm {
		rf.votedFor = -1
	}
	rf.currentTerm = term
	if shouldPersit {
		rf.persistLocked()
	}
}

func (rf *Raft) becomeCandidateLocked() {
	// 如果候选者已经是 Leader 则无需后续操作
	if rf.role == Leader {
		LOG(rf.me, rf.currentTerm, DError, "Leader can`t become candidate")
		return
	}

	// 转化角色
	LOG(rf.me, rf.currentTerm, DVote, "%s->Candidate, For T%d", rf.role, rf.currentTerm+1)
	rf.currentTerm++ //任期增加
	rf.role = Candidate
	rf.votedFor = rf.me // 投票给自己
	rf.persistLocked()

}

func (rf *Raft) becomeLeaderLocked() {
	// 如果不是候选者则保持（根据状态转移得知，只有 Candidate 才能转移成 Leader）
	if rf.role != Candidate {
		LOG(rf.me, rf.currentTerm, DError, "Only Candidate can become Leader")
		return
	}

	// 转化角色
	LOG(rf.me, rf.currentTerm, DLeader, "%s->Leader in T%d", rf.role, rf.currentTerm)
	rf.role = Leader

	// 遍历集群的所有节点
	for peer := 0; peer < len(rf.peers); peer++ {

		// 初始化 nextIndex 以及 matchIndex
		rf.nextIndex[peer] = rf.log.size()
		rf.matchIndex[peer] = 0

	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (PartA).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

func (rf *Raft) GetRaftStateSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.

// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
/* 	*/

// 用于在 Raft 节点中启动一个新的日志记录操作，通常由领导者节点（Leader）来执行，用于接收一个命令（command）并将其记录到本地日志中
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	/*
		index = -1
		term = -1
		isLeader = false
	*/

	// Your code here (PartB).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 判断当前节点的角色是否为领导者（Leader）
	if rf.role != Leader {
		return 0, 0, false
	}

	// 给 Leader 添加日志
	rf.log.append(LogEntry{
		CommandValid: true,
		Command:      command,
		Term:         rf.currentTerm,
	})
	LOG(rf.me, rf.currentTerm, DLeader, "leader accept log [%d]T%d", rf.log.size()-1, rf.currentTerm)
	rf.persistLocked()

	return rf.log.size() - 1, rf.currentTerm, true
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

// 检测当前任期是否和自身任期相同，让其状态是否和自身状态相同
func (rf *Raft) contextLostLocked(role Role, term int) bool {
	return !(rf.currentTerm == term && rf.role == role)
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
/*
服务或 tester 想要创建一个 Raft 服务器。端口
的所有 Raft 服务器（包括这个服务器）都在 peers[] 中。这
服务器的端口是 peers[me]。所有服务器的 peers[] 数组
具有相同的顺序。persister 是此服务器用于
save 的持久状态，并且最初也持有最多的
最近保存的状态（如果有）。applyCh 是一个通道，其中
tester 或服务期望 Raft 发送 ApplyMsg 消息。
Make（） 必须快速返回，因此它应该启动 goroutines
对于任何长时间运行的工作。
*/

/* 创建新的 Raft 实例 */
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (PartA, PartB, PartC).
	// 初始化代码
	rf.role = Follower
	rf.currentTerm = 1
	rf.votedFor = -1

	// 日志相关初始化
	rf.log = NewLog(InvalidIndex, InvalidTerm, nil, nil)

	//日志初始化为空 类似链表 dummy 节点
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// 日志应相关初始化
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.commitIndex = 0
	rf.snapPending = false
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	// 从崩溃前 persisted 状态初始化
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()
	go rf.applicationTicker()

	return rf
}
