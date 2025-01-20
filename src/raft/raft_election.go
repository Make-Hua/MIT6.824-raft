package raft

import (
	"fmt"
	"math/rand"
	"time"
)

/* 重置选举定时器相关的状态 */
func (rf *Raft) resetElectionTimerLocked() {

	// 定时器起始时间为当前
	rf.electionStart = time.Now()

	randRange := int64(electionTimeoutMax - electionTimeoutMin)

	// 定时器到期时间为 250 ms ~ 400 ms
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63()%randRange)
}

/* 检查是否已经达到选举超时时间 */
func (rf *Raft) isElectionTimeoutLocked() bool {
	return time.Since(rf.electionStart) > rf.electionTimeout
}

// 判断当前 Raft 节点（rf 所指向的节点）的日志是否比传入的候选节点（由 candidateIndex 和 candidateTerm 参数标识）的日志更新。
func (rf *Raft) isMoreUpToDateLocked(candidateIndex, candidateTerm int) bool {

	lastIndex, lastTerm := rf.log.last()

	LOG(rf.me, rf.currentTerm, DVote, "Comoaer last log, Me: [%d]T%d", lastIndex, lastTerm, candidateIndex)

	// 如果当前 Raft 节点最后一条日志的任期号（lastTerm）和候选节点的任期号（candidateTerm）不相等，
	// 那么就根据任期号来判断日志的新旧，任期号更大意味着日志更新，所以返回当前节点的任期号是否大于候选节点的任期号的比较结果。
	if lastTerm != candidateTerm {
		return lastTerm > candidateTerm
	}

	// 如果两个节点最后一条日志的任期号相等，那么再比较日志的索引，日志索引更大的节点意味着其日志包含更多的内容，也就更能反映更新的状态，
	// 所以返回当前节点最后一条日志的索引是否大于候选节点索引的比较结果，以此来最终判断当前节点的日志是否比候选节点的日志更新。
	return lastIndex > candidateIndex
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
// 请求参数值
type RequestVoteArgs struct {
	// Your data here (PartA, PartB).
	Term        int /* Term 字段表示发起请求投票的节点所处的任期号 */
	CandidateId int /* 该字段代表发起请求投票的候选者节点的唯一标识 */

	LastLogIndex int /* 发起投票的 candidate 节点对应的最后日志的下标 */
	LastLogTerm  int /* 发起投票的 candidate 节点对应的最后日志的任期号 */
}

// 日志相关
func (args *RequestVoteArgs) String() string {
	return fmt.Sprintf("Candidate-%d, T%d, Last: [%d]T[%d]", args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
// 请求回复值
type RequestVoteReply struct {
	// Your data here (PartA).
	Term        int  /* Term 字段表示发起请求投票的节点所处的任期号 */
	VoteGranted bool /* 表示本次投票请求是否被批准，即是否愿意将票投给 RPC 请求节点 */
}

// 日志相关
func (reply *RequestVoteReply) String() string {
	return fmt.Sprintf("T%d, VoteGranted: %v0", reply.Term, reply.VoteGranted)
}

// example RequestVote RPC handler.
/* 该方法是请求投票 RPC 的处理函数，也就是 RPC 提供方所注册函数，供给远程调用 */
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (PartA, PartB).

	// 加锁并在函数执行完之前解锁
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, VoteAsked, Args=%v", args.CandidateId, args.String())

	// 复位回复值
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	/* 对齐 Term */
	// 如果  RPC请求节点任期  <  自身节点任期  则报错
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Reject voted, higher term, T%d>T%d", args.CandidateId, rf.currentTerm, args.Term)
		return
	}

	// 如果  RPC请求节点任期  >  自身节点任期  则自身对齐 Term
	// 为什么不 >= 而是 > ？ 因为当两个节点均超时进行选举，我们是允许互相发送投票 RPC，因为在 PRC 提供方已经投票，在内部已经处理
	if args.Term > rf.currentTerm {
		// 降级
		rf.becomeFollowerLocked(args.Term)
	}

	// 如果已经投给其他人
	if rf.votedFor != -1 {
		LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Reject voted, Already voted to s%d", args.CandidateId, rf.votedFor)
		return
	}

	// 检查候选者是否更新（任期是否大）
	if rf.isMoreUpToDateLocked(args.LastLogIndex, args.LastLogTerm) {
		LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Reject voted, Candidate less up-to-date", args.CandidateId)
		return
	}

	// 将票投给请求节点，并且更新自身心跳定时器
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.persistLocked()
	rf.resetElectionTimerLocked()
	LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Vote granted", args.CandidateId)
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

// 集群内除自己之外所有其他节点发送 RPC,告诉对方给自己投票
func (rf *Raft) starElection(term int) {

	// 自身节点所收集票数
	votes := 0

	askVoteFromPeer := func(peer int, args *RequestVoteArgs) {

		// 封装 PRC 接收方所接受的消息包
		reply := &RequestVoteReply{}

		// RPC 的调用
		ok := rf.sendRequestVote(peer, args, reply)

		// 处理 reponse
		// 加锁并在函数执行完之前解锁
		rf.mu.Lock()
		defer rf.mu.Unlock()

		// 如果 RPC 调用失败
		if !ok {
			LOG(rf.me, rf.currentTerm, DDebug, "Ask vote from S%d, Lost or error", peer)
			return
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, AskVote Reply=%v", peer, reply.String())

		// 如果发现其他节点的任期高于自己，则进行降级操作(状态转移)
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		// 由于程序是在并发情况下执行的，可能存在执行到此时因为并发而发送了状态转移或者任期变化，所以需要判断
		if rf.contextLostLocked(Candidate, term) {
			LOG(rf.me, rf.currentTerm, DVote, "Lost context, abort ReqyesVoteReply for S%d", peer)
			return
		}

		// 如果投票节点确定给发送 RPC 的 Leader 投票
		if reply.VoteGranted {
			votes++

			// 当选举票超过半数以上，Candidate 立刻晋升 Leader
			if votes > len(rf.peers)/2 {

				rf.becomeLeaderLocked()

				// 当该节点晋升 Leader 后，开始执行 心跳逻辑
				go rf.replicationTicker(term)
			}
		}
	}

	// 加锁并在函数执行完之前解锁
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.contextLostLocked(Candidate, term) {
		LOG(rf.me, rf.currentTerm, DVote, "Lost Candidate to %s, abort ReqyesVote", rf.role)
		return
	}

	lastIdx, lastTerm := rf.log.last()
	// 遍历集群中所有节点并调用 RPC 告知其余节点给自己投票
	for peer := 0; peer < len(rf.peers); peer++ {

		// 如果遍历到自己则直接投票
		if peer == rf.me {
			votes++
			continue
		}

		// 封装 RPC 调用的参数
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastIdx,
			LastLogTerm:  lastTerm,
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, AskVote, %v", peer, args.String())

		// 丢给协程执行 RPC 调用，因为 RPC 调用后会阻塞于此等待 OK 的返回，所以可能回影响效率，所以丢给协程解决
		go askVoteFromPeer(peer, args)
	}
}

// 节点自身的循环，Follower 节点需要定时接受到 Candidate or Leader 的消息，如果触发该节点进行选举
func (rf *Raft) electionTicker() {
	for !rf.killed() {

		// Your code here (PartA)
		// Check if a leader election should be started.
		// 检查是否应启动领导者选举。

		rf.mu.Lock()

		// 如果自身不是 Leader 且 选举超时，则发起选举
		if rf.role != Leader && rf.isElectionTimeoutLocked() {
			// 切换状态
			rf.becomeCandidateLocked()
			// 集群内除自己之外所有其他节点发送 RPC,告诉对方给自己投票
			go rf.starElection(rf.currentTerm)
		}

		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350 milliseconds.
		// 在 50 到 350 之间随机暂停毫秒。
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
