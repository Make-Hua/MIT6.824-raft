package raft

import (
	"fmt"
	"sort"
	"time"
)

// 表示 Raft 算法中每个节点本地存储的日志条目信息
type LogEntry struct {
	Term         int         // 表示该日志条目所属的任期号
	CommandValid bool        // 用于标识当前日志条目中所包含的 Command 是否有效
	Command      interface{} // 用于存储具体的操作命令内容
}

// AppendEntriesArgs结构体用于定义AppendEntries（追加日志条目）RPC的参数结构。
// RPC 请求参数
type AppendEntriesArgs struct {
	Term     int /* Term字段表示领导者节点当前所处的任期号 */
	LeaderId int /* LeaderId字段代表发起AppendEntries请求的领导者节点的唯一标识 */

	PrevLogIndex int        /* 用于指定本次要追加的日志条目中，前一条日志在领导者节点自身日志中的索引位置 */
	PrevLogTerm  int        /* 用于指定在领导者节点自身日志中，PrevLogIndex 位置的日志条目的任期号 */
	Entries      []LogEntry /* 用于存储本次要追加到跟随者节点日志中的所有日志条目内容 */

	LeaderCommit int /* 领导者节点已经提交的日志索引位置 */
}

// 日志相关
func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, Prev:[%d]T[%d], (%d, %d], CommitIdx： %d",
		args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm,
		args.PrevLogIndex, args.PrevLogIndex+len(args.Entries), args.LeaderCommit)
}

// RPC 回复值
type AppendEntriesReply struct {
	Term    int  /* Term字段表示接收请求的节点（跟随者节点）当前所处的任期号 */
	Success bool /* 领导者节点本次的日志追加请求是否成功被接收并处理 */

	/* 优化日志回退所添加变量 */
	ConfilictIndex int /* 跟随者节点与领导者节点出现冲突位置索引 */
	ConfilictTerm  int /* 跟随者节点与领导者节点出现冲突位置的任期号 */
}

// 日志相关
func (reply *AppendEntriesReply) String() string {
	return fmt.Sprintf("T%d, Sucess:%v, ConfilicTerm: [%d]T[%d]", reply.Term, reply.Success, reply.ConfilictIndex, reply.ConfilictTerm)
}

// RPC注册方 心跳函数实现
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	// 加锁并在函数执行完之前解锁
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Appended, Args=%v", args.LeaderId, args.String())

	reply.Term = rf.currentTerm
	reply.Success = false

	/* 对齐 term */
	// 如果  RPC 调用方任期   <   RPC 提供方   则不响应
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Higher term, T%d<T%d", args.LeaderId, args.Term, rf.currentTerm)
		return
	}

	// 如果  RPC请求节点任期  >=  自身节点任期  则自身对齐 Term，不能同时出现两个 Leader，因为当前是 日志同步，此时已经有 Leader
	if args.Term >= rf.currentTerm {
		// 降级
		rf.becomeFollowerLocked(args.Term)
	}

	defer func() {
		rf.resetElectionTimerLocked()
		if !reply.Success {
			LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Follower Confilict: [%d]T[%d]", args.LeaderId, reply.ConfilictIndex, reply.ConfilictTerm)
			LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Follower log=%v", args.LeaderId, rf.log.String())
		}
	}()

	/* prot B 实现逻辑 */
	// 检查领导者节点指定的前一条日志索引是否超出了当前跟随者节点日志的长度
	if args.PrevLogIndex >= rf.log.size() {

		// 如果日志长度出错，则直接赋值异常值
		reply.ConfilictTerm = InvalidTerm
		reply.ConfilictIndex = rf.log.size()

		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Follower log too short, len:%d < Prev:%d", args.LeaderId, rf.log.size(), args.PrevLogIndex)
		return
	}

	// 追加日志处小于日志快照最后的下标，说明要追加日志已经被快照存储，则直接进行更新
	if args.PrevLogIndex < rf.log.snapLastIdx {
		reply.ConfilictTerm = rf.log.snapLastTerm
		reply.ConfilictIndex = rf.log.snapLastIdx
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Follower log truncated in %d", args.LeaderId, rf.log.snapLastIdx)
		return
	}

	// 判断当前需要增加日志的 Follower 节点最后一条日志的任期是否跟当前相同
	if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {

		// 由于需要日志回退，则提前记录 Follower 节点的 Term 和第一个记录该任期位置的 Term
		reply.ConfilictTerm = rf.log.at(args.PrevLogIndex).Term
		reply.ConfilictIndex = rf.log.firstFor(reply.ConfilictTerm)

		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Prev log not match, [%d]: T%d != T%d", args.LeaderId, args.PrevLogIndex, rf.log.at(args.PrevLogIndex).Term, args.PrevLogTerm)
		return
	}

	// 切片左开右闭，将 Leader 发送内容添加到 log[PrevLogIndex + 1] 处
	rf.log.appendFrom(args.PrevLogIndex, args.Entries)
	rf.persistLocked()

	// 日志追加发送成功
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DLog2, "Follower accept logs: (%d, %d)", args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))

	// 如果 Leader 提交某条消息则默认 Follower 节点提交，所以需要更新
	if args.LeaderCommit > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DApply, "Follower Update the commit index %d->%d", rf.commitIndex, args.LeaderCommit)

		// 更新
		rf.commitIndex = args.LeaderCommit

		// 如果更新则告诉 Wait
		rf.applyCond.Signal()
	}

}

// RPC 调用方调用函数
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 获取基于已匹配日志索引（matchIndex）的多数派索引值，返回值可以作为判断日志是否达到多数派确认、是否可以进行提交等操作的依据
func (rf *Raft) getMajorityIndexLocked() int {

	// 用于复制并存储当前节点记录的各个跟随者节点的已匹配日志索引（matchIndex）数据，方便后续对这些索引进行排序等操作。
	tmpIndexes := make([]int, len(rf.peers))

	// 将已经同步的日志 cp 到 tmp 中
	copy(tmpIndexes, rf.matchIndex)

	// 对 tmpIndexes 切片中的元素按照从小到大的顺序进行排序
	sort.Ints(sort.IntSlice(tmpIndexes))

	/*
		例如：Leader 节点记录 4 4 5 5 4 6 4 5
		sort   4 4 4 4 5 5 5 6
		说明已经有大部分节点的日志更新到 5 了
	*/
	// 计算多数派索引的位置，在 Raft 算法的多数派判断逻辑中，通常认为超过一半（向上取整）的节点达成一致就算多数派。
	// 这里通过计算 (len(rf.peers) - 1) / 2 来确定这个多数派索引在排序后的 tmpIndexes 切片中的位置，
	// 后续可以通过这个位置获取对应的索引值作为多数派索引。
	majorityIdx := (len(rf.peers) - 1) / 2

	LOG(rf.me, rf.currentTerm, DDebug, "Match index after sort: %v, maiority[%d]=[%d]", tmpIndexes, majorityIdx, tmpIndexes[majorityIdx])

	// 返回排序后的 tmpIndexes 切片中处于多数派位置的索引值，这个值可以作为判断日志是否达到多数派确认、是否可以进行提交等操作的依据
	return tmpIndexes[majorityIdx]
}

/* 启动日志复制流程（定时给其他节点发送心跳包） */
func (rf *Raft) startReplication(term int) bool {

	// 定义一个名为 replicateToPee 的匿名函数，该函数用于向单个指定的跟随者节点发送日志复制相关的 AppendEntries RPC 请求，并处理相应的回复情况。
	replicateToPeer := func(peer int, args *AppendEntriesArgs) {

		// reply 接收来自目标跟随者节点对 AppendEntries RPC 请求的回复信息
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, reply)

		// 加锁并在函数执行完之前解锁
		rf.mu.Lock()
		defer rf.mu.Unlock()

		// RPC 是否调用成功
		if !ok {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Lost or crashed", peer)
			return
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Append, Reply=%v", peer, reply.String())

		// 对齐 Term
		// 如果调用方任期号 大于 该节点，则自动降级
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		// 因为在等 RPC 的中途可能出现状态转换或者任期变化，所以需要判断
		if rf.contextLostLocked(Leader, term) {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Context Lost, T%d:%d->T%d:leader->T%d", peer, term, rf.currentTerm, rf.role)
			return
		}

		// 如果 Leader 本次所发送的日志追加失败，则处理日志回退相关逻辑
		if !reply.Success {

			/*
				idx, term := args.PrevLogIndex, args.PrevLogTerm
				// 日志 idx 回退
				for idx > 0 && rf.log[idx].Term == term {
					idx--
				}
				// 重置 nextIndex
				rf.nextIndex[peer] = idx + 1
			*/

			prevIndex := rf.nextIndex[peer]

			// 如果 Follower 节点的任期为异常值
			if reply.ConfilictTerm == InvalidTerm {

				// 则更新当前 Leader 节点的 next 为 ConfilictIndex
				rf.nextIndex[peer] = reply.ConfilictIndex
			} else {

				// 找到 Leader 节点中第一条与 reply.ConfilictTerm 相同任期的日志索引
				firstIndex := rf.log.firstFor(reply.ConfilictTerm)

				// 如果不为异常值
				if firstIndex != InvalidIndex {

					// 则更新
					rf.nextIndex[peer] = firstIndex
				} else {

					// 更新 Leader 节点的 nextIndex 为 返回 ConfilictIndex
					rf.nextIndex[peer] = reply.ConfilictIndex
				}
			}

			// 最后判断当前更新后的 nextIndex 如果还要大于未更新前的 nextIndex，则复原
			if rf.nextIndex[peer] > prevIndex {
				rf.nextIndex[peer] = prevIndex
			}

			nextPrevIndex := rf.nextIndex[peer] - 1
			nextPrevTerm := InvalidTerm
			if nextPrevIndex >= rf.log.snapLastIdx {
				nextPrevTerm = rf.log.at(nextPrevIndex).Term
			}

			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Not matched at Prev=[%d]T[%d], Try next Prev=[%d]T[%d]",
				peer, args.PrevLogIndex, args.PrevLogTerm, nextPrevIndex, nextPrevTerm)
			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Leader log=%v", peer, rf.log.String())
			return
		}

		// Leader 追加日志成功，更新 idx
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		// 获取当前多数 Follower 节点已经更新到哪了
		majorityMatched := rf.getMajorityIndexLocked()

		// 判断获取到的多数派索引值是否大于当前的提交索引同时需要保证任期相同，如果大了，说明又有新的操作可以提交给上层了，则此时触发 Wait
		if majorityMatched > rf.commitIndex && rf.log.at(majorityMatched).Term == rf.currentTerm {
			LOG(rf.me, rf.currentTerm, DApply, "Leader Update the commit index %d->%d", rf.commitIndex, majorityMatched)

			// 更新
			rf.commitIndex = majorityMatched

			// 告诉 applicationTicker 可以提交日志
			rf.applyCond.Signal()
		}

	}

	// 加锁并在函数执行完之前解锁
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 由于程序是在并发情况下执行的，可能存在执行到此时因为并发而发送了状态转移或者任期变化，所以需要判断
	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "Lost leader to %s[T%d]", term, rf.role, rf.currentTerm)
		return false
	}

	// 遍历集群中所有的节点，rf.peers 存储了所有节点的相关信息，索引表示节点在集群中的序号等标识信息
	for peer := 0; peer < len(rf.peers); peer++ {

		// 跳过自身请求
		if peer == rf.me {
			// 初始化自身 idx

			rf.matchIndex[peer] = rf.log.size() - 1
			rf.nextIndex[peer] = rf.log.size()
			continue
		}

		// 封装好 RPC 调用方所需要的函数参数
		prevIdx := rf.nextIndex[peer] - 1

		// 当集群中某个节点存储数据最后的索引还要比 Leader 节点已经快照的数据索引还要小，则直接将快照数据发给 Followed 节点添加快照
		if prevIdx < rf.log.snapLastIdx {
			args := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.log.snapLastIdx,
				LastIncludedTerm:  rf.log.snapLastTerm,
				Snapshot:          rf.log.snapshot,
			}

			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, SendSnap, Args=%v", peer, args.String())

			// 丢给协程执行 RPC 调用，因为 RPC 调用后会阻塞于此等待 OK 的返回，所以可能回影响效率，所以丢给协程解决
			go rf.installToPeer(peer, term, args)

			// 如果发送添加快照，则不需要发送同步日志 RPC
			continue
		}

		prevTerm := rf.log.at(prevIdx).Term
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      rf.log.tail(prevIdx + 1),
			LeaderCommit: rf.commitIndex,
		}

		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Append, Args=%v", peer, args.String())

		// 丢给协程执行 RPC 调用，因为 RPC 调用后会阻塞于此等待 OK 的返回，所以可能回影响效率，所以丢给协程解决
		go replicateToPeer(peer, args)
	}

	return true
}

// 函数实现了一个定时任务机制，用于周期性地触发心跳操作，日志复制操作
func (rf *Raft) replicationTicker(trem int) {
	for !rf.killed() {

		// 函数尝试启动日志复制流程
		ok := rf.startReplication(trem)
		if !ok {
			break
		}

		// 这样就实现了周期性地触发日志复制操作
		time.Sleep(replicateInterval)
	}
}
