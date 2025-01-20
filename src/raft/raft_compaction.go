package raft

import "fmt"

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// 用于在 Raft 节点上创建一个新的快照
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 调用接口，在指定 index 位置创建快照
	rf.log.doSnapshot(index, snapshot)
	rf.persistLocked()
}

// RPC 请求参数值
type InstallSnapshotArgs struct {
	// Your data here (PartA, PartB).
	Term     int /* Term 字段表示发起请求投票的节点所处的任期号 */
	LeaderId int /* LeaderId 字段表示发起安装快照请求的领导者节点的唯一标识 */

	LastIncludedIndex int /* LastIncludedIndex 表示要安装的快照中所涵盖的最后一条日志的索引值 */
	LastIncludedTerm  int /* LastIncludedTerm  表示要安装的快照中所涵盖的最后一条日志的任期号 */

	/* 在此当作最优情况解决，也就是一次 RPC 的可以把快照都发过去，但是实际工程实践，可能一次发不过去，需要考虑多次发送 */
	Snapshot []byte /* 需要安装快照 */
}

// 日志相关
func (args *InstallSnapshotArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, Last: [%d]T[%d]", args.LeaderId, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)
}

// 本 RPC 是单向 RPC，所以不需要返回值，但是需要 Term 对齐任期号
type InstallSnapshotReply struct {
	Term int /* Term 字段表示发起请求投票的节点所处的任期号 */
}

// 日志相关
func (reply *InstallSnapshotReply) String() string {
	return fmt.Sprintf("T%d", reply.Term)
}

// 具体 RPC 实现函数；用于处理领导者节点发送过来的快照安装请求
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

	// 加锁并在函数执行完之前解锁
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, RecvSnapshot, Args=%v", args.LeaderId, args.String())

	reply.Term = rf.currentTerm

	// RPC 固定判断格式
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DSnap, "<- S%d, Reject Snap, Higher Term：T%d>%d", args.LeaderId, rf.currentTerm, args.Term)
		return
	}

	// RPC 固定判断格式
	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	// check if there is already a snapshot contains the one in the RPC
	if rf.log.snapLastIdx >= args.LastIncludedIndex {
		LOG(rf.me, rf.currentTerm, DSnap, "<- S%d, Reject Snap, Already installed: %d>%d", args.LeaderId, rf.log.snapLastIdx, args.LastIncludedIndex)
	}

	// 将 Leader 节点的所有快照全部发送至对端节点，这样就能同步内容
	rf.log.installSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Snapshot)
	rf.persistLocked()
	rf.snapPending = true
	rf.applyCond.Signal()
}

// RPC 调用方调用接口
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// RPC 调用方执行函数，用来执行 Leader 发送添加日志快照 PRC
func (rf *Raft) installToPeer(peer, term int, args *InstallSnapshotArgs) {

	// reply 接收来自目标跟随者节点对 AppendEntries RPC 请求的回复信息
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(peer, args, reply)

	// 加锁并在函数执行完之前解锁
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// RPC 是否调用成功
	if !ok {
		LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Lost or crashed", peer)
		return
	}
	LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, SnapFunc, Reply=%v", peer, reply.String())

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

	// 如果发现添加日志快照最后索引还要比跟随者节点记录的已同步日志索引大，则对 matchIndex 和 nextIndex 进行更新
	// 因为已经 Leader 已经将日志快照发送给我且本地已经添加快照成功
	if args.LastIncludedIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludedIndex
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
	}
}
