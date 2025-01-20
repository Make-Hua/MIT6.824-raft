package raft

import (
	"course/labgob"
	"fmt"
)

// 日志结构
type RaftLog struct {
	snapLastIdx  int /* 表示快照（snapshot）所涵盖的最后一条日志的索引值 */
	snapLastTerm int /* 记录了与 snapLastIdx 对应的日志条目的任期号（term） */

	snapshot []byte     /* 存储了所有快照日志的实际内容  contains [0, snapLastIdx] */
	tailLog  []LogEntry /* 用于存储从snapLastIdx之后（不包含该索引）到实际日志末尾的所有日志条目  contains (snapLastIdx, snapLastIdx + len(tailLog) - 1] */
}

// 日志实例的构造函数
func NewLog(snapLastIdx, snapLastTerm int, snapshot []byte, entries []LogEntry) *RaftLog {

	// 建立实例
	rl := &RaftLog{
		snapLastIdx:  snapLastIdx,
		snapLastTerm: snapLastTerm,
		snapshot:     snapshot,
	}

	// 向 tailLog 切片中添加一个初始的日志条目，其任期设置为snapLastTerm
	rl.tailLog = append(rl.tailLog, LogEntry{
		Term: snapLastTerm,
	})

	rl.tailLog = append(rl.tailLog, entries...)

	return rl
}

// 该函数用于从给定的 labgob 解码器（d）中读取并解码数据，以恢复 RaftLog 对象的状态，
func (rl *RaftLog) readPersist(d *labgob.LabDecoder) error {

	// 尝试从解码器 d 中解码出表示日志最后索引的值，并将其赋给 lastIdx 变量
	var lastIdx int
	if err := d.Decode(&lastIdx); err != nil {
		return fmt.Errorf("decode last include index failed")
	}
	rl.snapLastIdx = lastIdx

	// 尝试从解码器 d 中解码出表示日志最后任期的值，并将其赋给 lastTerm 变量
	var lastTerm int
	if err := d.Decode(&lastTerm); err != nil {
		return fmt.Errorf("decode last include term failed")
	}
	rl.snapLastTerm = lastTerm

	// 尝试从解码器 d 中解码出表示实际日志条目的切片数据，并将其赋给 log 变量
	var log []LogEntry
	if err := d.Decode(&log); err != nil {
		return fmt.Errorf("decode last include index failed")
	}
	rl.tailLog = log

	return nil
}

// 用于将 RaftLog 对象的状态进行序列化，通过给定的 labgob 编码器（e）将日志的关键信息编码并保存
// 关键信息 ： 如快照的最后索引、最后任期以及实际日志条目等
func (rl *RaftLog) persist(e *labgob.LabEncoder) {
	e.Encode(rl.snapLastIdx)
	e.Encode(rl.snapLastTerm)
	e.Encode(rl.tailLog)
}

// snapshot 日志的结尾 + 还没持久化日志的长度：表示日志结构从开始到末尾的总长度
func (rl *RaftLog) size() int {
	return rl.snapLastIdx + len(rl.tailLog)
}

// 用于将逻辑索引（logicIdx）转换为实际在 tailLog 切片中的索引位置
// 因为我们在 RaftLog 日志外使用时并不知道哪些日志已经被压缩（持久化），所以我们使用时用的下标是按照全部日志来算的，
// 而 RaftLog 内部提供 idx 函数，就是将我们外部所使用的逻辑索引转化为 RaftLog 内部的实际索引位置
func (rl *RaftLog) idx(logicIdx int) int {

	// 如果超出界限
	if logicIdx < rl.snapLastIdx || logicIdx >= rl.size() {
		panic(fmt.Sprintf("%d is out of [%d, %d]", logicIdx, rl.snapLastIdx, rl.size()-1))
	}
	return logicIdx - rl.snapLastIdx
}

// 同 idx 函数一样的原因，所以在此封装 at 函数，用于访问日志 tailLog
func (rl *RaftLog) at(logicIdx int) LogEntry {
	return rl.tailLog[rl.idx(logicIdx)]
}

// 获取日志尾部下标以及对应的 Term
func (rl *RaftLog) last() (index, term int) {
	i := len(rl.tailLog) - 1
	return rl.snapLastIdx + i, rl.tailLog[i].Term
}

// firstLogFor 函数用于在当前 Raft 节点的日志（rf.log）中查找给定任期（term）对应的第一条日志的索引位置。
func (rl *RaftLog) firstFor(term int) int {
	// 开始遍历当前节点的日志切片 rf.log，这里使用 range 关键字，idx 表示日志条目的索引（从 0 开始计数），entry 表示每个日志条目对应的 LogEntry 结构体实例，包含了任期、命令有效性、命令内容等信息。
	for idx, entry := range rl.tailLog {
		// 检查当前遍历到的日志条目的任期（entry.Term）是否与传入的给定任期（term）相等，如果相等，说明找到了对应任期的日志条目，直接返回该条目的索引 idx，因为这个索引就是我们要找的给定任期的第一条日志在日志切片中的位置。
		if entry.Term == term {
			return idx + rl.snapLastIdx
		} else if entry.Term > term {
			// 如果当前日志条目的任期大于传入的给定任期，意味着已经遍历到了任期比目标任期更大的日志，那么在当前节点的日志中，给定任期的日志肯定是在此之前，所以不需要再继续往后遍历了，直接通过 break 语句跳出循环。
			break
		}
	}
	return InvalidIndex

	/* 二分查找 生产环境适用 */
	/*
		Len := len(rf.log)
		l, r := -1, Len

		if rf.log[Len-1].Term < term || Len == 0 {
			return InvalidIndex
		}

		if Len == 1 {
			if term == rf.log[0].Term {
				return 0
			} else {
				return InvalidIndex
			}
		}

		for l+1 != r {
			mid := l + (r-l)/2
			if rf.log[mid].Term >= term {
				r = mid
			} else {
				l = mid
			}
		}

		if rf.log[l+1].Term == term {
			return l + 1
		}
		return InvalidIndex
	*/
}

// 放回从该 idx 位置一直到结尾的切片
func (rl *RaftLog) tail(startIdx int) []LogEntry {

	if startIdx >= rl.size() {
		return nil
	}

	return rl.tailLog[rl.idx(startIdx):]
}

// 向 tailLog 添加一条实例
func (rl *RaftLog) append(e LogEntry) {
	rl.tailLog = append(rl.tailLog, e)
}

// 向 tailLog 添加多条实例
func (rl *RaftLog) appendFrom(logicPrevIndex int, entries []LogEntry) {
	rl.tailLog = append(rl.tailLog[:rl.idx(logicPrevIndex)+1], entries...)
}

// 日志相关
func (rl *RaftLog) String() string {

	var terms string
	prevTerm := rl.snapLastTerm
	prevStart := 0
	for i := 0; i < len(rl.tailLog); i++ {
		if rl.tailLog[i].Term != prevTerm {
			terms += fmt.Sprintf(" [%d, %d]T%d", prevStart, rl.snapLastIdx+i-1, prevTerm)
			prevTerm = rl.tailLog[i].Term
			prevStart = i
		}
	}

	terms += fmt.Sprintf(" [%d, %d]T%d", prevStart, rl.snapLastIdx+len(rl.tailLog)-1, prevTerm)
	return terms
}

// snapshot in the index
// 用于在指定的索引（index）位置创建一个新的快照（snapshot）
func (rl *RaftLog) doSnapshot(index int, snapshot []byte) {

	if index <= rl.snapLastIdx {
		return
	}

	idx := rl.idx(index)

	rl.snapLastIdx = index
	rl.snapLastTerm = rl.tailLog[idx].Term
	rl.snapshot = snapshot

	// make a new log array
	// 创建新的快照 snapshot, 如何在新快照中添加需要快照的数据
	newLog := make([]LogEntry, 0, rl.size()-rl.snapLastIdx)
	newLog = append(newLog, LogEntry{
		Term: rl.snapLastTerm,
	})

	// 最后将新快照加入原本的快照日志中
	newLog = append(newLog, rl.tailLog[idx+1:]...)
	rl.tailLog = newLog

}

// 用于在 Raft 日志结构（RaftLog）中安装一个新的快照
func (rl *RaftLog) installSnapshot(index, term int, snapshot []byte) {

	rl.snapLastIdx = index
	rl.snapLastTerm = term
	rl.snapshot = snapshot

	// make a new log array
	// 创建新的快照 snapshot, 如何在新快照中添加需要快照的数据
	newLog := make([]LogEntry, 0, 1)
	newLog = append(newLog, LogEntry{
		Term: rl.snapLastTerm,
	})

	rl.tailLog = newLog

}
