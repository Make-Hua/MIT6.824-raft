package raft

/* 状态持久化函数 */

import (
	"bytes"
	"course/labgob"
	"fmt"
)

func (rf *Raft) persistString() string {
	return fmt.Sprintf("T%d, VoteFor: %d, Log:[0: %d)", rf.currentTerm, rf.votedFor, rf.log.size())
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).

// labC 实现
// 用于将 Raft 节点的关键状态信息持久化保存
func (rf *Raft) persistLocked() {
	// Your code here (PartC).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	// 创建一个字节缓冲区，用于暂存要持久化的数据
	w := new(bytes.Buffer)

	// 创建一个 labgob 编码器，用于将 Go 语言中的数据结构编码成字节流，以便后续能够保存到磁盘或者其他持久化存储介质中。
	e := labgob.NewEncoder(w)

	// 使用编码器将当前节点的当前任期（rf.currentTerm）信息编码并写入字节缓冲区
	e.Encode(rf.currentTerm)

	// 同样地，将当前节点投票给的节点标识（rf.votedFor）进行编码并写入字节缓冲区，记录节点的投票相关状态，
	e.Encode(rf.votedFor)

	// 1
	rf.log.persist(e)

	// 获取字节缓冲区中的字节数据，这些数据就是经过编码后的包含节点状态信息的字节流
	raftstate := w.Bytes()

	// 调用 rf.persister 的 Save 方法，将包含节点状态信息的字节流（raftstate）保存起来
	// 111
	rf.persister.Save(raftstate, rf.log.snapshot)

	LOG(rf.me, rf.currentTerm, DPersist, "Persist: %v", rf.persistString())
}

// restore previously persisted state.
// 用于从之前保存的持久化数据中恢复 Raft 节点的状态信息
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (PartC).
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

	// 创建一个字节缓冲区，并将传入的持久化数据（data）作为其初始内容，这样就可以通过这个缓冲区来按顺序读取其中的数据进行解析操作，
	r := bytes.NewBuffer(data)
	// 创建一个 labgob 解码器，用于将字节缓冲区中的字节流数据解码还原成对应的 Go 语言数据结构，
	d := labgob.NewDecoder(r)

	// 定义变量用于存储即将从持久化数据中解析还原出来的当前任期信息
	var currentTerm int

	// 使用解码器尝试从字节缓冲区中解码出当前任期信息，并将解码后的值赋给 currentTerm 变量。
	// 如果解码过程出现错误（比如数据格式不匹配等原因导致无法正确解码），则输出相应的日志信息记录解码当前任期的错误情况，然后直接返回，不再进行后续的解码操作，
	if err := d.Decode(&currentTerm); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Read currenTerm error: %v", err)
		return
	}
	// 将成功解码得到的当前任期信息赋值给当前节点的 currentTerm 字段，完成对节点当前任期状态的恢复操作，
	rf.currentTerm = currentTerm

	// 定义变量用于存储即将从持久化数据中解析还原出来的投票给的节点标识信息
	var votedFor int

	// 同样地，使用解码器尝试从字节缓冲区中解码出投票给的节点标识信息，若出现解码错误，输出相应日志并返回，
	if err := d.Decode(&votedFor); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Read votedFor error: %v", err)
		return
	}
	// 将解码得到的投票给的节点标识赋值给当前节点的 votedFor 字段，恢复节点的投票状态，
	rf.votedFor = votedFor

	// 使用解码器尝试从字节缓冲区中解码出日志信息
	if err := rf.log.readPersist(d); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Read log error: %v", err)
		return
	}
	rf.log.snapshot = rf.persister.ReadSnapshot()

	// 111

	if rf.log.snapLastIdx > rf.commitIndex {
		rf.commitIndex = rf.log.snapLastIdx
		rf.lastApplied = rf.log.snapLastIdx
	}

	LOG(rf.me, rf.currentTerm, DPersist, "Read from persist: %v", rf.persistString())
}
