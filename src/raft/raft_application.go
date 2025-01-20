package raft

// applicationTicker 函数用于处理将已提交的日志应用到状态机的相关逻辑，它会在一个循环中持续运行，直到 Raft 节点被标记为停止（killed）状态。
func (rf *Raft) applicationTicker() {
	for !rf.killed() {
		// 获取互斥锁
		rf.mu.Lock()

		// 调用 applyCond.Wait 方法，这个操作会先释放当前持有的互斥锁（rf.mu），然后阻塞当前协程，等待其他地方通过 Signal 方法唤醒它。
		// 当被唤醒后，它会再次尝试获取互斥锁（rf.mu），获取成功后才能继续执行后续的操作。
		// 具体的 Signal 操作位于 raft_replication.go 文件中，通常是在某些条件满足时（比如有新的日志达到提交条件，更新了 commitIndex）被调用，以此来通知这里可以进行日志应用的相关操作了。
		rf.applyCond.Wait()

		// 创建一个空的 LogEntry 切片，用于存储从 lastApplied + 1 到 commitIndex 索引范围内的日志条目，这些日志就是接下来要应用到状态机的日志。
		entries := make([]LogEntry, 0)

		// 判断是否有快照安装操作需要处理
		snapPendingApply := rf.snapPending

		if !snapPendingApply {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				entries = append(entries, rf.log.at(i))
			}
		}
		rf.mu.Unlock()

		// 将已经同步的数据传给上层
		if !snapPendingApply {

			// 遍历 entries 切片，将每个日志条目对应的信息封装成 ApplyMsg 结构体，并通过 applyCh 通道发送出去。
			// 这里的接口可以向上层传递已经同步好的日志并应用，例如：如果是分布式 kvDB ，上层就可以知道数据库执行了哪些操作
			for i, entry := range entries {
				rf.applyCh <- ApplyMsg{
					CommandValid: entry.CommandValid,
					Command:      entry.Command,
					CommandIndex: rf.lastApplied + 1 + i,
				}
			}
		} else {

			// 将快照安装到上层
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.log.snapshot,
				SnapshotIndex: rf.log.snapLastIdx,
				SnapshotTerm:  rf.log.snapLastTerm,
			}
		}

		// 再次获取互斥锁，写日志需要加锁
		rf.mu.Lock()

		if !snapPendingApply {
			LOG(rf.me, rf.currentTerm, DApply, "Apply log for [%d, %d]", rf.lastApplied+1, rf.lastApplied+len(entries))
			// 更新 lastApplied 字段，将其值增加此次应用的日志条目数量

			// 更新下标
			rf.lastApplied += len(entries)
		} else {
			LOG(rf.me, rf.currentTerm, DApply, "Apply snapshot for [0, %d]", rf.log.snapLastIdx)

			// 已经传给上层的日志的最大下标
			rf.lastApplied = rf.log.snapLastIdx

			// 被确认提交的索引如果还要小于已经传给上层的日志数据的最大下标，则更新
			// 因为如果已经传给上层就一定能保证集群大部分节点本地已经存了日志
			// 如果需要提交 则一定要大部分节点已经存储；
			// 如果已经传给上层，说明节点已经快照这些数据；
			if rf.commitIndex < rf.lastApplied {
				rf.commitIndex = rf.lastApplied
			}
			rf.snapPending = false
		}

		rf.mu.Unlock()
	}
}
