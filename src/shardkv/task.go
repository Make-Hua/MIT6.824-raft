package shardkv

import (
	"sync"
	"time"
)

// 处理 apply 任务
func (kv *ShardKV) applyTask() {

	// 只要 ShardKV 实例没有被停止，就持续循环
	for !kv.killed() {

		select {
		// 从 applyCh 通道接收消息
		case message := <-kv.applyCh:

			// 如果该消息是日志
			if message.CommandValid {

				// 加锁，保护共享资源
				kv.mu.Lock()

				// 如果当前消息的索引小于等于已经处理过的最后一条消息的索引，说明该消息已经处理过，直接忽略
				if message.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}

				// 更新最后应用的日志索引为当前消息的索引
				kv.lastApplied = message.CommandIndex

				var opReply *OpReply
				// 将消息中的命令转换为 RaftCommand 类型
				raftCommand := message.Command.(RaftCommand)

				// 如果 raft 层传来的是 client 端的操作命令
				if raftCommand.CmdType == ClientOpeartion {

					// 如果是客户端操作命令
					// 取出用户的操作信息
					op := raftCommand.Data.(Op)

					// 应用客户端操作并获取回复
					opReply = kv.applyClientOperation(op)

				} else {

					// 否则是其他类型的命令（如配置变更等）
					// 处理配置变更消息并获取回复
					opReply = kv.handleConfigChangeMessage(raftCommand)
				}

				// 如果当前节点是 Leader，将结果通过对应的通知通道发送回去
				if _, isLeader := kv.rf.GetState(); isLeader {
					notifyCh := kv.getNotifyChannel(message.CommandIndex)
					notifyCh <- opReply
				}

				// 判断是否需要进行 snapshot
				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
					// 如果设置了最大 raft 状态大小，并且当前 raft 状态大小达到或超过该值
					// 进行 snapshot 操作，记录当前状态
					kv.makeSnapshot(message.CommandIndex)
				}

				// 解锁，释放共享资源
				kv.mu.Unlock()

				// 如果接收到的是有效的 snapshot 消息
			} else if message.SnapshotValid {

				kv.mu.Lock()

				// 从 snapshot 中恢复状态
				kv.restoreFromSnapshot(message.Snapshot)

				// 更新最后应用的日志索引为 snapshot 的索引
				kv.lastApplied = message.SnapshotIndex
				kv.mu.Unlock()
			}
		}
	}
}

// 定时获取配置消息
func (kv *ShardKV) fetchConfigTask() {

	// 只要 ShardKV 实例没有被停止，就持续循环
	for !kv.killed() {

		// 如果当前节点是 Leader
		if _, isLeader := kv.rf.GetState(); isLeader {

			// 标记是否需要获取新配置
			needFetch := true
			kv.mu.Lock()

			// 检查所有分片的状态
			for _, shard := range kv.shards {

				// 如果有任何一个分片的状态不是 Normal
				// 说明前一个配置变更的任务正在进行中，不需要获取新配置
				if shard.Status != Normal {
					needFetch = false
					break
				}
			}

			// 获取当前配置的编号
			currentNum := kv.currentConfig.Num
			kv.mu.Unlock()

			// 如果需要获取新配置
			if needFetch {

				// 向 ShardCtrler 查询下一个配置
				newConfig := kv.mck.Query(currentNum + 1)

				// 如果查询到的新配置编号是当前配置编号加 1
				if newConfig.Num == currentNum+1 {

					// 将配置变更命令传入 raft 模块进行同步
					kv.ConfigCommand(RaftCommand{ConfigChange, newConfig}, &OpReply{})
				}
			}
		}

		// 等待一段时间后再次检查
		time.Sleep(FetchConfigInterval)
	}
}

// shardMigrationTask 函数负责处理分片迁移任务
func (kv *ShardKV) shardMigrationTask() {

	// 只要 ShardKV 实例没有被停止，就持续循环
	for !kv.killed() {

		// 如果当前节点是 Leader
		if _, isLeader := kv.rf.GetState(); isLeader {
			// 加锁，保护共享资源
			kv.mu.Lock()

			// 找到所有状态为 MoveIn（正在移入）的分片，并按组 ID 分组
			gidToShards := kv.getShardByStatus(MoveIn)

			// 创建一个 WaitGroup 来等待所有 goroutine 完成
			var wg sync.WaitGroup

			// 遍历每个组及其对应的分片列表
			for gid, shardIds := range gidToShards {

				// 为每个 goroutine 添加一个等待任务
				wg.Add(1)

				// 启动一个 goroutine 来处理每个组的分片迁移
				go func(servers []string, configNum int, shardIds []int) {

					defer wg.Done()

					// 构造获取分片数据的参数
					getShardArgs := ShardOperationArgs{configNum, shardIds}

					// 遍历该组中的每一个服务器节点
					for _, server := range servers {

						// 用于存储获取分片数据的回复
						var getShardReply ShardOperationReply

						// 创建与服务器节点的连接端点
						clientEnd := kv.make_end(server)

						// 调用远程方法 "ShardKV.GetShardsData" 获取分片数据
						ok := clientEnd.Call("ShardKV.GetShardsData", &getShardArgs, &getShardReply)

						// 如果成功获取到数据且没有错误
						if ok && getShardReply.Err == OK {

							// 将包含分片数据的迁移命令传入 Raft 模块进行同步
							kv.ConfigCommand(RaftCommand{ShardMigration, getShardReply}, &OpReply{})
						}
					}
				}(kv.prevConfig.Groups[gid], kv.currentConfig.Num, shardIds)
			}

			// 解锁，释放共享资源
			kv.mu.Unlock()

			// 等待所有 goroutine 完成
			wg.Wait()
		}

		// 等待一段时间后再次检查
		time.Sleep(ShardMigrationInterval)
	}
}

// shardGCTask 函数负责处理分片垃圾回收任务
func (kv *ShardKV) shardGCTask() {

	// 只要 ShardKV 实例没有被停止，就持续循环
	for !kv.killed() {

		// 如果当前节点是 Leader
		if _, isLeader := kv.rf.GetState(); isLeader {

			// 加锁，保护共享资源
			kv.mu.Lock()

			// 找到所有状态为 GC（正在进行垃圾回收）的分片，并按组 ID 分组
			gidToShards := kv.getShardByStatus(GC)

			// 创建一个 WaitGroup 来等待所有 goroutine 完成
			var wg sync.WaitGroup

			// 遍历每个组及其对应的分片列表
			for gid, shardIds := range gidToShards {

				// 为每个 goroutine 添加一个等待任务
				wg.Add(1)

				// 启动一个 goroutine 来处理每个组的分片垃圾回收
				go func(servers []string, configNum int, shardIds []int) {

					// 注意：这里原代码中 wg.Done() 位置错误，应该放在函数末尾
					defer wg.Done()

					// 构造删除分片数据的参数
					shardGCArgs := ShardOperationArgs{configNum, shardIds}

					// 遍历该组中的每一个服务器节点
					for _, server := range servers {

						// 用于存储删除分片数据的回复
						var shardGCReply ShardOperationReply

						// 创建与服务器节点的连接端点
						clientEnd := kv.make_end(server)

						// 调用远程方法 "ShardKV.DeleteShardsData" 删除分片数据
						ok := clientEnd.Call("ShardKV.DeleteShardsData", &shardGCArgs, &shardGCReply)

						// 如果成功删除且没有错误
						if ok && shardGCReply.Err == OK {

							// 将包含分片垃圾回收的命令传入 Raft 模块进行同步
							kv.ConfigCommand(RaftCommand{ShardGC, shardGCArgs}, &OpReply{})
						}
					}
				}(kv.prevConfig.Groups[gid], kv.currentConfig.Num, shardIds)
			}

			// 解锁，释放共享资源
			kv.mu.Unlock()
			// 等待所有 goroutine 完成
			wg.Wait()
		}

		// 等待一段时间后再次检查
		time.Sleep(ShardGCInterval)
	}
}

// getShardByStatus 函数根据指定的分片状态查找对应的分片
func (kv *ShardKV) getShardByStatus(status ShardStatus) map[int][]int {

	// 创建一个 map 用于存储按组 ID 分组的分片列表
	gidToShards := make(map[int][]int)

	// 遍历所有分片
	for i, shard := range kv.shards {

		// 如果分片的状态与指定状态相同
		if shard.Status == status {

			// 获取该分片原来所属的组 ID
			gid := kv.prevConfig.Shards[i]

			// 如果组 ID 不为 0
			if gid != 0 {
				// 如果该组 ID 还没有在 map 中，初始化其分片列表
				if _, ok := gidToShards[gid]; !ok {
					gidToShards[gid] = make([]int, 0)
				}

				// 将该分片的索引添加到对应组的分片列表中
				gidToShards[gid] = append(gidToShards[gid], i)
			}
		}
	}

	// 返回按组 ID 分组的分片列表
	return gidToShards
}

// GetShardsData 获取 shard 数据
func (kv *ShardKV) GetShardsData(args *ShardOperationArgs, reply *ShardOperationReply) {

	// 只有当前节点是 Leader 时才能处理该请求
	if _, isLeader := kv.rf.GetState(); !isLeader {

		// 如果不是 Leader，设置错误为 ErrWrongLeader 并返回
		reply.Err = ErrWrongLeader
		return
	}

	// 加锁，保护共享资源
	kv.mu.Lock()

	// 函数结束时解锁
	defer kv.mu.Unlock()

	// 检查当前 Group 的配置编号是否满足请求
	if kv.currentConfig.Num < args.ConfigNum {

		// 如果当前配置编号小于请求的配置编号，说明还未准备好
		reply.Err = ErrNotReady
		return
	}

	// 初始化回复中的分片数据
	reply.ShardData = make(map[int]map[string]string)

	// 遍历请求的分片 ID 列表
	for _, shardId := range args.ShardIds {

		// 拷贝每个分片的数据到回复中
		reply.ShardData[shardId] = kv.shards[shardId].copyData()
	}

	// 初始化回复中的去重表数据
	reply.DuplicateTable = make(map[int64]LastOperationInfo)

	// 遍历当前的去重表
	for clientId, op := range kv.duplicateTable {

		// 拷贝每个客户端的操作信息到回复中
		reply.DuplicateTable[clientId] = op.copyData()
	}

	// 设置回复中的配置编号和错误信息
	reply.ConfigNum, reply.Err = args.ConfigNum, OK
}

// DeleteShardsData 删除分片数据
func (kv *ShardKV) DeleteShardsData(args *ShardOperationArgs, reply *ShardOperationReply) {
	// 只有当前节点是 Leader 时才能处理该请求
	if _, isLeader := kv.rf.GetState(); !isLeader {
		// 如果不是 Leader，设置错误为 ErrWrongLeader 并返回
		reply.Err = ErrWrongLeader
		return
	}

	// 加锁，保护共享资源
	kv.mu.Lock()

	// 如果当前配置编号大于请求的配置编号，说明可以直接返回成功
	if kv.currentConfig.Num > args.ConfigNum {
		reply.Err = OK
		// 解锁
		kv.mu.Unlock()
		return
	}
	// 解锁
	kv.mu.Unlock()

	var opReply OpReply
	// 将包含分片垃圾回收的命令传入 Raft 模块进行同步
	kv.ConfigCommand(RaftCommand{ShardGC, *args}, &opReply)

	// 设置回复中的错误信息
	reply.Err = opReply.Err
}

func (kv *ShardKV) applyClientOperation(op Op) *OpReply {

	// 检查操作的键是否匹配当前组
	if kv.matchGroup(op.Key) {

		var opReply *OpReply

		// 如果操作不是获取操作，并且该请求是重复请求
		if op.OpType != OpGet && kv.requestDuplicated(op.ClientId, op.SeqId) {

			// 直接从去重表中获取之前的回复
			opReply = kv.duplicateTable[op.ClientId].Reply

		} else {

			// 计算操作对应的分片 ID
			shardId := key2shard(op.Key)

			// 将操作应用到状态机中
			opReply = kv.applyToStateMachine(op, shardId)

			// 如果操作不是获取操作，将本次操作信息记录到去重表中
			if op.OpType != OpGet {
				kv.duplicateTable[op.ClientId] = LastOperationInfo{
					SeqId: op.SeqId,
					Reply: opReply,
				}
			}
		}
		return opReply
	}

	// 如果键不匹配当前组，返回错误为 ErrWrongGroup
	return &OpReply{Err: ErrWrongGroup}
}
