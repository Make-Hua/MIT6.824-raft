package shardkv

import (
	"course/shardctrler"
	"time"
)

// ConfigCommand 函数用于将命令通过 Raft 进行同步并等待结果
func (kv *ShardKV) ConfigCommand(commnd RaftCommand, reply *OpReply) {

	// 调用 raft 的 Start 方法，将命令存储到 raft 日志中并进行同步，返回日志索引、任期和是否为 Leader 的信息
	index, _, isLeader := kv.rf.Start(commnd)

	// 如果当前节点不是 Leader，设置错误信息并返回
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 等待操作结果
	kv.mu.Lock()

	// 获取与该日志索引对应的通知通道
	notifyCh := kv.getNotifyChannel(index)
	kv.mu.Unlock()

	// 使用 select 语句等待结果或超时
	select {

	// 从通知通道接收到结果
	case result := <-notifyCh:

		// 将结果中的值和错误信息设置到回复中
		reply.Value = result.Value
		reply.Err = result.Err

		// 等待超时
	case <-time.After(ClientRequestTimeout):

		// 设置错误为超时
		reply.Err = ErrTimeout
	}

	// 启动一个 goroutine 来删除通知通道，释放资源
	go func() {
		kv.mu.Lock()

		// 删除与该日志索引对应的通知通道
		kv.removeNotifyChannel(index)
		kv.mu.Unlock()
	}()
}

// handleConfigChangeMessage 函数根据不同的命令类型处理配置变更消息
func (kv *ShardKV) handleConfigChangeMessage(command RaftCommand) *OpReply {

	// 根据命令类型进行分支处理
	switch command.CmdType {
	case ConfigChange:

		// 将命令数据转换为分片控制器的配置类型
		newConfig := command.Data.(shardctrler.Config)

		// 应用新的配置并返回结果
		return kv.applyNewConfig(newConfig)
	case ShardMigration:

		// 将命令数据转换为分片操作回复类型
		shardData := command.Data.(ShardOperationReply)

		// 应用分片迁移操作并返回结果
		return kv.applyShardMigration(&shardData)
	case ShardGC:

		// 将命令数据转换为分片操作参数类型
		shardsInfo := command.Data.(ShardOperationArgs)

		// 应用分片垃圾回收操作并返回结果
		return kv.applyShardGC(&shardsInfo)
	default:

		// 如果遇到未知的命令类型，抛出异常
		panic("unknown config change type")
	}
}

// applyNewConfig 函数用于应用新的配置
func (kv *ShardKV) applyNewConfig(newConfig shardctrler.Config) *OpReply {

	// 检查新配置的编号是否是当前配置编号的下一个
	if kv.currentConfig.Num+1 == newConfig.Num {

		// 遍历所有分片
		for i := 0; i < shardctrler.NShards; i++ {

			// 如果当前配置中该分片不属于当前组，而新配置中属于当前组
			if kv.currentConfig.Shards[i] != kv.gid && newConfig.Shards[i] == kv.gid {

				// 该分片需要迁移进来
				gid := kv.currentConfig.Shards[i]
				if gid != 0 {

					// 将该分片的状态设置为 MoveIn（移入）
					kv.shards[i].Status = MoveIn
				}
			}

			// 如果当前配置中该分片属于当前组，而新配置中不属于当前组
			if kv.currentConfig.Shards[i] == kv.gid && newConfig.Shards[i] != kv.gid {

				// 该分片需要迁移出去
				gid := newConfig.Shards[i]
				if gid != 0 {

					// 将该分片的状态设置为 MoveOut（移出）
					kv.shards[i].Status = MoveOut
				}
			}
		}

		// 更新上一个配置为当前配置
		kv.prevConfig = kv.currentConfig

		// 更新当前配置为新配置
		kv.currentConfig = newConfig

		// 返回操作成功的回复
		return &OpReply{Err: OK}
	}
	// 如果新配置编号不符合预期，返回错误配置的回复
	return &OpReply{Err: ErrWrongConfig}
}

// applyShardMigration 函数用于应用分片迁移操作
func (kv *ShardKV) applyShardMigration(shardDataReply *ShardOperationReply) *OpReply {

	// 检查分片数据回复的配置编号是否与当前配置编号一致
	if shardDataReply.ConfigNum == kv.currentConfig.Num {

		// 遍历分片数据回复中的每个分片
		for shardId, shardData := range shardDataReply.ShardData {

			// 获取当前节点中对应分片对应的状态机
			shard := kv.shards[shardId]

			// 如果该分片的状态是 MoveIn（移入）
			if shard.Status == MoveIn {

				// 将接收到的分片数据存储到当前分片的 KV 存储中
				for k, v := range shardData {
					shard.KV[k] = v
				}

				// 将该分片的状态设置为 GC（等待清理）
				shard.Status = GC
			} else {

				// 如果分片状态不是 MoveIn，跳出循环
				break
			}
		}

		// 拷贝去重表数据
		for clientId, dupTable := range shardDataReply.DuplicateTable {
			table, ok := kv.duplicateTable[clientId]
			if !ok || table.SeqId < dupTable.SeqId {

				// 更新去重表中的数据
				kv.duplicateTable[clientId] = dupTable
			}
		}
	}

	// 返回错误配置的回复（这里可能存在逻辑问题，应该根据实际情况调整，比如如果操作成功应该返回 OK）
	return &OpReply{Err: ErrWrongConfig}
}

// applyShardGC 函数用于应用分片垃圾回收操作
func (kv *ShardKV) applyShardGC(shardsInfo *ShardOperationArgs) *OpReply {

	// 检查分片操作参数的配置编号是否与当前配置编号一致
	if shardsInfo.ConfigNum == kv.currentConfig.Num {

		// 遍历需要进行垃圾回收的分片编号列表
		for _, shardId := range shardsInfo.ShardIds {

			// 获取当前节点中对应的分片
			shard := kv.shards[shardId]

			// 如果该分片的状态是 GC（等待清理）
			if shard.Status == GC {

				// 将该分片的状态设置为 Normal（正常）
				shard.Status = Normal
			} else if shard.Status == MoveOut {

				// 如果该分片的状态是 MoveOut（移出），重置该分片为新的内存 KV 状态机
				kv.shards[shardId] = NewMemoryKVStateMachine()
			} else {

				// 如果分片状态不符合预期，跳出循环
				break
			}
		}
	}

	// 返回操作成功的回复
	return &OpReply{Err: OK}
}
