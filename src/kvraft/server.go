package kvraft

import (
	"bytes"
	"course/labgob"
	"course/labrpc"
	"course/raft"
	"sync"
	"sync/atomic"
	"time"
)

// KVServer 结构体定义了 KV 服务器的相关信息
type KVServer struct {
	mu           sync.Mutex         /* 互斥锁，用于保护共享资源的访问 */
	me           int                /* 服务器的唯一标识 */
	rf           *raft.Raft         /* 指向 Raft 实例的指针，用于实现分布式一致性 */
	applyCh      chan raft.ApplyMsg /* 用于接收 Raft 层应用消息的通道 */
	dead         int32              /* 用于标记服务器是否已停止运行，由 Kill() 函数设置 */
	maxraftstate int                /* 当 Raft 日志增长到这个大小时，进行快照 */

	// 自定义的一些字段
	lastApplied    int                         /* 记录最后应用的日志索引 */
	stateMachine   *MemoryKVStateMachine       /* 用于存储和管理 KV 数据的状态机 */
	notifyChans    map[int]chan *OpReply       /* 用于存储等待处理结果的通道，键为日志索引 */
	duplicateTable map[int64]LastOperationInfo /* 用于记录客户端请求的重复信息，防止重复操作 */
}

// Get 函数是处理 Get RPC 请求的方法
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// 将请求存储到 raft 日志中并进行同步
	// 将操作添加到 Raft 日志中，并返回日志索引、任期和是否为 Leader
	index, _, isLeader := kv.rf.Start(Op{Key: args.Key, OpType: OpGet})

	// 如果不是 Leader 的话，直接返回错误
	if !isLeader {
		reply.Err = ErrWrongLeader // 设置错误为不是正确的 Leader
		return
	}

	// 等待结果
	kv.mu.Lock()
	// 获取与该日志索引对应的通知通道
	notifyCh := kv.getNotifyChannel(index)
	kv.mu.Unlock()

	// 使用 select 语句等待结果或超时
	select {
	case result := <-notifyCh: // 从通知通道接收到结果
		reply.Value = result.Value
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeout): // 等待超时
		reply.Err = ErrTimeout
	}

	// 启动一个 goroutine 来清理通知通道
	go func() {
		kv.mu.Lock()
		// 移除与该日志索引对应的通知通道
		kv.removeNotifyChannel(index)
		kv.mu.Unlock()
	}()
}

// 用于判断客户端的请求是否为重复请求
func (kv *KVServer) requestDuplicated(clientId, seqId int64) bool {
	// 从 duplicateTable 中获取与 clientId 对应的操作信息
	info, ok := kv.duplicateTable[clientId]

	// 如果存在该 clientId 的记录，并且传入的 seqId 小于等于记录中的 SeqId ,则表示该请求是重复请求
	return ok && seqId <= info.SeqId
}

// PutAppend 函数是处理 PutAppend RPC 请求的方法
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	// 判断请求是否重复
	kv.mu.Lock()

	// 判断当前请求是否为重复请求
	if kv.requestDuplicated(args.ClientId, args.SeqId) {

		// 如果是重复请求，直接从 duplicateTable 中获取之前的回复结果
		opReply := kv.duplicateTable[args.ClientId].Reply
		reply.Err = opReply.Err
		kv.mu.Unlock()
		return
	}

	// 解锁，因为已经完成了对共享资源的操作
	kv.mu.Unlock()

	// 将请求存储到 raft 日志中并进行同步
	// 将操作添加到 Raft 日志中，并返回日志索引、任期和是否为 Leader
	index, _, isLeader := kv.rf.Start(Op{
		Key:      args.Key,
		Value:    args.Value,
		OpType:   getOperationType(args.Op), // 根据传入的操作类型获取实际的操作类型
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	})

	// 如果不是 Leader 的话，直接返回错误
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 等待结果
	kv.mu.Lock()
	// 获取与该日志索引对应的通知通道
	notifyCh := kv.getNotifyChannel(index)
	kv.mu.Unlock()

	// 使用 select 语句等待结果或超时
	select {
	case result := <-notifyCh: // 从通知通道接收到结果

		// 将结果中的错误信息设置到回复中
		reply.Err = result.Err

	case <-time.After(ClientRequestTimeout): // 等待超时

		// 设置错误为超时
		reply.Err = ErrTimeout
	}

	// 删除通知的 channel
	go func() {
		kv.mu.Lock()
		// 移除与该日志索引对应的通知通道
		kv.removeNotifyChannel(index)
		kv.mu.Unlock()
	}()
}

// Kill 方法由测试器调用，当一个 KVServer 实例不再需要时
// 为了方便，我们提供了设置 rf.dead 的代码（无需锁）
// 以及一个 killed() 方法来在长时间运行的循环中测试 rf.dead
// 你也可以在 Kill() 中添加自己的代码。不要求你做任何事情
// 但这可能很方便（例如）抑制已 Kill() 的实例的调试输出
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer 函数用于启动一个 KV 服务器实例
// servers[] 包含了一组服务器的端口，这些服务器将通过 Raft 协作
// 形成一个容错的键值服务
// me 是当前服务器在 servers[] 中的索引
// KV 服务器应该通过底层的 Raft 实现来存储快照
// Raft 应调用 persister.SaveStateAndSnapshot() 来原子性地保存 Raft 状态和快照
// 当 Raft 保存的状态超过 maxraftstate 字节时，KV 服务器应该进行快照
// 以便 Raft 能够对其日志进行垃圾回收。如果 maxraftstate 为 -1，则不需要进行快照
// StartKVServer() 必须快速返回，所以它应该为任何长时间运行的工作启动 goroutines
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// 注册需要 Go 的 RPC 库进行序列化/反序列化的结构
	// 这里注册 Op 结构体，以便在 RPC 通信中正确处理
	labgob.Register(Op{})

	// 创建一个新的 KVServer 实例
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// 创建一个用于接收 Raft 应用消息的通道以及该 kvserver 对应的 Raft
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// 初始化自定义字段
	kv.dead = 0
	kv.lastApplied = 0
	kv.stateMachine = NewMemoryKVStateMachine()
	kv.notifyChans = make(map[int]chan *OpReply)
	kv.duplicateTable = make(map[int64]LastOperationInfo)

	// 从持久化器中读取快照数据并恢复状态
	kv.restoreFromSnapshot(persister.ReadSnapshot())

	// 启动一个协程来单独跑 处理 Raft 应用的消息的 for(;;) task
	go kv.applyTask()

	return kv
}

// 处理 apply 任务
func (kv *KVServer) applyTask() {

	// 只要服务器没有被停止，就持续运行
	for !kv.killed() {
		select {
		// 从 applyCh 通道接收消息
		case message := <-kv.applyCh:

			// 如果接收到的消息是有效的命令
			if message.CommandValid {

				// 加锁
				kv.mu.Lock()

				// 如果当前消息的索引小于等于已经处理过的最后一条消息的索引，说明该消息已经处理过，直接忽略
				if message.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}

				// 更新最后应用的日志索引为当前消息的索引
				kv.lastApplied = message.CommandIndex

				// 从消息中取出用户的操作信息
				op := message.Command.(Op)
				var opReply *OpReply

				// 如果操作不是 Get 操作，并且该请求是重复请求
				if op.OpType != OpGet && kv.requestDuplicated(op.ClientId, op.SeqId) {

					// 直接从 duplicateTable 中获取之前的回复
					opReply = kv.duplicateTable[op.ClientId].Reply
				} else {

					// 将操作应用到状态机中，并获取操作结果
					opReply = kv.applyToStateMachine(op)

					// 如果操作不是 Get 操作，将本次操作的信息记录到 duplicateTable 中，用于判断重复请求
					if op.OpType != OpGet {
						kv.duplicateTable[op.ClientId] = LastOperationInfo{
							SeqId: op.SeqId,
							Reply: opReply,
						}
					}
				}

				// 如果当前节点是 Leader，将结果通过对应的通知通道发送回去
				if _, isLeader := kv.rf.GetState(); isLeader {
					notifyCh := kv.getNotifyChannel(message.CommandIndex)
					notifyCh <- opReply
				}

				// 判断是否需要进行 snapshot
				// 如果 maxraftstate 不为 -1 且当前 Raft 状态大小大于等于 maxraftstate
				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
					// 进行 snapshot 操作
					kv.makeSnapshot(message.CommandIndex)
				}

				kv.mu.Unlock()
			} else if message.SnapshotValid {

				// 如果接收到的消息是有效的快照
				kv.mu.Lock()

				// 从快照中恢复服务器状态
				kv.restoreFromSnapshot(message.Snapshot)

				// 更新最后应用的日志索引为快照的索引
				kv.lastApplied = message.SnapshotIndex
				kv.mu.Unlock()
			}
		}
	}
}

// 将操作应用到状态机中，并返回操作结果
func (kv *KVServer) applyToStateMachine(op Op) *OpReply {
	var value string
	var err Err

	// 根据操作类型执行不同的操作
	switch op.OpType {
	case OpGet:
		// 执行 Get 操作，从状态机中获取键对应的值和可能的错误
		value, err = kv.stateMachine.Get(op.Key)
	case OpPut:
		// 执行 Put 操作，将键值对存入状态机，并获取可能的错误
		err = kv.stateMachine.Put(op.Key, op.Value)
	case OpAppend:
		// 执行 Append 操作，将值追加到键对应的值后面，并获取可能的错误
		err = kv.stateMachine.Append(op.Key, op.Value)
	}

	// 返回包含操作结果值和错误信息的回复
	return &OpReply{Value: value, Err: err}
}

// 用于获取与指定索引对应的通知通道
func (kv *KVServer) getNotifyChannel(index int) chan *OpReply {

	// 检查是否已经存在与该索引对应的通知通道
	if _, ok := kv.notifyChans[index]; !ok {
		// 如果不存在，则创建一个容量为 1 的通知通道
		kv.notifyChans[index] = make(chan *OpReply, 1)
	}

	// 返回与该索引对应的通知通道
	return kv.notifyChans[index]
}

// 用于移除与指定索引对应的通知通道
func (kv *KVServer) removeNotifyChannel(index int) {
	// 使用 delete 函数从 notifyChans 映射中删除指定索引的通知通道
	delete(kv.notifyChans, index)
}

// 用于创建状态机和重复请求表的快照
func (kv *KVServer) makeSnapshot(index int) {

	// 创建一个字节缓冲区用于存储编码后的数据
	buf := new(bytes.Buffer)

	// 创建一个新的编码器，用于将数据编码到字节缓冲区
	enc := labgob.NewEncoder(buf)

	// 将状态机编码到字节缓冲区
	_ = enc.Encode(kv.stateMachine)

	// 将重复请求表编码到字节缓冲区
	_ = enc.Encode(kv.duplicateTable)

	// 调用 Raft 实例的 Snapshot 方法，将编码后的数据作为快照保存，并关联当前的日志索引
	kv.rf.Snapshot(index, buf.Bytes())
}

// 用于从快照中恢复状态机和重复请求表（解码函数）
func (kv *KVServer) restoreFromSnapshot(snapshot []byte) {

	// 如果快照数据为空，则直接返回
	if len(snapshot) == 0 {
		return
	}

	// 创建一个字节缓冲区，用于读取快照数据
	buf := bytes.NewBuffer(snapshot)
	// 创建一个新的解码器，用于从字节缓冲区解码数据
	dec := labgob.NewDecoder(buf)

	// 声明用于存储解码后状态机和重复请求表的变量
	var stateMachine MemoryKVStateMachine
	var dupTable map[int64]LastOperationInfo

	// 尝试从字节缓冲区解码状态机数据
	if dec.Decode(&stateMachine) != nil || dec.Decode(&dupTable) != nil {
		// 如果解码失败，抛出异常
		panic("failed to restore state from snapshpt")
	}

	// 将解码后的状态机和重复请求表赋值给服务器的相应字段
	kv.stateMachine = &stateMachine
	kv.duplicateTable = dupTable
}
