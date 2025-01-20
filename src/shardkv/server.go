package shardkv

import (
	"bytes"
	"course/labgob"
	"course/labrpc"
	"course/raft"
	"course/shardctrler"
	"sync"
	"sync/atomic"
	"time"
)

// ShardKV 结构体表示分片的键值存储系统
type ShardKV struct {
	mu           sync.Mutex                     /* 互斥锁，用于保护共享数据 */
	me           int                            /* 当前节点的标识 */
	rf           *raft.Raft                     /* Raft 实例，用于实现分布式一致性 */
	applyCh      chan raft.ApplyMsg             /* 用于接收 Raft 的应用消息 */
	make_end     func(string) *labrpc.ClientEnd /* 将服务器名称转换为客户端端点的函数 */
	gid          int                            /* 组的标识 */
	ctrlers      []*labrpc.ClientEnd            /* 控制器的客户端端点列表 */
	maxraftstate int                            /* 日志达到此大小时进行快照 */

	// 以下是你自己的定义部分
	dead           int32                         /* 标记节点是否死亡，可能用于节点状态管理 */
	lastApplied    int                           /* 最后应用的日志索引 */
	shards         map[int]*MemoryKVStateMachine /* 存储分片信息，键是分片 ID，值是存储分片数据的状态机 */
	notifyChans    map[int]chan *OpReply         /* 存储通知通道，键是日志索引，值是操作回复通道 */
	duplicateTable map[int64]LastOperationInfo   /* 存储客户端请求的去重表，键是客户端 ID，值是最后操作信息 */
	currentConfig  shardctrler.Config            /* 当前配置信息 */
	prevConfig     shardctrler.Config            /* 上一个配置信息 */
	mck            *shardctrler.Clerk            /* 分片控制器的客户端实例 */
}

// Get 方法用于处理 Get 请求
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {

	// 加锁，保护共享数据，确保对共享数据的操作是原子性的
	kv.mu.Lock()

	// 判断请求的键是否属于当前组

	if !kv.matchGroup(args.Key) {
		// 如果不属于当前组，设置错误信息
		reply.Err = ErrWrongGroup

		// 解锁，释放锁资源
		kv.mu.Unlock()
		return
	}

	// 解锁，因为后续操作不需要锁
	kv.mu.Unlock()

	// 调用 Raft，将请求存储到 Raft 日志中并进行同步
	// 构造一个 Raft 命令，类型为客户端操作，包含操作信息
	index, _, isLeader := kv.rf.Start(RaftCommand{
		ClientOpeartion,
		Op{Key: args.Key, OpType: OpGet},
	})

	// 如果当前节点不是 Leader
	if !isLeader {
		// 设置错误信息
		reply.Err = ErrWrongLeader
		return
	}

	// 加锁，获取与该请求对应的通知通道
	kv.mu.Lock()
	notifyCh := kv.getNotifyChannel(index)
	kv.mu.Unlock()

	// 使用 select 语句等待操作结果或超时
	select {
	// 从通知通道接收到操作结果
	case result := <-notifyCh:
		// 将结果的值和错误信息设置到回复中
		reply.Value = result.Value
		reply.Err = result.Err
	// 等待超时
	case <-time.After(ClientRequestTimeout):
		// 设置错误信息为超时
		reply.Err = ErrTimeout
	}

	// 启动一个 goroutine 来清理通知通道
	go func() {
		kv.mu.Lock()
		// 移除通知通道，释放资源
		kv.removeNotifyChannel(index)
		kv.mu.Unlock()
	}()
}

// matchGroup 函数用于判断给定的键是否属于当前组
func (kv *ShardKV) matchGroup(key string) bool {

	// 根据键计算其所属的分片
	shard := key2shard(key)

	// 获取该分片的状态
	shardStatus := kv.shards[shard].Status

	// 检查该分片是否属于当前组（通过比较组 ID），并且分片的状态是正常或正在进行垃圾回收
	// 如果满足这两个条件，则认为该键属于当前组
	return kv.currentConfig.Shards[shard] == kv.gid && (shardStatus == Normal || shardStatus == GC)
}

// requestDuplicated 函数用于检查客户端请求是否重复
func (kv *ShardKV) requestDuplicated(clientId, seqId int64) bool {

	// 从去重表中查找客户端的信息
	info, ok := kv.duplicateTable[clientId]

	// 如果找到了该客户端的信息，并且传入的序列号小于或等于该客户端的最新序列号，则认为是重复请求
	return ok && seqId <= info.SeqId
}

// PutAppend 函数用于处理 Put 和 Append 请求
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// 加锁，确保在操作过程中对共享数据的独占访问
	kv.mu.Lock()

	// 判断请求的键是否属于当前组
	if !kv.matchGroup(args.Key) {
		// 如果请求的键不属于当前组，设置错误为错误组
		reply.Err = ErrWrongGroup
		// 解锁，释放锁资源
		kv.mu.Unlock()
		return
	}

	// 判断请求是否为重复请求
	if kv.requestDuplicated(args.ClientId, args.SeqId) {
		// 如果是重复请求，直接使用之前的操作回复作为结果
		opReply := kv.duplicateTable[args.ClientId].Reply
		// 将错误信息设置为之前操作回复的错误信息
		reply.Err = opReply.Err
		// 解锁，释放锁资源
		kv.mu.Unlock()
		return
	}
	// 解锁，因为后续操作不需要对共享数据的独占访问
	kv.mu.Unlock()

	// 调用 Raft 协议将请求存储到 Raft 日志中并进行同步
	index, _, isLeader := kv.rf.Start(RaftCommand{
		ClientOpeartion,
		Op{
			Key:      args.Key,
			Value:    args.Value,
			OpType:   getOperationType(args.Op),
			ClientId: args.ClientId,
			SeqId:    args.SeqId,
		},
	})

	// 如果当前节点不是 Leader，则返回错误信息为错误的领导者
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 加锁，以获取与该请求对应的通知通道
	kv.mu.Lock()
	notifyCh := kv.getNotifyChannel(index)
	kv.mu.Unlock()

	// 等待操作结果或超时
	select {
	// 从通知通道接收到操作结果
	case result := <-notifyCh:
		// 将操作结果的错误信息设置到回复中
		reply.Err = result.Err
	// 等待超时
	case <-time.After(ClientRequestTimeout):
		// 如果超时，设置错误信息为超时
		reply.Err = ErrTimeout
	}

	// 启动一个 goroutine 来删除通知通道
	go func() {
		// 加锁，以删除通知通道
		kv.mu.Lock()
		kv.removeNotifyChannel(index)
		// 解锁，释放锁资源
		kv.mu.Unlock()
	}()
}

// Kill the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartServer 函数用于启动一个 ShardKV 服务器
// servers[] 包含了此组服务器的端口
// me 是当前服务器在 servers[] 中的索引
// k/v 服务器应该通过底层的 Raft 实现存储快照，它应该调用 persister.SaveStateAndSnapshot() 来原子性地保存 Raft 状态和快照
// 当 Raft 的保存状态超过 maxraftstate 字节时，k/v 服务器应该进行快照，以便 Raft 进行日志的垃圾回收，如果 maxraftstate 是 -1，则不需要进行快照
// gid 是此组的 GID，用于与 shardctrler 交互
// 将 ctrlers[] 传递给 shardctrler.MakeClerk() 以便可以向 shardctrler 发送 RPC
// make_end(servername) 将 Config.Groups[gid][i] 中的服务器名称转换为可发送 RPC 的 labrpc.ClientEnd，你需要用它向其他组发送 RPC
// 查看 client.go 以了解如何使用 ctrlers[] 和 make_end() 向拥有特定分片的组发送 RPC
// StartServer() 必须快速返回，因此它应该为任何长时间运行的工作启动 goroutine
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// 注册需要 Go 的 RPC 库进行编组/解组的结构
	labgob.Register(Op{})
	labgob.Register(RaftCommand{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(ShardOperationArgs{})
	labgob.Register(ShardOperationReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// 你的初始化代码在这里

	// 使用以下方式与 shardctrler 通信
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	// 创建并启动 Raft 实例，传入服务器列表、当前节点索引、持久化器和应用通道
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.dead = 0
	kv.lastApplied = 0
	// 存储分片状态机，键是分片 ID，值是分片的内存 KV 状态机
	kv.shards = make(map[int]*MemoryKVStateMachine)
	// 存储通知通道，键是日志索引，值是操作回复通道
	kv.notifyChans = make(map[int]chan *OpReply)
	// 存储客户端请求的去重表，键是客户端 ID，值是最后操作信息
	kv.duplicateTable = make(map[int64]LastOperationInfo)
	// 初始化当前配置为默认配置
	kv.currentConfig = shardctrler.DefaultConfig()
	// 初始化上一个配置为默认配置
	kv.prevConfig = shardctrler.DefaultConfig()

	// 从快照中恢复状态
	kv.restoreFromSnapshot(persister.ReadSnapshot())

	// 启动 goroutine 来处理应用任务
	go kv.applyTask()

	// 启动 goroutine 来获取配置任务
	go kv.fetchConfigTask()

	// 启动 goroutine 来处理分片迁移任务
	go kv.shardMigrationTask()

	// 启动 goroutine 来处理分片垃圾回收任务
	go kv.shardGCTask()

	return kv
}

// applyToStateMachine 函数将操作应用到状态机中
func (kv *ShardKV) applyToStateMachine(op Op, shardId int) *OpReply {
	var value string
	var err Err
	// 根据操作类型执行不同的操作
	switch op.OpType {
	case OpGet:
		// 执行 Get 操作
		value, err = kv.shards[shardId].Get(op.Key)
	case OpPut:
		// 执行 Put 操作
		err = kv.shards[shardId].Put(op.Key, op.Value)
	case OpAppend:
		// 执行 Append 操作
		err = kv.shards[shardId].Append(op.Key, op.Value)
	}
	// 返回操作的回复，包含操作结果的值和可能的错误
	return &OpReply{Value: value, Err: err}
}

// getNotifyChannel 函数根据日志索引获取通知通道
func (kv *ShardKV) getNotifyChannel(index int) chan *OpReply {
	// 检查是否已经存在该索引对应的通知通道，如果不存在则创建一个新的
	if _, ok := kv.notifyChans[index]; !ok {
		kv.notifyChans[index] = make(chan *OpReply, 1)
	}
	// 返回通知通道
	return kv.notifyChans[index]
}

// removeNotifyChannel 函数移除指定索引的通知通道
func (kv *ShardKV) removeNotifyChannel(index int) {
	// 从 notifyChans 映射中删除该索引对应的通知通道
	delete(kv.notifyChans, index)
}

// makeSnapshot 函数用于创建快照
func (kv *ShardKV) makeSnapshot(index int) {
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)

	// 编码分片信息
	_ = enc.Encode(kv.shards)

	// 编码去重表信息
	_ = enc.Encode(kv.duplicateTable)

	// 编码当前配置信息
	_ = enc.Encode(kv.currentConfig)

	// 编码上一个配置信息
	_ = enc.Encode(kv.prevConfig)

	// 调用 Raft 实例创建快照
	kv.rf.Snapshot(index, buf.Bytes())
}

// restoreFromSnapshot 函数用于从快照中恢复状态
func (kv *ShardKV) restoreFromSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		// 如果快照为空，则为每个分片创建新的内存 KV 状态机
		for i := 0; i < shardctrler.NShards; i++ {
			if _, ok := kv.shards[i]; !ok {
				kv.shards[i] = NewMemoryKVStateMachine()
			}
		}
		return
	}

	buf := bytes.NewBuffer(snapshot)
	dec := labgob.NewDecoder(buf)
	var stateMachine map[int]*MemoryKVStateMachine
	var dupTable map[int64]LastOperationInfo
	var currentConfig shardctrler.Config
	var prevConfig shardctrler.Config

	// 解码快照数据到相应的结构中，如果解码失败则 panic
	if dec.Decode(&stateMachine) != nil ||
		dec.Decode(&dupTable) != nil ||
		dec.Decode(&currentConfig) != nil ||
		dec.Decode(&prevConfig) != nil {
		panic("failed to restore state from snapshpt")
	}

	// 恢复分片状态机
	kv.shards = stateMachine

	// 恢复去重表
	kv.duplicateTable = dupTable

	// 恢复当前配置
	kv.currentConfig = currentConfig

	// 恢复上一个配置
	kv.prevConfig = prevConfig
}
