package shardctrler

import (
	"course/labgob"
	"course/labrpc"
	"course/raft"
	"sync"
	"sync/atomic"
	"time"
)

// ShardCtrler 结构体定义了分片控制器
type ShardCtrler struct {
	mu      sync.Mutex         /* 互斥锁，用于保护共享资源的并发访问 */
	me      int                /* 当前节点的标识 */
	rf      *raft.Raft         /* Raft 实例，用于实现分布式一致性 */
	applyCh chan raft.ApplyMsg /* 用于接收 Raft 层应用消息的通道 */

	// 自定义数据字段
	configs        []Config                    /* 存储所有配置的切片，通过配置编号索引 */
	dead           int32                       /* 标记该节点是否已停止运行，由 Kill() 函数设置 */
	lastApplied    int                         /* 记录最后应用的日志索引 */
	stateMachine   *CtrlerStateMachine         /* 用于管理分片控制器状态的状态机 */
	notifyChans    map[int]chan *OpReply       /* 用于存储等待处理结果的通知通道，键为日志索引 */
	duplicateTable map[int64]LastOperationInfo /* 用于记录客户端请求的重复信息，防止重复操作 */
}

// requestDuplicated 函数用于判断客户端的请求是否为重复请求
func (sc *ShardCtrler) requestDuplicated(clientId, seqId int64) bool {

	// 从 duplicateTable 中获取与 clientId 对应的操作信息
	info, ok := sc.duplicateTable[clientId]

	// 如果存在该 clientId 的记录，并且传入的 seqId 小于等于记录中的 SeqId
	// 则表示该请求是重复请求，返回 true；否则返回 false
	return ok && seqId <= info.SeqId
}

// Join 函数处理客户端的 Join 请求
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {

	// 声明一个用于存储操作回复的变量
	var opReply OpReply

	// 调用 command 方法处理请求，将操作封装为 Op 类型
	sc.command(Op{
		OpType:   OpJoin,        // 操作类型为 Join
		ClientId: args.ClientId, // 客户端标识
		SeqId:    args.SeqId,    // 客户端请求序列号
		Servers:  args.Servers,  // 要加入的服务器信息
	}, &opReply)

	// 将操作回复中的错误信息设置到 Join 回复中
	reply.Err = opReply.Err
}

// Leave 函数处理客户端的 Leave 请求
func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {

	// 声明一个用于存储操作回复的变量
	var opReply OpReply

	// 调用 command 方法处理请求，将操作封装为 Op 类型
	sc.command(Op{
		OpType:   OpLeave,       // 操作类型为 Leave
		ClientId: args.ClientId, // 客户端标识
		SeqId:    args.SeqId,    // 客户端请求序列号
		GIDs:     args.GIDs,     // 要离开的组 ID 列表
	}, &opReply)

	// 将操作回复中的错误信息设置到 Leave 回复中
	reply.Err = opReply.Err
}

// Move 函数处理客户端的 Move 请求
func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {

	// 声明一个用于存储操作回复的变量
	var opReply OpReply

	// 调用 command 方法处理请求，将操作封装为 Op 类型
	sc.command(Op{
		OpType:   OpMove,        // 操作类型为 Move
		ClientId: args.ClientId, // 客户端标识
		SeqId:    args.SeqId,    // 客户端请求序列号
		Shard:    args.Shard,    // 要移动的分片编号
		GID:      args.GID,      // 目标组 ID
	}, &opReply)

	// 将操作回复中的错误信息设置到 Move 回复中
	reply.Err = opReply.Err
}

// Query 函数处理客户端的 Query 请求
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {

	// 声明一个用于存储操作回复的变量
	var opReply OpReply

	// 调用 command 方法处理请求，将操作封装为 Op 类型
	sc.command(Op{
		OpType: OpQuery,  // 操作类型为 Query
		Num:    args.Num, // 期望查询的配置编号
	}, &opReply)

	// 将操作回复中的配置信息设置到 Query 回复中
	reply.Config = opReply.ControllerConfig

	// 将操作回复中的错误信息设置到 Query 回复中
	reply.Err = opReply.Err
}

// command 函数用于处理各种操作请求
func (sc *ShardCtrler) command(args Op, reply *OpReply) {

	// 加锁，保护共享资源的访问
	sc.mu.Lock()

	// 如果操作类型不是查询操作，并且该请求是重复请求
	if args.OpType != OpQuery && sc.requestDuplicated(args.ClientId, args.SeqId) {
		// 从重复请求表中获取之前的回复
		opReply := sc.duplicateTable[args.ClientId].Reply
		// 将之前的错误信息设置到当前回复中
		reply.Err = opReply.Err
		// 解锁
		sc.mu.Unlock()
		// 直接返回，不再进行后续处理
		return
	}

	// 解锁
	sc.mu.Unlock()

	// 调用 Raft 的 Start 方法，将操作请求存储到 Raft 日志中并进行同步
	// 返回日志索引、任期和是否为 Leader 的信息
	index, _, isLeader := sc.rf.Start(args)
	// 如果当前节点不是 Leader
	if !isLeader {
		// 设置错误为不是正确的 Leader
		reply.Err = ErrWrongLeader
		return
	}

	// 等待操作结果
	sc.mu.Lock()
	// 获取与该日志索引对应的通知通道
	notifyCh := sc.getNotifyChannel(index)
	sc.mu.Unlock()

	// 使用 select 语句等待结果或超时
	select {
	case result := <-notifyCh: // 从通知通道接收到结果
		// 将结果中的配置信息设置到回复中
		reply.ControllerConfig = result.ControllerConfig
		// 将结果中的错误信息设置到回复中
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeout): // 等待超时
		// 设置错误为超时
		reply.Err = ErrTimeout
	}

	// 启动一个 goroutine 来删除通知通道
	go func() {
		sc.mu.Lock()
		// 删除与该日志索引对应的通知通道
		sc.removeNotifyChannel(index)
		sc.mu.Unlock()
	}()
}

// Kill 函数用于停止 ShardCtrler 实例
// 测试器在不再需要 ShardCtrler 实例时会调用该函数
// 你不需要在这个函数中做任何事情，但可以在这里做一些清理工作，例如关闭调试输出
func (sc *ShardCtrler) Kill() {

	// 使用 atomic 包设置 dead 字段为 1，表示该实例已停止
	atomic.StoreInt32(&sc.dead, 1)

	// 调用 Raft 实例的 Kill 方法，停止 Raft 相关的操作
	sc.rf.Kill()
	// 你可以在这里添加自定义的清理代码
}

// killed 函数用于检查 ShardCtrler 实例是否已停止
func (sc *ShardCtrler) killed() bool {

	// 使用 atomic 包读取 dead 字段的值
	z := atomic.LoadInt32(&sc.dead)

	// 如果值为 1，表示该实例已停止，返回 true；否则返回 false
	return z == 1
}

// Raft 函数返回 ShardCtrler 实例中的 Raft 实例
// 用于 shardkv 测试
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// StartServer 函数用于启动一个 ShardCtrler 服务实例
// servers[] 包含了一组服务器的端口，这些服务器将通过 Raft 协作形成容错的分片控制器服务
// me 是当前服务器在 servers[] 中的索引
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {

	// 创建一个新的 ShardCtrler 实例
	sc := new(ShardCtrler)
	// 设置当前服务器的索引
	sc.me = me

	// 初始化配置切片，初始时只有一个配置
	sc.configs = make([]Config, 1)
	// 初始化第一个配置的 Groups 为空映射
	sc.configs[0].Groups = map[int][]string{}

	// 注册 Op 结构体，以便在 RPC 通信中进行序列化和反序列化
	labgob.Register(Op{})
	// 创建一个用于接收 Raft 应用消息的通道
	sc.applyCh = make(chan raft.ApplyMsg)
	// 创建一个 Raft 实例，传入服务器列表、当前服务器索引、持久化器和应用消息通道
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// 初始化其他字段
	sc.dead = 0                                           // 初始时该实例未停止
	sc.lastApplied = 0                                    // 初始时最后应用的日志索引为 0
	sc.stateMachine = NewCtrlerStateMachine()             // 创建一个新的状态机实例
	sc.notifyChans = make(map[int]chan *OpReply)          // 创建一个用于存储通知通道的映射
	sc.duplicateTable = make(map[int64]LastOperationInfo) // 创建一个用于存储重复请求信息的映射

	// 启动一个 goroutine 来处理 Raft 应用的消息
	go sc.applyTask()

	// 返回创建好的 ShardCtrler 实例
	return sc
}

// 处理 apply 任务
func (sc *ShardCtrler) applyTask() {
	// 只要 ShardCtrler 实例没有被停止，就持续循环
	for !sc.killed() {
		select {
		// 从 applyCh 通道接收消息
		case message := <-sc.applyCh:
			// 如果接收到的消息是有效的命令
			if message.CommandValid {
				sc.mu.Lock()
				// 如果当前消息的索引小于等于已经处理过的最后一条消息的索引，说明该消息已经处理过，直接忽略
				if message.CommandIndex <= sc.lastApplied {
					sc.mu.Unlock()
					continue
				}
				// 更新最后应用的日志索引为当前消息的索引
				sc.lastApplied = message.CommandIndex

				// 从消息中取出用户的操作信息
				op := message.Command.(Op)
				var opReply *OpReply
				// 如果操作不是查询操作，并且该请求是重复请求
				if op.OpType != OpQuery && sc.requestDuplicated(op.ClientId, op.SeqId) {
					// 直接从 duplicateTable 中获取之前的回复
					opReply = sc.duplicateTable[op.ClientId].Reply
				} else {
					// 将操作应用到状态机中
					opReply = sc.applyToStateMachine(op)
					// 如果操作不是查询操作，将本次操作的信息记录到 duplicateTable 中，用于判断重复请求
					if op.OpType != OpQuery {
						sc.duplicateTable[op.ClientId] = LastOperationInfo{
							SeqId: op.SeqId,
							Reply: opReply,
						}
					}
				}

				// 如果当前节点是 Leader，将结果通过对应的通知通道发送回去
				if _, isLeader := sc.rf.GetState(); isLeader {
					notifyCh := sc.getNotifyChannel(message.CommandIndex)
					notifyCh <- opReply
				}

				sc.mu.Unlock()
			}
		}
	}
}

// applyToStateMachine 函数将操作应用到状态机中，并返回操作结果
func (sc *ShardCtrler) applyToStateMachine(op Op) *OpReply {
	var err Err
	var cfg Config
	// 根据操作类型执行不同的操作
	switch op.OpType {
	case OpQuery:
		// 执行查询操作，从状态机中获取指定配置编号的配置信息和可能的错误
		cfg, err = sc.stateMachine.Query(op.Num)
	case OpJoin:
		// 执行加入操作，将新的服务器信息加入到状态机中，并获取可能的错误
		err = sc.stateMachine.Join(op.Servers)
	case OpLeave:
		// 执行离开操作，将指定的组从状态机中移除，并获取可能的错误
		err = sc.stateMachine.Leave(op.GIDs)
	case OpMove:
		// 执行移动操作，将指定的分片移动到指定的组，并获取可能的错误
		err = sc.stateMachine.Move(op.Shard, op.GID)
	}
	// 返回包含操作结果配置信息和错误信息的回复
	return &OpReply{ControllerConfig: cfg, Err: err}
}

// getNotifyChannel 函数用于获取与指定索引对应的通知通道
func (sc *ShardCtrler) getNotifyChannel(index int) chan *OpReply {
	// 检查是否已经存在与该索引对应的通知通道
	if _, ok := sc.notifyChans[index]; !ok {
		// 如果不存在，则创建一个容量为 1 的通知通道
		sc.notifyChans[index] = make(chan *OpReply, 1)
	}
	// 返回与该索引对应的通知通道
	return sc.notifyChans[index]
}

// removeNotifyChannel 函数用于移除与指定索引对应的通知通道
func (sc *ShardCtrler) removeNotifyChannel(index int) {
	// 使用 delete 函数从 notifyChans 映射中删除指定索引的通知通道
	delete(sc.notifyChans, index)
}
