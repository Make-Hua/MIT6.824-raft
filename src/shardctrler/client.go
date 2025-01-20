package shardctrler

//
// Shardctrler clerk.
//

import (
	"course/labrpc"
	"crypto/rand"
	"math/big"
)

// Clerk 结构体定义了 ShardCtrler 客户端的相关信息
type Clerk struct {
	servers  []*labrpc.ClientEnd /* 存储所有 ShardCtrler 服务器的客户端端点 */
	leaderId int                 /* 记录当前认为的 Leader 节点的 id，避免每次请求都去轮询查找 Leader */
	clientId int64               /* 客户端的唯一标识，用于区分不同客户端 */
	seqId    int64               /* 客户端发送的命令序列号，用于确保每个命令的唯一性 */
}

// nrand 函数生成一个随机的 62 位整数
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)     // 创建一个最大值为 2^62 - 1 的大整数
	bigx, _ := rand.Int(rand.Reader, max) // 从随机数生成器中生成一个在 [0, max) 范围内的大整数
	x := bigx.Int64()                     // 将大整数转换为 int64 类型
	return x
}

// MakeClerk 函数创建一个新的 ShardCtrler 客户端实例
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {

	// 创建一个新的 Clerk 结构体实例并初始化服务器列表
	ck := new(Clerk)
	ck.servers = servers

	// 初始化自定义字段
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.seqId = 0

	return ck
}

// 用于向 ShardCtrler 服务器查询配置信息
func (ck *Clerk) Query(num int) Config {

	// 创建查询参数对象
	args := &QueryArgs{}
	args.Num = num

	for {
		// 尝试向当前认为的 Leader 服务器发送查询请求
		var reply QueryReply

		// Query RPC
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Query", args, &reply)

		// 如果请求失败（连接失败、返回错误 Leader 或超时）
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			// 切换到下一个服务器重试
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}

		// 请求成功，返回查询到的配置信息
		return reply.Config
	}
}

// 用于向 ShardCtrler 服务器发送加入请求
func (ck *Clerk) Join(servers map[int][]string) {

	// 创建 RPC 请求参数对象
	args := &JoinArgs{ClientId: ck.clientId, SeqId: ck.seqId}
	args.Servers = servers

	for {
		// 尝试向当前认为的 Leader 服务器发送加入请求
		var reply JoinReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Join", args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			// 如果请求失败（连接失败、返回错误 Leader 或超时）
			// 切换到下一个服务器重试
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
		ck.seqId++
		return
	}
}

// 用于向 ShardCtrler 服务器发送离开请求
func (ck *Clerk) Leave(gids []int) {

	// 创建离开请求参数对象
	args := &LeaveArgs{ClientId: ck.clientId, SeqId: ck.seqId}
	args.GIDs = gids

	for {
		// 尝试向当前认为的 Leader 服务器发送离开请求
		var reply LeaveReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Leave", args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			// 如果请求失败（连接失败、返回错误 Leader 或超时）
			// 切换到下一个服务器重试
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		// 请求成功，更新序列号并返回
		ck.seqId++
		return
	}
}

// 用于向 ShardCtrler 服务器发送移动分片请求
func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{ClientId: ck.clientId, SeqId: ck.seqId} // 创建移动分片请求参数对象
	// 设置移动分片请求参数
	args.Shard = shard
	args.GID = gid

	for {
		// 尝试向当前认为的 Leader 服务器发送移动分片请求
		var reply MoveReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Move", args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			// 如果请求失败（连接失败、返回错误 Leader 或超时）
			// 切换到下一个服务器重试
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		// 请求成功，更新序列号并返回
		ck.seqId++
		return
	}
}
