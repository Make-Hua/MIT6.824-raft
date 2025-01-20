package kvraft

import (
	"course/labrpc"
	"crypto/rand"
	"math/big"
)

// Clerk 结构体定义了客户端的相关信息
type Clerk struct {
	servers []*labrpc.ClientEnd // 存储所有服务器的客户端端点

	// You will have to modify this struct.
	leaderId int // 记录 Leader 节点的 id，避免下一次请求的时候去轮询查找 Leader

	// clientID+seqId 确定一个唯一的命令
	clientId int64 // 客户端的唯一标识，用于区分不同客户端
	seqId    int64 // 客户端发送的命令序列号，用于确保每个命令的唯一性
}

// nrand 函数生成一个随机的 62 位整数
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// MakeClerk 函数创建一个新的 Clerk 实例
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {

	// 创建 Clerk 实例且初始化 server 列表
	ck := new(Clerk)
	ck.servers = servers

	// You'll have to add code here.
	ck.leaderId = 0       // 初始时认为第一个服务器是 Leader
	ck.clientId = nrand() // 客户端标识随机生成
	ck.seqId = 0          // 消息序列号从 0 开始

	return ck
}

// Get 函数用于获取指定 key 的当前值
// 如果 key 不存在，返回 ""
// 在遇到其他错误时会一直尝试
// 你可以像这样发送一个 RPC 调用：
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
// args 和 reply 的类型（包括是否为指针）
// 必须与 RPC 处理函数声明的参数类型匹配。并且 reply 必须作为指针传递
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{
		Key: key,
	}

	for {
		var reply GetReply // 用于存储 RPC 调用的回复

		// 调用 RPC，向当前认为的 Leader 发送 Get 请求
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)

		// 如果该节点不是 Leader 或者 RPC 超时
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {

			// 如果请求失败（连接失败、返回错误 Leader 或超时），则选择另一个节点重试
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers) // 切换到下一个服务器
			continue
		}

		// Get 调用成功，返回 value
		return reply.Value
	}
}

// PutAppend 函数用于 Put 和 Append 操作
// 你可以像这样发送一个 RPC 调用：
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
// args 和 reply 的类型（包括是否为指针）
// 必须与 RPC 处理函数声明的参数类型匹配。并且 reply 必须作为指针传递
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	// 封装好 RPC 调用参数
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}

	for {
		// 用于存储 RPC 调用的回复
		var reply PutAppendReply

		// 向当前认为的 Leader 发送 PutAppend 请求
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)

		// 如果该节点不是 Leader 或者 RPC 超时
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			// 如果请求失败（连接失败、返回错误 Leader 或超时）
			// 选择另一个节点重试
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers) // 切换到下一个服务器
			continue
		}
		// 调用成功，更新序列号
		ck.seqId++
		return
	}
}

// Put 函数用于执行 Put 操作
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append 函数用于执行 Append 操作
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
