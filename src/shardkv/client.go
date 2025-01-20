package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"course/labrpc"
	"course/shardctrler"
	"crypto/rand"
	"math/big"
	"time"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
// 根据键计算其所属的分片编号
func key2shard(key string) int {

	shard := 0
	if len(key) > 0 {

		// 取键的第一个字符的 ASCII 码值
		shard = int(key[0])
	}
	// 对分片总数取模，得到所属的分片编号
	shard %= shardctrler.NShards
	return shard
}

// 生成一个随机的 62 位整数
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// Clerk 结构体表示客户端
type Clerk struct {
	sm        *shardctrler.Clerk             /* 用于与分片控制器（shardctrler）进行交互的客户端实例 */
	config    shardctrler.Config             /* 当前的配置信息 */
	make_end  func(string) *labrpc.ClientEnd /* 用于将服务器名称转换为可以发送 RPC 请求的客户端端点 */
	leaderIds map[int]int                    /* 记录每个组对应的 Leader 节点的 id，避免下一次请求的时候去轮询查找 Leader */

	// clientID+seqId 确定一个唯一的命令
	clientId int64 /* 客户端的唯一标识 */
	seqId    int64 /* 客户端请求的序列号 */
}

// MakeClerk 函数用于创建一个客户端实例
// 测试器会调用这个函数
// ctrlers[] 用于调用 shardctrler.MakeClerk()
// make_end(servername) 用于将 Config.Groups[gid][i] 中的服务器名称转换为可以发送 RPC 请求的 labrpc.ClientEnd
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {

	ck := new(Clerk)

	// 创建与分片控制器交互的客户端实例
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end

	// 初始化 leaderIds 映射
	ck.leaderIds = make(map[int]int)

	// 生成一个随机的客户端唯一标识
	ck.clientId = nrand()

	// 初始化请求序列号为 0
	ck.seqId = 0

	return ck
}

// Get 函数用于获取指定键的值
// 如果键不存在，返回 ""
// 面对其他错误时会一直重试
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key

	// 如果没成功执行 Get 则不断重新请求
	for {
		// 计算键所属的分片编号
		shard := key2shard(key)

		// 获取该分片所属的组 ID
		gid := ck.config.Shards[shard]

		// 如果该组存在服务器列表
		if servers, ok := ck.config.Groups[gid]; ok {

			// 如果还没有记录该组的 Leader 节点 id，则初始化为 0
			if _, exist := ck.leaderIds[gid]; !exist {
				ck.leaderIds[gid] = 0
			}

			// 记录旧的 Leader 节点 id
			oldLeaderId := ck.leaderIds[gid]

			// 如果当前确定的 Server 不是 Leader 则继续请求下一个 Server
			for {

				// 获取当前认为的 Leader 节点的客户端端点
				srv := ck.make_end(servers[ck.leaderIds[gid]])
				var reply GetReply

				// 向该节点发送 Get 请求
				ok := srv.Call("ShardKV.Get", &args, &reply)

				// 如果请求成功，并且返回的错误是 OK 或者 ErrNoKey（键不存在）
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					// 返回获取到的值
					return reply.Value
				}

				// 如果请求成功，但是返回的错误是 ErrWrongGroup（错误的组）
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}

				// 如果请求失败，或者返回的错误是 ErrWrongLeader（错误的 Leader） 或者 ErrTimeout（超时）
				if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {

					// 尝试下一个服务器
					ck.leaderIds[gid] = (ck.leaderIds[gid] + 1) % len(servers)

					// 如果回到了最初尝试的服务器
					if ck.leaderIds[gid] == oldLeaderId {
						break
					}
					continue
				}
			}
		}

		// 等待一段时间后重试
		time.Sleep(100 * time.Millisecond)

		// 向分片控制器查询最新的配置
		ck.config = ck.sm.Query(-1)
	}
}

// PutAppend 函数是 Put 和 Append 操作的共用函数
func (ck *Clerk) PutAppend(key string, value string, op string) {

	// 封装 PutAppend RPC 的调用参数
	args := PutAppendArgs{
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}
	// 赋值
	args.Key = key
	args.Value = value
	args.Op = op

	/// 如果没成功执行 PutAppend 则不断重新请求
	for {

		// 计算键所属的分片编号
		shard := key2shard(key)

		// 获取该分片所属的组 ID
		gid := ck.config.Shards[shard]

		// 如果该组存在服务器列表
		if servers, ok := ck.config.Groups[gid]; ok {

			// 如果还没有记录该组的 Leader 节点 id，则初始化为 0
			if _, exist := ck.leaderIds[gid]; !exist {
				ck.leaderIds[gid] = 0
			}

			// 记录旧的 Leader 节点 id
			oldLeaderId := ck.leaderIds[gid]

			// 如果请求 Server 不是 Lerader 则请求下一个 Leader
			for {

				// 获取当前认为的 Leader 节点的客户端端点
				srv := ck.make_end(servers[ck.leaderIds[gid]])
				var reply PutAppendReply

				// 向该节点发送 PutAppend 请求
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)

				// 如果请求成功且没有错误
				if ok && reply.Err == OK {
					// 请求成功，增加序列号
					ck.seqId++
					return
				}

				// 如果请求成功，但是返回的错误是 ErrWrongGroup（错误的组）
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}

				// 如果请求失败，或者返回的错误是 ErrWrongLeader（错误的 Leader） 或者 ErrTimeout（超时）
				if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {

					// 尝试下一个服务器
					ck.leaderIds[gid] = (ck.leaderIds[gid] + 1) % len(servers)

					// 如果回到了最初尝试的服务器
					if ck.leaderIds[gid] == oldLeaderId {
						break
					}
					continue
				}
			}
		}

		// 等待一段时间后重试
		time.Sleep(100 * time.Millisecond)

		// 向分片控制器查询最新的配置
		ck.config = ck.sm.Query(-1)
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
