package shardctrler

import (
	"log"
	"time"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// NShards 定义了分片的数量
const NShards = 10

// Config 结构体表示一个配置，即分片到组的分配
type Config struct {
	Num    int              /* 配置编号 */
	Shards [NShards]int     /* 定义每个分片对应的组 ID，数组长度为 NShards */
	Groups map[int][]string /* 组 ID 到服务器列表的映射，即每个组包含哪些服务器 */
}

// 返回一个默认的配置
func DefaultConfig() Config {
	return Config{
		Groups: make(map[int][]string), // 初始化 Groups 为空的映射
	}
}

// 定义各种状态码和错误码
const (
	OK             = "OK"             /* 表示操作成功 */
	ErrNoKey       = "ErrNoKey"       /* 表示请求的键不存在的错误 */
	ErrWrongLeader = "ErrWrongLeader" /* 表示请求发送到了错误的 Leader 节点 */
	ErrTimeout     = "ErrTimeout"     /* 表示请求超时 */
)

// Err 类型定义为 string 的别名，用于表示错误信息
type Err string

// JoinArgs 结构体用于封装加入操作的参数
type JoinArgs struct {
	Servers  map[int][]string /* 新的组 ID 到服务器列表的映射，用于指定要加入的组及其服务器 */
	ClientId int64            /* 客户端的唯一标识 */
	SeqId    int64            /* 客户端请求的序列号 */
}

// JoinReply 结构体用于封装加入操作的回复
type JoinReply struct {
	WrongLeader bool /* 表示请求是否发送到了错误的 Leader 节点 */
	Err         Err  /* 操作执行过程中可能出现的错误信息 */
}

// LeaveArgs 结构体用于封装离开操作的参数
type LeaveArgs struct {
	GIDs     []int /* 要离开的组 ID 列表 */
	ClientId int64 /* 客户端的唯一标识 */
	SeqId    int64 /* 客户端请求的序列号 */
}

// LeaveReply 结构体用于封装离开操作的回复
type LeaveReply struct {
	WrongLeader bool /* 表示请求是否发送到了错误的 Leader 节点 */
	Err         Err  /* 操作执行过程中可能出现的错误信息 */
}

// MoveArgs 结构体用于封装移动操作的参数
type MoveArgs struct {
	Shard    int   /* 要移动的分片编号 */
	GID      int   /* 目标组 ID，即分片要移动到的组 */
	ClientId int64 /* 客户端的唯一标识 */
	SeqId    int64 /* 客户端请求的序列号 */
}

// MoveReply 结构体用于封装移动操作的回复
type MoveReply struct {
	WrongLeader bool /* 表示请求是否发送到了错误的 Leader 节点 */
	Err         Err  /* 操作执行过程中可能出现的错误信息 */
}

// QueryArgs 结构体用于封装查询操作的参数
type QueryArgs struct {
	Num int /* 期望查询的配置编号 */
}

// QueryReply 结构体用于封装查询操作的回复
type QueryReply struct {
	WrongLeader bool   /* 表示请求是否发送到了错误的 Leader 节点 */
	Err         Err    /* 操作执行过程中可能出现的错误信息 */
	Config      Config /* 查询到的配置信息 */
}

// 定义客户端请求的超时时间为 500 毫秒
const ClientRequestTimeout = 500 * time.Millisecond

// 定义 Debug 常量，用于控制是否开启调试模式，false 表示关闭
const Debug = false

// DPrintf 函数用于在调试模式下打印日志
// 如果 Debug 为 true，则使用 log.Printf 打印日志，否则不进行任何操作
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...) // 使用 log.Printf 按照指定格式打印日志
	}
	return // 返回默认的 n 和 err 值
}

// Op 结构体用于表示一个操作，包含了各种操作的通用信息
type Op struct {

	// Join 操作
	Servers map[int][]string /* 记录新的组 ID 到服务器列表的映射 */

	// Leave 操作
	GIDs []int /* 记录要离开的组 ID 列表 */

	// Move 操作
	Shard int /* 记录要移动的分片编号 */
	GID   int /* 记录目标组 ID */

	// Query 操作
	Num int /* 记录期望查询的配置编号 */

	OpType   OperationType /* 操作类型，使用 OperationType 枚举类型 */
	ClientId int64         /* 客户端的唯一标识 */
	SeqId    int64         /* 客户端请求的序列号 */
}

// OpReply 结构体用于封装操作的回复，包含操作结果的配置信息和可能的错误
type OpReply struct {
	ControllerConfig Config /* 操作结果的配置信息 */
	Err              Err    /* 操作执行过程中可能出现的错误信息 */
}

// OperationType 定义为 uint8 类型的别名，用于表示不同的操作类型
type OperationType uint8

// 定义操作类型的枚举值
const (
	OpJoin  OperationType = iota /* 表示加入操作 */
	OpLeave                      /* 表示离开操作 */
	OpMove                       /* 表示移动操作 */
	OpQuery                      /* 表示查询操作 */
)

// LastOperationInfo 结构体用于记录客户端的最后一次操作信息
type LastOperationInfo struct {
	SeqId int64    /* 操作的序列号 */
	Reply *OpReply /* 操作的回复 */
}
