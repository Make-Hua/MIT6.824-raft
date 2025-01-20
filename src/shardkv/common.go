package shardkv

import (
	"fmt"
	"log"
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

// 定义各种状态码和错误码
const (
	OK             = "OK"             /* 表示操作成功 */
	ErrNoKey       = "ErrNoKey"       /* 表示请求的键不存在 */
	ErrWrongGroup  = "ErrWrongGroup"  /* 表示请求涉及到错误的组 */
	ErrWrongLeader = "ErrWrongLeader" /* 表示请求发送到了错误的 Leader 节点 */
	ErrTimeout     = "ErrTimeout"     /* 表示请求超时 */
	ErrWrongConfig = "ErrWrongConfig" /* 表示请求的配置错误 */
	ErrNotReady    = "ErrNotReady"    /* 表示操作尚未准备好 */
)

// Err 类型定义为 string 的别名，用于表示错误信息
type Err string

/* PutAppend RPC */
// PutAppendArgs 结构体用于封装 Put 或 Append 操作的参数
type PutAppendArgs struct {
	Key      string /* 要操作的键 */
	Value    string /* 要操作的值 */
	Op       string /* 操作类型，取值为 "Put" 或 "Append" */
	ClientId int64  /* 客户端的唯一标识 */
	SeqId    int64  /* 客户端请求的序列号 */
}

// PutAppendReply 结构体用于封装 Put 或 Append 操作的回复
type PutAppendReply struct {
	Err Err /* 操作执行后的错误信息，如果操作成功，Err 为 OK */
}

/* Get RPC */
// GetArgs 结构体用于封装 Get 操作的参数
type GetArgs struct {
	Key string /* 要获取值的键 */
}

// GetReply 结构体用于封装 Get 操作的回复
type GetReply struct {
	Err   Err    /* 操作执行后的错误信息，如果操作成功，Err 为 OK */
	Value string /* 获取到的值，如果键不存在，Value 为空字符串 */
}

// 定义一些时间相关的常量
const (
	ClientRequestTimeout   = 500 * time.Millisecond /* 客户端请求的超时时间 */
	FetchConfigInterval    = 100 * time.Millisecond /* 获取配置的时间间隔 */
	ShardMigrationInterval = 50 * time.Millisecond  /* 分片迁移的时间间隔 */
	ShardGCInterval        = 50 * time.Millisecond  /* 分片垃圾回收的时间间隔 */
)

// 定义 Debug 常量，用于控制是否开启调试模式
const Debug = false

// DPrintf 函数用于在调试模式下打印日志
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...) // 如果开启调试模式，使用 log.Printf 打印日志
	}
	return // 返回默认的 n 和 err 值
}

// Op 结构体用于表示一个操作
type Op struct {
	Key      string        /* 要操作的键 */
	Value    string        /* 要操作的值 */
	OpType   OperationType /* 操作类型，使用 OperationType 枚举类型 */
	ClientId int64         /* 客户端的唯一标识 */
	SeqId    int64         /* 客户端请求的序列号 */
}

// OpReply 结构体用于封装操作的回复
type OpReply struct {
	Value string /* 操作结果的值 */
	Err   Err    /* 操作执行后的错误信息 */
}

// OperationType 定义为 uint8 类型的别名，用于表示不同的操作类型
type OperationType uint8

// 定义操作类型的枚举值
const (
	OpGet    OperationType = iota /* 表示获取操作 */
	OpPut                         /* 表示设置操作 */
	OpAppend                      /* 表示追加操作 */
)

// getOperationType 函数根据传入的字符串返回对应的 OperationType 枚举值
func getOperationType(v string) OperationType {
	switch v {
	case "Put":
		return OpPut
	case "Append":
		return OpAppend
	default:
		// 如果传入的字符串不是 "Put" 或 "Append"，抛出异常
		panic(fmt.Sprintf("unknown operation type %s", v))
	}
}

// LastOperationInfo 结构体用于记录客户端的最后一次操作信息
type LastOperationInfo struct {
	SeqId int64    /* 操作的序列号 */
	Reply *OpReply /* 操作的回复 */
}

// copyData 函数用于复制 LastOperationInfo 结构体的数据
func (op *LastOperationInfo) copyData() LastOperationInfo {
	return LastOperationInfo{
		SeqId: op.SeqId,
		Reply: &OpReply{
			Err:   op.Reply.Err,
			Value: op.Reply.Value,
		},
	}
}

// RaftCommandType 定义为 uint8 类型的别名，用于表示 Raft 命令的类型
type RaftCommandType uint8

// 定义 Raft 命令类型的枚举值
const (
	ClientOpeartion RaftCommandType = iota /* 表示客户端操作命令 */
	ConfigChange                           /* 表示配置变更命令 */
	ShardMigration                         /* 表示分片迁移命令 */
	ShardGC                                /* 表示分片垃圾回收命令 */
)

// RaftCommand 结构体用于封装 Raft 命令
type RaftCommand struct {
	CmdType RaftCommandType /* 命令类型 */
	Data    interface{}     /* 命令携带的数据 */
}

// ShardStatus 定义为 uint8 类型的别名，用于表示分片的状态
type ShardStatus uint8

// 定义分片状态的枚举值
const (
	Normal  ShardStatus = iota /* 表示分片处于正常状态 */
	MoveIn                     /* 表示分片正在移入 */
	MoveOut                    /* 表示分片正在移出 */
	GC                         /* 表示分片正在进行垃圾回收 */
)

/* RPC 统一封装 */
// ShardOperationArgs 结构体用于封装分片操作的参数
type ShardOperationArgs struct {
	ConfigNum int   /* 配置编号 */
	ShardIds  []int /* 分片编号列表 */
}

// ShardOperationReply 结构体用于封装分片操作的回复
type ShardOperationReply struct {
	Err            Err                         /* 操作执行后的错误信息 */
	ConfigNum      int                         /* 配置编号 */
	ShardData      map[int]map[string]string   /* 分片数据 */
	DuplicateTable map[int64]LastOperationInfo /* 重复请求表 */
}
