package kvraft

import (
	"fmt"
	"log"
	"time"
)

// 定义常量
const (
	OK             = "OK"             /* 表示操作成功的状态码 */
	ErrNoKey       = "ErrNoKey"       /* 表示请求的键不存在的错误码 */
	ErrWrongLeader = "ErrWrongLeader" /* 表示请求发送到了错误的 Leader 节点的错误码 */
	ErrTimeout     = "ErrTimeout"     /* 表示请求超时的错误码 */
)

// Err 类型定义为 string 类型的别名，用于表示各种错误情况
type Err string

/* PutAppend RPC 的发包和收包 */
// PutAppendArgs 结构体用于封装 Put 或 Append 操作的参数
type PutAppendArgs struct {
	Key      string /* 要操作的键 */
	Value    string /* 要操作的值 */
	Op       string /* 操作类型，取值为 "Put" 或 "Append" */
	ClientId int64  /* 客户端的唯一标识，用于防止重复请求和标识客户端 */
	SeqId    int64  /* 客户端请求的序列号，用于保证请求的顺序性和防止重复请求 */

	// 字段名必须以大写字母开头，否则 RPC 将无法正常工作（Go 语言 RPC 机制要求）
}

// PutAppendReply 结构体用于封装 Put 或 Append 操作的回复
type PutAppendReply struct {
	Err Err /* 操作执行后的错误信息，如果操作成功，Err 为 OK */
}

/* Get RPC 的发包和收包 */
// GetArgs 结构体用于封装 Get 操作的参数
type GetArgs struct {
	Key string /* 要获取值的键 */
}

// GetReply 结构体用于封装 Get 操作的回复
type GetReply struct {
	Err   Err    /* 操作执行后的错误信息，如果操作成功，Err 为 OK */
	Value string /* 获取到的值，如果键不存在，Value 为空字符串 */
}

// 定义客户端请求的超时时间为 500 毫秒
const ClientRequestTimeout = 500 * time.Millisecond

// 定义 Debug 常量，用于控制是否开启调试模式
// 为 false 时表示关闭调试模式
const Debug = false

// DPrintf 函数用于在调试模式下打印日志信息
// 它接受一个格式化字符串和可变参数列表
// 如果 Debug 为 true，则使用 log.Printf 打印日志，否则不进行任何操作
func DPrintf(format string, a ...interface{}) (n int, err error) {

	if Debug {
		// 如果 Debug 为 true，使用 log.Printf 按照指定格式打印日志
		log.Printf(format, a...)
	}
	// 返回打印的字符数和可能的错误，由于在不打印时没有实际操作，所以这里返回默认值
	return
}

// Op 结构体用于表示一个操作
// 包含了操作的各种信息，用于在 Raft 集群中传递和处理
type Op struct {
	Key      string        /* 要操作的键 */
	Value    string        /* 要操作的值 */
	OpType   OperationType /* 操作类型，使用 OperationType 枚举类型 */
	ClientId int64         /* 客户端的唯一标识，用于标识请求来自哪个客户端 */
	SeqId    int64         /* 客户端请求的序列号，用于保证请求的顺序性和防止重复请求 */
}

// OpReply 结构体用于封装操作的回复
// 包含操作结果的值和可能的错误信息
type OpReply struct {
	Value string /* 操作结果的值 */
	Err   Err    /* 操作执行后的错误信息，如果操作成功，Err 为 OK */
}

// OperationType 定义为 uint8 类型的别名，用于表示不同的操作类型
// 使用枚举方式定义不同的操作类型
type OperationType uint8

// 定义操作类型的枚举值
const (
	OpGet    OperationType = iota /* OpGet 表示获取操作 */
	OpPut                         /* OpPut 表示设置操作 */
	OpAppend                      /* OpAppend 表示追加操作 */
)

// getOperationType 函数根据传入的字符串返回对应的 OperationType 枚举值
// 如果传入的字符串不匹配任何已知的操作类型，会触发 panic
func getOperationType(v string) OperationType {
	switch v {
	case "Put":
		return OpPut
	case "Append":
		return OpAppend
	default:
		// 如果传入的字符串不是 "Put" 或 "Append"，抛出 panic 并提示未知的操作类型
		panic(fmt.Sprintf("unknown operation type %s", v))
	}
}

// LastOperationInfo 结构体用于记录客户端的最后一次操作信息
// 包含操作的序列号和对应的回复
type LastOperationInfo struct {
	SeqId int64    /* 操作的序列号 */
	Reply *OpReply /* 操作的回复 */
}
