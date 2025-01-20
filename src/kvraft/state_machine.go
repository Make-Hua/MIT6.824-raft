package kvraft

// MemoryKVStateMachine 结构体定义了一个基于内存的键值对状态机
type MemoryKVStateMachine struct {
	KV map[string]string // 用于存储键值对的 map
}

// 用于创建一个新的 MemoryKVStateMachine 实例
func NewMemoryKVStateMachine() *MemoryKVStateMachine {
	return &MemoryKVStateMachine{
		KV: make(map[string]string), // 初始化 KV map
	}
}

// Get 函数用于从状态机中获取指定键的值
func (mkv *MemoryKVStateMachine) Get(key string) (string, Err) {

	// 检查键是否存在于 KV map 中
	if value, ok := mkv.KV[key]; ok {
		// 如果存在，返回对应的值和 OK 错误码（表示成功）
		return value, OK
	}

	// 如果键不存在，返回空字符串和 ErrNoKey 错误码（表示键不存在）
	return "", ErrNoKey
}

// Put 函数用于将一个键值对存储到状态机中
func (mkv *MemoryKVStateMachine) Put(key, value string) Err {

	// 将键值对存储到 KV map 中
	mkv.KV[key] = value

	// 返回 OK 错误码，表示操作成功
	return OK
}

// Append 函数用于将给定的值追加到指定键对应的值后面
func (mkv *MemoryKVStateMachine) Append(key, value string) Err {

	// 将给定的值追加到键对应的值后面
	mkv.KV[key] += value
	// 返回 OK 错误码，表示操作成功
	return OK
}
