package shardkv

// MemoryKVStateMachine 结构体表示基于内存的键值对状态机
// 用于管理单个分片的键值对数据和状态
type MemoryKVStateMachine struct {
	KV     map[string]string /* 存储键值对数据的映射 */
	Status ShardStatus       /* 分片的状态，使用 ShardStatus 枚举类型表示 */
}

// NewMemoryKVStateMachine 函数用于创建一个新的基于内存的键值对状态机实例
func NewMemoryKVStateMachine() *MemoryKVStateMachine {
	return &MemoryKVStateMachine{
		KV:     make(map[string]string), // 初始化键值对映射
		Status: Normal,                  // 初始状态设置为 Normal
	}
}

// Get 函数用于从状态机中获取指定键的值
func (mkv *MemoryKVStateMachine) Get(key string) (string, Err) {
	// 检查键是否存在于键值对映射中
	if value, ok := mkv.KV[key]; ok {
		// 如果存在，返回对应的值和成功状态
		return value, OK
	}
	// 如果键不存在，返回空字符串和错误状态 ErrNoKey
	return "", ErrNoKey
}

// Put 函数用于在状态机中设置指定键的值
func (mkv *MemoryKVStateMachine) Put(key, value string) Err {
	// 将键值对存储到键值对映射中
	mkv.KV[key] = value
	// 返回成功状态
	return OK
}

// Append 函数用于将指定的值追加到状态机中指定键的值后面
func (mkv *MemoryKVStateMachine) Append(key, value string) Err {
	// 将值追加到键对应的值后面
	mkv.KV[key] += value
	// 返回成功状态
	return OK
}

// copyData 函数用于复制状态机中的键值对数据
func (mkv *MemoryKVStateMachine) copyData() map[string]string {
	// 创建一个新的键值对映射
	newKV := make(map[string]string)
	// 遍历当前状态机中的所有键值对
	for k, v := range mkv.KV {
		// 将每个键值对复制到新的映射中
		newKV[k] = v
	}
	// 返回复制后的键值对映射
	return newKV
}
