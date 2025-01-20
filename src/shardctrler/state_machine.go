package shardctrler

import "sort"

// CtrlerStateMachine 结构体表示分片控制器的状态机
// 用于管理和操作配置信息
type CtrlerStateMachine struct {
	Configs []Config /* 存储所有配置的切片 */
}

// NewCtrlerStateMachine 函数创建一个新的 CtrlerStateMachine 实例
func NewCtrlerStateMachine() *CtrlerStateMachine {
	// 创建一个 CtrlerStateMachine 实例，并初始化 Configs 切片，长度为 1
	cf := &CtrlerStateMachine{Configs: make([]Config, 1)}
	// 将第一个配置设置为默认配置
	cf.Configs[0] = DefaultConfig()
	return cf
}

// Query 函数用于查询指定编号的配置
func (csm *CtrlerStateMachine) Query(num int) (Config, Err) {
	// 如果查询的配置编号超出范围
	if num < 0 || num >= len(csm.Configs) {
		// 返回最后一个配置，并标记操作成功
		return csm.Configs[len(csm.Configs)-1], OK
	}
	// 返回指定编号的配置，并标记操作成功
	return csm.Configs[num], OK
}

// Join 加入新的 Group 到集群中，需要处理加入之后的负载均衡问题
func (csm *CtrlerStateMachine) Join(groups map[int][]string) Err {
	// 获取当前配置的数量
	num := len(csm.Configs)
	// 获取最后一个配置
	lastConfig := csm.Configs[num-1]
	// 构建新的配置
	newConfig := Config{
		Num:    num,
		Shards: lastConfig.Shards,
		Groups: copyGroups(lastConfig.Groups),
	}

	// 将新的 Group 加入到 Groups 中
	for gid, servers := range groups {
		// 如果该组 ID 不存在于新配置的 Groups 中
		if _, ok := newConfig.Groups[gid]; !ok {
			// 复制服务器列表
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			// 将新的组和服务器列表添加到新配置的 Groups 中
			newConfig.Groups[gid] = newServers
		}
	}

	// 构造 gid -> shardid 映射关系
	// 用于后续的分片负载均衡调整
	gidToShards := make(map[int][]int)
	for gid := range newConfig.Groups {
		gidToShards[gid] = make([]int, 0)
	}
	for shard, gid := range newConfig.Shards {
		gidToShards[gid] = append(gidToShards[gid], shard)
	}

	// 进行 shard 迁移，以实现负载均衡
	for {
		// 获取拥有最多分片的组 ID 和拥有最少分片的组 ID
		maxGid, minGid := gidWithMaxShards(gidToShards), gidWithMinShards(gidToShards)
		// 如果拥有最多分片的组和拥有最少分片的组的分片数量差不超过 1
		if maxGid != 0 && len(gidToShards[maxGid])-len(gidToShards[minGid]) <= 1 {
			// 说明负载均衡已达到较好状态，退出循环
			break
		}

		// 将拥有最多分片的组的第一个分片移动到拥有最少分片的组
		gidToShards[minGid] = append(gidToShards[minGid], gidToShards[maxGid][0])
		// 从拥有最多分片的组中移除第一个分片
		gidToShards[maxGid] = gidToShards[maxGid][1:]
	}

	// 得到新的 gid -> shard 信息之后，存储到 shards 数组中
	var newShards [NShards]int
	for gid, shards := range gidToShards {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	// 更新新配置的分片信息
	newConfig.Shards = newShards
	// 将新配置添加到 Configs 切片中
	csm.Configs = append(csm.Configs, newConfig)

	// 返回操作成功的标志
	return OK
}

// Leave 函数用于处理组离开集群的操作
func (csm *CtrlerStateMachine) Leave(gids []int) Err {
	// 获取当前配置的数量
	num := len(csm.Configs)
	// 获取最后一个配置
	lastConfig := csm.Configs[num-1]
	// 构建新的配置
	newConfig := Config{
		Num:    num,
		Shards: lastConfig.Shards,
		Groups: copyGroups(lastConfig.Groups),
	}

	// 构造 gid -> shard 的映射关系
	gidToShards := make(map[int][]int)
	for gid := range newConfig.Groups {
		gidToShards[gid] = make([]int, 0)
	}
	for shard, gid := range newConfig.Shards {
		gidToShards[gid] = append(gidToShards[gid], shard)
	}

	// 删除对应的 gid，并且将对应的 shard 暂存起来
	var unassignedShards []int
	for _, gid := range gids {
		// 如果该组 ID 在 Group 中，则删除掉
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}
		// 取出对应的 shard
		if shards, ok := gidToShards[gid]; ok {
			unassignedShards = append(unassignedShards, shards...)
			delete(gidToShards, gid)
		}
	}

	var newShards [NShards]int
	// 重新分配被删除的 gid 对应的 shard
	if len(newConfig.Groups) != 0 {
		for _, shard := range unassignedShards {
			// 获取拥有最少分片的组 ID
			minGid := gidWithMinShards(gidToShards)
			// 将分片添加到拥有最少分片的组
			gidToShards[minGid] = append(gidToShards[minGid], shard)
		}

		// 重新存储 shards 数组
		for gid, shards := range gidToShards {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	}

	// 将配置保存
	newConfig.Shards = newShards
	// 将新配置添加到 Configs 切片中
	csm.Configs = append(csm.Configs, newConfig)
	// 返回操作成功的标志
	return OK
}

// Move 函数用于将指定的分片移动到指定的组
func (csm *CtrlerStateMachine) Move(shardid, gid int) Err {
	// 获取当前配置的数量
	num := len(csm.Configs)
	// 获取最后一个配置
	lastConfig := csm.Configs[num-1]
	// 构建新的配置
	newConfig := Config{
		Num:    num,
		Shards: lastConfig.Shards,
		Groups: copyGroups(lastConfig.Groups),
	}

	// 更新新配置中指定分片的所属组
	newConfig.Shards[shardid] = gid
	// 将新配置添加到 Configs 切片中
	csm.Configs = append(csm.Configs, newConfig)
	// 返回操作成功的标志
	return OK
}

// copyGroups 函数用于复制组的服务器列表
func copyGroups(groups map[int][]string) map[int][]string {
	// 创建一个新的组映射，大小与传入的组映射相同
	newGroup := make(map[int][]string, len(groups))
	for gid, servers := range groups {
		// 复制每个组的服务器列表
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		// 将复制后的服务器列表添加到新的组映射中
		newGroup[gid] = newServers
	}
	return newGroup
}

// gidWithMaxShards 函数用于获取拥有最多分片的组 ID
func gidWithMaxShards(gidToShars map[int][]int) int {
	// 如果组 ID 为 0 的组存在且拥有分片
	if shard, ok := gidToShars[0]; ok && len(shard) > 0 {
		// 直接返回 0
		return 0
	}

	// 为了确保每个节点获取到的配置一致，对组 ID 进行排序
	var gids []int
	for gid := range gidToShars {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	// 初始化最大组 ID 和最大分片数量
	maxGid, maxShards := -1, -1
	for _, gid := range gids {
		// 如果当前组的分片数量大于最大分片数量
		if len(gidToShars[gid]) > maxShards {
			// 更新最大组 ID 和最大分片数量
			maxGid, maxShards = gid, len(gidToShars[gid])
		}
	}
	return maxGid
}

// gidWithMinShards 函数用于获取拥有最少分片的组 ID
func gidWithMinShards(gidToShars map[int][]int) int {
	// 为了确保每个节点获取到的配置一致，对组 ID 进行排序
	var gids []int
	for gid := range gidToShars {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	// 初始化最小组 ID 和最小分片数量
	minGid, minShards := -1, NShards+1
	for _, gid := range gids {
		// 如果当前组 ID 不为 0 且分片数量小于最小分片数量
		if gid != 0 && len(gidToShars[gid]) < minShards {
			// 更新最小组 ID 和最小分片数量
			minGid, minShards = gid, len(gidToShars[gid])
		}
	}
	return minGid
}
