# Raft 分布式共识算法

## Raft 概念

Raft 是一个管理复制式日志（replicated log）的共识算法 （consensus algorithm）。它的最终结果与 (multi-) Paxos 等价，也与 Paxos 一样高效，但结构（structure）与 Paxos 不同 —— 这使得它比 Paxos 更好理解，也更易于构建实际系统。

为易于理解，Raft 在设计上将共识拆分为 leader election、log replication、safety 等几个模块； 另外，为减少状态数量，它要求有更强的一致性（a stronger degree of coherency）。 从结果来看，学生学习 Raft 要比学习 Paxos 容易。

Raft 还引入了一个新的集群变更（添加或删除节点）机制，利用 重叠大多数（overlapping majorities）特性来保证安全（不会发生冲突）。

而 paxos 相较于 Raft  主要有两个致命的缺点：

1. 出名地难理解（exceptionally difficult）；
2. 没有考虑真实系统的实现，很难用于实际系统。

在此认为 Paxos 既不能为开发真实系统提供良好基础，又不适用于教学。 而考虑到大型软件系统中共识的重要性，所以经过 Diego Ongaro 的沉淀，他设计一种替代 Paxos、有更好特性的共识算法，Raft 就是这一实验的产物。

注：下述多半内容均翻译自 Raft 论文原文：https://raft.github.io/raft.pdf 

## Raft 基础

一个 Raft cluster 包括若干台节点，例如典型的 5 台，这样一个集群能容忍 2 台节点发生故障。

### **Raft 状态机**

在任意给定时刻，每个节点处于以下三种状态之一： leader（领导者）、follower（追随者）、candidate（候选人）。

- 正常情况下，集群中有且只有一个 leader，剩下的都是 follower。
- Follower 都是被动的（passive）：它们不会主动发出请求，只会简单响应来自 leader 和 candidate 的请求。
- Leader 负责处理所有的客户端请求；如果一个客户端向 follower 发起请求，后者会将它重定向到 leader。
- Candidate 是一个特殊状态，选举新 leader 时会用到，将在 5.2 节介绍。

![img](https://cdn.nlark.com/yuque/0/2024/png/51059439/1734144388487-4a8ff1f4-5702-4124-9d32-d7309fbd581d.png)

几点说明：

① Follower 只会响应来自其他节点的请求；如果一个 follower 某段时间内收不到 leader 的请求，它会变成一个 candidate 然后发起一轮选举

② 获得大多数选票的 candidate 将成为新的 leader

③ 通常情况下，除非发生故障，否则在任的 leader 会持续担任下去

### **Raft 任期**

Raft 将时间划分为长度不固定的任期，任期用连续的整数表示，如下所示：

![img](https://cdn.nlark.com/yuque/0/2024/png/51059439/1733917420522-5516efb3-25e9-4a9a-aa68-f70dca00f390.png)

几点说明：

① 每个任期都是从选举开始的，此时多个 candidate 都试图成为 leader。

② 某个 candidate 赢得选举后，就会成为该任期内的 leader。 Raft 保证了在任意一个任期内，最多只会有一个 leader。

③ 有时选举会失败（投票结果会分裂），这种情况下该任期内就没有 leader。 但问题不大，因为很快将开始新一轮选举（产生一个新任期）。

④ 不同节点上看到的任期转换时刻可能会不同。 在某些情况下，一个节点可能观察不到某次选举甚至整个任期。

另外，

- Raft 中，任期是一个逻辑时钟，用来让各节点检测过期信息，例如过期的 leader。
- 每个节点都记录了当前任期号 currentTerm，这个编号随着时间单调递增。
- 节点之间通信时，会带上它们各自的 currentTerm 信息；

- - 如果一个节点发现自己的 currentTerm 小于其他节点的，要立即更新自己的；
  - 如果一个 candidate 或 leader 发现自己的任期过期了，要立即切换到 follower 状态。
  - 如果一个节点接收到携带了过期任期编号（stale term）的请求，会拒绝这个请求。

### **节点之间通信 RPC**

Raft 服务器之间通过 RPC 通信，基础的共识算法只需要两种 RPC（前面已经给出了参考实现）：

1. RequestVote：由 candidates 在选举期间发起 (Section 5.2)；
2. AppendEntries：由 leader 发起，用于 replicate log entries 并作为一种 heartbeat 方式 (Section 5.3). 

如果在一段时间内没有收到响应，服务器会对 RPC 进行重试；另外，为了最佳性能，服务器会并发执行 RPC。