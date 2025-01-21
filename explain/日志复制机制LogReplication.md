## 日志复制机制 Log Replication

### 复制流程

选出一个 leader 之后，它就开始服务客户端请求。

- 每个客户端请求中都包含一条命令，将由 replicated state machine 执行。
- leader 会将这个命令追加到它自己的 log，然后并发地通过 AppendEntries RPC 复制给其他节点。
- 复制成功（无冲突）之后，leader 才会将这个 entry 应用到自己的状态机，然后将执行结果返回给客户端。
- 如果 follower 挂掉了或很慢，或者发生了丢包，leader 会无限重试 AppendEntries 请求（即使它已经给客户端发送了响应）， 直到所有 follower 最终都存储了所有的 log entries。

### Log 文件组织结构

Log 文件组织结构具体如下所示：

![img](https://cdn.nlark.com/yuque/0/2024/png/51059439/1733988581116-d7dc13a5-e4ca-4897-aaf0-9958e367f57a.png)

1. Log 由 log entry 组成，每个 entry 都是顺序编号的，这个整数索引标识了该 entry 在 log 中的位置。
2. 每个 entry 包含了

1. 1. Leader 创建该 entry 时的任期（term，每个框中的数字），用于检测 logs 之间的不一致及确保图 3 中的某些特性；
   2. 需要执行的命令。

1. 当一条 entry 被安全地应用到状态机之后，就认为这个 entry 已经提交了（committed）。

Leader 来判断何时将一个 log entry 应用到状态机是安全的。

### 提交（commit）的定义

Raft 保证已提交的记录都是持久的，并且最终会被所有可用的状态机执行。

- 只要创建这个 entry 的 leader 将它成功复制到大多数节点，这个 entry 就算提交了。
- 这也提交了 leader log 中的所有前面的 entry，包括那些之前由其他 leader 创建的 entry。

会讨论 ldeader 变更之后应用这个规则时的情况，那时将会看到这种对于 commit 的定义也是安全的。 follower 一旦确定某个 entry 被提交了，就将这个 entry 应用到它自己的状态机（in log order）。

### Log matching 特性（保证 log 内容一致）

​	Raft 这种 log 机制的设计是为了保持各节点 log 的高度一致性（coherency）。 它不仅简化了系统行为，使系统更加可预测，而且是保证安全（无冲突）的重要组件。

如果不同 log 中的两个 entry 有完全相同的 index 和 term，那么

1. 这两个 entry 一定包含了相同的命令； 这来源于如下事实：leader 在任意给定 term 和 log index 的情况下，最多只会创建 一个 entry，并且其在 log 中的位置永远不会发生变化。
2. 这两个 log 中，从该 index 往前的所有 entry 都分别相同。 这一点是通过 AppendEntries 中简单的一致性检查来保证的：

- - AppendEntries 请求中，leader 会带上 log 中前一个紧邻 entry 的 index 和 term 信息。
  - 如果 follower log 中以相同的 index 位置没有 entry，或者有 entry 但 term 不同，follower 就会拒绝新的 entry。

以上两个条件组合起来，用归纳法可以证明 Log Matching Property：

1. 新加一个 entry 时，简单一致性检查会确保这个 entry 之前的所有 entry 都是满足 Log Matching 特性的；因此只要初始状态也满足这个特性就行了；
2. 初始状态：各 log 文件都是空的，满足 Log Matching 特性；

因此得证。

### Log 不一致场景

​	正常情况下，leader 和 follower 的 log 能保持一致，但 leader 挂掉会导致 log 不一致 （leader 还未将其 log 中的 entry 都复制到其他节点就挂了）。 这些不一致会导致一系列复杂的 leader 和 follower crash。下图展示了 follower log 与新的 leader log 的几种可能不同：

![img](https://cdn.nlark.com/yuque/0/2024/png/51059439/1733990285723-428b484f-05c4-4153-a997-7e71c624307f.png)

图中每个方框表示一个 log entry，其中的数字表示它的 term。可能的情况包括：

- 丢失记录(a–b) ；
- 有额外的未提交记录 (c–d)；
- 或者以上两种情况发生 (e–f)。
- log 中丢失或多出来的记录可能会跨多个 term。

​	例如，scenario (f) 可能是如下情况：从 term 2 开始成为 leader，然后向自己的 log 添加了一些 entry， 但还没来得及提交就挂了；然后重启后迅速又成为 term 3 期间的 leader，然后又加了一些 entry 到自己的 log， 在提交 term 2& 3 期间的 entry 之前，又挂了；随后持续挂了几个 term。

### 避免 log 不一致：`AppendEntries` 中的一致性检查

​	Raft 处理不一致的方式是强制 follower 复制一份 leader 的 log， 这意味着 follower log 中冲突的 entry 会被 leader log 中的 entry 覆盖。 后续将会看到再加上另一个限制条件后，这个日志复制机制就是安全的。

解决冲突的具体流程：

1. 找到 leader 和 follower 的最后一个共同认可的 entry，
2. 将 follower log 中从这条 entry 开始往后的 entries 全部删掉，
3. 将 leader log 中从这条记录开始往后的所有 entries 同步给 follower。

整个过程都发生在 AppendEntries RPC 中的一致性检查环节。

```go
// https://github.com/etcd-io/etcd/blob/release-0.4/third_party/github.com/goraft/raft/server.go#L939

// Processes the "append entries" request.
func (s *server) processAppendEntriesRequest(req *AppendEntriesRequest) (*AppendEntriesResponse, bool) {
    if req.Term < s.currentTerm
    return _, false

    if req.Term == s.currentTerm {
        if s.state == Candidate  // step-down to follower when it is a candidate
        s.setState(Follower)
        s.leader = req.LeaderName
    } else {
        s.updateCurrentTerm(req.Term, req.LeaderName)
    }

    // Reject if log doesn't contain a matching previous entry.
    if err := s.log.truncate(req.PrevLogIndex, req.PrevLogTerm); err != nil {
        return newAppendEntriesResponse(s.currentTerm, false, s.log.currentIndex(), s.log.CommitIndex()), true
    }

    s.log.appendEntries(req.Entries)      // Append entries to the log.
    s.log.setCommitIndex(req.CommitIndex) // Commit up to the commit index.

    // once the server appended and committed all the log entries from the leader
    return newAppendEntriesResponse(s.currentTerm, true, s.log.currentIndex(), s.log.CommitIndex()), true
}

// https://github.com/etcd-io/etcd/blob/release-0.4/third_party/github.com/goraft/raft/log.go#L399
// Truncates the log to the given index and term. This only works if the log
// at the index has not been committed.
func (l *Log) truncate(index uint64, term uint64) error {
    if index < l.commitIndex // Do not allow committed entries to be truncated.
    return fmt.Errorf("raft.Log: Index is already committed (%v): (IDX=%v, TERM=%v)", l.commitIndex, index, term)

    if index > l.startIndex + len(l.entries) // Do not truncate past end of entries.
    return fmt.Errorf("raft.Log: Entry index does not exist (MAX=%v): (IDX=%v, TERM=%v)", len(l.entries), index, term)

    // If we're truncating everything then just clear the entries.
    if index == l.startIndex {
        l.file.Truncate(0)
        l.file.Seek(0, os.SEEK_SET)
        l.entries = []*LogEntry{}
    } else {
        // Do not truncate if the entry at index does not have the matching term.
        entry := l.entries[index-l.startIndex-1]
        if len(l.entries) > 0 && entry.Term != term
        return fmt.Errorf("raft.Log: Entry at index does not have matching term (%v): (IDX=%v, TERM=%v)", entry.Term, index, term)

        // Otherwise truncate up to the desired entry.
        if index < l.startIndex+uint64(len(l.entries)) {
            position := l.entries[index-l.startIndex].Position
            l.file.Truncate(position)
            l.file.Seek(position, os.SEEK_SET)
            l.entries = l.entries[0 : index-l.startIndex]
        }
    }

    return nil
}
```

- Leader 为每个 follower 维护了后者下一个要使用的 log entry index，即 `nextIndex[followerID]` 变量；
- 一个节点成为 leader 时，会将整个 `nextIndex[]` 都初始化为它自己的 log 文件的下一个 index。
- 如果一个 follower log 与 leader 的不一致，AppendEntries 一致性检查会失败，从 而拒绝这个请求；leader 收到拒绝之后，将减小 `nextIndex[followerID]` 然后重试这个 AppendEntries RPC 请求；如此下去，直到某个 index 时成功，这说明此时 leader log 和 follower logs 已经匹配了。
- 然后 follower log 会删掉 index 之后的所有 entry，并将 leader 中的 entry 应用到 follower log 中（如果有）。 此时 follower log 就与 leader 一致了，在之后的整个 term 中都会保持一致。

### Log Replication 日志复制可视化过程

​	可视化网站：https://thesecretlivesofdata.com/raft/#intro，根据可视化网站提供的完全过程，我们逐步分析，并且对细节处进行标记。

| ![img](https://cdn.nlark.com/yuque/0/2024/png/51059439/1733990997614-9b8eab1b-853d-4fe8-a42e-3eb5cd045c9a.png) | ![img](https://cdn.nlark.com/yuque/0/2024/png/51059439/1733991159914-18a95435-9b8d-4f24-9e51-6680bd50ae7d.png) |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| 图1  日志机制过程                                            |                                                              |

​	衔接 Leader 选举机制后，一旦我们选出了领导者，我们就需要将系统的所有更改复制到所有节点。红色信号是通过使用用于检测信号的相同 Append Entries 消息来完成的。此时我们模拟下当有客户端发送消息后，我们是如何通过日志实现一致性的。

​	首先，客户端会向 leader 节点发送更改消息，leader 节点会将更改消息将附加到 leader 的日志中然后在下一个心跳时将更改发送给 followers 节点，如下所示：

| ![img](https://cdn.nlark.com/yuque/0/2024/png/51059439/1733991327766-d8dbe416-4099-4a89-904f-766c0cb8e6d1.png) | ![img](https://cdn.nlark.com/yuque/0/2024/png/51059439/1733997132448-be0097d8-dc51-44ef-801c-d31a97da109c.png) |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| 图2  日志机制过程                                            |                                                              |

​	一旦大多数关注者承认，一个条目就会被提交（如下述：红色代表为 commit，黑色代表已经 commit）并将响应交给客户端，最后在 leader 发送下次 Append Entries 消息时告诉 Follower 节点已提交，则 Follower 节点自身也会提交。

| ![img](https://cdn.nlark.com/yuque/0/2024/png/51059439/1733997396081-97febb95-023a-490f-8b3c-013a9c92a5d7.png) | ![img](https://cdn.nlark.com/yuque/0/2024/png/51059439/1733997531219-8518a48b-b6a7-421c-9814-03c0e5b89f36.png) |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| ![img](https://cdn.nlark.com/yuque/0/2024/png/51059439/1733997615218-c0bf40f5-8e57-4263-af6c-2c43c1b248bf.png) | ![img](https://cdn.nlark.com/yuque/0/2024/png/51059439/1733997763405-e7d00b70-f062-4322-805a-b8ef865c2663.png) |
| 图3  日志机制过程                                            |                                                              |

​	Raft 甚至可以在面对网络分区时保持一致。接下来我们模拟这个过程，在下述集群种存在 5 个节点，通过进行网络分区从而观察 Raft 如何保持集群节点的一致性。

| ![img](https://cdn.nlark.com/yuque/0/2024/png/51059439/1733998315179-32296113-76d7-4373-af1f-6afa33ec2391.png) | ![img](https://cdn.nlark.com/yuque/0/2024/png/51059439/1733998424719-89e0e5e9-39ba-4cf6-a3a6-dbeaf3935e32.png) |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| 图4  Raft 面对网络分区                                       |                                                              |

​	根据上述图片得知，当我们添加一个分区来分隔 A 和 B 与 C，D 和 E，在此分裂出来的 C、D、E 节点收不到 leader 节点的心跳包，则会在当前小分区内经历一次 Leader 选举，最终选出节点 C 成为 leader 节点；由于我们的分裂，我们现在有两位不同任期的领导人。

| ![img](https://cdn.nlark.com/yuque/0/2024/png/51059439/1733998790161-4170b82c-e444-4041-8f0a-03cd02faa024.png) | ![img](https://cdn.nlark.com/yuque/0/2024/png/51059439/1733999145204-9bc2faf7-10ff-4e91-8ea0-5fc507958270.png) |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| 图5  Raft 面对网络分区                                       |                                                              |

​	在此，让我们添加另一个客户端并尝试更新两个 leader。一个客户端将尝试将节点 B 的值设置为 “3”。节点 B 无法复制到多数节点，因为并没有多数节点回复节点 B 在收到 “SET 3”后所发送的 Append Entries 消息，因此其日志条目保持未提交状态，此时 A、B 所处分区则会一直保持该状态。

| ![img](https://cdn.nlark.com/yuque/0/2024/png/51059439/1733999313990-9046a76e-2220-4602-904f-bf205a9438bb.png) | ![img](https://cdn.nlark.com/yuque/0/2024/png/51059439/1733999357842-331bc0f6-213d-4366-9a01-357ea5eaaf13.png) |
| :----------------------------------------------------------: | :----------------------------------------------------------: |
|                    图6  Raft 面对网络分区                    |                                                              |

​	另一个客户端将尝试将节点 C 的值设置为 “8”，该次提交将成功，因为它在本次辅助过程种，将得到多数节点的回复，从而能够向客户端回复响应，并在下次 Append Entries 消息告诉跟随着节点提交，成功的核心在于“本次内容复制到多数节点”。

| ![img](https://cdn.nlark.com/yuque/0/2024/png/51059439/1733999620356-533955af-ca96-45cc-bb4e-fb2b042305ae.png) |
| :----------------------------------------------------------: |
|                    图7  Raft 面对网络分区                    |

​	接下来，让我们修复网络分区。节点 C 和节点 B 都将向其余节点发送  Append Entries 消息，此时节点 B 与节点 A 会发现节点 C 的选举期限更高，则 节点 B 将降级，节点 A 将 leader 信息更新。同时节点 A 和 B 都将回滚其未提交的条目。

​	节点 B 将看到更高的选举期限并降级。节点 A 和 B 都将回滚其未提交的条目并匹配新领导者的日志。

​	至此，我们的日志在整个集群中是一致的。

| ![img](https://cdn.nlark.com/yuque/0/2024/png/51059439/1734000027913-a05036f1-77fd-4909-a399-818b277602fa.png) | ![img](https://cdn.nlark.com/yuque/0/2024/png/51059439/1734000364188-801789a5-7cf1-4bb9-8025-b6da905acf73.png) |
| :----------------------------------------------------------: | ------------------------------------------------------------ |
|             图8  Raft 面对网络分区后修复网络分区             |                                                              |

### 优化

​	最后，还可以对协议进行优化，来减小被拒的 AppendEntries RPC 数量。 例如，当拒绝一个 AppendEntries 请求时，follower 可以将冲突 entry 的 term 以及 log 中相同 term 的最小 index 信息包含到响应中。 有了这个信息，leader 就可以直接跳过这个 term 中的所有冲突记录，将 `nextIndex[followerID]` 降低到一个合理值；这样每个 term 内所有的冲突记录只需要一次触发 AppendEntries RPC ， 而不是每个记录一个 RPC。

​	但实际中，我们怀疑是否有必要做这种优化，因为故障很少发生，不太可能有很多的不一致记录。 有了这个机制，新 leader 上台之后无需执行任何特殊操作来恢复 log 一致性。 它只需要执行正常操作，logs 会通过 AppendEntries 一致性检查来收敛。 leader 永远不会覆盖或删除自己 log 中的记录（Leader Append-Only Property in Figure 3）。

这种 log replication 机制展示了第二节中描述的理想共识特性：

1. 只要集群的大多数节点都是健康的，Raft 就能接受、复制和应用新的 log entry；
2. 正常情况下只需一轮 RPC 就能将一个新 entry 复制到集群的大多数节点；
3. 个别比较慢的 follower 不影响集群性能。