![9e582bec-11dc-46fc-a1f3-bc83a925f635](./assets/9e582bec-11dc-46fc-a1f3-bc83a925f635.jpeg)

​	**Q：为什么leader只能提交当前任期的日志**

​	  A：[为什么leader只能提交当前任期的日志](https://zhuanlan.zhihu.com/p/369989974)



​	**Q：raft 算法中 leader 提交日志后，follower 只能通过 leader 心跳决定日志是否提交吗？**
​	A：不对。Leader 将日志复制到多数节点后标记为已提交并更新 commitIndex。Follower 主要通过 Leader 发送的 AppendEntries RPC 确定日志提交，该请求含新日志条目与 commitIndex，Follower 接收后更新自身 commitIndex 并应用日志。心跳主要用于维持 Leader 权威，虽也带 commitIndex，但 Follower 确定提交靠 AppendEntries RPC。



​	**Q：leader 不发起 AppendEntries RPC，follower 会落后一条日志吗？**
​	A：不一定。若 Leader 不发 AppendEntries RPC，可能因故障或网络问题，Follower 无法及时接收新日志，会逐渐落后，且可能不止落后一条。若 Follower 自身有问题，也会导致日志同步延迟。不过 Raft 有容错和恢复机制，如 Leader 恢复后会补发日志，Follower 发现不一致会请求缺失日志



​	**Q：CurrentTerm 的作用是什么？**

1. 决定节点身份：

   - 节点发现自身 `CurrentTerm` 小于其他节点时，立即转为 Follower（即使是 Leader）。
   - Candidate 收到更高任期响应时，放弃竞选并转为 Follower。
   - Leader 发现更高任期后，主动退位为 Follower。

2. **全局时钟同步**：确保所有节点对 “当前选举轮次” 一致，避免过期节点干扰集群。

   

​	**Q：日志条目的 Term 的作用是什么？**

1. 决定投票结果
   - Follower 仅投票给日志 “至少和自己一样新” 的 Candidate。
   - “一样新” 的判断标准：
     - **日志最后一条的任期号更大**（优先）。
     - **任期号相同，但日志索引更大**（条目更多）。
2. **安全性保证**：确保新当选 Leader 包含所有已提交日志条目（通过多数派投票和日志比较实现）。



​	**Q：CurrentTerm 和日志条目的 Term 有哪些核心区别？**

| **比较维度** | **CurrentTerm**                  | **日志条目的 Term**         |
| ------------ | -------------------------------- | --------------------------- |
| **比较对象** | 节点自身任期号 vs 其他节点任期号 | 日志条目创建时的任期号      |
| **触发时机** | 节点间通信时（如收到 RPC 响应）  | 选举阶段（RequestVote RPC） |
| **决策逻辑** | 任期小的节点更新为任期大的节点   | 日志新的节点获得投票        |
| **核心目的** | 维护全局状态一致性               | 保证新 Leader 日志完整性    |
| **最终效果** | 确保单一 Leader 存在             | 防止已提交日志被覆盖        |



​	**Q:每一个节点都需要维护一个VoteFor，那么这个VoteFor的变更规则是什么？**

​	A:首先VoteFor和CurrentTerm是配对的，一开始的时候是空。

1. 当candidate请求投票的任期与该节点的任期相同时，比较日志条目再决定是否投票，若投票就修改VoteFor。
2. 当candidate的任期小于该节点的任期的时候直接拒绝投票，并且会返回这个节点的term，candidate接收到比自己大的term就应该终止选举。
3. 当candidate的任期大于该节点的任期的时候，先将votefor设为空，该节点的任期变为较大的那个任期，然后比较任期和日志条目来决定是否投票。
