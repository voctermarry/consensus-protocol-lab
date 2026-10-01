## 用途

本项目是「分布式共识协议实验平台」的代码仓库，用于逐步实现该方向的共识流程仿真、故障注入与不变量校验能力。

当前已实现确定性的 Raft 仿真：选主、心跳、日志复制与提交。只推进虚拟时间，不读取墙钟、不使用随机数，同一输入产生逐字节一致的输出。

## 环境与安装

- Python 3.11 及以上

```bash
python -m pip install -e .
```

## 测试

```bash
python -m pytest
```

## 命令行入口

安装后提供 `consensus-protocol-lab` 命令：

```bash
consensus-protocol-lab version              # 打印版本号
consensus-protocol-lab simulate SCENARIO    # 运行 Raft 选主仿真
consensus-protocol-lab --help               # 打印用法
```

## simulate 子命令

`simulate` 读取一个 UTF-8 JSON 场景文件，只运行仿真，不创建任何文件。

### 输入字段

- `nodes`（必填）：至少三个不重复的非空名称。
- `duration`（必填）：仿真时长，非负整数毫秒；只处理该时刻及之前的事件。
- `electionTimeouts`（必填）：正整数（全体节点共用），或按节点名给出正整数的对象（必须恰好覆盖所有节点）。
- `heartbeatInterval`（必填）：正整数毫秒，leader 的心跳周期。
- `messageDelay`（必填）：非负整数毫秒，消息投递延迟。
- `faults`（可选）：故障列表，按输入顺序处理同一时刻的故障：
  - `{"time": t, "action": "partition", "groups": [[...], [...]]}`：两个分组互不重叠、各自非空并覆盖全部节点；组间消息在投递时丢弃。
  - `{"time": t, "action": "heal"}`：恢复全连通。
  - `time` 必须在 `[0, duration]` 范围内。
- `clientCommands`（可选）：客户命令列表，每项为 `{"time": t, "node": name, "id": id, "command": value}`：
  - `time` 在 `[0, duration]` 范围内；`node` 必须是已声明的节点；`id` 为全局唯一非空字符串；`command` 为任意 JSON 值。
  - 同一时刻的命令按输入顺序处理；字段缺失或未知、类型非法、id 重复都会报错。

### 语义

- 节点从任期 0 的 follower 开始；选举超时后成为 candidate、任期加一、自投并广播 RequestVote。
- 每任期最多投一票；选票还要求候选日志不旧于投票者（先比较最后日志任期，再比较最后日志索引），日志较旧者不能获票；获得全体节点严格多数者成为 leader。
- 收到更高任期的消息会更新任期并退回 follower。
- leader 当选后立即并在每个心跳周期发送心跳；follower 收到合法心跳后重置固定超时。
- 客户命令发给非 leader 时不写日志，时间线记录 `rejected`/`notLeader` 与当前 `knownLeader`；发给 leader 时追加含索引、任期、id、命令的日志条目并记录 `accepted`，随后向所有节点发送 AppendEntries。
- AppendEntries 携带前一索引与任期、待复制条目和 `leaderCommit`；follower 仅在前缀匹配时接受：前一条目任期冲突时删除冲突位置及后缀后拒绝，前缀不足时直接拒绝；接受时合并条目并按 `leaderCommit` 推进本地提交位置。leader 对失败的回复逐项回退 `nextIndex` 并重试。
- 当前任期的条目被包含 leader 的严格多数复制后，leader 才推进 `commitIndex`；节点按索引顺序应用已提交条目。
- 同一时刻依次处理故障、消息、客户命令、超时、心跳；故障按输入顺序、客户命令按输入顺序、节点事件按 `nodes` 顺序，timeline 以递增 `seq` 记录。

### 输出

成功时标准输出只写一个 JSON 对象：

- `timeline`：按 `seq` 递增的事件列表（`timeout`、`stateChange`、`messageSend`、`messageResult`、`fault`、`clientResult`、`apply`），含 `time`、`node`、`term`、`peer`/`reason` 等字段；复制与回复以 `appendEntries`/`appendEntriesReply` 消息记录，客户结果记录 `accepted`/`rejected`/`committed`/`superseded`。
- `nodes`：各节点最终的 `role`、`term`、`votedFor`、`knownLeader`，以及 `log`（含 `index`、`term`、`id`、`command` 的条目）、`commitIndex`、`lastApplied`、`applied`（已应用条目的 `index`、`id`、`command`）。
- `clients`：按输入顺序汇总每条客户命令的最终状态：`committed`、`superseded`、`pending` 或 `rejected`；被接受的命令附 `index`/`term`，被拒绝的附 `knownLeader`。同一 id 的提交结果至多记录一次。
- `electionSafety`：`leadersByTerm` 按任期列出当选的 leader；同一任期出现多个 leader 时记入 `violations`，否则为空列表。
- `logMatching`：若两个节点在同一索引存有同任期条目但前缀不同，记入 `violations`，否则为空列表。
- `stateMachineSafety`：若不同节点在同一索引应用了不同命令，记入 `violations`，否则为空列表。

未提供 `clientCommands` 时，各节点日志为空、`clients` 为空、两份新报告的 `violations` 均为空，选举轨迹与既有字段值与之前版本一致。

### 错误

文件不可读、非 UTF-8、JSON 语法错误、字段缺失或未知、节点引用非法、故障超界或分区不合法、客户命令字段/时间/节点/id 非法或 id 重复时，不输出部分结果：标准错误写一行以 `error: ` 开头的说明并返回退出码 2。

## 现有公开接口

- 命令行程序 `consensus-protocol-lab`（`version`、`simulate` 子命令）
- Python 包 `consensus_lab`，其 `__version__` 为当前版本号

## 限制

- 仿真不读取墙钟、不使用随机数，也不持久化任何状态。
