## 用途

本项目是「分布式共识协议实验平台」的代码仓库，用于逐步实现该方向的共识流程仿真、故障注入与不变量校验能力。

当前已实现确定性的 Raft 选主与日志复制仿真：只推进虚拟时间，不读取墙钟、不使用随机数，同一输入产生逐字节一致的输出。

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
consensus-protocol-lab simulate SCENARIO    # 运行 Raft 选主与日志复制仿真
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
- `clientCommands`（可选）：客户命令列表，同一时刻先处理故障与已到达消息，再按输入顺序处理客户命令，最后处理超时与心跳。每项包含：
  - `time`：`[0, duration]` 内的非负整数虚拟时间。
  - `node`：接收命令的已有节点名（非空字符串）。
  - `id`：全局唯一的非空字符串，用于汇总该命令的最终结局。
  - `command`：任意 JSON 值。
- `nodeEvents`（可选）：节点崩溃与重启事件列表，同一时刻在网络故障之后、消息之前按输入顺序生效。每项包含：
  - `time`：`[0, duration]` 内的非负整数虚拟时间。
  - `node`：已有节点名。
  - `action`：`crash` 或 `restart`。同一节点的事件必须从 `crash` 开始并严格交替。

### 语义

- 节点从任期 0 的 follower 开始；选举超时后成为 candidate、任期加一、自投并广播 RequestVote。
- 每任期最多投一票；RequestVote 携带最后日志任期与索引，候选人日志必须与投票人至少同样新才能获票；获得全体节点严格多数者成为 leader。
- 收到更高任期的消息会更新任期并退回 follower。
- leader 当选后立即并在每个心跳周期向每个节点发送消息：对落后节点发送携带日志的 appendEntries（前一索引与任期、待复制条目、leaderCommit），对已追平节点发送携带 leaderCommit 的空心跳。
- 命令发给非 leader 时不写日志，记录 `rejected`/`notLeader` 及当时的 `knownLeader`；发给 leader 时追加含 `index`、`term`、`id`、`command` 的条目并记录 `accepted`。
- follower 仅在前一索引处任期匹配（前缀匹配）时接受 appendEntries；冲突时删除冲突位置及其后缀，再追加；否则回复拒绝，leader 将该节点的下一索引确定性地回退一位并重试。
- 当某索引的条目（其任期须为 leader 当前任期）被包含 leader 自身的严格多数节点复制后，leader 才推进 `commitIndex`；节点按索引顺序应用已提交条目。后续有效复制（含心跳携带的 leaderCommit）使 follower 更新提交位置并应用。
- 同一时刻依次处理故障、nodeEvents、消息、客户命令及其零延迟复制级联、超时、心跳；同刻事件按 `nodes` 顺序，timeline 以全局递增 `seq` 记录。
- 节点在线时，任期、投票、日志、`commitIndex`、`lastApplied` 与 `applied` 的每次变化都在相关响应之前同步持久化（leader 接受命令前先持久化日志）；持久化不读取墙钟、不使用随机数。
- `crash` 后节点离线：不发送或处理任何消息，不触发选举超时、心跳或客户命令；发往离线节点的消息在到达时记为 `dropped`/`nodeDown`，而崩溃前已发出的消息仍按原时间投递。发给离线节点的客户命令记为 `rejected`/`nodeDown`，`knownLeader` 为 `null`。
- `restart` 恢复持久化状态（`term`、`votedFor`、`log`、`commitIndex`、`lastApplied`、`applied`），以 follower、`knownLeader` 为 `null` 上线，清空候选票与 leader 复制进度，选举超时从重启时刻重新计算；已恢复的条目不会重复应用。

### 输出

成功时标准输出只写一个 JSON 对象：

- `timeline`：按 `seq` 递增的事件列表。除原有的 `timeout`、`stateChange`、`messageSend`、`messageResult`、`fault` 外，新增：
  - `clientResult`：`accepted`（含 `index`、`term`）或 `rejected`（含 `reason: notLeader`、`knownLeader`）。
  - 复制消息 `appendEntries`/`appendReply` 的发送与 `messageResult`（`accepted`、`conflict`、`staleTerm`、`matched`、`higherTerm`、`ignored` 等）。
  - `commitAdvance`：leader 推进 `commitIndex`。
  - `applied`：节点按索引应用一条已提交条目（含 `index`、`term`、`id`）。
  - `nodeLifecycle`：节点 `crash` 或 `restart`（含 `node`、`action`），仅在提供 `nodeEvents` 时出现。
- `nodes`：各节点最终的 `role`、`term`、`votedFor`、`knownLeader`，以及 `log`（`index`/`term`/`id`/`command`）、`commitIndex`、`lastApplied`、`applied`；提供 `nodeEvents` 时另含 `online` 与 `restartCount`。
- `clients`：按输入顺序汇总每个 `id` 的最终结局，恰为四类之一：
  - `committed`：已被某节点应用（含最终 `index`、`term`），同一 `id` 至多一次。
  - `superseded`：曾被接受但在提交前被更高任期的日志覆盖删除。
  - `pending`：仍存在于某节点日志中但未达提交多数。
  - `rejected`：发给非 leader（`reason: notLeader`）或离线节点（`reason: nodeDown`，`knownLeader` 为 `null`），含 `reason`、`knownLeader`。
- `electionSafety`：`leadersByTerm` 按任期列出当选的 leader；同一任期出现多个 leader 时记入 `violations`，否则为空列表。
- `logMatching`：`violations` 列出“同索引同任期但内容（id/command）不同”的情况。
- `stateMachineSafety`：`violations` 列出不同节点在同一索引应用了不同命令的情况。

未提供 `clientCommands` 时，所有节点日志为空、`clients` 四类皆为空列表、两个新增报告为空，且原选举轨迹与既有字段值保持不变。未提供 `nodeEvents` 时，不新增 `nodeLifecycle` 事件与 `online`/`restartCount` 字段，既有合法场景的输出逐字节不变。

### 错误

文件不可读、非 UTF-8、JSON 语法错误、字段缺失或未知、节点引用非法、故障超界或分区不合法、`clientCommands` 的字段/类型/时间/节点/id 非法或 id 重复、`nodeEvents` 的字段/取值/节点引用/时间非法或同一节点未从 `crash` 开始严格交替时，不输出部分结果：标准错误写一行以 `error: ` 开头的说明并返回退出码 2。

## 现有公开接口

- 命令行程序 `consensus-protocol-lab`（`version`、`simulate` 子命令）
- Python 包 `consensus_lab`，其 `__version__` 为当前版本号

## 限制

- 实现 Raft 选主、心跳与日志复制/提交，以及节点崩溃与基于持久化状态的重启恢复；不包含集群成员变更、快照与日志压缩。
- 仿真不读取墙钟、不使用随机数；持久化为同步建模，不在磁盘上创建任何文件。
