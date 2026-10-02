## 用途

本项目是「分布式共识协议实验平台」的代码仓库，用于逐步实现该方向的共识流程仿真、故障注入与不变量校验能力。

当前已实现确定性的 Raft 选主与日志复制仿真（含快照、日志压缩与基于联合共识的集群成员变更）：只推进虚拟时间，不读取墙钟、不使用随机数，同一输入产生逐字节一致的输出。

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
- `snapshotThreshold`（可选）：正整数。节点顺序应用提交项后，若 `lastApplied` 距上次快照位置达到该阈值，就在 `lastApplied` 处保存快照（索引、任期与已应用状态）并删除此前日志。未提供时不启用快照，输出逐字节不变。
- `initialMembers`（可选）：初始投票成员（voter）名称列表，至少三个、互不重复，且均为 `nodes` 中已声明的节点；可为 `nodes` 的真子集（此时其余节点为不投票、不参选的 learner），也可等于 `nodes`（无 learner）。
- `membershipChanges`（可选）：成员变更请求列表，同一时刻在客户命令之后按输入顺序处理，每项包含 `time`、`node`（接收节点）、`id`（全局唯一、且不得与 `clientCommands` 的 id 重复）、`action`（`add` 或 `remove`）与 `member`（已声明的节点名），`time` 在 `[0, duration]` 内。

`initialMembers` 与 `membershipChanges` 必须同时提供或同时缺省；两者均缺省时不产生任何成员变更相关事件、汇总或节点字段，既有合法场景输出逐字节不变。

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
- `restart` 恢复持久化状态（`term`、`votedFor`、`log`、快照、`commitIndex`、`lastApplied`、`applied`），以 follower、`knownLeader` 为 `null` 上线，清空候选票与 leader 复制进度，选举超时从重启时刻重新计算；已恢复的条目不会重复应用。
- 启用 `snapshotThreshold` 后：剩余日志继续使用全局索引，选举比较、前缀匹配、`commitIndex` 与 `lastApplied` 均不重新编号；快照与剩余日志一并持久化，重启后恢复，快照内命令不会再次产生 `applied` 事件。
- leader 发现 follower 的 `nextIndex` 已被自身快照覆盖时发送 `installSnapshot`（携带 `lastIncludedIndex`、`lastIncludedTerm` 与快照内已应用状态），沿用现有延迟、乱序、分区与离线规则。follower 对旧任期消息返回 `staleTerm`；同任期且 `lastIncludedIndex` 不大于本地快照位置时返回 `ignored`；接受新快照时恢复状态，将 `commitIndex` 与 `lastApplied` 至少推进到该位置，仅当本地同索引条目任期相同才保留其后的日志，否则删除后缀，返回 `installed`。leader 收到 `installed` 后从快照后一项继续复制。更高任期仍使接收方转为 follower，同刻处理顺序、全局 `seq` 与确定性输出保持不变。
- 启用成员变更时：非 `initialMembers` 的节点为 learner，不发起选举、不获票、不成为 leader，只接收 leader 的日志复制或快照安装。
- 成员变更请求按以下顺序依次判定：接收节点离线（`nodeDown`）、接收节点非 leader（`notLeader`）、已有变更进行中（`changeInProgress`）、成员状态不适用（`add` 已在投票集合中为 `alreadyMember`，`remove` 不在其中为 `notMember`）、移除后投票成员少于三个（`minimumClusterSize`）。判定失败记为 `rejected`，不写日志、不阻塞后续请求。
- 接受的 `add` 先让 learner 通过日志复制或 `installSnapshot` 追平 leader 当前日志末尾（catch-up）；追平后 leader 追加“联合配置”日志项。`remove` 不做 catch-up，直接追加联合配置项。配置日志项带 `entryType: "configuration"` 与 `configuration` 字段，与客户命令项可明确区分。
- 联合配置同时包含新旧两个投票集合。联合阶段的选举当选、日志提交都必须分别满足新、旧两个集合各自的严格多数（联合多数）；集合之外的节点不投票、不参选。联合配置项提交后，leader 立即追加“稳定配置”项（仅新集合）；稳定配置项提交后本次变更才结束。联合与稳定配置项一经追加即在本地生效，提交时通过 `configurationApplied` 生效。
- 稳定配置提交后，被移除的 leader 立即转为 follower（`reason: configurationCommitted`），此后既不参选也不投票；被移除的 follower 同样成为 learner。
- 配置、所处阶段与 learner 的复制进度均随日志、快照与其他持久状态在崩溃重启后恢复；leader 更换或重启后由复制日志中最新的配置项恢复进行中的变更（联合已提交而稳定未追加时补追加，不重复应用已提交配置，也不遗失已提交成员关系）。

### 输出

成功时标准输出只写一个 JSON 对象：

- `timeline`：按 `seq` 递增的事件列表。除原有的 `timeout`、`stateChange`、`messageSend`、`messageResult`、`fault` 外，新增：
  - `clientResult`：`accepted`（含 `index`、`term`）或 `rejected`（含 `reason: notLeader`、`knownLeader`）。
  - 复制消息 `appendEntries`/`appendReply` 的发送与 `messageResult`（`accepted`、`conflict`、`staleTerm`、`matched`、`higherTerm`、`ignored` 等）。
  - `commitAdvance`：leader 推进 `commitIndex`。
  - `applied`：节点按索引应用一条已提交条目（含 `index`、`term`、`id`）。
  - `snapshotCreated`：节点在应用后保存快照（含 `node`、`lastIncludedIndex`、`lastIncludedTerm`），仅在提供 `snapshotThreshold` 时出现。
  - `snapshotInstalled`：follower 接受 `installSnapshot`（含 `node`、`peer`、`lastIncludedIndex`、`lastIncludedTerm`），仅在提供 `snapshotThreshold` 时出现。
  - 快照消息 `installSnapshot`/`installSnapshotReply` 的发送与 `messageResult`（`installed`、`ignored`、`staleTerm`、`higherTerm`，跨分区或离线投递为 `dropped`）。
  - `nodeLifecycle`：节点 `crash` 或 `restart`（含 `node`、`action`），仅在提供 `nodeEvents` 时出现。
  - `membershipResult`：成员变更请求结果（仅在提供成员变更字段时出现）。接受时含 `action`、`member`、`result: accepted` 与 `phase`（`add` 为 `catchup`、`remove` 为 `joint`）；拒绝时含 `result: rejected` 与 `reason`（`nodeDown`/`notLeader`/`changeInProgress`/`alreadyMember`/`notMember`/`minimumClusterSize`）。
  - `configurationApplied`：某节点应用（提交）一个配置日志项，含 `node`、`index`、`term`、`phase`（`joint`/`stable`）、`id` 与完整 `configuration`；不产生客户命令风格的 `applied` 事件。
  - 携带配置项的 `appendEntries` 其 `messageResult` 在确有新追加配置项时附 `configurations`（每项含 `index`、`term`、`phase`、`id`）。
- `nodes`：各节点最终的 `role`、`term`、`votedFor`、`knownLeader`，以及 `log`（仅含未压缩后缀，仍带全局 `index`/`term`；客户命令项带 `id`/`command`，配置项带 `entryType: "configuration"`/`id`/`configuration`，两类互不重叠）、`commitIndex`、`lastApplied`、`applied`；提供成员变更字段时每个节点另含 `membershipRole`（稳定配置下为 `voter`/`learner`；联合阶段为 `voterOld`/`voterNew`/`voter`/`learner`）；提供 `snapshotThreshold` 时另含 `snapshot`（`{"lastIncludedIndex", "lastIncludedTerm"}`，未创建快照时为 `null`）；提供 `nodeEvents` 时另含 `online` 与 `restartCount`。
- `clients`：按输入顺序汇总每个 `id` 的最终结局，恰为四类之一：
  - `committed`：已被某节点应用（含最终 `index`、`term`；已被快照压缩的命令同样归入此类），同一 `id` 至多一次。
  - `superseded`：曾被接受但在提交前被更高任期的日志覆盖删除。
  - `pending`：仍存在于某节点日志中但未达提交多数。
  - `rejected`：发给非 leader（`reason: notLeader`）或离线节点（`reason: nodeDown`，`knownLeader` 为 `null`），含 `reason`、`knownLeader`。
- `electionSafety`：`leadersByTerm` 按任期列出当选的 leader；同一任期出现多个 leader 时记入 `violations`，否则为空列表。联合阶段只有同时取得新旧两个集合各自严格多数的候选者才能当选。
- `logMatching`：`violations` 列出“同索引同任期但内容不同”的情况（客户命令项比较 id/command，配置项比较 configuration，两类互不匹配）；跨配置项与快照边界检查。
- `stateMachineSafety`：`violations` 列出不同节点在同一索引应用了不同条目的情况（客户命令与配置项均参与，跨配置项与快照边界检查）。
- `membership`（仅在提供成员变更字段时出现）：
  - `initialMembers`：初始投票集合。
  - `currentMembers`：最新已提交稳定配置的投票集合。
  - `joint`：非联合阶段为 `null`；联合阶段为 `{"old": [...], "new": [...]}`。
  - `changes`：按输入顺序汇总每个成员变更 `id` 的最终结局：`committed`（稳定配置已提交，含 `jointIndex`/`stableIndex`）、`pending`（仿真结束时仍在进行，含所处 `phase`：`catchup`/`joint`/`stable` 及对应索引）、或 `rejected`（含拒绝 `reason`）。

未提供 `clientCommands` 时，所有节点日志为空、`clients` 四类皆为空列表、两个新增报告为空，且原选举轨迹与既有字段值保持不变。未提供 `nodeEvents` 时，不新增 `nodeLifecycle` 事件与 `online`/`restartCount` 字段；未提供 `snapshotThreshold` 时，不新增 `snapshot` 字段、快照事件与快照消息；未同时提供 `initialMembers` 与 `membershipChanges` 时，不新增 `membership` 汇总、`membershipRole` 字段及任何成员变更事件，既有合法场景的输出逐字节不变。

### 错误

文件不可读、非 UTF-8、JSON 语法错误、字段缺失或未知、节点引用非法、故障超界或分区不合法、`clientCommands` 的字段/类型/时间/节点/id 非法或 id 重复、`nodeEvents` 的字段/取值/节点引用/时间非法或同一节点未从 `crash` 开始严格交替、`snapshotThreshold` 为布尔值、非整数或小于一、`initialMembers`/`membershipChanges` 只出现其一、`initialMembers` 少于三个/含未知或重复节点、`membershipChanges` 的字段/取值/时间/接收节点/成员节点非法或 id（含与 `clientCommands`）重复时，不输出部分结果：标准错误写一行以 `error: ` 开头的说明并返回退出码 2。

## 现有公开接口

- 命令行程序 `consensus-protocol-lab`（`version`、`simulate` 子命令）
- Python 包 `consensus_lab`，其 `__version__` 为当前版本号

## 限制

- 实现 Raft 选主、心跳、日志复制/提交、快照与日志压缩、节点崩溃与基于持久化状态的重启恢复，以及基于联合共识（joint consensus）的单次串行成员变更：同一时刻只允许一个变更进行中，每个变更新增或移除一个预声明节点，投票集合始终不少于三个节点；learner 只能来自 `nodes` 中预先声明的节点。
- 仿真不读取墙钟、不使用随机数；持久化为同步建模，不在磁盘上创建任何文件。
