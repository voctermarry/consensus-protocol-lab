## 用途

本项目是「分布式共识协议实验平台」的代码仓库，用于逐步实现该方向的共识流程仿真、故障注入与不变量校验能力。

当前已实现确定性的 Raft 选主与日志复制仿真（含快照与日志压缩、联合共识成员变更、只读查询与线性一致性报告）：只推进虚拟时间，不读取墙钟、不使用随机数，同一输入产生逐字节一致的输出。

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
- `messageFaults`（可选）：消息级故障规则列表，精确命中某一次发送。每项包含：
  - `from`、`to`：两个不同的已有节点名（非空字符串）。
  - `message`：`requestVote`、`voteReply`、`heartbeat`、`appendEntries`、`appendReply`、`installSnapshot`、`installSnapshotReply`、`readProbe`、`readReply` 之一。
  - `occurrence`：正整数，按 `(from, to, message)` 相同的选择器从仿真开始对实际发送计数，命中第几次发送。
  - `action`：`drop` 或 `delay`。`drop` 不得携带 `delay` 字段；`delay` 必须携带非负整数 `delay`，实际到达时刻为发送时刻加 `messageDelay` 再加该值。
  - 完整选择器 `(from, to, message, occurrence)` 不得重复；未匹配到任何发送的规则不报错、不产生输出。
  - 省略该字段（或为空列表、或规则均未命中）时，合法旧场景的标准输出逐字节不变。
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
- `initialMembers`（可选，必须与 `membershipChanges` 同时提供或同时省略）：初始投票集合，是 `nodes` 的一个至少含三个不重复名称的子集；其余节点为 learner，不参选也不投票。
- `membershipChanges`（可选，与 `initialMembers` 成对出现）：成员变更请求列表，按输入顺序提供 `time`、`node`、`id`、`action` 与 `member`：
  - `time`：`[0, duration]` 内的非负整数虚拟时间；`node`：接收请求的已有节点名；`id`：全局唯一的非空字符串（不得与客户命令 id 重复）；`member`：目标节点名；`action`：`add` 或 `remove`。
  - 两字段均省略时，不产生任何成员变更相关事件、汇总或节点字段，既有合法场景输出逐字节不变。
- `readQueries`（可选）：只读查询列表。同一时刻在故障、节点事件、已到达消息、客户命令及其零延迟反应、成员变更及其零延迟反应之后，按输入顺序受理查询，再排空查询自身的零延迟确认级联，最后处理超时与心跳。每项只含：
  - `time`：`[0, duration]` 内的非负整数虚拟时间；`node`：接收查询的已有节点名（非空字符串）；`id`：全局唯一的非空字符串，不得与客户命令、成员变更或其他查询的 id 重复。
  - 查询发给离线节点立即得到 `rejected`/`nodeDown`（`knownLeader` 为 `null`）；发给非 leader 得到 `rejected`/`notLeader` 并带当时的 `knownLeader`；发给 leader 先记录 `accepted`，再走只读多数确认。
  - 省略该字段时，不产生任何只读相关事件与报告，既有合法场景输出逐字节不变（提供空列表时出现空的 `reads` 与 `linearizability` 报告）。

### 语义

- 节点从任期 0 的 follower 开始；选举超时后成为 candidate、任期加一、自投并广播 RequestVote。
- 每任期最多投一票；RequestVote 携带最后日志任期与索引，候选人日志必须与投票人至少同样新才能获票；获得全体节点严格多数者成为 leader。
- 收到更高任期的消息会更新任期并退回 follower。
- leader 当选后立即并在每个心跳周期向每个节点发送消息：对落后节点发送携带日志的 appendEntries（前一索引与任期、待复制条目、leaderCommit），对已追平节点发送携带 leaderCommit 的空心跳。
- 命令发给非 leader 时不写日志，记录 `rejected`/`notLeader` 及当时的 `knownLeader`；发给 leader 时追加含 `index`、`term`、`id`、`command` 的条目并记录 `accepted`。
- follower 仅在前一索引处任期匹配（前缀匹配）时接受 appendEntries；冲突时删除冲突位置及其后缀，再追加；否则回复拒绝，leader 将该节点的下一索引确定性地回退一位并重试。
- 当某索引的条目（其任期须为 leader 当前任期）被包含 leader 自身的严格多数节点复制后，leader 才推进 `commitIndex`；节点按索引顺序应用已提交条目。后续有效复制（含心跳携带的 leaderCommit）使 follower 更新提交位置并应用。
- 同一时刻依次处理故障、nodeEvents、消息、客户命令及其零延迟复制级联、成员变更及其零延迟级联、只读查询及其零延迟确认级联、超时、心跳；同刻事件按 `nodes` 顺序，timeline 以全局递增 `seq` 记录。
- `messageFaults` 规则命中某次发送时，仍先记录原有的 `messageSend`，随后在同一时刻记录 `messageFault`；`drop` 规则的消息在原计划到达时刻（发送时刻加 `messageDelay`）记录 `result: "dropped"`、`reason: "messageFault"` 的 `messageResult`，不执行任何接收逻辑；`delay` 规则的消息改在实际到达时刻（原计划到达时刻再加规则的 `delay`）按原语义处理，目标是否在线、链路是否分区均在到达时判断，因此 `nodeDown` 或 `partition` 仍可决定最终结果，延后也可能使消息乱序到达。到达时刻超过 `duration` 时只保留发送与 `messageFault` 记录；同刻到达的消息继续按全局发送顺序处理。
- 节点在线时，任期、投票、日志、`commitIndex`、`lastApplied` 与 `applied` 的每次变化都在相关响应之前同步持久化（leader 接受命令前先持久化日志）；持久化不读取墙钟、不使用随机数。
- `crash` 后节点离线：不发送或处理任何消息，不触发选举超时、心跳或客户命令；发往离线节点的消息在到达时记为 `dropped`/`nodeDown`，而崩溃前已发出的消息仍按原时间投递。发给离线节点的客户命令记为 `rejected`/`nodeDown`，`knownLeader` 为 `null`。
- `restart` 恢复持久化状态（`term`、`votedFor`、`log`、快照、`commitIndex`、`lastApplied`、`applied`），以 follower、`knownLeader` 为 `null` 上线，清空候选票与 leader 复制进度，选举超时从重启时刻重新计算；已恢复的条目不会重复应用。
- 启用 `snapshotThreshold` 后：剩余日志继续使用全局索引，选举比较、前缀匹配、`commitIndex` 与 `lastApplied` 均不重新编号；快照与剩余日志一并持久化，重启后恢复，快照内命令不会再次产生 `applied` 事件。
- leader 发现 follower 的 `nextIndex` 已被自身快照覆盖时发送 `installSnapshot`（携带 `lastIncludedIndex`、`lastIncludedTerm` 与快照内已应用状态），沿用现有延迟、乱序、分区与离线规则。follower 对旧任期消息返回 `staleTerm`；同任期且 `lastIncludedIndex` 不大于本地快照位置时返回 `ignored`；接受新快照时恢复状态，将 `commitIndex` 与 `lastApplied` 至少推进到该位置，仅当本地同索引条目任期相同才保留其后的日志，否则删除后缀，返回 `installed`。leader 收到 `installed` 后从快照后一项继续复制。更高任期仍使接收方转为 follower，同刻处理顺序、全局 `seq` 与确定性输出保持不变。
- 启用联合共识成员变更后：
  - learner 复制日志但不计入任何多数，也不发起或获得选票；其选举定时器不启动。
  - 请求按以下顺序判定并给出对应结果：接收节点离线→`nodeDown`；不是 leader→`notLeader`；已有变更进行中→`changeInProgress`；成员状态不适用→`alreadyMember`（add 已在稳定集合中）或 `notMember`（remove 不在稳定集合中）；移除后投票成员少于三个→`minimumClusterSize`。
  - `add` 先让 learner 通过日志复制或 `installSnapshot` 追平 leader 日志末端，然后才追加联合配置项；`remove` 直接追加联合配置项。
  - 联合配置项一经追加即生效：联合阶段的选举、日志提交与只读多数都必须同时满足新旧两个集合各自的严格多数。联合配置提交后 leader 立即追加稳定配置项；稳定配置提交后该变更才结束（结局 `committed`）。
  - 稳定配置提交时，被移除的原 leader 立即成为 follower（原因 `removedFromCluster`），此后既不参选也不投票；该节点仅在之后又被 add 的配置追加后才恢复资格。
  - 上一任 leader 在追加联合项前下线时，追赶状态随之下线，未追加的变更保持未完成；联合项已提交但稳定项未追加时，新当选 leader（含通过快照持有该联合项者）会补追加稳定项，变更不会丢失。
  - 配置项、联合阶段与 learner 进度随日志与快照持久化；崩溃重启后由日志与快照恢复，已提交成员关系不会重复应用或丢失。
- 启用只读查询后：
  - leader 受理查询时把该轮确认绑定到自身身份（节点与任期）与当时的最新配置：稳定配置取其投票集合的严格多数（含 leader 自身），联合阶段取新旧两个投票集合各自的严格多数，learner 既不接收 `readProbe` 也不计入任何多数。
  - leader 先向有效配置中的其他投票者发送携带查询 `id` 的 `readProbe`；接收者对旧任期消息记 `staleTerm`，否则像心跳一样承认领导地位（必要时退回 follower）并回复携带自身任期的 `readReply`。两类消息服从消息延迟、`messageFaults`、分区、乱序与离线规则。
  - 只有身份（节点、任期）未变且在查询之后收到同一轮的有效多数确认时，查询才 `completed`：即使没有后续写入，只要 leader 与多数持续可达，确认到齐即完成；`readIndex` 取完成时刻 leader 已提交并应用的位置，`state` 按索引顺序列出截至该位置的客户命令（每项含 `index`、`term`、`id`、`command`，不含配置项；被快照压缩的命令仍从保留的已应用历史返回）。
  - leader 在查询受理后下线（崩溃）、退位（被成员变更移除等）或任期改变（收到更高任期消息）时，该查询结局为 `rejected`/`leadershipLost`；不同轮次或不同任期的回复一律 `ignored`，不得混用。
  - 仿真结束时仍未取得同一任期、有效配置多数确认的受理查询结局为 `pending`。

### 输出

成功时标准输出只写一个 JSON 对象：

- `timeline`：按 `seq` 递增的事件列表。除原有的 `timeout`、`stateChange`、`messageSend`、`messageResult`、`fault` 外，新增：
  - `messageFault`：一条 `messageFaults` 规则命中某次发送（含 `rule` 规则在输入列表中的序号、`from`、`to`、`message`、`occurrence`、`action`、`scheduledTime` 原计划到达时刻；`delay` 规则另含 `arrivalTime` 实际到达时刻），仅在提供 `messageFaults` 且规则命中时出现。
  - `clientResult`：`accepted`（含 `index`、`term`）或 `rejected`（含 `reason: notLeader`、`knownLeader`）。
  - 复制消息 `appendEntries`/`appendReply` 的发送与 `messageResult`（`accepted`、`conflict`、`staleTerm`、`matched`、`higherTerm`、`ignored` 等）。
  - `commitAdvance`：leader 推进 `commitIndex`。
  - `applied`：节点按索引应用一条已提交条目（含 `index`、`term`、`id`）。
  - `snapshotCreated`：节点在应用后保存快照（含 `node`、`lastIncludedIndex`、`lastIncludedTerm`），仅在提供 `snapshotThreshold` 时出现。
  - `snapshotInstalled`：follower 接受 `installSnapshot`（含 `node`、`peer`、`lastIncludedIndex`、`lastIncludedTerm`），仅在提供 `snapshotThreshold` 时出现。
  - 快照消息 `installSnapshot`/`installSnapshotReply` 的发送与 `messageResult`（`installed`、`ignored`、`staleTerm`、`higherTerm`，跨分区或离线投递为 `dropped`）。
  - `nodeLifecycle`：节点 `crash` 或 `restart`（含 `node`、`action`），仅在提供 `nodeEvents` 时出现。
  - `membershipResult`：成员变更请求结果。`accepted`（add 且尚未追加联合项时含 `phase: "catchingUp"`）或 `rejected`（含 `reason`，取值 `nodeDown`/`notLeader`/`changeInProgress`/`alreadyMember`/`notMember`/`minimumClusterSize`），仅在提供成员变更字段时出现。
  - `configurationApplied`：节点按索引应用一个已提交配置项（含 `index`、`term`、`id`、`entryType` 为 `joint`/`stable`、`config`、`action`、`member`），仅在提供成员变更字段时出现。
  - 复制配置项的 `appendEntries` 消息结果另含 `configEntries`（每项含 `index`、`id`、`entryType`），与客户命令复制明确区分。
  - `readResult`：只读查询结局。`accepted`（含 `term`，先于探针发送记录）或最终结局 `completed`（含 `term`、`readIndex`、`state`）、`rejected`（`reason` 为 `nodeDown`/`notLeader` 时带 `knownLeader`，或 `leadershipLost`）、`pending`，仅在提供 `readQueries` 时出现。
  - 只读确认消息 `readProbe`/`readReply` 的发送与 `messageResult`（`readProbe` 为 `accepted`/`staleTerm`，`readReply` 为 `acknowledged`/`ignored`/`higherTerm`，跨分区或离线投递为 `dropped`），均携带 `readId`，仅在提供 `readQueries` 且实际发送时出现。
- `nodes`：各节点最终的 `role`、`term`、`votedFor`、`knownLeader`，以及 `log`（仅含未压缩后缀，仍带全局 `index`/`term`/`id`/`command`；启用成员变更时客户命令条目含 `kind: "command"`，配置项含 `kind: "config"`、`entryType`、`config`、`action`、`member`）、`commitIndex`、`lastApplied`、`applied`（同样以 `kind` 区分两类条目）；提供 `snapshotThreshold` 时另含 `snapshot`（`{"lastIncludedIndex", "lastIncludedTerm"}`，未创建快照时为 `null`）；提供 `nodeEvents` 时另含 `online` 与 `restartCount`；提供成员变更字段时另含 `membershipRole`（按该节点最新配置取 `voter` 或 `learner`）。
- `membership`（仅在提供成员变更字段时出现）：
  - `initial`：初始投票集合；`current`：当前已提交的稳定投票集合（联合阶段仍为旧稳定集合）；`joint`：联合阶段为 `{"id", "old", "new"}`，否则为 `null`。
  - `changes`：按输入顺序给出每个 id 的结局：`committed`（含稳定配置项的 `index`、`term`）、`pending`（含 `phase: "catchingUp"` 或 `phase: "joint"`，后者另含 `joint`）或 `rejected`（含 `reason`）。
- `reads`（仅在提供 `readQueries` 时出现）：按输入顺序汇总每个查询的最终结局，每项以 `id`、`node` 开头：
  - `completed`：含 `result`、`term`、`readIndex`、`state`；`state` 为截至 `readIndex` 的客户命令前缀（仅 `index`/`term`/`id`/`command`）。
  - `rejected`：含 `reason`（`nodeDown`/`notLeader` 时另含 `knownLeader`，`leadershipLost` 无该字段）。
  - `pending`：仿真结束时未取得同任期有效配置多数。
- `linearizability`（仅在提供 `readQueries` 时出现）：`violations` 列出只读一致性问题；`completed` 查询返回的 `state` 不是其 `readIndex` 对应的完整客户命令前缀时记 `nonPrefix`（含 `readIndex`）；`readIndex` 落后于查询受理前 leader 已提交位置而遗漏已提交写入时记 `staleRead`（含 `readIndex`、`committedIndex`）。
- `clients`：按输入顺序汇总每个 `id` 的最终结局，恰为四类之一：
  - `committed`：已被某节点应用（含最终 `index`、`term`；已被快照压缩的命令同样归入此类），同一 `id` 至多一次。
  - `superseded`：曾被接受但在提交前被更高任期的日志覆盖删除。
  - `pending`：仍存在于某节点日志中但未达提交多数。
  - `rejected`：发给非 leader（`reason: notLeader`）或离线节点（`reason: nodeDown`，`knownLeader` 为 `null`），含 `reason`、`knownLeader`。
- `electionSafety`：`leadersByTerm` 按任期列出当选的 leader；同一任期出现多个 leader 时记入 `violations`，否则为空列表。
- `logMatching`：`violations` 列出“同索引同任期但内容（客户命令的 id/command，或配置项负载）不同”的情况；跨配置项与快照边界检查（已压缩索引取自已应用历史）。
- `stateMachineSafety`：`violations` 列出不同节点在同一索引应用了不同条目的情况（配置项与客户命令一并参与索引对齐，并跨快照边界检查）。

未提供 `clientCommands` 时，所有节点日志为空、`clients` 四类皆为空列表、两个新增报告为空，且原选举轨迹与既有字段值保持不变。未提供 `nodeEvents` 时，不新增 `nodeLifecycle` 事件与 `online`/`restartCount` 字段；未提供 `snapshotThreshold` 时，不新增 `snapshot` 字段、快照事件与快照消息；未同时提供 `initialMembers` 与 `membershipChanges` 时，不新增 `membership` 汇总、`membershipRole`、成员事件与配置项标记；未提供 `messageFaults` 时，不新增 `messageFault` 事件与 `messageFault` 原因的丢弃结果；未提供 `readQueries` 时，不新增 `readResult` 事件、`readProbe`/`readReply` 消息与 `reads`/`linearizability` 报告，既有合法场景的输出逐字节不变。

### 错误

文件不可读、非 UTF-8、JSON 语法错误、字段缺失或未知、节点引用非法、故障超界或分区不合法、`messageFaults` 非列表或条目的字段缺失/未知、节点非法或两端相同、消息类型非法、`occurrence` 非正整数、`action` 非法、`drop` 携带 `delay`、`delay` 缺失或不是非负整数、完整选择器重复、`clientCommands` 的字段/类型/时间/节点/id 非法或 id 重复、`nodeEvents` 的字段/取值/节点引用/时间非法或同一节点未从 `crash` 开始严格交替、`snapshotThreshold` 为布尔值、非整数或小于一、`initialMembers` 与 `membershipChanges` 只出现一个、`initialMembers` 少于三个/重复/引用未知节点、`membershipChanges` 的字段/时间/取值/接收节点/目标节点非法或 id（含与客户命令 id）重复、`readQueries` 的类型/字段/时间/节点/id 非法或 id 与客户命令、成员变更及其他查询重复时，不输出部分结果：标准错误写一行以 `error: ` 开头的说明并返回退出码 2。

## 现有公开接口

- 命令行程序 `consensus-protocol-lab`（`version`、`simulate` 子命令）
- Python 包 `consensus_lab`，其 `__version__` 为当前版本号

## 限制

- 实现 Raft 选主、心跳、日志复制/提交、快照与日志压缩、联合共识（joint consensus）成员变更、基于心跳确认轮次的只读查询（readIndex）与线性一致性报告，以及节点崩溃与基于持久化状态的重启恢复。
- 成员变更采用联合共识两阶段（联合配置项提交后再提交稳定配置项）；learner 先追平日志再进入联合集合；不实现单节点一次多变更等额外成员变更扩展。
- 仿真不读取墙钟、不使用随机数；持久化为同步建模，不在磁盘上创建任何文件。
