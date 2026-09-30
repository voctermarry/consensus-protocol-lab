## 用途

本项目是「分布式共识协议实验平台」的代码仓库，用于逐步实现该方向的共识流程仿真、故障注入与不变量校验能力。

当前已实现确定性的 Raft 选主仿真：只推进虚拟时间，不读取墙钟、不使用随机数，同一输入产生逐字节一致的输出。

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

### 语义

- 节点从任期 0 的 follower 开始；选举超时后成为 candidate、任期加一、自投并广播 RequestVote。
- 每任期最多投一票，候选日志视为同样新；获得全体节点严格多数者成为 leader。
- 收到更高任期的消息会更新任期并退回 follower。
- leader 当选后立即并在每个心跳周期发送心跳；follower 收到合法心跳后重置固定超时。
- 同一时刻依次处理故障、消息、超时、心跳；故障按输入顺序、节点事件按 `nodes` 顺序，timeline 以递增 `seq` 记录。

### 输出

成功时标准输出只写一个 JSON 对象：

- `timeline`：按 `seq` 递增的事件列表（`timeout`、`stateChange`、`messageSend`、`messageResult`、`fault`），含 `time`、`node`、`term`、`peer`/`reason` 等字段。
- `nodes`：各节点最终的 `role`、`term`、`votedFor`、`knownLeader`。
- `electionSafety`：`leadersByTerm` 按任期列出当选的 leader；同一任期出现多个 leader 时记入 `violations`，否则为空列表。

### 错误

文件不可读、非 UTF-8、JSON 语法错误、字段缺失或未知、节点引用非法、故障超界或分区不合法时，不输出部分结果：标准错误写一行以 `error: ` 开头的说明并返回退出码 2。

## 现有公开接口

- 命令行程序 `consensus-protocol-lab`（`version`、`simulate` 子命令）
- Python 包 `consensus_lab`，其 `__version__` 为当前版本号

## 限制

- 仅实现 Raft 选主与心跳，不包含日志复制。
- 仿真不读取墙钟、不使用随机数，也不持久化任何状态。
