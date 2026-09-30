## 用途

本项目是「分布式共识协议实验平台」的代码仓库，用于逐步实现该方向的共识流程仿真、故障注入与不变量校验能力。

当前已实现确定性的 Raft 选主仿真：只推进虚拟时间，不读取墙钟、不使用随机数，同一输入始终产生逐字节一致的输出。

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
consensus-protocol-lab --help               # 打印用法
consensus-protocol-lab simulate SCENARIO    # 运行 Raft 选主仿真
```

`simulate` 只读取给定的 UTF-8 JSON 场景文件并向标准输出写结果，不创建任何文件。

### 输入格式

SCENARIO 为一个 JSON 对象：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `nodes` | string[] | 至少 3 个不重复的非空节点名 |
| `duration` | integer | 仿真时长（非负整数毫秒，含边界时刻） |
| `electionTimeouts` | integer[] | 每个节点的固定选举超时（正整数毫秒），顺序与 `nodes` 对齐 |
| `heartbeatInterval` | integer | leader 心跳间隔（正整数毫秒） |
| `messageDelay` | integer | 消息投递延迟（正整数毫秒） |
| `faults` | object[] | 可选，故障列表 |

每个故障形如 `{"time": 70, "action": "partition", "groups": [["n1"], ["n2", "n3"]]}`，
或 `{"time": 200, "action": "heal"}`：

- `time` 必须在 `0..duration` 之间；
- `partition` 的两个分组互不重叠且恰好覆盖全部节点，跨组消息在投递时被丢弃；
- `heal` 恢复全网连通，且不可携带 `groups`。

### 仿真语义

- 节点从任期 0 的 follower 开始；选举超时后成为 candidate，任期加一、自投并向其他节点发送 `RequestVote`。
- 收到更高任期消息立即更新任期并退回 follower；每任期最多投一票，所有候选日志视为同样新。
- 获得全体节点严格多数选票者成为 leader；leader 周期性发送 `AppendEntries` 心跳，follower 收到合法心跳后重置固定超时。
- 消息在 `messageDelay` 后投递，投递时若跨分区则丢弃；只处理 `duration` 内的事件。
- 同一时刻按「故障 → 消息投递 → 选举超时 → 心跳」处理；同时刻故障按输入顺序、节点事件按 `nodes` 顺序，timeline 记录带递增 `seq`。

### 输出格式

成功时标准输出为一个 JSON 对象（无多余文字），包含：

- `timeline`：按 `seq` 排列的事件（状态变化、消息发送与投递结果、故障），每条含 `time`、`node`、`term`、`peer` 及类型相关字段（如 `role`、`reason`、`result`、`message`）；
- `nodes`：各节点最终的 `role`、`term`、`votedFor`、`knownLeader`；
- `electionSafety`：按任期列出 `leaders`，同一任期出现多个 leader 时在 `violations` 中记录，否则为空列表。

### 错误处理

文件不可读、JSON 非法、字段缺失或未知、节点引用非法、故障时刻超界或分区不合法时，不输出任何部分结果：标准错误写一行以 `error: ` 开头的说明，进程返回码为 2。

## 现有公开接口

- 命令行程序 `consensus-protocol-lab`（`version`、`simulate`）
- Python 包 `consensus_lab`：
  - `__version__` 为当前版本号；
  - `consensus_lab.simulator` 提供 `parse_scenario`、`run_simulation` 与 `SimulationError`。

## 限制

- 只仿真选主与心跳，不包含日志复制、提交与成员变更。
- 选举超时为每节点固定值，仿真内部不引入随机性。
