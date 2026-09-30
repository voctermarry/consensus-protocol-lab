## 用途

本项目是「分布式共识协议实验平台」的代码仓库，用于逐步实现该方向的共识流程仿真、故障注入与不变量校验能力。

当前处于基线状态：只有项目骨架，尚未实现任何业务算法。

## 环境与安装

- Python 3.11 及以上

```bash
python -m pip install -e .
```

## 测试

```bash
python -m pytest
```

基线尚无测试用例，收集到 0 个用例属预期结果。

## 命令行入口

安装后提供 `consensus-protocol-lab` 命令：

```bash
consensus-protocol-lab version    # 打印版本号
consensus-protocol-lab --help     # 打印用法
```

## 现有公开接口

- 命令行程序 `consensus-protocol-lab`
- Python 包 `consensus_lab`，其 `__version__` 为当前版本号

## 限制

- 除版本查询外没有其他功能。
- 输入输出格式、数据来源与算法均尚未定义。
