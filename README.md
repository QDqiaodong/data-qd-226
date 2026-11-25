# 本地构建步骤执行器

供本地构建集成调用，提供两个入口，均只依赖标准库（Python 3.9+）。

## `execute`：按进程数并行的基础入口

```python
from executor import Step, execute

execute({name: Step(argv, after, timeout)}, workers=2, cwd=...)
```

启动前核依赖引用和环；运行时最多同时启动指定数量的步骤，只在直接依赖全部
成功后启动新步骤。失败、超时和启动错误阻止依赖分支，不中断独立步骤。
调用者获得按输入顺序排列的终态、退出码和直接阻断原因。

## `execute_with_artifacts`：带暂存额度的构建入口

```python
from executor import Step, execute_with_artifacts

outcomes = execute_with_artifacts(
    plan,                       # 名称 -> Step，与 execute 相同
    outputs={"compile": 4096},  # 生产者名 -> 声明预留字节数（一步至多一个产物）
    uses={"check": ("compile",)},  # 消费者名 -> 所读生产者名元组
    capacity=8192,              # 暂存区总额度（字节）
    scratch_dir=scratch_root,   # 已存在的目录；每次调用独占其下一个临时子目录
    workers=2,
    cwd=None,
)
```

### 产物契约

- 生产者从环境变量 `BUILD_OUTPUT` 拿到一个绝对路径，把产物写成**普通文件**
  （不能是符号链接），且最终大小不得超过声明预留，否则该步终态为
  `artifact_error`，文件不会交给消费者。
- 消费者从 `BUILD_INPUTS` 读取一个 JSON 对象：生产者名 -> 绝对路径，只包含
  自己在 `uses` 中声明读取的产物；不读产物时是空对象 `{}`。
- 没有输出的步骤不会设置 `BUILD_OUTPUT`，即使父进程原本设置过该变量也会被
  移除。其余环境变量照常继承；命令不经 shell。
- `uses` 会隐式增加成功依赖，与 `Step.after` 合并后一起做引用和环检查。
  普通 `after` 依赖不会借用产物。
- 跳过步骤的 `blocked_by` 按 `after` 再 `uses` 的次序去重，列出所有非成功
  的直接依赖。

### 额度口径

- 生产者启动前预留其**全部声明额度**，包括最终可能失败的步骤；成功后按原
  预留继续计费，不按实测大小折算。
- 只要还有任一声明消费者未到终态（成功、失败、跳过都算终态），产物及其额度
  就必须保留。最后一个消费者终态后删除文件，额度才可以给新步骤。
- 没有消费者的产物在生产者终态时立即清除；失败生产者的半成品不发布，清除后
  释放预留。调用返回前清理本次独占子目录，不动暂存根里的其他内容。
- 启动任何命令之前，先在“所有命令都成功”的假设下搜索一条遵守依赖和额度的
  逐步完成次序；即使某一步是某输入的最后一个消费者，也必须先为自己的输出
  找到额度，不能与尚未释放的输入抵扣。格式、依赖或环问题抛普通 `ValueError`；
  额度不可行时错误信息以 `capacity infeasible` 开头，且不会启动任何命令。
- 运行时每批准入都重新验证残余构建仍存在额度内的完成次序，因此不会为了眼下
  装下几个步骤而发放会卡死剩余构建的预留，也不依赖命令失败或超时来解锁。
- 当 `workers` 允许且至少有两个就绪步骤能安全同批（同时预留放得下，且这批
  成功后剩余步骤仍可行）时，同一批至少放行两个，不会退化为永远串行。

### 校验边界

- 单次调用最多 18 个步骤；预留为 `1 .. 2^40` 的整数，`capacity` 为
  `0 .. 2^40` 的整数，布尔值不算整数。
- `uses` 的每个元组不能重复引用同一生产者；所有名字必须存在，且引用必须
  指向声明了输出的步骤。
- `scratch_dir` 必须是已存在的目录。

### 终态

`ok`、`failed`、`timeout`、`launch_error`、`artifact_error`、`skipped`。
失败只阻断依赖它的分支；`Outcome` 的字段与基础入口一致。

## 运行测试与演示

```sh
python3 -m venv .venv
.venv/bin/python -m pip install --no-index -r requirements.txt
# 隔离模式下显式指定本项目路径，避免受当前目录其他模块影响
PYTHONPATH=. .venv/bin/python -m unittest -v
.venv/bin/python demo.py
```

测试全部使用真实子进程、临时目录和文件握手，不依赖固定睡眠猜测调度顺序，
可完全离线运行。

仅运行调用者信任的 argv，无 shell、无沙箱；继承调用进程环境和设定的 cwd，
丢弃标准输入输出。超时由 subprocess 管理直接子进程，不跟踪后代进程；
不持久化任务、不自动重试。没有依赖约束的步骤不保证完成顺序。
