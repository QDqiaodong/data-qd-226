# 本地构建步骤执行器

供本地构建集成调用，提供三个入口，均只依赖标准库（Python 3.9+）。

## `execute`：按进程数并行的基础入口

```python
from executor import Step, execute

execute({name: Step(argv, after, timeout)}, workers=2, cwd=...)
```

启动前核依赖引用和环；运行时最多同时启动指定数量的步骤，只在直接依赖全部
成功后启动新步骤。失败、超时和启动错误阻止依赖分支，不中断独立步骤。
调用者获得按输入顺序排列的终态、退出码和直接阻断原因。

## `execute_incremental`：按目标执行、按内容复用的构建入口

```python
from executor import Step, execute_incremental

result = execute_incremental(
    plan,                          # 名称 -> Step
    targets=("docs", "binary"),    # 非空、不重复的步骤名元组
    sources={"generator": ("src/input.txt",)},
    outputs={
        "generator": ("out/app.bin", "out/app.txt"),
        "binary": ("deliver/app.bin",),
        "docs": ("deliver/app.txt",),
    },
    reads={
        "binary": (("generator", "out/app.bin"),),
        "docs": (("generator", "out/app.txt"),),
    },
    context="python-3.11/linux/x86_64/recipe-v7",
    state_file="/var/cache/my-build/executor-state.json",  # 工作区外
    workers=2,
    cwd=workspace,
)

result.outcomes["binary"].status
result.source_bytes_read
```

返回冻结的 `IncrementalResult(outcomes, source_bytes_read)`：

- `outcomes` 是普通 dict，只包含 `targets` 及其 `after`、`reads` 合并图中
  递归需要的步骤，并按原始 `plan` 的输入顺序排列。
- `source_bytes_read` 是本轮执行器为计算源指纹实际读取的字节总数；不包括
  子进程自己的读取，也不包括复用/发布时对声明输出的校验读取。
- 新终态包括 `reused`、`input_error`、`output_error`；原有
  `ok`、`failed`、`timeout`、`launch_error`、`skipped` 语义不变。

### 声明和路径

- `sources[name]`、`outputs[name]` 都是相对工作区的 `/` 分隔路径元组；缺
  少的键视为 `()`。`reads[consumer]` 是
  `(producer_name, output_path)` 二元组的元组。
- 路径必须是规范相对路径：不能是绝对路径，不能有空段、`.` 或 `..`，UTF-8
  编码后最长 256 字节。
- 每个输出路径只有一个拥有者；源路径不能同时是输出路径；任意已声明路径不
  能是另一个已声明路径的严格祖先。不同步骤可以声明相同源路径。
- 每个读取项必须精确指向某个生产者在 `outputs` 中声明的路径；同一消费者不
  能重复声明同一读取项。
- `reads` 自动增加生产者到消费者的成功依赖；`Step.after` 仍是另一类只负责
  顺序和成功的边。两类边可以重复指向同一步，阻断原因按 `after` 后 `reads`
  的声明顺序去重。
- 限制：最多 256 步、256 个目标、8 个 worker、4096 个互异路径、三张声明
  表合计最多 16384 个路径/读取项；每步原始 `after` 最多 256 项。
- `workers` 必须是 1–8 的整数，布尔值不算整数。`context` 必须是非空且
  UTF-8 编码后不超过 256 字节的字符串。

完整 `plan`（包括不在目标闭包中的步骤）的类型、路径、引用和合并图都会在启
动子进程或写入状态前校验；坏声明、未知目标、跨 `after`/`reads` 环都抛
`ValueError`。命令仍无 shell、stdin 为 `DEVNULL`、继承环境，并使用给定
`cwd`；执行器不通过环境变量传递路径，命令按 argv 中的约定路径访问文件。

### 两个独立条件：本轮准入与跨次复用

一个步骤必须先满足**本轮依赖准入**：所有直接依赖都已到终态，并且状态是
`ok` 或 `reused`。否则该步为 `skipped`，`blocked_by` 列出所有直接不成
功依赖。磁盘上是否已有旧输出或旧状态不能绕过这一门槛；因此检查、环境准备
等无输出 `after` 步骤本轮失败时，下游即使键和文件都没变也会被跳过。

通过本轮门槛后，有输出步骤才检查**跨次复用**。复用键包含：

1. 步骤名；
2. 原始 `argv`；
3. `timeout`；
4. 调用方提供的 `context`；
5. 顺序无关的声明输出路径集合；
6. 以相对路径标识的直接源文件 SHA-256 指纹；
7. 以 `(生产者名, 输出路径)` 标识的显式 `reads` 输出字节指纹。

`after` 不进入复用键。输出、源和读取映射均按路径/生产者排序后编码，因此
声明排列不改变键。`context` 不自动包含编译器版本、操作系统、环境变量等；
这些因素若会改变确定性结果，调用方必须自行纳入 `context`。

键匹配后，执行器仍会逐个读取当前磁盘上的声明输出，要求其为普通文件且
SHA-256 与状态记录一致；内容按完整字节判断，不使用大小或时间戳代替。全
部匹配才返回 `reused` 且不启动命令。任何输出缺失、类型不对或字节变化都会
导致实际重跑。

没有声明输出的步骤每次进入目标闭包并通过依赖门槛后都会实际运行，也不保存
可复用记录。

### 源、读取指纹和内存边界

- 同一相对源路径在一次调用中最多完整读取一次；指纹结果或读取失败由所有需
  要它的步骤共享。一个分支的源错误只产生该步的 `input_error` 并阻断其下
  游，不会提前终止独立分支。
- 源文件以 64 KiB 块流式哈希，不把整个文件读入内存，也不复制文件；可处理
  最大 2 GiB 的合法源文件。
- 生产者输出只在复用时校验读取一次，或在实际成功后取得发布指纹时读取一
  次。该指纹同时作为所有 `reads` 该输出的消费者的读取指纹，消费者不会为
  了算键再次整读生产者输出，也不会按消费者数量倍增读取。
- 生产者本轮重跑后，消费者使用本轮新校验的输出指纹：只有显式读取的输出字
  节变化才会改变消费者键。生产者只是执行过、退出码变化、配方变化但产物字
  节相同、或另一个未读取输出变化，都不会单独使消费者失效。

### 运行前错误、输出清理和终态

- 准入后直接源缺失、不可读或不是普通文件：`input_error`，不启动命令。
- 实际运行前，执行器会尝试准备嵌套输出的父目录，并只清除该步声明路径上已
  有的普通文件。无法删除/准备，或已存在路径不是普通文件（例如目录或符号
  链接）时返回 `output_error`；不会递归删除目录。
- 非零退出为 `failed`，启动问题为 `launch_error`，超时为 `timeout`。
- 零退出后，只有全部声明输出都是可读普通文件并成功取得指纹，才是
  `ok`；否则为 `output_error`，其 `detail` 会包含问题路径。零退出后的
  `output_error.returncode` 为 `0`。
- 失败步骤的文件不形成有效记录，其数据下游会因本轮依赖不成功而跳过。

### 状态文件和复用边界

`state_file` 由调用方指定在工作区外，其父目录必须已存在；文件不存在时为
冷启动。状态是 JSON，包含版本、按绝对路径标识的 `workspace` 和记录。状态
损坏、版本无法识别或工作区不匹配时，在任何子进程启动前抛 `ValueError`，
不会被静默当成冷启动。

一次正常调用只在结束时通过同目录临时文件加 `os.replace` 原子提交一次状
态：

- `ok` 且有声明输出、或本轮校验成功的 `reused` 步骤保存/保留有效记录；
- `failed`、`timeout`、`launch_error`、`input_error`、`output_error`
  等实际失败步骤清除旧记录；
- `skipped` 或不在目标闭包中的当前 plan 步骤保留旧记录，但这些记录下轮仍
  必须重新通过依赖门槛、键匹配和磁盘字节校验；
- 无输出的成功步骤不保存记录；
- 已不在当前 plan 中的名称可以并会在提交时清理。

因此，“上次成功过”永远不能单独决定本轮可交付：本轮直接依赖结果负责准
入，文件内容和状态键负责复用。

## `execute_guarded`：带 stderr 检查协议和日志尾部的受监督入口

```python
from executor import Step, execute_guarded

results = execute_guarded(
    plan,                         # 名称 -> Step，依赖/cwd/stdin/shell 约定同 execute
    checks={"abi": ("ABI",)},     # None 或 步骤名 -> 必报检查 ID 元组
    tail_bytes=4096,              # 每条 stdout/stderr 保留的原始字节后缀上限
    workers=2,                    # 1..8
    cwd=None,
)

guarded = results["abi"]
guarded.outcome.status            # 原有 Outcome；新增检查相关终态见下
guarded.outcome.returncode
guarded.outcome.blocked_by
guarded.outcome.detail
guarded.stdout_tail               # bytes，最多 tail_bytes
guarded.stderr_tail               # bytes，最多 tail_bytes
```

返回普通字典，顺序与输入 plan 相同；每个值都是冻结的
`GuardedOutcome(outcome, stdout_tail, stderr_tail)`。跳过或未启动步骤的两个
尾部均为 `b""`。调度仍只允许 `outcome.status == "ok"` 的步骤放行 `after`
依赖；直接依赖全部到终态后才生成 `skipped`，`blocked_by` 按 `after` 顺序
去重。一个分支被检查拒绝不会停止其他无依赖关系的分支。

`outcome.status` 的可能值为：`ok`、`failed`、`timeout`、`launch_error`、
`skipped`、`check_failed`、`check_protocol_error`、`check_incomplete`。
其中后三个是受监督入口的检查终态。

### 预校验和准入限制

- 最多 256 步；`workers` 必须是 `1..8` 的整数，`tail_bytes` 必须是
  `0..16384` 的整数；布尔值不算整数。
- `checks` 为 `None` 或映射。值必须是 ID 元组；每步最多 64 个互异 ID。
  ID 长度为 1..32，只能包含 ASCII 字母、数字、下划线和连字符。
- `checks` 中未列出的步骤或空元组不启用协议；其 stderr 全部按普通日志处理。
- 所有参数、ID、依赖引用和依赖环都在任何子进程启动前验证；坏声明或环抛出
  `ValueError`，因此坏 plan 中排在前面的独立步骤也不会产生副作用。

### stderr 检查协议

只有启用协议的步骤会解析 **stderr**；stdout 永远不参与准入判定。检查行必须
从字节流开头或某个 LF 之后开始，并精确为以下两种形式之一：

```text
@@BUILD-CHECK <id> PASS\n
@@BUILD-CHECK <id> FAIL\n
```

前缀 `@@BUILD-CHECK ` 本身包含 `CHECK` 后的一个 ASCII 空格；ID 与结论之间也
只能有一个 ASCII 空格，行尾只能是 LF。CRLF、多余空格、错误结论等均为协议
错误。

- 不是以完整 `@@BUILD-CHECK ` 前缀开始的行是普通日志，包括只是部分匹配该
  前缀、或前缀后语法错误但该步骤未启用协议的情况。
- 启用协议后，带完整前缀但已可确定无法成为合法行（语法非法、ID 过长、超过
  合法行最大长度、EOF 时仍没有结束 LF），或报告未声明 ID、重复报告同一 ID，
  终态为 `check_protocol_error`。`Outcome.detail` 会写出已知 ID 和原因。
- 按 stderr 字节顺序处理。第一个明确的 `FAIL` 终态为 `check_failed`；已知
  拒绝 ID 会写入 `detail`。这两类拒绝以及 `check_protocol_error` 的
  `returncode` 都是 `None`，并且不会等待命令自然退出、其他检查或超时。
- 即使全部必报 ID 都已 `PASS`，也不会提前成功：必须主进程退出码为 0 且
  stdout/stderr 两条管道都读到 EOF，才是 `ok`。
- 正常收集结束后，主进程非零退出仍为 `failed` 并保留原退出码；退出码为 0
  但有必报 ID 未收到 `PASS` 时是 `check_incomplete`，其 `returncode` 为 `0`，
  `detail` 按声明顺序列出缺失 ID。
- 找不到可执行文件等启动问题保持 `launch_error`，`detail` 含系统错误，尾部
  为空。

完整行识别是增量、跨写入分块的，不依赖 `tail_bytes`。普通日志不会按整行
缓存；解析器只保留最多 14 字节的前缀匹配状态和至多 51 字节的合法检查行体。

### 日志尾部和内存边界

- stdout、stderr 分别只保留最后 `tail_bytes` 个**已读原始字节**，因此诊断
  可以包含非 UTF-8 数据。
- 正常读到两条管道 EOF 时，返回值分别是完整 stdout/stderr 的精确后缀。
- 主动拒绝或超时时，只保证是截至实际收尾已读字节的后缀；触发拒绝的报文也
  遵循同一个尾部上限，不会额外复制或附加。
- `tail_bytes=0` 时返回两个 `b""`，但仍会读取并排空两条管道、解析 stderr
  协议，避免管道阻塞和漏判。
- 执行器不会把完整日志保存在内存中，也不会落盘；读取块和有界尾部之外不
  累积流式日志。

### 超时、进程组和资源边界

- 每个步骤从 `Popen` 成功启动开始计算自己的 `Step.timeout`；固定截止时间不
  因持续读到日志而延长，且覆盖主进程退出后的管道收集。
- 主进程已退出但同组辅助进程仍持有 stdout/stderr 写端时，步骤不算完成；到
  期按 `timeout` 收尾，`returncode=None`。
- `execute_guarded` 不经 shell，设置 `stdin=DEVNULL`，并通过
  `start_new_session=True` 让每个直接子进程成为独立步骤进程组/会话的组长。
- 检查拒绝、协议错误或超时后，执行器向该步骤组发送 `SIGKILL`，回收直接子
  进程并关闭两条管道，之后对应 worker 名额才归还调度器。因此同组、未主动
  脱离的辅助进程也会被结束，而其他步骤位于各自进程组，不受影响。
- 受信任命令若主动把后代移出该进程组，执行器不承诺控制这些后代；被系统领养
  且已无法执行的僵尸进程也不由本入口清理。
- 本入口不提供 `execute_with_artifacts` 的暂存文件、产物校验或额度管理。

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
# 在项目根目录中；-m 会把当前目录加入 sys.path，可与 -I 一起使用
python3 -I -m unittest -v

# 或使用虚拟环境
python3 -m venv .venv
.venv/bin/python -m pip install --no-index -r requirements.txt
PYTHONPATH=. .venv/bin/python -m unittest -v
.venv/bin/python demo.py
```

`test_guarded.py` 全部使用标准库和真实子进程、临时目录及文件握手；其中
160 MiB `RLIMIT_AS` 的大流量测试只在 Linux 上运行。`test_incremental.py`
同样离线覆盖冷/暖两轮、新 Python 进程状态复用、等长源编辑与时间戳恢复、
输出磁盘字节损坏、`after` 检查门槛、零退出漏输出、未选分支、跨类边环、
并行握手，以及 Linux 上 128 MiB `RLIMIT_AS` 与 192 MiB 共享源。测试不依赖
固定睡眠猜测辅助进程启动顺序。

所有入口都仅运行调用者信任的 argv，无 shell、无沙箱，继承调用进程环境和
设定的 cwd，并把步骤 stdin 设为 `/dev/null`。基础 `execute` 的超时由
subprocess 管理直接子进程、不跟踪后代；`execute_guarded` 则使用独立进程组
并在拒绝/超时时 kill 整个步骤组。`execute_with_artifacts` 另有暂存额度和
清理契约。除 `execute_incremental` 按上文持久化内容指纹和有效输出记录
外，其他入口不持久化任务；所有入口均不自动重试，没有依赖约束的步骤不保证
完成顺序。
