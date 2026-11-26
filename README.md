# 本地构建步骤执行器

供本地构建集成调用，提供多个均只依赖标准库（Python 3.9+）的入口。

## `execute`：按进程数并行的基础入口

```python
from executor import Step, execute

execute({name: Step(argv, after, timeout)}, workers=2, cwd=...)
```

启动前核依赖引用和环；运行时最多同时启动指定数量的步骤，只在直接依赖全部
成功后启动新步骤。失败、超时和启动错误阻止依赖分支，不中断独立步骤。
调用者获得按输入顺序排列的终态、退出码和直接阻断原因。

## `execute_discovered`：按扫描清单按需发现依赖

```python
from executor import Step, execute_discovered

outcomes = execute_discovered(
    plan,                          # 名称 -> Step，包含所有候选生成器
    targets=("app",),              # 非空、不重复的步骤名元组
    scans={                        # 可选：步骤名 -> 只做依赖探测的 Step
        "app": Step((... "scan_app.py" ...)),
        "generate_header": Step((... "scan_generator.py" ...)),
    },
    workers=2,                     # 1..8，布尔值不算整数
    cwd=workspace,
)

outcomes["generate_header"].status  # ok / failed / timeout / ... / cycle
```

返回普通 dict，只包含从 `targets` 沿静态 `Step.after` 和成功提交的扫描清单
递归选中的步骤，并按原始 `plan` 的输入顺序排列。未选中的候选生成器不会运行
扫描或构建，也不出现在结果中。

### 预校验和限制

启动任何扫描或构建前，会验证整个 `plan`（包括目标闭包外的声明）、`targets`、
`scans`、名称、引用和完整静态 `after` 图：

- 最多 256 个步骤、256 个目标；每步最多 256 项原始 `after`。
- 名称必须是非空、无 NUL、UTF-8 编码后至多 128 字节的字符串。
- `targets` 必须是非空、不重复的步骤名 `tuple`。
- `workers` 必须是 `1..8` 的整数，`True`/`False` 不接受。
- `scans` 为 `None` 或“步骤名 -> `Step`”映射；键必须是已知步骤，扫描
  `Step.after` 必须为空。扫描命令自身的 `argv`、`timeout` 等仍按 `Step`
  校验。
- 完整静态 `after` 图必须无环；静态坏声明、未知引用或静态环抛 `ValueError`，
  且不启动任何命令。运行期扫描边可以形成环，按下文 `cycle` 处理。

### 扫描清单协议

扫描是步骤被选中后立即安排的前置探测，独立于该步骤的静态依赖结果，也不需要
任何构建步骤先产出文件。每个步骤最多扫描一次；经多份清单共享发现的步骤也只
扫描、构建一次。

扫描子进程继承常规环境，并额外得到 `BUILD_DEPS_FILE`：执行器独占临时目录下
属于该扫描、时尚不存在的清单路径。构建命令不会收到这个执行器协议变量；其他
环境变量照常继承。

扫描只有同时满足以下条件才成功：

1. 进程零退出；
2. `BUILD_DEPS_FILE` 是可读普通文件，不是符号链接；
3. 文件不超过 64 KiB；
4. 内容是 UTF-8 JSON 数组；
5. 数组最多 256 项，且全部是不重复的已知步骤名。

执行器先完整验证清单，再采纳其中任何动态边。因此“合法名字 + 未知名字”这类
坏清单不会因为前缀合法而提前选中生产者。文件缺失、符号链接、格式错误、未知
或重复名称、超限、非零退出、启动失败和超时都得到 `scan_error`；`detail`
包含可区分的原因标记（如 `manifest_unknown_name`、`manifest_symlink`、
`manifest_too_large`、`invalid_json`、`nonzero_exit`、`timeout`、
`launch_error`）。若扫描进程已有退出码，`Outcome.returncode` 会保留（包括
坏清单但进程为 0 时的 `0`）；超时和启动错误没有退出码。扫描失败的步骤不会
启动构建，其 `blocked_by=()`。

调用正常返回前会删除本次调用的扫描临时目录；唯一临时目录和独立扫描子目录不
会清理其他调用的临时文件。

### 调度、阻断和运行期环

对扫描成功或没有扫描的步骤，其直接依赖表为：

```text
Step.after 声明顺序 + 扫描 JSON 数组声明顺序，然后全局去重
```

只有本步清单已经确定、所有直接依赖都到达终态且状态均为 `ok` 时，才启动构建。
构建终态沿用 `ok`、`failed`、`launch_error`、`timeout`。直接依赖全部终态但
存在非 `ok` 项时，普通未启动步骤为 `skipped`，`blocked_by` 按上述直接依赖
顺序列出所有非 `ok` 项；不会把更远层的失败平铺进来。

扫描进程和实际构建进程合计最多占用 `workers` 个名额；等待依赖的逻辑步骤不占
名额。执行器不会等待某个无关分支的扫描收敛后才运行独立目标的可运行扫描或
构建，也不会因为运行期环或扫描失败取消无关工作。

所有已选扫描均终态后，最终图固定。从中移除 `scan_error` 节点及其关联边，再
计算有向强连通分量：

- 属于任意有向环的步骤（包括动态自环）返回 `cycle`，不启动构建，
  `blocked_by=()`；即使它还有其他非成功依赖，也仍判为 `cycle`。
- 只依赖环节点、自身不在环中的步骤不是 `cycle`；它在直接依赖终态后返回
  `skipped`，并按完整直接依赖表把环节点和其他失败依赖列入 `blocked_by`。
- 环内步骤的扫描作为发现边的探测仍会按契约运行一次；“不启动”指实际构建
  `Step.argv`。

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

### 状态文件、跨进程共享和复用边界

`state_file` 由调用方指定在工作区外，其父目录必须已存在；文件不存在时为
冷启动。状态是 JSON，包含版本、按绝对路径标识的 `workspace` 和记录。状态
损坏、版本无法识别或工作区不匹配时，在任何子进程启动前抛 `ValueError`，
不会被静默当成冷启动。并发提交期间重新读取状态时若发现损坏，也会拒绝继续
覆盖。

在 Linux 上，多个独立 Python 进程可以把同一个规范化工作区和同一个
`state_file` 传给 `execute_incremental`。重叠调用必须使用相同的完整
`plan`、`sources`、`outputs`、`reads` 和 `context`；`targets` 可以不同。
调用方式没有新增参数，也不需要启动守护进程：

```python
# 进程 1
execute_incremental(..., targets=("bin",), state_file="/var/cache/build/state.json")
# 进程 2（相同 workspace、完整声明和 state_file；targets 可不同）
execute_incremental(..., targets=("docs",), state_file="/var/cache/build/state.json")
```

执行器在 `state_file + ".locks"/` 旁维护标准库 `fcntl.flock` 所有权文件；
该 sidecar 与状态文件位于同一本机文件系统。锁规则如下：

- 每个有声明输出的步骤使用独立的 `step-<sha256(step_name)>.lock`。同一
  步骤的清理、启动、输出校验和记录提交在该锁内完成，因此两个进程不会同时
  运行同一步命令或争写同一路径；不同步骤使用不同锁，仍可跨进程重叠。单次
  调用的 `workers` 上限和进程内去重/调度语义不变，也不引入跨进程全局
  worker 名额。
- 状态合并使用 `state.lock`。成功且通过全部普通文件/内容校验的记录会在
  该步骤完成时立即以“重新读取最新 JSON → 合并 → 同目录临时文件 +
  `os.replace`”发布，不必等整个调用返回。调用结束时还会做一次加锁合并，
  只清理当前 plan 中已移除的名称和无输出步骤记录；其他进程刚提交的不同
  步骤记录不会被旧快照抹掉。
- 等待者取得步骤锁后会重新读取最新状态，并重新哈希当前声明输出。只有复用
  键一致且所有字节与已提交记录一致时才报告 `reused` 且不启动命令。失败、
  超时、启动失败或输出校验失败的步骤会在释放步骤锁前删除自己的旧记录；
  等待者不会把该失败当成可复用结果。
- 无输出步骤不获取结果所有权，仍按“每次进入闭包并通过依赖门槛后实际运行”
  的单进程语义在每个调用中执行。
- 步骤锁描述符会显式传给受信任的构建子进程。若 Python 调用进程被
  `SIGKILL`，内核会在 Python 描述符关闭后继续因构建命令仍持有继承的锁
  描述符而保留 `flock`。因此只要旧命令仍存活并可能写输出，等待者就不能
  清理或重写该输出。旧命令退出后锁才释放；本实现随后重新读取状态，缺少
  完整提交记录时会清理并自行重跑。锁不需要人工删除：进程和继承描述符全部
  关闭后内核自动释放 `flock`。锁文件本身可以保留在 sidecar 目录中，未被
  `flock` 持有时不是活动锁。
- 锁等待采用带超时的非阻塞轮询并在超时时给出锁路径和步骤说明；步骤锁上限
  至少 60 秒且覆盖 `Step.timeout + 30s`，状态锁为 120 秒。正常握手和构建
  会在持有者结束后立即交接，不靠固定睡眠猜测锁状态。

单进程语义中的记录保留规则仍然成立，只是提交改为上述并发安全合并：

- `ok` 且有声明输出、或本轮校验成功的 `reused` 步骤保存/保留有效记录；
- `failed`、`timeout`、`launch_error`、`input_error`、`output_error`
  等实际失败步骤清除旧记录；
- `skipped` 或不在目标闭包中的当前 plan 步骤保留旧记录，但这些记录下轮仍
  必须重新通过依赖门槛、键匹配和磁盘字节校验；
- 无输出的成功步骤不保存记录；
- 已不在当前 plan 中的名称可以并会在最终合并时清理。

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
失败只阻断依赖它的分支；`Outcome` 的字段与前面入口一致。

## `execute_release`：双配方、ABI 探针与原子交付

```python
from executor import ReleaseCohort, Step, execute_release

cohort = ReleaseCohort(
    primary={                          # 首选配方：名称 -> Step
        "compile": Step((...), after=("prep",)),
        "abi_probe": Step((... "probe.py", "primary"), after=("compile",)),
        "package": Step((... "pack.py"), after=("compile",)),
    },
    fallback={                         # 备用配方：步骤/依赖可以完全不同
        "compile": Step((...), after=("prep", "fb_prep")),
        "abi_probe": Step((... "probe.py", "fallback"), after=("compile",)),
        "package": Step((... "pack.py"), after=("compile",)),
    },
    probe="abi_probe",                 # 两套配方都存在的探针步骤名
    package="package",                 # 两套配方都存在的打包步骤名
)

results = execute_release(
    shared={                           # 不受配方影响的公共步骤
        "prep": Step((... "resource_table.py")),
        "fb_prep": Step((... "alt_resource_table.py")),
    },
    cohorts={"linux-x86_64": cohort},  # 组名 -> ReleaseCohort，1..8 组
    release_dir=release_dir,           # 已存在的目录；正式包写到其下
    workers=2,                         # 1..8 的整数，布尔值不算整数
)
result = results["linux-x86_64"]
result.status                # published / incompatible / failed
result.published_profile     # "primary" / "fallback" / None
result.probes                # 已启用配方名 -> 探针 Outcome（未启用为 skipped）
result.detail                # 未发布时的定位原因；成功发布时为 ""
```

返回按 `cohorts` 输入顺序排列的普通 dict，值是冻结的 `ReleaseResult`。
本入口是库接口，不新增 CLI，也没有第三方依赖。

### 配方选择与探针语义

- 每组先跑 `primary`。探针零退出表示当前配方通过；退出码 **42** 表示 ABI
  不兼容，组内已启动的首选命令全部收尾并回收名额后，才切换到 `fallback`，
  在独立的空工作目录里重跑备用配方自己依赖的组内步骤（即使名字或 argv 与
  首选相同也是新的执行身份），可复用的只有本次已成功的公共步骤。
- 探针的其他非零退出、启动失败、超时，或前置步骤失败都是**构建错误**，不
  触发换配方；打包失败同样不换配方。备用探针也判 42 时返回 `incompatible`，
  不再尝试。若打包先于探针结束，仍以探针结论为准：不兼容结论不会被先结束
  的打包结果覆盖。
- 公共步骤只在已选配方（含间接依赖）确实需要时才选中，本次调用内至多执行
  一次；公共步骤失败只阻断依赖它的组和步骤。备用步骤不会继承同名首选步骤
  的失败或产物。

### 工作目录与环境

- 每次调用在系统临时目录下新建一个独占根目录：其中 `shared/` 是公共工作
  目录，每组每套启用配方各有一个独立的空工作目录（`<根>/<组名>/primary`、
  `<根>/<组名>/fallback`）。
- 公共命令以公共目录为 cwd；组内命令以对应配方目录为 cwd，并通过环境变量
  `BUILD_RELEASE_SHARED_DIR` 拿到公共目录的绝对路径。配方命令不应写公共
  目录；构建子进程不直接写正式发布路径。
- 命令沿用 `Step` 的 argv、超时、不经 shell、stdin 为 `DEVNULL`、继承环境
  的语义。调用返回（成功或失败）后收走本轮临时目录，不动发布目录中的其他
  文件。同一 `release_dir` 同时只允许一个发布构建（目录内 `flock` 锁），
  但其他进程可以随时读取其中的正式包。

### 打包合规与原子发布

- 打包命令把**单个文件**写到自己 cwd 下的 `bundle`，不超过 512 MiB。允许
  在探针结论前做候选打包，也允许等探针批准后再打包；候选包只存在于临时
  工作目录，发布路径上不会出现候选字节。
- 只有**同一配方**的探针零退出、打包零退出，且该配方的 `bundle` 是合规、
  可读的普通文件（不是符号链接）时，才发布到
  `release_dir/<组名>.bundle`。符号链接、非常规文件、不可读或超限都判失败。
- 正式路径的切换是单次 `os.replace`：已打开的旧句柄仍能读完旧字节，新打开
  者只能看到完整的旧包或完整的新包。首次发布前正式包不存在；探针拒绝、打包
  失败、`bundle` 不合规或发布操作失败时，已有正式包保持原样，原来没有就
  继续没有。暂存文件与临时目录在返回前清理。

### 校验、限制与调度

- 组名是 1..32 个 ASCII 字母、数字、`-` 或 `_`；步骤名是非空、无 NUL、
  UTF-8 编码至多 128 字节的字符串。1..8 个组；公共步骤加两套配方合计至多
  256 步；每步至多 256 项 `after`。
- 公共步骤只能依赖公共步骤；组内步骤只能依赖本组当前配方的步骤或公共
  步骤，且不能与公共步骤同名。探针和打包步骤都没有下游；每个普通组内步骤
  必须是探针或打包至少其一的前置。两套配方（包括不会启用的备用配方）的
  类型、引用和依赖环都在启动任何命令前校验，无效时抛 `ValueError`，不启动
  命令、不改发布目录。
- 公共步骤、探针、实际构建和打包命令共用 `workers` 个名额；等待依赖或探针
  结论不占名额。必需工作按组轮询分配名额，任何组都不能独占整组额度；候选
  打包只能占用没有必需工作可用的空闲名额，不会抢占已启动的命令。一个组的
  探测或换配方不会阻塞其他组已经就绪的工作与发布。

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
并行握手，以及 Linux 上 128 MiB `RLIMIT_AS` 与 192 MiB 共享源。
`test_incremental_concurrent.py` 只使用临时目录和本地全新 Python 子进程，
通过文件握手和 `/proc/<pid>/fd` 锁描述符检查覆盖：两个进程竞争同一生成
步骤时只启动一次、负者重新校验输出后 `reused`、不同步骤双向握手重叠且
状态记录合并保留、首个持有者失败后等待者接管，以及 `SIGKILL` Python 调
用进程后仍存活的孤儿构建命令通过继承的 `flock` 阻止提前清理/重写并最终
恢复。可单独运行：

```sh
python3 -m unittest -v test_incremental_concurrent
```

`test_discovered.py` 也全部离线运行，用真实子进程和文件握手覆盖平台配置选择
不同生成器、共享步骤只扫描/构建一次、坏清单整单拒绝、静态边与扫描边合成运
行期环、动态自环、`blocked_by` 顺序、两个名额下的慢扫描/快分支握手、扫描与
构建共用并发上限，以及扫描错误分类和临时清单目录清理。所有握手均使用有界等
待，不靠固定睡眠猜测辅助进程启动顺序。

`test_release.py` 同样只用标准库、真实子进程和有界文件握手，覆盖：首选探针
批准时只发布首选字节且备用步骤不启动、退出 42 时整组切换到备用配方并发布备用
字节（公共准备只跑一次、备用专用公共步骤按需启动、两套同名编译步骤使用各自
空工作目录）、普通探针错误不换配方、两次 42 返回 `incompatible`、前置失败不
换配方且探针记为 `skipped`、备用打包留下半成品或符号链接/超限 `bundle` 时不
覆盖旧包、坏声明（含未启用备用配方中的坏边）在启动前拒绝、两个组在两个名额下
并行而不互相独占额度、慢组探针与快组发布之间的握手、切换前打开的旧句柄读完旧
字节而新句柄只见完整新包，以及同目录并发发布被锁拒绝和无关发布文件保留。

所有入口都仅运行调用者信任的 argv，无 shell、无沙箱，继承调用进程环境和
设定的 cwd，并把步骤 stdin 设为 `/dev/null`。基础 `execute` 的超时由
subprocess 管理直接子进程、不跟踪后代；`execute_discovered` 的扫描和构建采
用同样的直接子进程边界，不做进程组监督或跨次状态。`execute_guarded` 则使用
独立进程组并在拒绝/超时时 kill 整个步骤组。`execute_with_artifacts` 另有暂
存额度和清理契约；`execute_release` 用独立工作目录、`BUILD_RELEASE_SHARED_DIR`
环境变量和单次 `os.replace` 原子切换保证候选字节不出现在正式发布路径。除
`execute_incremental` 按上文持久化内容指纹和有效输出记录外，其他入口不持久
化任务；所有入口均不自动重试，没有依赖约束的步骤不保证完成顺序。
