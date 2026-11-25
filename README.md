# 本地构建步骤执行器

供本地构建集成调用：`execute({name: Step(argv, after, timeout)}, workers=2, cwd=...)`。
启动前核依赖引用和环；运行时最多同时启动指定数量的步骤，只在直接依赖全部
成功后启动新步骤。失败、超时和启动错误阻止依赖分支，不中断独立步骤。
调用者获得按输入顺序排列的终态、退出码和直接阻断原因。

Python 3.9+，标准库。自制演示在临时目录使用真实 Python 子进程：

```sh
python3 -m venv .venv
.venv/bin/python -m pip install --no-index -r requirements.txt
.venv/bin/python -m unittest -v
.venv/bin/python demo.py
```

演示应产生 `emit ok`、`check ok`、`lint failed 7`、`publish skipped`，并验证发布文件未生成。

仅运行调用者信任的 argv，无 shell、无沙箱；继承调用进程环境和设定的 cwd，
丢弃标准输入输出。超时由 subprocess 管理直接子进程，不跟踪后代进程；
不持久化任务、不自动重试。没有依赖约束的步骤不保证完成顺序。
