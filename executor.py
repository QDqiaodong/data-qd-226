"""Execute a trusted local build plan using bounded parallel subprocesses."""
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from graphlib import CycleError, TopologicalSorter
import math
import subprocess
from typing import Optional


@dataclass(frozen=True)
class Step:
    argv: tuple
    after: tuple = ()
    timeout: float = 30


@dataclass(frozen=True)
class Outcome:
    status: str
    returncode: Optional[int] = None
    blocked_by: tuple = ()
    detail: str = ""


def _launch(step, cwd):
    try:
        proc = subprocess.run(step.argv, cwd=cwd, timeout=step.timeout,
                              stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL, check=False)
    except subprocess.TimeoutExpired:
        return Outcome("timeout")
    except OSError as exc:
        return Outcome("launch_error", detail=str(exc))
    return Outcome("ok" if proc.returncode == 0 else "failed", proc.returncode)


def execute(plan, *, workers=2, cwd=None):
    """Return step -> terminal outcome. An invalid plan starts no processes."""
    if type(workers) is not int or workers < 1:
        raise ValueError("workers must be a positive integer")
    plan = dict(plan)
    for name, step in plan.items():
        if not isinstance(name, str) or not name or not isinstance(step, Step):
            raise ValueError("plan must map nonempty names to Step objects")
        if (not isinstance(step.argv, tuple) or not step.argv
                or not all(isinstance(x, str) and "\x00" not in x for x in step.argv)
                or not step.argv[0]):
            raise ValueError(f"{name}: invalid argv")
        if not isinstance(step.after, tuple) or any(
                not isinstance(dep, str) or dep not in plan for dep in step.after):
            raise ValueError(f"{name}: invalid dependency")
        if (type(step.timeout) not in (float, int)
                or not math.isfinite(step.timeout) or step.timeout <= 0):
            raise ValueError(f"{name}: timeout must be finite and positive")
    graph = TopologicalSorter({n: s.after for n, s in plan.items()})
    try:
        graph.prepare()
    except CycleError as exc:
        raise ValueError(f"dependency cycle: {exc.args[1]}") from exc

    ready = deque(graph.get_ready())
    result, active = {}, {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        while ready or active:
            while ready and len(active) < workers:
                name = ready.popleft()
                blockers = tuple(dict.fromkeys(d for d in plan[name].after
                                                if result[d].status != "ok"))
                if blockers:
                    result[name] = Outcome("skipped", blocked_by=blockers)
                    graph.done(name)
                    ready.extend(graph.get_ready())
                else:
                    active[pool.submit(_launch, plan[name], cwd)] = name
            if active:
                completed, _ = wait(active, return_when=FIRST_COMPLETED)
                for future in completed:
                    name = active.pop(future)
                    result[name] = future.result()
                    graph.done(name)
                ready.extend(graph.get_ready())
    return {name: result[name] for name in plan}
