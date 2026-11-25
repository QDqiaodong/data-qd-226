"""Execute a trusted local build plan using bounded parallel subprocesses."""
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from graphlib import CycleError, TopologicalSorter
import json
import math
import os
import shutil
import stat as stat_mod
import subprocess
import tempfile
from typing import Optional


CAP_LIMIT = 1 << 40
MAX_STEPS = 18
SCRATCH_PREFIX = "build-scratch-"
TERMINAL_STATES = frozenset(
    ("ok", "failed", "timeout", "launch_error", "artifact_error", "skipped"))


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


def _launch_env(step, cwd, env):
    try:
        proc = subprocess.run(step.argv, cwd=cwd, env=env, timeout=step.timeout,
                              stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL, check=False)
    except subprocess.TimeoutExpired:
        return Outcome("timeout")
    except OSError as exc:
        return Outcome("launch_error", detail=str(exc))
    return Outcome("ok" if proc.returncode == 0 else "failed", proc.returncode)


def _validate_plan(plan, workers):
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
    return list(plan), plan


def execute(plan, *, workers=2, cwd=None):
    """Return step -> terminal outcome. An invalid plan starts no processes."""
    names, plan = _validate_plan(plan, workers)
    graph = TopologicalSorter({n: plan[n].after for n in names})
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
    return {name: result[name] for name in names}


def _feasible(names, deps, reserve, consumers, assumed_done, held0,
              held_records, running, capacity):
    """Depth-first search for a capacity-feasible completion order.

    ``names`` lists the steps still free to run. ``assumed_done`` are steps
    (typically the batch about to launch) that are assumed to finish
    successfully before any of ``names`` runs; their reservations are part of
    ``held0``. ``held_records`` lists every reservation already included in
    ``held0`` as ``(size, open_consumer_names, stuck)``; ``stuck`` means a
    consumer is currently running, so the record cannot release inside this
    search. The search only decides when held0's reservations may be released
    and when new reservations may be added.
    """
    index = {name: i for i, name in enumerate(names)}
    full = (1 << len(names)) - 1

    dep_mask = [0] * len(names)
    for i, name in enumerate(names):
        mask = 0
        for dep in deps[name]:
            if dep in index:
                mask |= 1 << index[dep]
        dep_mask[i] = mask

    sizes = [reserve.get(name, 0) for name in names]
    open_mask = [0] * len(names)
    stuck = [False] * len(names)
    for producer, readers in consumers.items():
        mask, is_stuck = 0, False
        for reader in readers:
            if reader in index:
                mask |= 1 << index[reader]
            elif reader in running:
                is_stuck = True
        if producer in index:
            i = index[producer]
            open_mask[i] = mask
            stuck[i] = is_stuck

    records = []
    for size, open_names, is_stuck in held_records:
        mask = 0
        for reader in open_names:
            if reader in index:
                mask |= 1 << index[reader]
        records.append((size, mask, is_stuck))

    def held(done):
        # held0 covers assumed_done producers and every currently held record.
        value = held0
        not_done = (~done) & full
        for i, size in enumerate(sizes):
            if size and (done >> i) & 1:
                if stuck[i] or (open_mask[i] & not_done):
                    value += size
        for size, mask, is_stuck in records:
            if not is_stuck and not (mask & not_done):
                value -= size
        return value

    failed = set()

    def search(done):
        if done == full:
            return True
        if done in failed:
            return False
        current = held(done)
        not_done = (~done) & full
        available = 0
        for i in range(len(names)):
            if not ((done >> i) & 1) and not (dep_mask[i] & not_done):
                available |= 1 << i
        while available:
            bit = available & -available
            available -= bit
            i = bit.bit_length() - 1
            if current + sizes[i] <= capacity and search(done | bit):
                return True
        failed.add(done)
        return False

    return search(0)


def execute_with_artifacts(plan, *, outputs, uses, capacity, scratch_dir,
                           workers=2, cwd=None):
    """Run a build plan whose producers leave scratch artifacts for readers.

    Returns a name -> Outcome mapping in plan input order. Reservations are
    granted in full before a producer launches and released only once every
    declared consumer is terminal. A ``ValueError`` is raised before any
    process starts when the plan is malformed, cyclic, or has no
    capacity-feasible completion order; capacity failures use a message
    starting with "capacity infeasible".
    """
    names, plan = _validate_plan(plan, workers)
    if len(names) > MAX_STEPS:
        raise ValueError(f"at most {MAX_STEPS} steps per call")

    if not isinstance(outputs, dict):
        raise ValueError("outputs must be a mapping of step name to bytes")
    reserve = {}
    for name, size in outputs.items():
        if name not in plan:
            raise ValueError(f"outputs: unknown step {name!r}")
        if type(size) is not int or not 1 <= size <= CAP_LIMIT:
            raise ValueError(
                f"outputs[{name!r}]: reservation must be an int in [1, 2^40]")
        reserve[name] = size

    if not isinstance(uses, dict):
        raise ValueError("uses must be a mapping of step name to producer tuple")
    readers = {}
    for name, refs in uses.items():
        if name not in plan:
            raise ValueError(f"uses: unknown step {name!r}")
        if not isinstance(refs, tuple) or any(
                not isinstance(ref, str) for ref in refs):
            raise ValueError(f"uses[{name!r}]: must be a tuple of step names")
        if len(set(refs)) != len(refs):
            raise ValueError(f"uses[{name!r}]: duplicate producer reference")
        for ref in refs:
            if ref not in plan:
                raise ValueError(f"uses[{name!r}]: unknown step {ref!r}")
            if ref not in reserve:
                raise ValueError(
                    f"uses[{name!r}]: {ref!r} declares no output artifact")
        readers[name] = refs

    if type(capacity) is not int or not 0 <= capacity <= CAP_LIMIT:
        raise ValueError("capacity must be an int in [0, 2^40]")
    if not isinstance(scratch_dir, str) or not os.path.isdir(scratch_dir):
        raise ValueError("scratch_dir must be an existing directory")
    scratch_root = os.path.abspath(scratch_dir)

    deps = {}
    for name in names:
        combined = list(plan[name].after) + list(readers.get(name, ()))
        deps[name] = tuple(dict.fromkeys(combined))
    try:
        TopologicalSorter(deps).prepare()
    except CycleError as exc:
        raise ValueError(f"dependency cycle: {exc.args[1]}") from exc

    consumers = {producer: [] for producer in reserve}
    for reader, refs in readers.items():
        for producer in refs:
            consumers[producer].append(reader)

    if not _feasible(names, deps, reserve, consumers, (), 0, (),
                     frozenset(), capacity):
        raise ValueError(
            "capacity infeasible: no step ordering can keep every reserved "
            "artifact within the declared scratch capacity")

    scratch = tempfile.mkdtemp(prefix=SCRATCH_PREFIX, dir=scratch_root)
    paths = {
        name: os.path.join(scratch, f"artifact_{i}")
        for i, name in enumerate(names) if name in reserve
    }
    state = {name: "pending" for name in names}
    result = {}
    running = set()
    active = {}
    held_bytes = 0
    held_producers = set()

    def open_consumers(producer):
        # Terminal consumers (including skipped/failed) no longer pin the
        # artifact; only pending or running ones keep its reservation held.
        return tuple(c for c in consumers[producer]
                     if state[c] not in TERMINAL_STATES)

    def residual_feasible(batch):
        batch = set(batch)
        remaining = [n for n in names
                     if state[n] == "pending" and n not in batch]
        held0 = held_bytes + sum(reserve.get(n, 0) for n in batch)
        # Every reservation in held0 gets exactly one release record; batch
        # members already in held_producers must not be recorded twice.
        records = []
        for producer in held_producers:
            records.append((
                reserve[producer], open_consumers(producer),
                any(state[c] == "running" for c in consumers[producer])))
        for name in batch:
            if name in reserve and name not in held_producers:
                records.append((
                    reserve[name], open_consumers(name),
                    any(state[c] == "running" for c in consumers[name])))
        return _feasible(remaining, deps, reserve, consumers, batch, held0,
                         records, frozenset(running), capacity)

    def choose_batch(ready):
        slots = workers - len(running)

        def safe(batch):
            if held_bytes + sum(reserve.get(n, 0) for n in batch) > capacity:
                return False
            return residual_feasible(batch)

        best = []
        for first in ready:
            if not safe([first]):
                continue
            batch = [first]
            for name in ready:
                if name in batch or len(batch) >= slots:
                    continue
                trial = batch + [name]
                if safe(trial):
                    batch = trial
            if len(batch) > len(best):
                best = batch
            if len(best) == slots or (not running and len(best) >= 2):
                break
        return best

    def propagate_skips():
        changed = True
        while changed:
            changed = False
            for name in names:
                if state[name] != "pending":
                    continue
                direct = tuple(dict.fromkeys(
                    list(plan[name].after) + list(readers.get(name, ()))))
                if all(state[d] in TERMINAL_STATES for d in direct):
                    blockers = tuple(d for d in direct if state[d] != "ok")
                    if blockers:
                        state[name] = "skipped"
                        result[name] = Outcome(
                            "skipped", blocked_by=blockers)
                        changed = True

    def release_finished_artifacts():
        # Run only after every completion has been finalized. A producer
        # whose own step just finished is handled inside finish(); here we
        # release artifacts whose last consumer has reached a terminal state.
        nonlocal held_bytes
        for producer in list(held_producers):
            if state[producer] not in TERMINAL_STATES:
                continue
            if all(state[c] in TERMINAL_STATES
                   for c in consumers[producer]):
                os.remove(paths[producer])
                held_producers.discard(producer)
                held_bytes -= reserve[producer]

    def finish(name, outcome):
        nonlocal held_bytes
        state[name] = outcome.status
        result[name] = outcome
        running.discard(name)
        if name not in reserve:
            return
        path, limit = paths[name], reserve[name]
        if outcome.status == "ok":
            try:
                info = os.lstat(path)
            except FileNotFoundError:
                info = None
            if info is None:
                verdict = Outcome(
                    "artifact_error", 0,
                    detail=f"{name}: producer created no output file")
            elif stat_mod.S_ISLNK(info.st_mode):
                os.remove(path)
                verdict = Outcome(
                    "artifact_error", 0,
                    detail=f"{name}: output path is a symbolic link")
            elif not stat_mod.S_ISREG(info.st_mode):
                os.remove(path)
                verdict = Outcome(
                    "artifact_error", 0,
                    detail=f"{name}: output is not a regular file")
            elif info.st_size > limit:
                os.remove(path)
                verdict = Outcome(
                    "artifact_error", 0,
                    detail=(f"{name}: output is {info.st_size} bytes, "
                            f"reservation was {limit}"))
            else:
                verdict = None
            if verdict is not None:
                state[name] = verdict.status
                result[name] = verdict
                held_producers.discard(name)
                held_bytes -= limit
            elif not consumers[name]:
                if name in held_producers:
                    os.remove(path)
                    held_producers.discard(name)
                    held_bytes -= limit
        else:
            if name in held_producers:
                if os.path.lexists(path):
                    os.remove(path)
                held_producers.discard(name)
                held_bytes -= limit

    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            while any(state[n] not in TERMINAL_STATES for n in names) or active:
                propagate_skips()
                release_finished_artifacts()
                if len(running) < workers:
                    ready = [
                        n for n in names
                        if state[n] == "pending"
                        and all(state[d] == "ok" for d in deps[n])]
                    for name in choose_batch(ready):
                        env = os.environ.copy()
                        env["BUILD_INPUTS"] = json.dumps({
                            producer: paths[producer]
                            for producer in readers.get(name, ())
                        })
                        if name in reserve:
                            env["BUILD_OUTPUT"] = paths[name]
                            held_producers.add(name)
                            held_bytes += reserve[name]
                        else:
                            env.pop("BUILD_OUTPUT", None)
                        state[name] = "running"
                        running.add(name)
                        active[pool.submit(
                            _launch_env, plan[name], cwd, env)] = name
                if not active:
                    unfinished = [n for n in names
                                  if state[n] not in TERMINAL_STATES]
                    if not unfinished:
                        break
                    # A pending step with no running peers means the
                    # feasibility invariant regressed: every admission was
                    # supposed to preserve a valid completion order.
                    raise RuntimeError(
                        "capacity scheduler stalled; unfinished steps: "
                        + ", ".join(unfinished))
                completed, _ = wait(active, return_when=FIRST_COMPLETED)
                for future in completed:
                    name = active.pop(future)
                    finish(name, future.result())
    finally:
        shutil.rmtree(scratch)

    return {name: result[name] for name in names}
