"""Execute a trusted local build plan using bounded parallel subprocesses."""
from collections import deque
from collections.abc import Mapping
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from graphlib import CycleError, TopologicalSorter
import json
import math
import os
import selectors
import shutil
import signal
import stat as stat_mod
import subprocess
import tempfile
import time
from typing import Optional


CAP_LIMIT = 1 << 40
MAX_STEPS = 18
MAX_GUARDED_STEPS = 256
MAX_GUARDED_WORKERS = 8
MAX_TAIL_BYTES = 16384
MAX_CHECKS_PER_STEP = 64
CHECK_PREFIX = b"@@BUILD-CHECK "
CHECK_ID_MAX = 32
# Prefix + id + one separator + four-byte PASS/FAIL (LF excluded).
CHECK_LINE_BODY_MAX = len(CHECK_PREFIX) + CHECK_ID_MAX + 1 + 4
READ_CHUNK = 64 * 1024
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


@dataclass(frozen=True)
class GuardedOutcome:
    """Outcome plus bounded raw diagnostic tails for a supervised step."""
    outcome: Outcome
    stdout_tail: bytes = b""
    stderr_tail: bytes = b""


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


def _valid_check_id(check_id):
    return (
        isinstance(check_id, str)
        and 1 <= len(check_id) <= CHECK_ID_MAX
        and all(
            "a" <= ch <= "z"
            or "A" <= ch <= "Z"
            or "0" <= ch <= "9"
            or ch in "_-"
            for ch in check_id
        )
    )


def _validate_guarded_plan(plan, checks, tail_bytes, workers):
    if type(workers) is not int or not 1 <= workers <= MAX_GUARDED_WORKERS:
        raise ValueError(
            f"workers must be an integer in [1, {MAX_GUARDED_WORKERS}]")
    if type(tail_bytes) is not int or not 0 <= tail_bytes <= MAX_TAIL_BYTES:
        raise ValueError(
            f"tail_bytes must be an integer in [0, {MAX_TAIL_BYTES}]")

    try:
        materialized_plan = dict(plan)
    except (TypeError, ValueError) as exc:
        raise ValueError("plan must be a name to Step mapping") from exc
    if len(materialized_plan) > MAX_GUARDED_STEPS:
        raise ValueError(f"at most {MAX_GUARDED_STEPS} steps per guarded call")

    names, validated_plan = _validate_plan(materialized_plan, workers)

    required = {}
    if checks is not None:
        if not isinstance(checks, Mapping):
            raise ValueError("checks must be None or a mapping")
        for name, ids in checks.items():
            if name not in validated_plan:
                raise ValueError(f"checks: unknown step {name!r}")
            if not isinstance(ids, tuple):
                raise ValueError(f"checks[{name!r}] must be a tuple of IDs")
            if len(ids) > MAX_CHECKS_PER_STEP:
                raise ValueError(
                    f"checks[{name!r}]: at most {MAX_CHECKS_PER_STEP} IDs")
            seen = set()
            for check_id in ids:
                if not _valid_check_id(check_id):
                    raise ValueError(
                        f"checks[{name!r}]: invalid check ID {check_id!r}")
                if check_id in seen:
                    raise ValueError(
                        f"checks[{name!r}]: duplicate check ID {check_id!r}")
                seen.add(check_id)
            if ids:
                required[name] = ids

    try:
        TopologicalSorter(
            {name: validated_plan[name].after for name in names}).prepare()
    except CycleError as exc:
        raise ValueError(f"dependency cycle: {exc.args[1]}") from exc

    return names, validated_plan, required


def _append_tail(tail, chunk, limit):
    if limit <= 0:
        return
    tail.extend(chunk)
    extra = len(tail) - limit
    if extra > 0:
        del tail[:extra]


def _is_check_id_byte(value):
    return (
        48 <= value <= 57       # 0-9
        or 65 <= value <= 90    # A-Z
        or 97 <= value <= 122   # a-z
        or value == 95          # _
        or value == 45          # -
    )


class _CheckProtocolParser:
    """Incremental parser which never buffers an entire ordinary log line."""

    def __init__(self, required_ids):
        self.required = tuple(required_ids)
        self.required_set = set(required_ids)
        self.reported = set()
        self.passed = set()
        self.fatal = None
        self._reset_line()

    def _reset_line(self):
        self.ordinary = False
        self.matched_prefix = 0
        self.in_check_line = False
        self.id_bytes = bytearray()
        self.current_id = None
        self.phase = "id"
        self.status_bytes = bytearray()

    def _protocol_error(self, reason, check_id=None):
        if check_id is None:
            check_id = self.current_id
        if check_id:
            detail = (
                f"check protocol error for ID {check_id!r}: {reason}")
        else:
            detail = f"check protocol error: {reason}"
        self.fatal = ("check_protocol_error", detail)
        return self.fatal

    def _check_failed(self, check_id):
        detail = f"BUILD-CHECK reported FAIL for required ID {check_id!r}"
        self.fatal = ("check_failed", detail)
        return self.fatal

    def feed(self, data):
        if self.fatal is not None:
            return self.fatal

        pos = 0
        size = len(data)
        while pos < size:
            if self.ordinary:
                newline = data.find(b"\n", pos)
                if newline < 0:
                    return None
                pos = newline + 1
                self._reset_line()
                continue

            if not self.in_check_line:
                remaining = CHECK_PREFIX[self.matched_prefix:]
                available = min(size - pos, len(remaining))
                offset = 0
                while (offset < available
                       and data[pos + offset] == remaining[offset]):
                    offset += 1

                if offset < available:
                    # The mismatching byte may itself be LF.  Ordinary-line
                    # scanning handles that and starts the next line correctly.
                    self.ordinary = True
                    pos += offset
                    continue

                self.matched_prefix += offset
                pos += offset
                if self.matched_prefix == len(CHECK_PREFIX):
                    self.in_check_line = True
                continue

            while pos < size:
                value = data[pos]
                pos += 1

                if self.phase == "id":
                    if value == 32:  # ASCII space
                        if not self.id_bytes:
                            return self._protocol_error("empty check ID")
                        check_id = self.id_bytes.decode("ascii")
                        if check_id not in self.required_set:
                            return self._protocol_error(
                                "undeclared check ID", check_id)
                        if check_id in self.reported:
                            return self._protocol_error(
                                "duplicate report for check ID", check_id)
                        self.current_id = check_id
                        self.phase = "status"
                    elif value == 10:  # LF
                        return self._protocol_error(
                            "check line ended before its verdict")
                    else:
                        if len(self.id_bytes) >= CHECK_ID_MAX:
                            return self._protocol_error(
                                "check ID is longer than 32 ASCII characters")
                        if not _is_check_id_byte(value):
                            return self._protocol_error(
                                f"invalid check ID byte 0x{value:02x}")
                        self.id_bytes.append(value)

                elif self.phase == "status":
                    if value == 10:
                        return self._protocol_error(
                            "check line ended before a complete PASS/FAIL")
                    self.status_bytes.append(value)
                    status = bytes(self.status_bytes)
                    if (not b"PASS".startswith(status)
                            and not b"FAIL".startswith(status)):
                        return self._protocol_error(
                            "verdict must be exactly PASS or FAIL")
                    if len(self.status_bytes) == 4:
                        self.phase = "lf"

                else:  # Exactly one LF must terminate the four-byte verdict.
                    if value != 10:
                        return self._protocol_error(
                            "expected LF immediately after PASS/FAIL")
                    check_id = self.current_id
                    self.reported.add(check_id)
                    if bytes(self.status_bytes) == b"FAIL":
                        return self._check_failed(check_id)
                    self.passed.add(check_id)
                    self._reset_line()
                    break

        return None

    def handle_eof(self):
        if self.fatal is not None:
            return self.fatal
        if self.in_check_line:
            return self._protocol_error(
                "check-prefixed line has no terminating LF at EOF")
        return None

    def missing_ids(self):
        return tuple(cid for cid in self.required if cid not in self.passed)


def _kill_step_group(proc):
    if proc is None:
        return
    try:
        # execute_guarded starts each child as a session/group leader.
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    except OSError:
        try:
            proc.kill()
        except OSError:
            pass


def _run_guarded_step(name, step, required_ids, tail_limit, cwd):
    try:
        proc = subprocess.Popen(
            step.argv,
            cwd=cwd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
    except OSError as exc:
        return GuardedOutcome(
            Outcome("launch_error", detail=str(exc)), b"", b"")

    parser = _CheckProtocolParser(required_ids) if required_ids else None
    stdout_tail = bytearray()
    stderr_tail = bytearray()
    selector = None
    verdict = None

    try:
        deadline = time.monotonic() + step.timeout
        stdout_fd = proc.stdout.fileno()
        stderr_fd = proc.stderr.fileno()
        selector = selectors.DefaultSelector()
        selector.register(stdout_fd, selectors.EVENT_READ)
        selector.register(stderr_fd, selectors.EVENT_READ)
        open_fds = {stdout_fd, stderr_fd}

        while open_fds and verdict is None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                verdict = ("timeout", None, "")
                break

            events = selector.select(remaining)
            for key, _event_mask in events:
                fd = key.fd
                try:
                    chunk = os.read(fd, READ_CHUNK)
                except InterruptedError:
                    continue
                except OSError:
                    chunk = b""

                if chunk:
                    if fd == stdout_fd:
                        _append_tail(stdout_tail, chunk, tail_limit)
                    else:
                        _append_tail(stderr_tail, chunk, tail_limit)
                        if parser is not None:
                            fatal = parser.feed(chunk)
                            if fatal is not None:
                                verdict = (fatal[0], None, fatal[1])
                                break
                    continue

                if fd not in open_fds:
                    continue
                open_fds.remove(fd)
                try:
                    selector.unregister(fd)
                except (KeyError, ValueError, OSError):
                    pass

                if fd == stderr_fd and parser is not None:
                    fatal = parser.handle_eof()
                    if fatal is not None:
                        verdict = (fatal[0], None, fatal[1])
                        break

        if verdict is None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                verdict = ("timeout", None, "")
            else:
                try:
                    returncode = proc.wait(timeout=remaining)
                except subprocess.TimeoutExpired:
                    verdict = ("timeout", None, "")
                else:
                    if returncode != 0:
                        verdict = ("failed", returncode, "")
                    elif parser is None:
                        verdict = ("ok", 0, "")
                    else:
                        missing = parser.missing_ids()
                        if missing:
                            if len(missing) == 1:
                                detail = (
                                    "missing required check ID: "
                                    + ", ".join(missing))
                            else:
                                detail = (
                                    "missing required check IDs: "
                                    + ", ".join(missing))
                            verdict = ("check_incomplete", 0, detail)
                        else:
                            verdict = ("ok", 0, "")

        # A check rejection or deadline can race with process exit.  The
        # verdict chosen above is authoritative; only reap/cleanup differs.
        if proc.returncode is None:
            _kill_step_group(proc)
            try:
                proc.wait()
            except OSError:
                pass

        status, returncode, detail = verdict
        outcome = Outcome(
            status=status, returncode=returncode, detail=detail)
        return GuardedOutcome(
            outcome, bytes(stdout_tail), bytes(stderr_tail))

    except BaseException:
        if proc.returncode is None:
            _kill_step_group(proc)
            try:
                proc.wait()
            except OSError:
                pass
        raise
    finally:
        if selector is not None:
            try:
                selector.close()
            except Exception:
                pass
        for stream in (proc.stdout, proc.stderr):
            if stream is not None:
                try:
                    stream.close()
                except OSError:
                    pass


def execute_guarded(plan, *, checks=None, tail_bytes=4096, workers=2,
                    cwd=None):
    """Run a plan with stderr check protocol and bounded diagnostic tails.

    All declarations and the dependency graph are validated before a command
    starts.  Enabled steps parse ``@@BUILD-CHECK ...`` lines on stderr.  A
    FAIL/protocol error or timeout kills the step's own process group and
    blocks that step's downstream branch without stopping independent work.
    """
    names, validated_plan, required = _validate_guarded_plan(
        plan, checks, tail_bytes, workers)

    graph = TopologicalSorter(
        {name: validated_plan[name].after for name in names})
    graph.prepare()
    ready = deque(graph.get_ready())
    results = {}
    active = {}

    with ThreadPoolExecutor(max_workers=workers) as pool:
        while ready or active:
            while ready and len(active) < workers:
                name = ready.popleft()
                blockers = tuple(dict.fromkeys(
                    dep for dep in validated_plan[name].after
                    if results[dep].outcome.status != "ok"
                ))
                if blockers:
                    results[name] = GuardedOutcome(
                        Outcome("skipped", blocked_by=blockers))
                    graph.done(name)
                    ready.extend(graph.get_ready())
                    continue

                future = pool.submit(
                    _run_guarded_step,
                    name,
                    validated_plan[name],
                    required.get(name, ()),
                    tail_bytes,
                    cwd,
                )
                active[future] = name

            if active:
                completed, _still_active = wait(
                    active, return_when=FIRST_COMPLETED)
                for future in completed:
                    name = active.pop(future)
                    results[name] = future.result()
                    graph.done(name)
                ready.extend(graph.get_ready())

    return {name: results[name] for name in names}


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
