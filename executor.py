"""Execute a trusted local build plan using bounded parallel subprocesses."""
from collections import deque
from collections.abc import Mapping
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from graphlib import CycleError, TopologicalSorter
import hashlib
import json
import math
import os
import selectors
import shutil
import signal
import stat as stat_mod
import subprocess
import tempfile
import threading
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

MAX_INCREMENTAL_STEPS = 256
MAX_TARGETS = 256
MAX_INCREMENTAL_WORKERS = 8
MAX_DECLARED_FILE_PATHS = 4096
MAX_DECLARATION_ITEMS = 16384
MAX_RELATIVE_PATH_BYTES = 256
MAX_AFTER_ITEMS = 256
MAX_CONTEXT_BYTES = 256
INCREMENTAL_STATE_VERSION = 1
INCREMENTAL_SUCCESS_STATES = frozenset(("ok", "reused"))
SHA256_PREFIX = "sha256:"


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


@dataclass(frozen=True)
class IncrementalResult:
    """Selected target closure outcomes and source bytes fingerprinted here."""
    outcomes: dict
    source_bytes_read: int


def _timeout_is_finite_positive(timeout):
    if type(timeout) not in (float, int):
        return False
    try:
        return math.isfinite(timeout) and timeout > 0
    except OverflowError:
        return False


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
        if not _timeout_is_finite_positive(step.timeout):
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


def _valid_relative_workspace_path(value):
    if not isinstance(value, str) or not value:
        return False
    if "\x00" in value or value.startswith("/"):
        return False
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    if len(encoded) > MAX_RELATIVE_PATH_BYTES:
        return False
    # Canonical POSIX-relative form: no empty component, ".", or "..".
    return all(part not in ("", ".", "..") for part in value.split("/"))


def _workspace_fs_path(root, relative_path):
    return os.path.join(root, *relative_path.split("/"))


def _valid_sha256(value):
    return (
        isinstance(value, str)
        and len(value) == len(SHA256_PREFIX) + 64
        and value.startswith(SHA256_PREFIX)
        and all(ch in "0123456789abcdef" for ch in value[len(SHA256_PREFIX):])
    )


def _regular_file_digest(relative_path, root, label):
    """Hash one regular file in bounded memory.

    Returns ``(digest, detail, bytes_read)``.  ``bytes_read`` includes a prefix
    already consumed if a read fails midway.
    """
    fs_path = _workspace_fs_path(root, relative_path)
    try:
        info = os.lstat(fs_path)
    except OSError as exc:
        return None, f"{label} path {relative_path!r}: {exc}", 0

    if stat_mod.S_ISLNK(info.st_mode):
        kind = "symbolic link"
    elif not stat_mod.S_ISREG(info.st_mode):
        kind = "not a regular file"
    else:
        kind = ""
    if kind:
        return None, f"{label} path {relative_path!r}: {kind}", 0

    digest = hashlib.sha256()
    total = 0
    try:
        fd = os.open(fs_path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            while True:
                chunk = os.read(fd, READ_CHUNK)
                if not chunk:
                    break
                digest.update(chunk)
                total += len(chunk)
        finally:
            os.close(fd)
    except OSError as exc:
        return None, f"{label} path {relative_path!r}: {exc}", total

    return SHA256_PREFIX + digest.hexdigest(), None, total


class _SourceRead:
    __slots__ = ("lock", "done", "digest", "detail", "bytes_read")

    def __init__(self):
        self.lock = threading.Lock()
        self.done = False
        self.digest = None
        self.detail = None
        self.bytes_read = 0


class _SourceFingerprintCache:
    """Deduplicates both successful hashes and source read failures by path."""

    def __init__(self, root):
        self.root = root
        self._map_lock = threading.Lock()
        self._reads = {}
        self._counter_lock = threading.Lock()
        self.bytes_read = 0

    def get(self, relative_path):
        with self._map_lock:
            read = self._reads.get(relative_path)
            if read is None:
                read = _SourceRead()
                self._reads[relative_path] = read

        with read.lock:
            if not read.done:
                digest, detail, count = _regular_file_digest(
                    relative_path, self.root, "source")
                read.digest = digest
                read.detail = detail
                read.bytes_read = count
                read.done = True
                with self._counter_lock:
                    self.bytes_read += count
            return read.digest, read.detail, read.bytes_read


def _path_argument(value, argument_name):
    if isinstance(value, str):
        text = value
    elif isinstance(value, os.PathLike):
        raw = os.fspath(value)
        if not isinstance(raw, str):
            raise ValueError(f"{argument_name} must be a string path")
        text = raw
    else:
        raise ValueError(f"{argument_name} must be a path")
    if "\x00" in text:
        raise ValueError(f"{argument_name} must not contain NUL")
    return text


def _validate_incremental_call(
        plan, targets, sources, outputs, reads, context, state_file,
        workers, cwd):
    if type(workers) is not int or not 1 <= workers <= MAX_INCREMENTAL_WORKERS:
        raise ValueError(
            f"workers must be an integer in [1, {MAX_INCREMENTAL_WORKERS}]")

    if not isinstance(context, str) or not context:
        raise ValueError("context must be a nonempty string")
    try:
        context_size = len(context.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise ValueError("context must be valid UTF-8") from exc
    if context_size > MAX_CONTEXT_BYTES:
        raise ValueError(
            f"context must be at most {MAX_CONTEXT_BYTES} UTF-8 bytes")

    if not isinstance(plan, Mapping):
        raise ValueError("plan must be a name to Step mapping")
    try:
        materialized_plan = dict(plan)
    except (TypeError, ValueError) as exc:
        raise ValueError("plan must be a name to Step mapping") from exc
    if len(materialized_plan) > MAX_INCREMENTAL_STEPS:
        raise ValueError(f"at most {MAX_INCREMENTAL_STEPS} steps per call")

    names, validated_plan = _validate_plan(materialized_plan, workers)
    for name in names:
        if len(validated_plan[name].after) > MAX_AFTER_ITEMS:
            raise ValueError(
                f"{name}: at most {MAX_AFTER_ITEMS} raw after entries")

    if not isinstance(targets, tuple) or not targets:
        raise ValueError("targets must be a nonempty tuple of step names")
    if len(targets) > MAX_TARGETS:
        raise ValueError(f"at most {MAX_TARGETS} targets")
    if any(not isinstance(target, str) or not target for target in targets):
        raise ValueError("targets must contain nonempty step-name strings")
    if len(set(targets)) != len(targets):
        raise ValueError("targets must not contain duplicates")
    for target in targets:
        if target not in validated_plan:
            raise ValueError(f"unknown target step {target!r}")

    for value, label in ((sources, "sources"), (outputs, "outputs"),
                         (reads, "reads")):
        if not isinstance(value, Mapping):
            raise ValueError(f"{label} must be a mapping")

    source_paths = {name: () for name in names}
    output_paths = {name: () for name in names}
    read_refs = {name: () for name in names}
    declaration_items = 0

    for step_name, paths in sources.items():
        if step_name not in validated_plan:
            raise ValueError(f"sources: unknown step {step_name!r}")
        if not isinstance(paths, tuple):
            raise ValueError(f"sources[{step_name!r}] must be a tuple")
        for path in paths:
            if not _valid_relative_workspace_path(path):
                raise ValueError(
                    f"sources[{step_name!r}]: invalid workspace path {path!r}")
        source_paths[step_name] = paths
        declaration_items += len(paths)

    output_owner = {}
    distinct_paths = set()
    for step_name, paths in outputs.items():
        if step_name not in validated_plan:
            raise ValueError(f"outputs: unknown step {step_name!r}")
        if not isinstance(paths, tuple):
            raise ValueError(f"outputs[{step_name!r}] must be a tuple")
        for path in paths:
            if not _valid_relative_workspace_path(path):
                raise ValueError(
                    f"outputs[{step_name!r}]: invalid workspace path {path!r}")
            previous_owner = output_owner.get(path)
            if previous_owner is not None and previous_owner != step_name:
                raise ValueError(
                    f"output path {path!r} is declared by both "
                    f"{previous_owner!r} and {step_name!r}")
            output_owner[path] = step_name
            distinct_paths.add(path)
        output_paths[step_name] = paths
        declaration_items += len(paths)

    for paths in source_paths.values():
        for path in paths:
            if path in output_owner:
                raise ValueError(
                    f"path {path!r} cannot be both source and output")
            distinct_paths.add(path)

    if len(distinct_paths) > MAX_DECLARED_FILE_PATHS:
        raise ValueError(
            f"at most {MAX_DECLARED_FILE_PATHS} distinct declared file paths")

    # A declared path cannot be a directory ancestor of another.  Path component
    # depth is bounded by the 256 UTF-8 byte path limit.
    for path in distinct_paths:
        components = path.split("/")
        prefix_components = []
        for component in components[:-1]:
            prefix_components.append(component)
            if "/".join(prefix_components) in distinct_paths:
                raise ValueError(
                    f"declared path {'/'.join(prefix_components)!r} is an "
                    f"ancestor of {path!r}")

    output_sets = {name: set(output_paths[name]) for name in names}
    for consumer, items in reads.items():
        if consumer not in validated_plan:
            raise ValueError(f"reads: unknown consumer {consumer!r}")
        if not isinstance(items, tuple):
            raise ValueError(f"reads[{consumer!r}] must be a tuple")
        seen = set()
        normalized = []
        for item in items:
            if not isinstance(item, tuple) or len(item) != 2:
                raise ValueError(
                    f"reads[{consumer!r}] entries must be "
                    "(producer_name, output_path) tuples")
            producer, path = item
            if not isinstance(producer, str) or producer not in validated_plan:
                raise ValueError(
                    f"reads[{consumer!r}]: unknown producer {producer!r}")
            if not _valid_relative_workspace_path(path):
                raise ValueError(
                    f"reads[{consumer!r}]: invalid output path {path!r}")
            if path not in output_sets[producer]:
                raise ValueError(
                    f"read {producer!r}/{path!r} does not point to a declared "
                    "output")
            identity = (producer, path)
            if identity in seen:
                raise ValueError(
                    f"reads[{consumer!r}]: duplicate read {identity!r}")
            seen.add(identity)
            normalized.append(identity)
        read_refs[consumer] = tuple(normalized)
        declaration_items += len(items)

    if declaration_items > MAX_DECLARATION_ITEMS:
        raise ValueError(
            f"at most {MAX_DECLARATION_ITEMS} path/read declaration items")

    combined_dependencies = {}
    for name in names:
        combined_dependencies[name] = tuple(dict.fromkeys(
            tuple(validated_plan[name].after)
            + tuple(producer for producer, _path in read_refs[name])
        ))
    try:
        TopologicalSorter(combined_dependencies).prepare()
    except CycleError as exc:
        raise ValueError(f"dependency cycle: {exc.args[1]}") from exc

    state_text = _path_argument(state_file, "state_file")
    cwd_text = None if cwd is None else _path_argument(cwd, "cwd")
    workspace = os.getcwd() if cwd_text is None else os.path.abspath(cwd_text)
    state_path = os.path.abspath(state_text)

    return (
        names, validated_plan, targets, source_paths, output_paths,
        read_refs, combined_dependencies, context, workspace, state_path,
    )


def _validate_loaded_state(records):
    if not isinstance(records, dict):
        raise ValueError("corrupt incremental state: records must be an object")

    for name, record in records.items():
        if not isinstance(name, str) or not name:
            raise ValueError("corrupt incremental state: invalid record name")
        if not isinstance(record, dict):
            raise ValueError(f"corrupt incremental state for {name!r}")
        if set(record) != {"key", "outputs"}:
            raise ValueError(f"corrupt incremental state for {name!r}")

        key = record["key"]
        outputs = record["outputs"]
        if not isinstance(key, dict) or not isinstance(outputs, dict):
            raise ValueError(f"corrupt incremental state for {name!r}")
        if set(key) != {
                "step", "argv", "timeout", "context", "outputs",
                "sources", "reads"}:
            raise ValueError(f"corrupt incremental state for {name!r}")
        if key.get("step") != name:
            raise ValueError(f"corrupt incremental state for {name!r}")

        output_paths = key.get("outputs")
        if not isinstance(output_paths, list) or not all(
                _valid_relative_workspace_path(path) for path in output_paths):
            raise ValueError(f"corrupt incremental state for {name!r}")
        if len(set(output_paths)) != len(output_paths):
            raise ValueError(f"corrupt incremental state for {name!r}")
        if set(outputs) != set(output_paths):
            raise ValueError(f"corrupt incremental state for {name!r}")
        for path, digest in outputs.items():
            if not _valid_relative_workspace_path(path) or not _valid_sha256(digest):
                raise ValueError(f"corrupt incremental state for {name!r}")

        sources = key.get("sources")
        if not isinstance(sources, dict):
            raise ValueError(f"corrupt incremental state for {name!r}")
        for path, digest in sources.items():
            if not _valid_relative_workspace_path(path) or not _valid_sha256(digest):
                raise ValueError(f"corrupt incremental state for {name!r}")

        reads = key.get("reads")
        if not isinstance(reads, list):
            raise ValueError(f"corrupt incremental state for {name!r}")
        for read in reads:
            if not isinstance(read, dict) or set(read) != {
                    "producer", "output", "fingerprint"}:
                raise ValueError(f"corrupt incremental state for {name!r}")
            if (not isinstance(read["producer"], str)
                    or not _valid_relative_workspace_path(read["output"])
                    or not _valid_sha256(read["fingerprint"])):
                raise ValueError(f"corrupt incremental state for {name!r}")

        if not isinstance(key.get("argv"), list):
            raise ValueError(f"corrupt incremental state for {name!r}")
        timeout = key.get("timeout")
        if type(timeout) not in (int, float) or type(timeout) is bool:
            raise ValueError(f"corrupt incremental state for {name!r}")
        if not isinstance(key.get("context"), str):
            raise ValueError(f"corrupt incremental state for {name!r}")


def _load_incremental_state(state_path, workspace):
    if not os.path.exists(state_path):
        return {}

    try:
        with open(state_path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise ValueError("corrupt incremental state file") from exc

    if not isinstance(data, dict):
        raise ValueError("corrupt incremental state file")
    version = data.get("version")
    if type(version) is not int or version != INCREMENTAL_STATE_VERSION:
        raise ValueError(
            f"unsupported incremental state version: {version!r}")
    if data.get("workspace") != workspace:
        raise ValueError(
            "incremental state file belongs to a different workspace")
    records = data.get("records")
    _validate_loaded_state(records)
    return records


def _write_incremental_state(state_path, workspace, records):
    payload = {
        "version": INCREMENTAL_STATE_VERSION,
        "workspace": workspace,
        "records": records,
    }
    parent = os.path.dirname(state_path)
    fd, temporary_path = tempfile.mkstemp(
        prefix=".incremental-state-", suffix=".tmp", dir=parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, state_path)
    except BaseException:
        try:
            os.unlink(temporary_path)
        except OSError:
            pass
        raise


def _prepare_declared_outputs(root, relative_paths):
    for relative_path in relative_paths:
        parent_relative = relative_path.rsplit("/", 1)[0] if "/" in relative_path else ""
        if parent_relative:
            parent_fs = _workspace_fs_path(root, parent_relative)
            try:
                os.makedirs(parent_fs, exist_ok=True)
            except OSError as exc:
                return (
                    f"output path {relative_path!r}: cannot prepare parent "
                    f"directory: {exc}")

        fs_path = _workspace_fs_path(root, relative_path)
        try:
            info = os.lstat(fs_path)
        except FileNotFoundError:
            continue
        except OSError as exc:
            return f"output path {relative_path!r}: {exc}"

        if not stat_mod.S_ISREG(info.st_mode):
            return (
                f"output path {relative_path!r}: existing path is not a "
                "regular file")
        try:
            os.remove(fs_path)
        except OSError as exc:
            return f"output path {relative_path!r}: cannot remove old file: {exc}"
    return ""


def _run_incremental_step(
        name, step, *, root, launch_cwd, source_paths, output_paths, context,
        read_fingerprints, old_record, source_cache):
    source_digests = {}
    for relative_path in dict.fromkeys(source_paths):
        digest, detail, _count = source_cache.get(relative_path)
        if detail is not None:
            return Outcome("input_error", detail=detail), {}, None
        source_digests[relative_path] = digest

    declared_outputs = tuple(dict.fromkeys(output_paths))
    key = {
        "step": name,
        "argv": list(step.argv),
        "timeout": step.timeout,
        "context": context,
        "outputs": sorted(declared_outputs),
        "sources": {
            path: source_digests[path]
            for path in sorted(source_digests)
        },
        "reads": sorted(
            read_fingerprints,
            key=lambda item: (item["producer"], item["output"])),
    }

    current_digests = {}
    if declared_outputs and old_record is not None and old_record.get("key") == key:
        expected_outputs = old_record.get("outputs", {})
        reusable = True
        for relative_path in declared_outputs:
            digest, detail, _count = _regular_file_digest(
                relative_path, root, "output")
            if (detail is not None
                    or digest != expected_outputs.get(relative_path)):
                reusable = False
                break
            current_digests[relative_path] = digest
        if reusable:
            record = {
                "key": key,
                "outputs": dict(sorted(current_digests.items())),
            }
            return Outcome("reused"), current_digests, record

    preparation_error = _prepare_declared_outputs(root, declared_outputs)
    if preparation_error:
        return Outcome("output_error", detail=preparation_error), {}, None

    command_outcome = _launch(step, launch_cwd)
    if command_outcome.status != "ok":
        return command_outcome, {}, None

    if not declared_outputs:
        return command_outcome, {}, None

    current_digests = {}
    for relative_path in declared_outputs:
        digest, detail, _count = _regular_file_digest(
            relative_path, root, "declared output")
        if detail is not None:
            return Outcome(
                "output_error", returncode=0, detail=detail), {}, None
        current_digests[relative_path] = digest

    record = {
        "key": key,
        "outputs": dict(sorted(current_digests.items())),
    }
    return Outcome("ok", returncode=0), current_digests, record


def execute_incremental(
        plan, *, targets, sources, outputs, reads, context, state_file,
        workers=2, cwd=None) -> IncrementalResult:
    """Execute only a target closure with content-based reuse.

    Current-round dependency outcomes gate execution independently from the
    persistent content/addressed reuse key.  State is loaded and validated
    before any subprocess starts and is atomically committed once on return.
    """
    (names, validated_plan, target_tuple, source_paths, output_paths,
     read_refs, dependencies, context, workspace, state_path) = (
        _validate_incremental_call(
            plan, targets, sources, outputs, reads, context, state_file,
            workers, cwd))

    # State corruption/workspace mismatch is also reported before side effects.
    old_records = _load_incremental_state(state_path, workspace)

    closure = set()
    stack = list(target_tuple)
    while stack:
        name = stack.pop()
        if name in closure:
            continue
        closure.add(name)
        stack.extend(dependencies[name])
    selected_names = [name for name in names if name in closure]

    source_cache = _SourceFingerprintCache(workspace)
    outcomes = {}
    output_digests = {}
    records_to_write = {}
    active = {}
    running = set()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        while len(outcomes) < len(selected_names) or active:
            scheduler_progress = False
            if len(active) < workers:
                for name in selected_names:
                    if name in outcomes or name in running:
                        continue

                    direct_dependencies = dependencies[name]
                    if not all(dep in outcomes for dep in direct_dependencies):
                        continue

                    blockers = tuple(dict.fromkeys(
                        dep for dep in direct_dependencies
                        if outcomes[dep].status not in INCREMENTAL_SUCCESS_STATES
                    ))
                    if blockers:
                        outcomes[name] = Outcome(
                            "skipped", blocked_by=blockers)
                        scheduler_progress = True
                        continue

                    read_fingerprints = []
                    for producer, output_path in read_refs[name]:
                        read_fingerprints.append({
                            "producer": producer,
                            "output": output_path,
                            "fingerprint": output_digests[producer][output_path],
                        })

                    running.add(name)
                    future = pool.submit(
                        _run_incremental_step,
                        name,
                        validated_plan[name],
                        root=workspace,
                        launch_cwd=cwd,
                        source_paths=source_paths[name],
                        output_paths=output_paths[name],
                        context=context,
                        read_fingerprints=read_fingerprints,
                        old_record=old_records.get(name),
                        source_cache=source_cache,
                    )
                    active[future] = name
                    scheduler_progress = True
                    if len(active) >= workers:
                        break

            if active:
                completed, _not_completed = wait(
                    set(active), return_when=FIRST_COMPLETED)
                for future in completed:
                    name = active.pop(future)
                    running.discard(name)
                    outcome, digests, record = future.result()
                    outcomes[name] = outcome
                    if outcome.status in INCREMENTAL_SUCCESS_STATES:
                        output_digests[name] = digests
                        if record is not None:
                            records_to_write[name] = record
            else:
                unfinished = [
                    name for name in selected_names if name not in outcomes]
                if unfinished and not scheduler_progress:
                    # Validation proves the combined graph is acyclic.  This is
                    # a defensive scheduler stall rather than a user graph error.
                    raise RuntimeError(
                        "incremental scheduler stalled; unfinished steps: "
                        + ", ".join(unfinished))

    persisted_records = {}
    for name in names:
        if name in records_to_write:
            persisted_records[name] = records_to_write[name]
        elif name not in closure:
            old_record = old_records.get(name)
            if old_record is not None:
                persisted_records[name] = old_record
        elif outcomes[name].status == "skipped":
            old_record = old_records.get(name)
            if old_record is not None:
                persisted_records[name] = old_record
        # Selected ok steps without outputs have no reusable record; all other
        # terminal statuses are actual failures and lose their old record.

    _write_incremental_state(
        state_path, workspace, persisted_records)

    ordered_outcomes = {
        name: outcomes[name] for name in names if name in closure}
    return IncrementalResult(
        outcomes=ordered_outcomes,
        source_bytes_read=source_cache.bytes_read)
