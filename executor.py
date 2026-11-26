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
try:
    import fcntl
except ImportError:  # The cross-process incremental guarantees target Linux.
    fcntl = None
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

MAX_DISCOVERED_STEPS = 256
MAX_DISCOVERED_TARGETS = 256
MAX_DISCOVERED_WORKERS = 8
MAX_DISCOVERED_NAME_BYTES = 128
MAX_SCAN_MANIFEST_BYTES = 64 * 1024
MAX_SCAN_MANIFEST_ITEMS = 256
DISCOVERY_DEPS_ENV = "BUILD_DEPS_FILE"
INCREMENTAL_STATE_VERSION = 1
INCREMENTAL_SUCCESS_STATES = frozenset(("ok", "reused"))
INCREMENTAL_LOCK_POLL_SECONDS = 0.02
INCREMENTAL_STATE_LOCK_TIMEOUT = 120.0
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


def _valid_discovered_name(name):
    if not isinstance(name, str) or not name or "\x00" in name:
        return False
    try:
        return len(name.encode("utf-8")) <= MAX_DISCOVERED_NAME_BYTES
    except UnicodeEncodeError:
        return False


def _validate_discovery_step_arguments(name, step):
    if (not isinstance(step.argv, tuple) or not step.argv
            or not all(
                isinstance(arg, str) and "\x00" not in arg
                for arg in step.argv)
            or not step.argv[0]):
        raise ValueError(f"{name}: invalid argv")
    if not _timeout_is_finite_positive(step.timeout):
        raise ValueError(f"{name}: timeout must be finite and positive")


def _validate_discovered_call(plan, targets, scans, workers, cwd):
    if (type(workers) is not int
            or not 1 <= workers <= MAX_DISCOVERED_WORKERS):
        raise ValueError(
            "workers must be an integer in [1, "
            f"{MAX_DISCOVERED_WORKERS}] and must not be a bool")

    if not isinstance(plan, Mapping):
        raise ValueError("plan must be a name to Step mapping")
    try:
        materialized_plan = dict(plan)
    except (TypeError, ValueError) as exc:
        raise ValueError("plan must be a name to Step mapping") from exc

    if len(materialized_plan) > MAX_DISCOVERED_STEPS:
        raise ValueError(f"at most {MAX_DISCOVERED_STEPS} steps per call")

    names, validated_plan = _validate_plan(materialized_plan, workers)
    for name in names:
        if not _valid_discovered_name(name):
            raise ValueError(
                f"step name {name!r} must be a nonempty NUL-free string of "
                f"at most {MAX_DISCOVERED_NAME_BYTES} UTF-8 bytes")
        if len(validated_plan[name].after) > MAX_AFTER_ITEMS:
            raise ValueError(
                f"{name}: at most {MAX_AFTER_ITEMS} raw after entries")

    try:
        TopologicalSorter(
            {name: validated_plan[name].after for name in names}).prepare()
    except CycleError as exc:
        raise ValueError(f"static dependency cycle: {exc.args[1]}") from exc

    if not isinstance(targets, tuple) or not targets:
        raise ValueError("targets must be a nonempty tuple of step names")
    if len(targets) > MAX_DISCOVERED_TARGETS:
        raise ValueError(f"at most {MAX_DISCOVERED_TARGETS} targets")
    seen_targets = set()
    for target in targets:
        if not _valid_discovered_name(target):
            raise ValueError("targets must contain valid step-name strings")
        if target not in validated_plan:
            raise ValueError(f"unknown target step {target!r}")
        if target in seen_targets:
            raise ValueError(f"targets must not contain duplicates: {target!r}")
        seen_targets.add(target)

    scan_steps = {}
    if scans is not None:
        if not isinstance(scans, Mapping):
            raise ValueError("scans must be None or a name to Step mapping")
        try:
            scan_items = dict(scans)
        except (TypeError, ValueError) as exc:
            raise ValueError("scans must be a name to Step mapping") from exc

        for scan_name, scan_step in scan_items.items():
            if scan_name not in validated_plan:
                raise ValueError(f"scans: unknown step {scan_name!r}")
            if not isinstance(scan_step, Step):
                raise ValueError(f"scans[{scan_name!r}] must be a Step")
            _validate_discovery_step_arguments(
                f"scan {scan_name!r}", scan_step)
            if (not isinstance(scan_step.after, tuple)
                    or len(scan_step.after) != 0):
                raise ValueError(
                    f"scans[{scan_name!r}]: scan Step.after must be empty")
            scan_steps[scan_name] = scan_step

    cwd_text = None if cwd is None else _path_argument(cwd, "cwd")
    return names, validated_plan, targets, scan_steps, workers, cwd_text


def _read_discovery_manifest(path, known_names):
    """Validate a scan manifest completely before any edge is adopted."""
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        return None, (
            f"scan manifest_missing: {DISCOVERY_DEPS_ENV} file {path} "
            "does not exist")
    except OSError as exc:
        return None, f"scan manifest_stat_error: cannot stat {path}: {exc}"

    mode = info.st_mode
    if stat_mod.S_ISLNK(mode):
        return None, (
            f"scan manifest_symlink: {path} must not be a symbolic link")
    if not stat_mod.S_ISREG(mode):
        return None, (
            f"scan manifest_not_regular: {path} must be a regular file")
    if info.st_size > MAX_SCAN_MANIFEST_BYTES:
        return None, (
            "scan manifest_too_large: manifest is "
            f"{info.st_size} bytes; limit is {MAX_SCAN_MANIFEST_BYTES}")

    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        return None, f"scan manifest_unreadable: {path}: {exc}"

    try:
        with os.fdopen(fd, "rb") as handle:
            data = handle.read(MAX_SCAN_MANIFEST_BYTES + 1)
    except OSError as exc:
        return None, f"scan manifest_unreadable: cannot read {path}: {exc}"

    if len(data) > MAX_SCAN_MANIFEST_BYTES:
        return None, (
            "scan manifest_too_large: manifest exceeds "
            f"{MAX_SCAN_MANIFEST_BYTES} bytes")

    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        return None, f"scan manifest_not_utf8: {exc}"

    try:
        manifest = json.loads(text)
    except (json.JSONDecodeError, ValueError) as exc:
        return None, f"scan manifest_invalid_json: {exc}"

    if not isinstance(manifest, list):
        return None, "scan manifest_not_array: manifest must be a JSON array"
    if len(manifest) > MAX_SCAN_MANIFEST_ITEMS:
        return None, (
            "scan manifest_too_many_items: manifest has "
            f"{len(manifest)} entries; limit is {MAX_SCAN_MANIFEST_ITEMS}")

    dependencies = []
    seen = set()
    for item in manifest:
        if not isinstance(item, str) or item not in known_names:
            return None, (
                f"scan manifest_unknown_name: {item!r} is not a known step")
        if item in seen:
            return None, (
                f"scan manifest_duplicate_name: {item!r} appears more than once")
        seen.add(item)
        dependencies.append(item)

    return tuple(dependencies), None


def _run_discovery_scan(step, cwd, deps_path, known_names):
    env = os.environ.copy()
    env[DISCOVERY_DEPS_ENV] = deps_path
    try:
        proc = subprocess.run(
            step.argv,
            cwd=cwd,
            env=env,
            timeout=step.timeout,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return Outcome(
            "scan_error",
            detail=f"scan timeout: exceeded {step.timeout!r} seconds"), None
    except OSError as exc:
        return Outcome(
            "scan_error",
            detail=f"scan launch_error: {exc}"), None

    if proc.returncode != 0:
        return Outcome(
            "scan_error",
            returncode=proc.returncode,
            detail=(
                "scan process_nonzero_exit: scan exited with returncode "
                f"{proc.returncode}")), None

    dependencies, detail = _read_discovery_manifest(deps_path, known_names)
    if detail is not None:
        return Outcome("scan_error", returncode=0, detail=detail), None
    return None, dependencies


def _launch_discovered_build(step, cwd):
    # The discovery path is protocol state for scans only.  Copy the inherited
    # environment so an ambient value cannot leak an executor-owned path into a
    # build; all other environment variables remain inherited.
    env = os.environ.copy()
    env.pop(DISCOVERY_DEPS_ENV, None)
    return _launch_env(step, cwd, env)


def _cyclic_vertices(graph):
    """Return every vertex in a directed cycle, including self cycles."""
    indices = {}
    low_links = {}
    stack = []
    on_stack = set()
    next_index = 0
    cyclic = set()

    def visit(vertex):
        nonlocal next_index
        indices[vertex] = next_index
        low_links[vertex] = next_index
        next_index += 1
        stack.append(vertex)
        on_stack.add(vertex)

        for neighbor in graph.get(vertex, ()):
            if neighbor not in indices:
                visit(neighbor)
                low_links[vertex] = min(
                    low_links[vertex], low_links[neighbor])
            elif neighbor in on_stack:
                low_links[vertex] = min(
                    low_links[vertex], indices[neighbor])

        if low_links[vertex] != indices[vertex]:
            return

        component = []
        while True:
            current = stack.pop()
            on_stack.remove(current)
            component.append(current)
            if current == vertex:
                break

        if len(component) > 1:
            cyclic.update(component)
        elif component[0] in graph.get(component[0], ()):
            cyclic.add(component[0])

    for vertex in graph:
        if vertex not in indices:
            visit(vertex)
    return cyclic


def execute_discovered(plan, *, targets, scans=None, workers=2, cwd=None):
    """Execute target closures with scan-discovered dependency edges.

    Each selected step's static ``after`` edges are always used.  A step named
    in ``scans`` is probed exactly once as soon as it is selected; its JSON
    manifest is adopted only when the complete manifest is valid.  Scans and
    builds share the ``workers`` concurrency limit, while dependency waits do
    not occupy a slot.  Runtime manifest cycles are reported as ``cycle`` and
    never launch their builds; independent branches continue to run.
    """
    names, validated_plan, target_tuple, scan_steps, workers, cwd_text = (
        _validate_discovered_call(plan, targets, scans, workers, cwd))

    scan_root = os.path.abspath(
        tempfile.mkdtemp(prefix="executor-discovered-"))
    try:
        selected = set()

        def select_static_closure(initial_names):
            stack = list(initial_names)
            while stack:
                name = stack.pop()
                if name in selected:
                    continue
                selected.add(name)
                stack.extend(validated_plan[name].after)

        select_static_closure(target_tuple)

        # Valid scan manifests, keyed by the scanned step.  A missing key means
        # that step's manifest is not yet final; a scan failure is in outcomes.
        manifests = {}
        outcomes = {}
        scans_started = set()
        builds_started = set()
        active = {}

        def direct_dependencies(name):
            return tuple(dict.fromkeys(
                tuple(validated_plan[name].after)
                + manifests.get(name, ())))

        def discovery_settled():
            for name in selected:
                if (name in scan_steps and name not in outcomes
                        and name not in manifests):
                    return False
            return True

        def classify_final_cycles_and_skips():
            if not discovery_settled():
                return

            scan_failures = {
                name for name, outcome in outcomes.items()
                if outcome.status == "scan_error"
            }
            graph = {}
            for name in names:
                if name not in selected or name in scan_failures:
                    continue
                graph[name] = tuple(
                    dep for dep in direct_dependencies(name)
                    if dep not in scan_failures)

            for name in _cyclic_vertices(graph):
                # No cyclic build can have been admitted because admission
                # requires every direct dependency to already be terminal ok.
                if name not in outcomes:
                    outcomes[name] = Outcome("cycle")

            # Cascade skips after cycle classification.  Outcomes are terminal,
            # so this reaches a fixed point in at most len(names) passes.
            changed = True
            while changed:
                changed = False
                for name in names:
                    if (name not in selected or name in outcomes
                            or name in builds_started):
                        continue
                    if name in scan_steps and name not in manifests:
                        continue
                    deps = direct_dependencies(name)
                    if not all(dep in outcomes for dep in deps):
                        continue
                    blockers = tuple(dict.fromkeys(
                        dep for dep in deps
                        if outcomes[dep].status != "ok"))
                    if blockers:
                        outcomes[name] = Outcome(
                            "skipped", blocked_by=blockers)
                        changed = True

        def launch_available(pool):
            free_slots = workers - len(active)
            if free_slots <= 0:
                return

            for name in names:
                if free_slots <= 0:
                    break
                if name not in selected or name in outcomes:
                    continue

                scan_step = scan_steps.get(name)
                if scan_step is not None and name not in scans_started:
                    scan_dir = tempfile.mkdtemp(
                        prefix="scan-", dir=scan_root)
                    deps_path = os.path.join(
                        scan_dir, f"{DISCOVERY_DEPS_ENV.lower()}.json")
                    future = pool.submit(
                        _run_discovery_scan,
                        scan_step,
                        cwd_text,
                        deps_path,
                        frozenset(names),
                    )
                    active[future] = ("scan", name)
                    scans_started.add(name)
                    free_slots -= 1
                    continue

                if name in builds_started:
                    continue
                if scan_step is not None and name not in manifests:
                    # Its scan is active, failed into outcomes, or about to be
                    # started; never build before the manifest is final.
                    continue

                deps = direct_dependencies(name)
                if not all(dep in outcomes for dep in deps):
                    continue
                if any(outcomes[dep].status != "ok" for dep in deps):
                    # Final cycle/skip propagation assigns the precise terminal
                    # state once global discovery has settled.
                    continue

                builds_started.add(name)
                future = pool.submit(
                    _launch_discovered_build,
                    validated_plan[name],
                    cwd_text,
                )
                active[future] = ("build", name)
                free_slots -= 1

        with ThreadPoolExecutor(max_workers=workers) as pool:
            while True:
                classify_final_cycles_and_skips()
                launch_available(pool)

                if not active:
                    unfinished = [
                        name for name in selected if name not in outcomes]
                    if unfinished:
                        raise RuntimeError(
                            "execute_discovered scheduler stalled; unfinished "
                            "steps: " + ", ".join(unfinished))
                    break

                completed, _not_completed = wait(
                    set(active), return_when=FIRST_COMPLETED)
                for future in completed:
                    kind, name = active.pop(future)
                    result = future.result()

                    if kind == "scan":
                        scan_outcome, dependencies = result
                        if dependencies is None:
                            outcomes[name] = scan_outcome
                            continue

                        # The manifest was wholly validated before adoption.
                        manifests[name] = dependencies
                        select_static_closure(dependencies)
                    else:
                        outcomes[name] = result

        return {
            name: outcomes[name]
            for name in names
            if name in selected
        }
    finally:
        shutil.rmtree(scan_root, ignore_errors=True)


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


def _incremental_lock_directory(state_path):
    # A stable sidecar directory keeps all ownership files next to the state.
    # Lock files may safely remain after a run; flock semantics make them
    # non-owners when no process has them locked.
    return state_path + ".locks"


def _ensure_incremental_lock_directory(state_path):
    lock_directory = _incremental_lock_directory(state_path)
    os.makedirs(lock_directory, mode=0o700, exist_ok=True)
    return lock_directory


def _incremental_state_lock_path(state_path):
    return os.path.join(
        _incremental_lock_directory(state_path), "state.lock")


def _incremental_step_lock_path(state_path, step_name):
    name_digest = hashlib.sha256(os.fsencode(step_name)).hexdigest()
    return os.path.join(
        _incremental_lock_directory(state_path), f"step-{name_digest}.lock")


class _IncrementalFileLock:
    """Exclusive cross-process lock with a bounded non-blocking poll.

    The lock descriptor can be inherited by the trusted build command via
    ``pass_fds``.  Consequently, if the Python caller is killed while the
    build process is still alive, the kernel retains the flock until that
    inherited descriptor is closed.  A waiter that acquires the lock can then
    be certain no process from the prior ownership generation still holds it
    open for writing.
    """

    def __init__(self, path, *, timeout, description):
        self.path = path
        self.timeout = timeout
        self.description = description
        self.fd = None

    def __enter__(self):
        if fcntl is None:
            # The new cross-process guarantee is Linux-specific.  Preserve the
            # old in-process behavior on platforms without fcntl rather than
            # making the incremental entry unusable there.
            return None
        parent = os.path.dirname(self.path)
        os.makedirs(parent, mode=0o700, exist_ok=True)
        self.fd = os.open(
            self.path,
            os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0),
            0o600,
        )
        deadline = time.monotonic() + self.timeout
        try:
            while True:
                try:
                    fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    return self.fd
                except BlockingIOError as exc:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError(
                            f"timed out after {self.timeout:.1f}s waiting for "
                            f"{self.description} lock {self.path!r}; the previous "
                            "owner or a build command that inherited the lock may "
                            "still be active"
                        ) from exc
                    time.sleep(min(
                        INCREMENTAL_LOCK_POLL_SECONDS, max(0.0, remaining)))
        except BaseException:
            self._close_fd()
            raise

    def _close_fd(self):
        if self.fd is not None:
            try:
                if fcntl is not None:
                    fcntl.flock(self.fd, fcntl.LOCK_UN)
            except OSError:
                pass
            try:
                os.close(self.fd)
            except OSError:
                pass
            self.fd = None

    def __exit__(self, exc_type, exc, traceback):
        self._close_fd()
        return False


def _locked_incremental_state_update(state_path, workspace, mutate):
    """Reload, mutate, and atomically write state while holding its lock."""
    lock_path = _incremental_state_lock_path(state_path)
    with _IncrementalFileLock(
            lock_path,
            timeout=INCREMENTAL_STATE_LOCK_TIMEOUT,
            description="incremental state commit"):
        records = _load_incremental_state(state_path, workspace)
        mutate(records)
        _write_incremental_state(state_path, workspace, records)
        return records


def _commit_incremental_step_record(
        state_path, workspace, step_name, record):
    def mutate(records):
        if record is None:
            records.pop(step_name, None)
        else:
            records[step_name] = record

    _locked_incremental_state_update(state_path, workspace, mutate)


def _run_incremental_build_command(step, launch_cwd, lock_fd):
    """Run one incremental command while optionally leaking the lease fd.

    ``pass_fds`` intentionally gives the build process a reference to the
    step's flock open-file description.  Descendants that inherit descriptors
    extend the lease only while they remain alive; trusted builds are required
    not to leave undeclared background writers.
    """
    pass_fds = () if lock_fd is None else (lock_fd,)
    try:
        process = subprocess.Popen(
            step.argv,
            cwd=launch_cwd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            pass_fds=pass_fds,
        )
    except OSError as exc:
        return Outcome("launch_error", detail=str(exc))

    try:
        try:
            returncode = process.wait(timeout=step.timeout)
        except subprocess.TimeoutExpired:
            try:
                process.kill()
            except OSError:
                pass
            try:
                process.wait()
            except OSError:
                pass
            return Outcome("timeout")
        except OSError as exc:
            return Outcome("launch_error", detail=str(exc))

        if returncode != 0:
            return Outcome("failed", returncode)
        return Outcome("ok", returncode=0)
    except BaseException:
        # Match subprocess.run's broad cleanup: ordinary interpreter errors
        # must not leave this direct child running.  A SIGKILL of the Python
        # process cannot execute this block; the inherited flock then protects
        # outputs until the orphaned trusted command exits.
        if process.poll() is None:
            try:
                process.kill()
            except OSError:
                pass
            try:
                process.wait()
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


def _incremental_step_lock_timeout(step):
    # Cover the holder's own bounded wait plus process/JSON replacement overhead.
    # The common acceptance paths hand off much sooner; this is only a diagnostic
    # upper bound rather than a sleep used to infer lock state.
    try:
        return max(60.0, float(step.timeout) + 30.0)
    except (TypeError, ValueError, OverflowError):
        return 60.0


def _run_incremental_step_body(
        name, step, *, root, launch_cwd, source_paths, output_paths, context,
        read_fingerprints, source_cache, state_path, lock_fd):
    declared_outputs = tuple(dict.fromkeys(output_paths))

    def failure_result(outcome):
        if declared_outputs:
            # Remove any stale success generation before releasing ownership.
            # A later process can therefore never inherit this attempt's
            # failure as though it were a committed reusable result.
            _commit_incremental_step_record(
                state_path, root, name, None)
        return outcome, {}, None

    source_digests = {}
    for relative_path in dict.fromkeys(source_paths):
        digest, detail, _count = source_cache.get(relative_path)
        if detail is not None:
            return failure_result(Outcome("input_error", detail=detail))
        source_digests[relative_path] = digest

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
    if declared_outputs:
        # The lock acquisition happens-after the previous owner's commit and
        # happens-before this attempt's cleanup/launch.  Reload rather than
        # using the call-start snapshot to observe cross-process records.
        latest_records = _load_incremental_state(state_path, root)
        latest_record = latest_records.get(name)
        if latest_record is not None and latest_record.get("key") == key:
            expected_outputs = latest_record.get("outputs", {})
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
        return failure_result(Outcome(
            "output_error", detail=preparation_error))

    command_outcome = _run_incremental_build_command(
        step, launch_cwd, lock_fd)
    if command_outcome.status != "ok":
        return failure_result(command_outcome)

    if not declared_outputs:
        return command_outcome, {}, None

    current_digests = {}
    for relative_path in declared_outputs:
        digest, detail, _count = _regular_file_digest(
            relative_path, root, "declared output")
        if detail is not None:
            return failure_result(Outcome(
                "output_error", returncode=0, detail=detail))
        current_digests[relative_path] = digest

    record = {
        "key": key,
        "outputs": dict(sorted(current_digests.items())),
    }
    # Publish before releasing the per-step flock.  This makes the successful
    # record durable independently of when the whole caller invocation returns.
    _commit_incremental_step_record(state_path, root, name, record)
    return Outcome("ok", returncode=0), current_digests, record


def _run_incremental_step(
        name, step, *, root, launch_cwd, source_paths, output_paths, context,
        read_fingerprints, source_cache, state_path):
    declared_outputs = tuple(dict.fromkeys(output_paths))
    if not declared_outputs:
        return _run_incremental_step_body(
            name, step, root=root, launch_cwd=launch_cwd,
            source_paths=source_paths, output_paths=output_paths,
            context=context, read_fingerprints=read_fingerprints,
            source_cache=source_cache, state_path=state_path, lock_fd=None)

    lock_path = _incremental_step_lock_path(state_path, name)
    with _IncrementalFileLock(
            lock_path,
            timeout=_incremental_step_lock_timeout(step),
            description=f"incremental output step {name!r}") as lock_fd:
        return _run_incremental_step_body(
            name, step, root=root, launch_cwd=launch_cwd,
            source_paths=source_paths, output_paths=output_paths,
            context=context, read_fingerprints=read_fingerprints,
            source_cache=source_cache, state_path=state_path,
            lock_fd=lock_fd)


def execute_incremental(
        plan, *, targets, sources, outputs, reads, context, state_file,
        workers=2, cwd=None) -> IncrementalResult:
    """Execute only a target closure with content-based reuse.

    Current-round dependency outcomes gate execution independently from the
    persistent content/addressed reuse key.  State is loaded and validated
    before any subprocess starts.  Valid output records are merged and
    committed as soon as their step succeeds, followed by a final locked
    merge before this call returns.
    """
    (names, validated_plan, target_tuple, source_paths, output_paths,
     read_refs, dependencies, context, workspace, state_path) = (
        _validate_incremental_call(
            plan, targets, sources, outputs, reads, context, state_file,
            workers, cwd))

    # State corruption/workspace mismatch is also reported before side effects.
    # The returned snapshot is not used for decisions: output steps reload the
    # newest state under their cross-process ownership lock.
    _load_incremental_state(state_path, workspace)
    _ensure_incremental_lock_directory(state_path)

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
                        source_cache=source_cache,
                        state_path=state_path,
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
                    outcome, digests, _record = future.result()
                    outcomes[name] = outcome
                    # The record itself was already committed under the step
                    # lock; only digests are needed to build read fingerprints.
                    if outcome.status in INCREMENTAL_SUCCESS_STATES:
                        output_digests[name] = digests
            else:
                unfinished = [
                    name for name in selected_names if name not in outcomes]
                if unfinished and not scheduler_progress:
                    # Validation proves the combined graph is acyclic.  This is
                    # a defensive scheduler stall rather than a user graph error.
                    raise RuntimeError(
                        "incremental scheduler stalled; unfinished steps: "
                        + ", ".join(unfinished))

    def merge_final_records(latest_records):
        # Commit only the newest on-disk generation.  Every local success or
        # failure has already published its generation under the per-step lock;
        # do not fall back to this call's stale in-memory record, because a
        # later cross-process attempt may have legitimately deleted it.
        persisted_records = {}
        for name in names:
            if output_paths[name] and name in latest_records:
                persisted_records[name] = latest_records[name]

        latest_records.clear()
        latest_records.update(persisted_records)

    _locked_incremental_state_update(
        state_path, workspace, merge_final_records)

    ordered_outcomes = {
        name: outcomes[name] for name in names if name in closure}
    return IncrementalResult(
        outcomes=ordered_outcomes,
        source_bytes_read=source_cache.bytes_read)


# ---------------------------------------------------------------------------
# execute_release: two-recipe cohorts with an ABI probe and atomic delivery.
# ---------------------------------------------------------------------------

MAX_RELEASE_COHORTS = 8
MAX_RELEASE_STEPS = 256
MAX_RELEASE_WORKERS = 8
MAX_RELEASE_AFTER_ITEMS = 256
MAX_RELEASE_COHORT_NAME_BYTES = 32
MAX_RELEASE_NAME_BYTES = 128
MAX_BUNDLE_BYTES = 512 * 1024 * 1024
RELEASE_SHARED_DIR_ENV = "BUILD_RELEASE_SHARED_DIR"
RELEASE_INCOMPATIBLE_RETURNCODE = 42
RELEASE_SCRATCH_PREFIX = "executor-release-"
RELEASE_LOCK_NAME = ".executor-release.lock"
RELEASE_STAGE_PREFIX = ".bundle-stage-"


@dataclass(frozen=True)
class ReleaseCohort:
    """The two recipes for one platform plus its probe/package step names.

    ``primary`` and ``fallback`` are name -> Step mappings that may contain
    different ordinary steps and dependencies.  ``probe`` and ``package`` must
    name two distinct steps present in both mappings.
    """

    primary: dict
    fallback: dict
    probe: str
    package: str


@dataclass(frozen=True)
class ReleaseResult:
    """Final verdict for one cohort, in the order of the input mapping."""

    status: str
    published_profile: Optional[str]
    probes: dict
    detail: str


def _valid_release_name(name):
    if not isinstance(name, str) or not name or "\x00" in name:
        return False
    try:
        return len(name.encode("utf-8")) <= MAX_RELEASE_NAME_BYTES
    except UnicodeEncodeError:
        return False


def _valid_release_cohort_name(name):
    if not isinstance(name, str) or not name:
        return False
    try:
        encoded = name.encode("ascii")
    except UnicodeEncodeError:
        return False
    if not 1 <= len(encoded) <= MAX_RELEASE_COHORT_NAME_BYTES:
        return False
    return all(
        "a" <= ch <= "z" or "A" <= ch <= "Z" or "0" <= ch <= "9"
        or ch in "-_"
        for ch in name)


def _materialize_mapping(mapping, label):
    if not isinstance(mapping, Mapping):
        raise ValueError(f"{label} must be a name to Step mapping")
    try:
        return dict(mapping)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a name to Step mapping") from exc


def _validate_release_recipe(recipe, probe, package, shared_names, label):
    """Validate one recipe of one cohort, including its full graph."""
    if len(recipe) < 2:
        raise ValueError(f"{label}: a recipe must contain at least probe and package")
    if not _valid_release_name(probe):
        raise ValueError(f"{label}: probe step name must be a valid step name")
    if not _valid_release_name(package):
        raise ValueError(f"{label}: package step name must be a valid step name")
    if probe == package:
        raise ValueError(f"{label}: probe and package must be distinct steps")
    if probe not in recipe:
        raise ValueError(f"{label}: probe step {probe!r} is not in the recipe")
    if package not in recipe:
        raise ValueError(f"{label}: package step {package!r} is not in the recipe")

    for name, step in recipe.items():
        if not _valid_release_name(name):
            raise ValueError(
                f"{label}: step name {name!r} must be a nonempty NUL-free "
                f"string of at most {MAX_RELEASE_NAME_BYTES} UTF-8 bytes")
        if not isinstance(step, Step):
            raise ValueError(f"{label}[{name!r}] must be a Step")
        if (not isinstance(step.argv, tuple) or not step.argv
                or not all(isinstance(arg, str) and "\x00" not in arg
                           for arg in step.argv)
                or not step.argv[0]):
            raise ValueError(f"{label}[{name!r}]: invalid argv")
        if not _timeout_is_finite_positive(step.timeout):
            raise ValueError(
                f"{label}[{name!r}]: timeout must be finite and positive")
        if not isinstance(step.after, tuple):
            raise ValueError(f"{label}[{name!r}]: after must be a tuple")
        if len(step.after) > MAX_RELEASE_AFTER_ITEMS:
            raise ValueError(
                f"{label}[{name!r}]: at most {MAX_RELEASE_AFTER_ITEMS} "
                "after entries")
        for dep in step.after:
            if not isinstance(dep, str):
                raise ValueError(f"{label}[{name!r}]: non-string dependency")
            if dep not in recipe and dep not in shared_names:
                raise ValueError(
                    f"{label}[{name!r}]: unknown dependency {dep!r}")
        if name in shared_names:
            raise ValueError(
                f"{label}[{name!r}]: cohort steps must not share a name with "
                "a shared step")

    # Probe and package are the terminal vertices: nothing may depend on them.
    for name, step in recipe.items():
        if probe in step.after:
            raise ValueError(f"{label}: probe {probe!r} must have no downstream")
        if package in step.after:
            raise ValueError(
                f"{label}: package {package!r} must have no downstream")

    # Every ordinary step must be an ancestor of the probe or the package.
    dependents = {name: [] for name in recipe}
    for name, step in recipe.items():
        for dep in step.after:
            if dep in recipe:
                dependents[dep].append(name)

    def reaches_terminal(start):
        seen = set()
        stack = [start]
        while stack:
            current = stack.pop()
            if current in seen:
                continue
            seen.add(current)
            if current == probe or current == package:
                return True
            stack.extend(dependents[current])
        return False

    for name in recipe:
        if name in (probe, package):
            continue
        if not reaches_terminal(name):
            raise ValueError(
                f"{label}: ordinary step {name!r} is not a prerequisite of "
                "the probe or the package")

    # Validate the recipe graph including shared edges.  Shared steps are
    # represented as source vertices, so cycles within the recipe surface here.
    graph = {
        name: tuple(dep for dep in step.after if dep in recipe)
        for name, step in recipe.items()
    }
    try:
        TopologicalSorter(graph).prepare()
    except CycleError as exc:
        raise ValueError(
            f"{label}: dependency cycle: {exc.args[1]}") from exc


def _validate_release_call(shared, cohorts, release_dir, workers):
    if type(workers) is not int or not 1 <= workers <= MAX_RELEASE_WORKERS:
        raise ValueError(
            f"workers must be an integer in [1, {MAX_RELEASE_WORKERS}] and "
            "must not be a bool")

    if not isinstance(cohorts, Mapping) or not cohorts:
        raise ValueError("cohorts must be a nonempty name to ReleaseCohort mapping")
    cohort_items = list(cohorts.items())
    if len(cohort_items) > MAX_RELEASE_COHORTS:
        raise ValueError(f"at most {MAX_RELEASE_COHORTS} cohorts per call")
    cohort_names = []
    for cohort_name, cohort in cohort_items:
        if not _valid_release_cohort_name(cohort_name):
            raise ValueError(
                f"cohort name {cohort_name!r} must be 1.."
                f"{MAX_RELEASE_COHORT_NAME_BYTES} ASCII letters, digits, "
                "'-' or '_'")
        if not isinstance(cohort, ReleaseCohort):
            raise ValueError(
                f"cohorts[{cohort_name!r}] must be a ReleaseCohort")
        cohort_names.append(cohort_name)

    shared_plan = _materialize_mapping(shared, "shared")
    shared_names = set(shared_plan)
    for name, step in shared_plan.items():
        if not _valid_release_name(name):
            raise ValueError(
                f"shared step name {name!r} must be a nonempty NUL-free "
                f"string of at most {MAX_RELEASE_NAME_BYTES} UTF-8 bytes")
        if not isinstance(step, Step):
            raise ValueError(f"shared[{name!r}] must be a Step")
        if (not isinstance(step.argv, tuple) or not step.argv
                or not all(isinstance(arg, str) and "\x00" not in arg
                           for arg in step.argv)
                or not step.argv[0]):
            raise ValueError(f"shared[{name!r}]: invalid argv")
        if not _timeout_is_finite_positive(step.timeout):
            raise ValueError(
                f"shared[{name!r}]: timeout must be finite and positive")
        if not isinstance(step.after, tuple):
            raise ValueError(f"shared[{name!r}]: after must be a tuple")
        if len(step.after) > MAX_RELEASE_AFTER_ITEMS:
            raise ValueError(
                f"shared[{name!r}]: at most {MAX_RELEASE_AFTER_ITEMS} "
                "after entries")
        for dep in step.after:
            if not isinstance(dep, str) or dep not in shared_plan:
                raise ValueError(
                    f"shared[{name!r}]: shared steps may only depend on "
                    f"shared steps; bad dependency {dep!r}")

    try:
        TopologicalSorter(
            {name: shared_plan[name].after for name in shared_plan}).prepare()
    except CycleError as exc:
        raise ValueError(
            f"shared dependency cycle: {exc.args[1]}") from exc

    total_steps = len(shared_plan)
    validated_cohorts = []
    for cohort_name, cohort in cohort_items:
        primary = _materialize_mapping(cohort.primary, f"cohort {cohort_name!r} primary")
        fallback = _materialize_mapping(cohort.fallback, f"cohort {cohort_name!r} fallback")
        primary_label = f"cohort {cohort_name!r} primary"
        fallback_label = f"cohort {cohort_name!r} fallback"
        _validate_release_recipe(
            primary, cohort.probe, cohort.package, shared_names, primary_label)
        _validate_release_recipe(
            fallback, cohort.probe, cohort.package, shared_names, fallback_label)
        total_steps += len(primary) + len(fallback)
        validated_cohorts.append((cohort_name, cohort, primary, fallback))

    if total_steps > MAX_RELEASE_STEPS:
        raise ValueError(
            f"shared steps plus both recipes must total at most "
            f"{MAX_RELEASE_STEPS} steps (declared {total_steps})")

    release_path = _path_argument(release_dir, "release_dir")
    if not os.path.isdir(release_path):
        raise ValueError("release_dir must be an existing directory")

    return shared_plan, validated_cohorts, os.path.abspath(release_path)


def _release_identity_label(identity):
    """Human-readable step name for a blocked_by tuple."""
    if identity[0] == "shared":
        return identity[1]
    return identity[2]


def _run_release_step(step, cwd, shared_dir):
    """Run one release command with the shared directory in its environment."""
    env = os.environ.copy()
    env[RELEASE_SHARED_DIR_ENV] = shared_dir
    return _launch_env(step, cwd, env)


def _verify_release_bundle(bundle_path):
    """Return None when bundle is a readable regular file within the limit."""
    try:
        info = os.lstat(bundle_path)
    except OSError as exc:
        return f"bundle stat error: {exc}"
    if stat_mod.S_ISLNK(info.st_mode):
        return "bundle is a symbolic link, not a regular file"
    if not stat_mod.S_ISREG(info.st_mode):
        return "bundle is not a regular file"
    if info.st_size > MAX_BUNDLE_BYTES:
        return (f"bundle is {info.st_size} bytes; limit is "
                f"{MAX_BUNDLE_BYTES} bytes")
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(bundle_path, flags)
        try:
            while True:
                chunk = os.read(fd, READ_CHUNK)
                if not chunk:
                    break
        finally:
            os.close(fd)
    except OSError as exc:
        return f"bundle unreadable: {exc}"
    return None


def _stage_and_publish(bundle_path, destination, release_dir):
    """Atomically replace destination with bundle_path's bytes.

    Returns None on success, otherwise an error detail.  All staging happens
    on the destination filesystem so the final switch is one os.replace call.
    """
    fd, stage_path = tempfile.mkstemp(
        prefix=RELEASE_STAGE_PREFIX, dir=release_dir)
    os.close(fd)
    try:
        os.unlink(stage_path)
        try:
            # Same filesystem: hard links give an exact, copy-free staging copy.
            os.link(bundle_path, stage_path)
        except OSError:
            # Cross-device or hard links unsupported: stream a private copy.
            flags_in = os.O_RDONLY
            if hasattr(os, "O_NOFOLLOW"):
                flags_in |= os.O_NOFOLLOW
            src = os.open(bundle_path, flags_in)
            try:
                dst = os.open(
                    stage_path,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    0o600)
                try:
                    while True:
                        chunk = os.read(src, READ_CHUNK)
                        if not chunk:
                            break
                        view = memoryview(chunk)
                        while view:
                            written = os.write(dst, view)
                            view = view[written:]
                finally:
                    os.close(dst)
            finally:
                os.close(src)
        try:
            mode = stat_mod.S_IMODE(os.stat(bundle_path).st_mode)
            os.chmod(stage_path, mode)
        except OSError:
            pass
        os.replace(stage_path, destination)
        return None
    except OSError as exc:
        return f"publish failed: {exc}"
    finally:
        # On success the staged path no longer exists; on any failure make sure
        # no partial staging file is left next to the formal bundles.
        try:
            if os.path.lexists(stage_path):
                os.unlink(stage_path)
        except OSError:
            pass


def execute_release(shared, cohorts, *, release_dir, workers=2):
    """Build every cohort with primary/fallback recipes and an ABI probe.

    Each cohort starts on its primary recipe.  A probe exiting 42 means the
    recipe is ABI-incompatible and the cohort switches, once, to its fallback
    recipe after every already-started primary command has finished.  Any
    other probe failure, build failure, timeout, launch error or failed
    prerequisite is a build error and never triggers the other recipe.  A
    bundle is published to ``release_dir/<cohort>.bundle`` only when the same
    recipe's probe approved (exit 0), its package exited 0, and its bundle is
    a compliant readable regular file; the switch is atomic and an existing
    bundle is left untouched on any failure.
    """
    shared_plan, validated_cohorts, release_path = _validate_release_call(
        shared, cohorts, release_dir, workers)

    lock_fd = None
    scratch_root = None
    try:
        if fcntl is not None:
            lock_path = os.path.join(release_path, RELEASE_LOCK_NAME)
            try:
                lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError as exc:
                    os.close(lock_fd)
                    lock_fd = None
                    raise ValueError(
                        "another release build is already running against "
                        f"{release_path}") from exc
            except OSError as exc:
                if isinstance(exc, ValueError):
                    raise
                raise ValueError(
                    f"cannot acquire release lock {lock_path}: {exc}") from exc

        # Only create scratch space once the call is fully committed, so a
        # rejected concurrent run leaves no temp directory behind.
        scratch_root = tempfile.mkdtemp(prefix=RELEASE_SCRATCH_PREFIX)
        shared_dir = os.path.join(scratch_root, "shared")
        os.mkdir(shared_dir)

        cohort_names = [item[0] for item in validated_cohorts]

        # Node bookkeeping.  Identity tuple kinds:
        #   ("shared", name)
        #   (cohort_name, profile, name)
        node_step = {}
        node_cwd = {}
        node_deps = {}
        node_state = {}
        node_outcome = {}
        selected_shared = set()
        cohort_state = {}
        cohort_recipes = {}
        primary_nodes_by_cohort = {}

        for name, step in shared_plan.items():
            identity = ("shared", name)
            node_step[identity] = step
            node_cwd[identity] = shared_dir
            node_deps[identity] = [
                ("shared", dep) for dep in step.after]
            node_state[identity] = "pending"

        def shared_ancestors(names, recipe):
            # Collect every shared step reachable through after-edges, starting
            # from the recipe's direct shared dependencies and then following
            # the shared graph itself (including shared-only chains).
            result = set()
            stack = []
            for step_name in names:
                for dep in recipe[step_name].after:
                    if dep in shared_plan:
                        stack.append(dep)
            while stack:
                current = stack.pop()
                if current in result:
                    continue
                result.add(current)
                stack.extend(
                    dep for dep in shared_plan[current].after
                    if dep in shared_plan)
            return result

        for cohort_name, cohort, primary, fallback in validated_cohorts:
            cohort_dir = os.path.join(scratch_root, cohort_name)
            os.mkdir(cohort_dir)
            cohort_recipes[cohort_name] = (cohort, primary, fallback)
            primary_nodes_by_cohort[cohort_name] = set()

            for profile, recipe in (("primary", primary),
                                    ("fallback", fallback)):
                profile_dir = os.path.join(cohort_dir, profile)
                os.mkdir(profile_dir)
                for step_name, step in recipe.items():
                    identity = (cohort_name, profile, step_name)
                    node_step[identity] = step
                    node_cwd[identity] = profile_dir
                    node_deps[identity] = []
                    for dep in step.after:
                        if dep in shared_plan:
                            node_deps[identity].append(("shared", dep))
                        else:
                            node_deps[identity].append(
                                (cohort_name, profile, dep))
                    node_state[identity] = "pending"

            # The primary recipe's shared closure is needed from the start;
            # additional fallback-only shared steps are selected on switch.
            selected_shared.update(shared_ancestors(primary.keys(), primary))
            for step_name in primary:
                primary_nodes_by_cohort[cohort_name].add(
                    (cohort_name, "primary", step_name))
            cohort_state[cohort_name] = {
                "phase": "primary",            # primary / switching / fallback / done
                "probe": cohort.probe,
                "package": cohort.package,
                "probe_outcomes": {},
                "publish_attempted": {"primary": False, "fallback": False},
                "status": None,
                "profile": None,
                "detail": "",
            }

        def failure_detail(state, cohort_name, probe_outcome, profile):
            if state["detail"]:
                return state["detail"]
            package_id = (cohort_name, profile, state["package"])
            package_outcome = node_outcome.get(package_id)
            if package_outcome is not None and package_outcome.status != "ok":
                suffix = f": {package_outcome.detail}" \
                    if package_outcome.detail else ""
                return f"{profile} package {package_outcome.status}{suffix}"
            if probe_outcome is None:
                return f"{profile} probe did not run"
            if probe_outcome.status == "skipped":
                blockers = ", ".join(probe_outcome.blocked_by) \
                    or "unspecified"
                return (f"{profile} probe blocked by failed prerequisite(s): "
                        f"{blockers}")
            suffix = f": {probe_outcome.detail}" if probe_outcome.detail else ""
            return f"{profile} probe {probe_outcome.status}{suffix}"

        active = {}
        with ThreadPoolExecutor(max_workers=workers) as pool:
            while True:
                made_progress = False

                # 1. State transitions for cohorts.
                for cohort_name in cohort_names:
                    state = cohort_state[cohort_name]
                    if state["status"] is not None:
                        continue
                    _, primary, fallback = cohort_recipes[cohort_name]

                    if state["phase"] == "primary":
                        primary_terminal = all(
                            node_state[identity] == "terminal"
                            for identity in primary_nodes_by_cohort[cohort_name])
                        if primary_terminal:
                            probe_id = (cohort_name, "primary", state["probe"])
                            probe_outcome = node_outcome[probe_id]
                            state["probe_outcomes"]["primary"] = probe_outcome
                            # Only a probe that actually ran and exited 42 is an
                            # ABI verdict; skipped/timeout/launch probes are build
                            # errors that must not activate the fallback recipe.
                            if probe_outcome.status == "failed" \
                                    and probe_outcome.returncode == \
                                    RELEASE_INCOMPATIBLE_RETURNCODE:
                                state["phase"] = "switching"
                            else:
                                state["phase"] = "done"

                    if state["phase"] == "switching":
                        # Every already-started primary command must have been
                        # reaped before a fallback command may launch.
                        primary_running = any(
                            node_state[identity] == "running"
                            for identity in primary_nodes_by_cohort[cohort_name])
                        if not primary_running:
                            added = shared_ancestors(fallback.keys(), fallback)
                            selected_shared.update(added)
                            state["phase"] = "fallback"
                            made_progress = True

                    if state["phase"] == "fallback":
                        fallback_terminal = all(
                            node_state[(cohort_name, "fallback", step_name)]
                            == "terminal"
                            for step_name in fallback)
                        if fallback_terminal:
                            probe_id = (
                                cohort_name, "fallback", state["probe"])
                            state["probe_outcomes"]["fallback"] = \
                                node_outcome[probe_id]
                            state["phase"] = "done"

                # 2. Cascade skips for pending nodes whose deps are terminal.
                for identity, status in list(node_state.items()):
                    if status != "pending":
                        continue
                    kind = identity[0]
                    if kind == "shared":
                        if identity[1] not in selected_shared:
                            continue
                    else:
                        owner, profile, _ = identity
                        if profile == "fallback" and \
                                cohort_state[owner]["phase"] != "fallback":
                            # A fallback recipe is only live while its cohort
                            # is actively running it.  Before a switch it is
                            # simply dormant; after primary success or a primary
                            # failure the cohort never enters the fallback
                            # phase, so these nodes stay pending and are never
                            # run, skipped, or reported as probes.
                            continue
                    deps = node_deps[identity]
                    if not all(node_state[dep_id] == "terminal"
                               for dep_id in deps):
                        continue
                    blockers = tuple(dict.fromkeys(
                        _release_identity_label(dep_id) for dep_id in deps
                        if node_outcome[dep_id].status != "ok"))
                    if blockers:
                        node_outcome[identity] = Outcome(
                            "skipped", blocked_by=blockers)
                        node_state[identity] = "terminal"
                        made_progress = True

                # 3. Publish completed, approved packages.
                for cohort_name in cohort_names:
                    state = cohort_state[cohort_name]
                    if state["status"] is not None:
                        continue
                    _, primary, fallback = cohort_recipes[cohort_name]
                    for profile, recipe in (("primary", primary),
                                            ("fallback", fallback)):
                        if state["publish_attempted"][profile]:
                            continue
                        probe_id = (cohort_name, profile, state["probe"])
                        package_id = (cohort_name, profile, state["package"])
                        if node_state.get(probe_id) != "terminal" \
                                or node_state.get(package_id) != "terminal":
                            continue
                        probe_outcome = node_outcome[probe_id]
                        package_outcome = node_outcome[package_id]
                        state["probe_outcomes"].setdefault(
                            profile, probe_outcome)
                        state["publish_attempted"][profile] = True
                        if probe_outcome.status != "ok":
                            continue
                        if package_outcome.status != "ok":
                            continue
                        bundle_path = os.path.join(
                            node_cwd[package_id], "bundle")
                        detail = _verify_release_bundle(bundle_path)
                        if detail is not None:
                            state["detail"] = detail
                            continue
                        destination = os.path.join(
                            release_path, f"{cohort_name}.bundle")
                        detail = _stage_and_publish(
                            bundle_path, destination, release_path)
                        if detail is not None:
                            state["detail"] = detail
                            continue
                        state["status"] = "published"
                        state["profile"] = profile
                        state["detail"] = ""
                        made_progress = True

                # 4. Finalize cohorts whose enabled profile(s) are terminal.
                for cohort_name in cohort_names:
                    state = cohort_state[cohort_name]
                    if state["status"] is not None:
                        continue
                    if state["phase"] != "done":
                        continue
                    _, primary, fallback = cohort_recipes[cohort_name]
                    primary_probe = state["probe_outcomes"].get("primary")
                    fallback_probe = state["probe_outcomes"].get("fallback")

                    if primary_probe is not None and \
                            primary_probe.status == "failed" and \
                            primary_probe.returncode == \
                            RELEASE_INCOMPATIBLE_RETURNCODE:
                        if fallback_probe is None:
                            continue
                        if fallback_probe.status == "failed" and \
                                fallback_probe.returncode == \
                                RELEASE_INCOMPATIBLE_RETURNCODE:
                            state["status"] = "incompatible"
                            state["detail"] = (
                                "primary and fallback probes both reported "
                                "ABI incompatibility (exit 42)")
                        else:
                            state["status"] = "failed"
                            state["detail"] = failure_detail(
                                state, cohort_name,
                                fallback_probe, "fallback")
                        made_progress = True
                        continue

                    state["status"] = "failed"
                    state["detail"] = failure_detail(
                        state, cohort_name, primary_probe, "primary")
                    made_progress = True

                # 5. Launch ready, required work; candidate packages last.
                # Required work is handed out round-robin across cohorts so no
                # cohort can hoard the shared process slots; shared steps are
                # common prerequisites and go first.  Candidate packages only
                # take slots that no required work can use.
                free_slots = workers - len(active)
                if free_slots > 0:
                    def is_launchable(identity):
                        if node_state[identity] != "pending":
                            return False
                        kind = identity[0]
                        if kind == "shared":
                            if identity[1] not in selected_shared:
                                return False
                        else:
                            owner, profile, _ = identity
                            owner_state = cohort_state[owner]
                            # Fallback commands may launch only while the
                            # cohort is actively running its fallback recipe.
                            if profile == "fallback" and \
                                    owner_state["phase"] != "fallback":
                                return False
                        return all(
                            node_state[dep_id] == "terminal"
                            and node_outcome[dep_id].status == "ok"
                            for dep_id in node_deps[identity])

                    shared_required = []
                    per_cohort_required = {name: [] for name in cohort_names}
                    candidate_packages = []
                    # Snapshot: launching below mutates node_state, which must
                    # not happen while iterating the live dictionary.
                    for identity, status in list(node_state.items()):
                        if status != "pending" or not is_launchable(identity):
                            continue
                        if identity[0] == "shared":
                            shared_required.append(identity)
                        elif identity[2] == cohort_state[identity[0]]["package"]:
                            candidate_packages.append(identity)
                        else:
                            per_cohort_required[identity[0]].append(identity)
                    # Common shared prerequisites go first, then required work
                    # is interleaved round-robin across cohorts so no cohort
                    # can hoard the shared process slots.
                    launch_order = list(shared_required)
                    cohort_queues = [
                        per_cohort_required[name] for name in cohort_names]
                    while any(cohort_queues):
                        for queue in cohort_queues:
                            if queue:
                                launch_order.append(queue.pop(0))
                    # Candidate packages only take slots no required work can.
                    launch_order.extend(candidate_packages)

                    for identity in launch_order:
                        if free_slots <= 0:
                            break
                        if node_state[identity] != "pending":
                            continue
                        node_state[identity] = "running"
                        future = pool.submit(
                            _run_release_step,
                            node_step[identity],
                            node_cwd[identity],
                            shared_dir)
                        active[future] = identity
                        free_slots -= 1
                        made_progress = True

                if not active:
                    if all(cohort_state[name]["status"] is not None
                           for name in cohort_names):
                        break
                    if not made_progress:
                        unfinished = [
                            str(identity)
                            for identity, status in node_state.items()
                            if status != "terminal"]
                        raise RuntimeError(
                            "execute_release scheduler stalled; unfinished "
                            "nodes: " + ", ".join(unfinished))
                    continue

                completed, _not_completed = wait(
                    set(active), return_when=FIRST_COMPLETED)
                for future in completed:
                    identity = active.pop(future)
                    node_outcome[identity] = future.result()
                    node_state[identity] = "terminal"

        results = {}
        for cohort_name in cohort_names:
            state = cohort_state[cohort_name]
            probes = dict(state["probe_outcomes"])
            results[cohort_name] = ReleaseResult(
                status=state["status"],
                published_profile=state["profile"],
                probes=probes,
                detail=state["detail"])
        return results
    finally:
        if lock_fd is not None and fcntl is not None:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            except OSError:
                pass
            os.close(lock_fd)
        if scratch_root is not None:
            shutil.rmtree(scratch_root, ignore_errors=True)
