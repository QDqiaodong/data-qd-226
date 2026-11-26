"""Offline cross-process regression tests for ``execute_incremental``.

Run from the project directory::

    python3 -m unittest -v test_incremental_concurrent

The acceptance scenarios spawn fresh Python interpreters and synchronize all
important states through files.  There are no fixed sleeps used to guess lock
state: tests wait for command markers or for a driver PID to own an open file
descriptor for the exact per-step flock file.
"""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest

PROJECT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT))

from executor import Step, execute_incremental  # noqa: E402

PY = sys.executable
TEST_FILE = PROJECT / "test_incremental_concurrent.py"
CONTEXT = "concurrent-acceptance-v1"
EXPECTED_GEN = b"shared:input-v1"
EXPECTED_ALPHA = b"alpha:" + EXPECTED_GEN
EXPECTED_BETA = b"beta:" + EXPECTED_GEN


BOILERPLATE = r'''
import json
import os
import time
import uuid
from pathlib import Path

def wait_for_file(name, timeout=45):
    path = Path(name)
    deadline = time.monotonic() + timeout
    while not path.exists():
        if time.monotonic() >= deadline:
            raise RuntimeError("timed out waiting for handshake " + name)
        time.sleep(0.01)

def unique_start(step):
    invocation = uuid.uuid4().hex
    Path("start-" + step + "-" + invocation).write_text(str(os.getpid()))
    return invocation

def append_interval(event, invocation):
    record = json.dumps({
        "event": event,
        "invocation": invocation,
        "monotonic_ns": time.monotonic_ns(),
        "pid": os.getpid(),
    }) + "\n"
    fd = os.open(
        "write-intervals.log",
        os.O_WRONLY | os.O_CREAT | os.O_APPEND,
        0o600,
    )
    try:
        os.write(fd, record.encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)
'''


def command(body):
    return Step((PY, "-c", BOILERPLATE + textwrap.dedent(body)), timeout=60)


SHARED_GEN_BODY = r'''
    invocation = unique_start("gen")
    append_interval("start", invocation)
    Path("gen-started").write_text(invocation)
    wait_for_file("release-shared")
    source = Path("input.txt").read_bytes()
    Path("gen.out").write_bytes(b"shared:" + source)
    append_interval("end", invocation)
'''


def downstream_body(input_path, output_path, prefix):
    return f'''
    from pathlib import Path
    data = Path({input_path!r}).read_bytes()
    Path({output_path!r}).write_bytes({prefix!r}.encode("ascii") + data)
    '''


INDEPENDENT_BODY = r'''
    mine = {mine!r}
    other = {other!r}
    invocation = unique_start(mine)
    append_interval("start", invocation)
    Path(mine + "-started").write_text(invocation)
    wait_for_file(other + "-started")
    Path(mine + ".out").write_bytes(mine.encode("ascii") + b"-content-v1\n")
    append_interval("end", invocation)
    Path(mine + "-done").write_text(invocation)
'''.replace("{mine!r}", repr("__MINE__")).replace("{other!r}", repr("__OTHER__"))
# The odd replacement above keeps the executable source explicit and avoids
# accidental str.format interpretation of the boilerplate braces.


def independent_command(mine, other):
    source = INDEPENDENT_BODY.replace("__MINE__", mine).replace(
        "__OTHER__", other)
    return command(source)


FAILURE_GEN_BODY = r'''
    invocation = unique_start("gen")
    try:
        fd = os.open(
            "failure-first",
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            0o600,
        )
        os.close(fd)
        first = True
    except FileExistsError:
        first = False

    if first:
        Path("failure-started").write_text(invocation)
        wait_for_file("failure-release")
        raise SystemExit(7)

    Path("failure-successor-" + invocation).touch()
    Path("gen.out").write_bytes(EXPECTED_GEN)
'''.replace("EXPECTED_GEN", repr(EXPECTED_GEN))


KILL_GEN_BODY = r'''
    invocation = unique_start("gen")
    try:
        fd = os.open(
            "kill-first",
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            0o600,
        )
        os.close(fd)
        old_owner = True
    except FileExistsError:
        old_owner = False

    append_interval("start", invocation)

    if old_owner:
        Path("kill-old-pid").write_text(str(os.getpid()))
        Path("gen.out").write_bytes(b"temporary-partial-content\n")
        Path("kill-temp-written").write_text(invocation)
        wait_for_file("kill-release-old")
        # The still-live orphaned direct command deliberately writes the same
        # declared path again after the Python caller has been killed.
        Path("gen.out").write_bytes(EXPECTED_GEN)
        Path("kill-old-final-written").write_text(invocation)
        append_interval("end", invocation)
        Path("kill-old-finished").write_text(invocation)
    else:
        Path("kill-successor-started").write_text(invocation)
        Path("gen.out").write_bytes(EXPECTED_GEN)
        append_interval("end", invocation)
        Path("kill-successor-finished").write_text(invocation)
'''.replace("EXPECTED_GEN", repr(EXPECTED_GEN))


def shared_plan(gen_body=SHARED_GEN_BODY):
    return {
        "gen": command(gen_body),
        "alpha": command(downstream_body("gen.out", "alpha.out", "alpha:")),
        "beta": command(downstream_body("gen.out", "beta.out", "beta:")),
    }


def independent_plan():
    return {
        "a": independent_command("a", "b"),
        "b": independent_command("b", "a"),
    }


FOUR_NAMES = ("p0", "p1", "p2", "p3")


def four_step(name):
    output_name = f"{name}.out"
    content = f"{name}-content\n"
    code = (
        "from pathlib import Path; "
        f"Path({output_name!r}).write_text({content!r})"
    )
    return Step((PY, "-c", code))


def declarations_for(scenario):
    if scenario == "independent":
        return {
            "sources": {},
            "outputs": {"a": ("a.out",), "b": ("b.out",)},
            "reads": {},
        }
    if scenario == "four":
        return {
            "sources": {},
            "outputs": {
                name: (f"{name}.out",) for name in FOUR_NAMES
            },
            "reads": {},
        }
    if scenario == "nooutput":
        return {"sources": {}, "outputs": {}, "reads": {}}
    return {
        "sources": {"gen": ("input.txt",)},
        "outputs": {"gen": ("gen.out",),
                    "alpha": ("alpha.out",),
                    "beta": ("beta.out",)},
        "reads": {
            "alpha": (("gen", "gen.out"),),
            "beta": (("gen", "gen.out"),),
        },
    }


def run_driver(spec_path):
    spec = json.loads(Path(spec_path).read_text(encoding="utf-8"))
    scenario = spec["scenario"]
    if scenario == "independent":
        plan = independent_plan()
    elif scenario == "four":
        plan = {name: four_step(name) for name in FOUR_NAMES}
    elif scenario == "nooutput":
        plan = {"check": command("invocation = unique_start('check')\n")}
    elif scenario == "failure":
        plan = shared_plan(FAILURE_GEN_BODY)
    elif scenario == "kill":
        plan = shared_plan(KILL_GEN_BODY)
    else:
        plan = shared_plan()

    declarations = declarations_for(scenario)
    try:
        result = execute_incremental(
            plan,
            targets=tuple(spec["targets"]),
            sources=declarations["sources"],
            outputs=declarations["outputs"],
            reads=declarations["reads"],
            context=CONTEXT + ":" + scenario,
            state_file=spec["state"],
            workers=int(spec.get("workers", 2)),
            cwd=spec["root"],
        )
        payload = {
            "ok": True,
            "statuses": {
                name: outcome.status
                for name, outcome in result.outcomes.items()
            },
            "returncodes": {
                name: outcome.returncode
                for name, outcome in result.outcomes.items()
            },
            "blocked_by": {
                name: list(outcome.blocked_by)
                for name, outcome in result.outcomes.items()
            },
            "details": {
                name: outcome.detail
                for name, outcome in result.outcomes.items()
                if outcome.detail
            },
            "source_bytes_read": result.source_bytes_read,
            "pid": os.getpid(),
        }
    except Exception as exc:  # Report subprocess failures to the test.
        import traceback
        payload = {
            "ok": False,
            "error": repr(exc),
            "traceback": traceback.format_exc(),
            "pid": os.getpid(),
        }

    Path(spec["result_path"]).write_text(
        json.dumps(payload, sort_keys=True), encoding="utf-8")


class DriverHandle:
    def __init__(self, label, proc, result_path, log_path):
        self.label = label
        self.proc = proc
        self.result_path = Path(result_path)
        self.log_path = Path(log_path)

    def log_text(self):
        try:
            return self.log_path.read_text(encoding="utf-8", errors="replace")
        except OSError as exc:
            return f"<cannot read log {self.log_path}: {exc}>"


@unittest.skipUnless(
    sys.platform.startswith("linux"),
    "cross-process flock and /proc/fd handoff tests are Linux-specific",
)
class ConcurrentIncrementalTest(unittest.TestCase):
    def setUp(self):
        self._temp = tempfile.TemporaryDirectory(prefix="inc-concurrent-")
        self.base = Path(self._temp.name)
        self.root = self.base / "workspace"
        self.state_dir = self.base / "state"
        self.spec_dir = self.base / "specs"
        self.log_dir = self.base / "logs"
        self.result_dir = self.base / "results"
        for directory in (
                self.root, self.state_dir, self.spec_dir,
                self.log_dir, self.result_dir):
            directory.mkdir()
        self.driver_handles = []
        (self.root / "input.txt").write_bytes(b"input-v1")
        self.state = self.state_dir / "incremental-state.json"

    def tearDown(self):
        for handle in getattr(self, "driver_handles", ()):
            if handle.proc.poll() is None:
                handle.proc.kill()
                try:
                    handle.proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    pass
        self._temp.cleanup()

    def start_driver(self, label, scenario, targets, *, workers=2):
        spec_path = self.spec_dir / f"{label}.json"
        result_path = self.result_dir / f"{label}.json"
        log_path = self.log_dir / f"{label}.log"
        spec = {
            "scenario": scenario,
            "root": str(self.root),
            "state": str(self.state),
            "targets": list(targets),
            "workers": workers,
            "result_path": str(result_path),
        }
        spec_path.write_text(json.dumps(spec), encoding="utf-8")
        log_handle = open(log_path, "w", encoding="utf-8")
        try:
            proc = subprocess.Popen(
                [PY, "-I", str(TEST_FILE), "--driver", str(spec_path)],
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                cwd=str(PROJECT),
            )
        finally:
            log_handle.close()
        handle = DriverHandle(label, proc, result_path, log_path)
        self.driver_handles.append(handle)
        return handle

    def wait_driver(self, handle, timeout=60):
        try:
            returncode = handle.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            handle.proc.kill()
            try:
                handle.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pass
            self.fail(
                f"driver {handle.label!r} pid={handle.proc.pid} timed out; "
                f"log:\n{handle.log_text()}")

        if not handle.result_path.exists():
            self.fail(
                f"driver {handle.label!r} exited {returncode} without a "
                f"result; log:\n{handle.log_text()}")
        payload = json.loads(
            handle.result_path.read_text(encoding="utf-8"))
        if returncode != 0 or not payload.get("ok"):
            self.fail(
                f"driver {handle.label!r} failed rc={returncode}; "
                f"payload={payload!r}; log:\n{handle.log_text()}")
        return payload

    def wait_until(self, description, predicate, *, timeout=30, handles=()):
        deadline = time.monotonic() + timeout
        last_error = None
        while True:
            for handle in handles:
                returncode = handle.proc.poll()
                if returncode is not None:
                    raise AssertionError(
                        f"driver {handle.label!r} exited with "
                        f"{returncode} while waiting for {description}; "
                        f"log:\n{handle.log_text()}")
            try:
                value = predicate()
            except Exception as exc:  # Diagnostics for malformed handshake.
                last_error = exc
                value = False
            if value:
                return value
            if time.monotonic() >= deadline:
                diagnostics = []
                for path in sorted(self.root.iterdir()):
                    diagnostics.append(path.name)
                extra = f"; last_error={last_error!r}" if last_error else ""
                self.fail(
                    f"timed out waiting for {description}{extra}; "
                    f"workspace files={diagnostics}")
            time.sleep(0.01)

    def wait_for_file(self, name, *, timeout=30, handles=()):
        path = self.root / name
        self.wait_until(
            f"file {name!r}", path.exists, timeout=timeout, handles=handles)
        return path

    def step_lock_path(self, step):
        digest = hashlib.sha256(os.fsencode(step)).hexdigest()
        return (
            Path(str(self.state) + ".locks") / f"step-{digest}.lock")

    def pid_has_open_path(self, pid, target_path):
        target = os.path.realpath(os.fspath(target_path))
        fd_dir = f"/proc/{pid}/fd"
        try:
            entries = os.listdir(fd_dir)
        except OSError:
            return False
        for fd in entries:
            try:
                link = os.readlink(os.path.join(fd_dir, fd))
            except OSError:
                continue
            if link == target:
                return True
            # /proc may normalize a deleted/suffixed path; filename plus stat
            # identity is enough in this private temporary directory.
            if os.path.basename(link) == os.path.basename(target):
                try:
                    if os.stat(link).st_ino == os.stat(target).st_ino:
                        return True
                except OSError:
                    pass
        return False

    def wait_for_step_lock_open(self, handle, step, *, timeout=30):
        lock_path = self.step_lock_path(step)
        self.wait_until(
            f"{handle.label} pid {handle.proc.pid} opens {lock_path.name}",
            lambda: self.pid_has_open_path(handle.proc.pid, lock_path),
            timeout=timeout,
            handles=(handle,),
        )

    def gen_start_count(self):
        return len(list(self.root.glob("start-gen-*")))

    def assert_lock_busy(self, step):
        import fcntl

        lock_path = self.step_lock_path(step)
        fd = os.open(lock_path, os.O_RDWR | os.O_CLOEXEC)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return
            self.fail(f"lock for step {step!r} was unexpectedly free")
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            except OSError:
                pass
            os.close(fd)

    def read_intervals(self):
        path = self.root / "write-intervals.log"
        intervals = {}
        if not path.exists():
            return []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            record = json.loads(line)
            invocation = record["invocation"]
            current = intervals.setdefault(invocation, {})
            current[record["event"]] = record["monotonic_ns"]
        result = []
        for invocation, times in intervals.items():
            if "start" in times and "end" in times:
                result.append((times["start"], times["end"], invocation))
        return result

    def assert_no_overlapping_intervals(self):
        intervals = self.read_intervals()
        for left_index, (left_start, left_end, left_id) in enumerate(intervals):
            for right_start, right_end, right_id in intervals[left_index + 1:]:
                overlap = left_start < right_end and right_start < left_end
                self.assertFalse(
                    overlap,
                    f"write intervals overlap: {left_id} and {right_id}: "
                    f"{intervals}")

    def test_shared_generator_two_processes_then_fresh_warm_process(self):
        first = self.start_driver("alpha", "shared", ("alpha",))
        second = self.start_driver("beta", "shared", ("beta",))

        self.wait_for_file("gen-started", handles=(first, second))
        self.wait_until(
            "exactly one generator start marker",
            lambda: self.gen_start_count() == 1,
            handles=(first, second),
        )
        # Both driver PIDs have opened and are contending on the exact lock.
        self.wait_for_step_lock_open(first, "gen")
        self.wait_for_step_lock_open(second, "gen")

        (self.root / "release-shared").touch()
        first_result = self.wait_driver(first)
        second_result = self.wait_driver(second)

        results = [first_result, second_result]
        gen_statuses = [item["statuses"]["gen"] for item in results]
        self.assertEqual(sorted(gen_statuses), ["ok", "reused"], results)
        ok_result = next(
            item for item in results if item["statuses"]["gen"] == "ok")
        reused_result = next(
            item for item in results if item["statuses"]["gen"] == "reused")
        ok_label = "alpha" if first_result is ok_result else "beta"
        reused_label = "beta" if ok_label == "alpha" else "alpha"
        self.assertEqual(
            ok_result["statuses"][ok_label], "ok", ok_result)
        self.assertEqual(
            reused_result["statuses"][reused_label], "ok", reused_result)

        self.assertEqual(self.gen_start_count(), 1)
        self.assertEqual((self.root / "gen.out").read_bytes(), EXPECTED_GEN)
        self.assertEqual((self.root / "alpha.out").read_bytes(), EXPECTED_ALPHA)
        self.assertEqual((self.root / "beta.out").read_bytes(), EXPECTED_BETA)

        warm = self.start_driver(
            "warm-both", "shared", ("alpha", "beta"))
        warm_result = self.wait_driver(warm)
        self.assertEqual(
            warm_result["statuses"],
            {"gen": "reused", "alpha": "reused", "beta": "reused"},
            warm_result,
        )
        self.assertEqual(self.gen_start_count(), 1)

        state = json.loads(self.state.read_text(encoding="utf-8"))
        self.assertEqual(state["workspace"], os.path.abspath(str(self.root)))
        self.assertEqual(
            set(state["records"]), {"gen", "alpha", "beta"})

    def test_same_full_target_can_be_reused_by_second_process(self):
        first = self.start_driver("same-a", "shared", ("alpha",))
        second = self.start_driver("same-b", "shared", ("alpha",))

        self.wait_for_file("gen-started", handles=(first, second))
        self.wait_for_step_lock_open(first, "gen")
        self.wait_for_step_lock_open(second, "gen")
        (self.root / "release-shared").touch()

        first_result = self.wait_driver(first)
        second_result = self.wait_driver(second)
        results = [first_result, second_result]
        self.assertEqual(
            sorted(item["statuses"]["gen"] for item in results),
            ["ok", "reused"],
            results,
        )
        self.assertEqual(
            sorted(item["statuses"]["alpha"] for item in results),
            ["ok", "reused"],
            results,
        )
        self.assertEqual(self.gen_start_count(), 1)

        warm = self.start_driver("same-warm", "shared", ("alpha",))
        warm_result = self.wait_driver(warm)
        self.assertEqual(
            warm_result["statuses"],
            {"gen": "reused", "alpha": "reused"},
            warm_result,
        )
        self.assertEqual(self.gen_start_count(), 1)

    def test_independent_steps_overlap_and_state_keeps_both_records(self):
        first = self.start_driver("a", "independent", ("a",))
        second = self.start_driver("b", "independent", ("b",))

        self.wait_for_file("a-started", handles=(first, second))
        self.wait_for_file("b-started", handles=(first, second))
        # The two markers form a bidirectional rendezvous: each command waits
        # for the other marker before leaving its own active write interval.

        first_result = self.wait_driver(first)
        second_result = self.wait_driver(second)
        self.assertEqual(first_result["statuses"], {"a": "ok"})
        self.assertEqual(second_result["statuses"], {"b": "ok"})
        self.assertEqual((self.root / "a.out").read_bytes(), b"a-content-v1\n")
        self.assertEqual((self.root / "b.out").read_bytes(), b"b-content-v1\n")

        intervals = self.read_intervals()
        self.assertEqual(len(intervals), 2)
        # Independent commands were inside their write active regions at the
        # same time (the bidirectional file handshake is the stronger proof).
        starts = [item[0] for item in intervals]
        ends = [item[1] for item in intervals]
        self.assertLess(max(starts), min(ends))

        warm = self.start_driver(
            "warm-independent", "independent", ("a", "b"))
        warm_result = self.wait_driver(warm)
        self.assertEqual(
            warm_result["statuses"], {"a": "reused", "b": "reused"})

        state = json.loads(self.state.read_text(encoding="utf-8"))
        self.assertEqual(set(state["records"]), {"a", "b"})

    def test_failed_first_owner_is_not_inherited_by_waiter(self):
        holder = self.start_driver(
            "failure-alpha", "failure", ("alpha",))
        waiter = self.start_driver(
            "failure-beta", "failure", ("beta",))

        self.wait_for_file("failure-started", handles=(holder, waiter))
        self.wait_for_step_lock_open(holder, "gen")
        self.wait_for_step_lock_open(waiter, "gen")
        (self.root / "failure-release").touch()

        holder_result = self.wait_driver(holder)
        waiter_result = self.wait_driver(waiter)
        results = [holder_result, waiter_result]
        gen_statuses = [item["statuses"]["gen"] for item in results]
        self.assertEqual(sorted(gen_statuses), ["failed", "ok"], results)

        failed = next(
            item for item in results if item["statuses"]["gen"] == "failed")
        successful = next(
            item for item in results if item["statuses"]["gen"] == "ok")
        self.assertEqual(failed["returncodes"]["gen"], 7)

        if "alpha" in failed["statuses"]:
            failed_target = "alpha"
            successful_target = "beta"
        else:
            failed_target = "beta"
            successful_target = "alpha"
        self.assertEqual(failed["statuses"][failed_target], "skipped")
        self.assertEqual(
            failed["blocked_by"][failed_target], ["gen"])
        self.assertEqual(successful["statuses"][successful_target], "ok")
        expected_target = (
            EXPECTED_ALPHA if successful_target == "alpha" else EXPECTED_BETA)
        self.assertEqual(
            (self.root / f"{successful_target}.out").read_bytes(),
            expected_target,
        )
        self.assertEqual((self.root / "gen.out").read_bytes(), EXPECTED_GEN)

        # The waiter's successful record is independently reusable.
        warm = self.start_driver(
            "failure-warm", "failure", (successful_target,))
        warm_result = self.wait_driver(warm)
        self.assertEqual(
            warm_result["statuses"]["gen"], "reused", warm_result)
        self.assertEqual(
            warm_result["statuses"][successful_target], "reused", warm_result)

    def test_killed_python_owner_orphan_command_is_safe_and_recoverable(self):
        holder = self.start_driver("kill-holder", "kill", ("alpha",))
        self.wait_for_file("kill-temp-written", handles=(holder,))
        old_pid_text = (self.root / "kill-old-pid").read_text(
            encoding="utf-8").strip()
        old_command_pid = int(old_pid_text)

        waiter = self.start_driver("kill-waiter", "kill", ("beta",))
        self.wait_for_step_lock_open(waiter, "gen")
        self.wait_for_step_lock_open(holder, "gen")

        # Kill only the Python driver process, not its process group and not
        # the direct build command whose PID is recorded by the command itself.
        holder.proc.kill()
        holder_returncode = holder.proc.wait(timeout=10)
        self.assertNotEqual(holder_returncode, 0)

        # The orphaned command inherited the flock descriptor, so the step is
        # still owned while that command remains alive at its handshake.
        self.assert_lock_busy("gen")
        self.assertTrue(self._pid_alive(old_command_pid))
        self.assertEqual(
            (self.root / "gen.out").read_bytes(),
            b"temporary-partial-content\n")
        self.assertTrue(waiter.proc.poll() is None)
        self.assertFalse(waiter.result_path.exists())
        self.assertFalse((self.root / "beta.out").exists())
        # A broken implementation could have released after only the Python
        # caller's death and started a replacement before the old command was
        # released.  This marker must not exist in that interval.
        self.assertFalse((self.root / "kill-successor-started").exists())

        (self.root / "kill-release-old").touch()
        self.wait_for_file(
            "kill-old-finished", timeout=45, handles=(waiter,))
        self._wait_pid_dead(old_command_pid, timeout=20)

        waiter_result = self.wait_driver(waiter, timeout=60)
        self.assertIn(waiter_result["statuses"]["gen"], ("ok", "reused"))
        self.assertEqual(waiter_result["statuses"]["beta"], "ok")
        self.assertEqual((self.root / "gen.out").read_bytes(), EXPECTED_GEN)
        self.assertEqual((self.root / "beta.out").read_bytes(), EXPECTED_BETA)

        # Whichever legal recovery was used, old and takeover write regions
        # must not overlap.
        self.assert_no_overlapping_intervals()

        starts_before_warm = self.gen_start_count()
        warm = self.start_driver("kill-warm", "kill", ("beta",))
        warm_result = self.wait_driver(warm)
        self.assertEqual(
            warm_result["statuses"], {"gen": "reused", "beta": "reused"})
        self.assertEqual(self.gen_start_count(), starts_before_warm)

    def _pid_alive(self, pid):
        return Path(f"/proc/{pid}").exists()

    def _wait_pid_dead(self, pid, *, timeout):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if not self._pid_alive(pid):
                return
            # A zombie is already stopped and cannot execute another write.
            try:
                stat_text = Path(f"/proc/{pid}/stat").read_text()
                state = stat_text[stat_text.rfind(")") + 2:].split()[0]
                if state == "Z":
                    return
            except OSError:
                return
            time.sleep(0.01)
        self.fail(f"old command pid {pid} did not stop within {timeout}s")

    def test_outputless_step_runs_once_per_process(self):
        first = self.start_driver("noout-a", "nooutput", ("check",))
        second = self.start_driver("noout-b", "nooutput", ("check",))
        first_result = self.wait_driver(first)
        second_result = self.wait_driver(second)
        self.assertEqual(first_result["statuses"], {"check": "ok"})
        self.assertEqual(second_result["statuses"], {"check": "ok"})
        self.assertEqual(len(list(self.root.glob("start-check-*"))), 2)

        warm = self.start_driver("noout-c", "nooutput", ("check",))
        warm_result = self.wait_driver(warm)
        self.assertEqual(warm_result["statuses"], {"check": "ok"})
        self.assertEqual(len(list(self.root.glob("start-check-*"))), 3)

    def test_four_processes_distinct_steps_all_records_survive(self):
        # Four is the explicit cross-process concurrency bound.  Every process
        # receives the same complete plan; only its target tuple differs.
        step_names = FOUR_NAMES
        handles = [
            self.start_driver(f"four-{name}", "four", (name,), workers=1)
            for name in step_names
        ]

        results = [self.wait_driver(handle) for handle in handles]
        for name, result in zip(step_names, results):
            self.assertEqual(result["statuses"], {name: "ok"}, result)
            self.assertEqual(
                (self.root / f"{name}.out").read_bytes(),
                f"{name}-content\n".encode())

        warm = self.start_driver("four-warm", "four", step_names, workers=4)
        warm_result = self.wait_driver(warm)
        self.assertEqual(
            warm_result["statuses"],
            {name: "reused" for name in step_names},
            warm_result,
        )
        state = json.loads(self.state.read_text())
        self.assertEqual(set(state["records"]), set(step_names))


def four_one_driver(name, root_text, state_text, result_path):
    result = execute_incremental(
        {name: four_step(name)},
        targets=(name,),
        sources={},
        outputs={name: (f"{name}.out",)},
        reads={},
        context="four-distinct-v1",
        state_file=state_text,
        workers=1,
        cwd=root_text,
    )
    Path(result_path).write_text(json.dumps({
        "ok": True,
        "statuses": {
            key: outcome.status for key, outcome in result.outcomes.items()
        },
    }), encoding="utf-8")


def four_warm_driver(root_text, state_text):
    plan = {name: four_step(name) for name in FOUR_NAMES}
    result = execute_incremental(
        plan,
        targets=FOUR_NAMES,
        sources={},
        outputs={name: (f"{name}.out",) for name in FOUR_NAMES},
        reads={},
        context="four-distinct-v1",
        state_file=state_text,
        workers=4,
        cwd=root_text,
    )
    statuses = {
        key: outcome.status for key, outcome in result.outcomes.items()
    }
    expected = {name: "reused" for name in FOUR_NAMES}
    if statuses != expected:
        raise AssertionError(statuses)
    print("FOUR_WARM_OK")


def _driver_main(spec_path):
    run_driver(spec_path)


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--driver":
        _driver_main(sys.argv[2])
    elif len(sys.argv) == 6 and sys.argv[1] == "--four-one":
        four_one_driver(*sys.argv[2:])
    elif len(sys.argv) == 4 and sys.argv[1] == "--four-warm":
        four_warm_driver(sys.argv[2], sys.argv[3])
    else:
        unittest.main(verbosity=2)
