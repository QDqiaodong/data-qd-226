"""Stdlib tests for execute_guarded and its check supervision protocol."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from tempfile import TemporaryDirectory
import unittest

from executor import GuardedOutcome, Step, execute_guarded

PY = sys.executable


def gstep(code, after=(), timeout=10):
    return Step((PY, "-c", code), tuple(after), timeout)


def touch_step(marker, after=(), timeout=10):
    return gstep(
        "from pathlib import Path\n"
        f"Path({str(marker)!r}).touch()\n",
        after=after,
        timeout=timeout,
    )


def wait_for_file(path, timeout=5.0):
    deadline = time.monotonic() + timeout
    path = Path(path)
    while time.monotonic() < deadline:
        if path.exists():
            return True
        time.sleep(0.01)
    return path.exists()


def assert_never_appears(testcase, path, grace=2.0):
    """Wait long enough to catch a wrongly surviving polling helper."""
    path = Path(path)
    deadline = time.monotonic() + grace
    while time.monotonic() < deadline:
        if path.exists():
            testcase.fail(f"unexpected side effect: {path}")
        time.sleep(0.02)
    testcase.assertFalse(path.exists())


class GuardedValidationTest(unittest.TestCase):
    def test_bad_arguments_and_declarations_start_nothing(self):
        with TemporaryDirectory() as tmp:
            marker = Path(tmp) / "started"
            good = touch_step(marker)
            plan = {"good": good}

            bad_calls = [
                dict(workers=True),
                dict(workers=0),
                dict(workers=9),
                dict(workers=1.5),
                dict(tail_bytes=True),
                dict(tail_bytes=-1),
                dict(tail_bytes=16385),
                dict(checks=[]),
                dict(checks={"missing": ("a",)}),
                dict(checks={"good": ["a"]}),
                dict(checks={"good": ("",)}),
                dict(checks={"good": ("a" * 33,)}),
                dict(checks={"good": ("bad id",)}),
                dict(checks={"good": ("bad\tnope",)}),
                dict(checks={"good": ("a", "a")}),
                dict(checks={"good": tuple(f"i{x}" for x in range(65))}),
            ]
            for kwargs in bad_calls:
                with self.subTest(kwargs=kwargs):
                    with self.assertRaises(ValueError):
                        execute_guarded(plan, cwd=tmp, **kwargs)
            self.assertFalse(marker.exists())

    def test_too_many_steps_rejected_before_launch(self):
        with TemporaryDirectory() as tmp:
            marker = Path(tmp) / "started"
            plan = {
                f"s{i}": touch_step(marker)
                for i in range(257)
            }
            with self.assertRaises(ValueError):
                execute_guarded(plan, workers=8, cwd=tmp)
            self.assertFalse(marker.exists())

    def test_cycle_and_missing_dependency_start_nothing(self):
        with TemporaryDirectory() as tmp:
            marker = Path(tmp) / "started"
            good = touch_step(marker)
            cyclic = {
                "a": gstep("pass", ("b",)),
                "b": gstep("pass", ("a",)),
            }
            missing = {
                "good": good,
                "bad": gstep("pass", ("absent",)),
            }
            with self.assertRaises(ValueError):
                execute_guarded(cyclic, cwd=tmp)
            with self.assertRaises(ValueError):
                execute_guarded(missing, cwd=tmp)
            self.assertFalse(marker.exists())

    def test_blocked_by_deduplicates_after_order(self):
        with TemporaryDirectory() as tmp:
            outcomes = execute_guarded({
                "b": gstep("raise SystemExit(2)"),
                "a": gstep("raise SystemExit(3)"),
                "c": gstep("pass", ("b", "a", "b")),
            }, cwd=tmp)
            self.assertEqual(outcomes["c"].outcome.status, "skipped")
            self.assertEqual(outcomes["c"].outcome.blocked_by, ("b", "a"))
            self.assertEqual(outcomes["c"].stdout_tail, b"")
            self.assertEqual(outcomes["c"].stderr_tail, b"")

    def test_result_type_and_input_order(self):
        plan = {name: gstep("pass") for name in ("z", "a", "m")}
        outcomes = execute_guarded(plan, workers=3)
        self.assertEqual(list(outcomes), ["z", "a", "m"])
        for item in outcomes.values():
            self.assertIsInstance(item, GuardedOutcome)
            self.assertEqual(item.stdout_tail, b"")
            self.assertEqual(item.stderr_tail, b"")


class GuardedProtocolTest(unittest.TestCase):
    def test_all_pass_admits_downstream(self):
        with TemporaryDirectory() as tmp:
            marker = Path(tmp) / "package-started"
            plan = {
                "check": gstep(
                    "import os\n"
                    "os.write(2, b'prefix\\n')\n"
                    "os.write(2, b'@@BUILD-CHECK a PASS\\n')\n"
                    "os.write(2, b'@@BUILD-CHECK b PASS\\n')\n"),
                "package": touch_step(marker, ("check",)),
            }
            outcomes = execute_guarded(
                plan, checks={"check": ("a", "b")}, cwd=tmp)
            self.assertEqual(outcomes["check"].outcome.status, "ok",
                             outcomes["check"].outcome.detail)
            self.assertEqual(outcomes["check"].outcome.returncode, 0)
            self.assertEqual(outcomes["package"].outcome.status, "ok")
            self.assertTrue(marker.exists())

    def test_missing_checks_blocks_with_zero_exit(self):
        with TemporaryDirectory() as tmp:
            marker = Path(tmp) / "package-started"
            plan = {
                "check": gstep(
                    "import sys\n"
                    "sys.stderr.write('@@BUILD-CHECK a PASS\\n')\n"),
                "package": touch_step(marker, ("check",)),
            }
            outcomes = execute_guarded(
                plan, checks={"check": ("a", "b", "c")}, cwd=tmp)
            result = outcomes["check"].outcome
            self.assertEqual(result.status, "check_incomplete")
            self.assertEqual(result.returncode, 0)
            self.assertIn("b", result.detail)
            self.assertIn("c", result.detail)
            package = outcomes["package"].outcome
            self.assertEqual(package.status, "skipped")
            self.assertEqual(package.blocked_by, ("check",))
            self.assertFalse(marker.exists())

    def test_nonzero_exit_is_failed_even_when_checks_missing(self):
        outcomes = execute_guarded(
            {"s": gstep("raise SystemExit(7)")},
            checks={"s": ("a",)})
        result = outcomes["s"].outcome
        self.assertEqual(result.status, "failed")
        self.assertEqual(result.returncode, 7)

    def test_stdout_check_lines_have_no_authority(self):
        with TemporaryDirectory() as tmp:
            marker = Path(tmp) / "downstream"
            plan = {
                "s": gstep(
                    "import sys\n"
                    "sys.stdout.buffer.write("
                    "b'@@BUILD-CHECK a PASS\\n')\n"),
                "d": touch_step(marker, ("s",)),
            }
            outcomes = execute_guarded(
                plan, checks={"s": ("a",)}, cwd=tmp)
            guarded = outcomes["s"]
            self.assertEqual(guarded.outcome.status, "check_incomplete")
            self.assertEqual(
                guarded.stdout_tail, b"@@BUILD-CHECK a PASS\n")
            self.assertEqual(guarded.stderr_tail, b"")
            self.assertEqual(outcomes["d"].outcome.status, "skipped")
            self.assertFalse(marker.exists())

    def test_protocol_errors_are_distinct_from_nonzero_exit(self):
        cases = {
            "undeclared": b"@@BUILD-CHECK ghost PASS\n",
            "duplicate": (b"@@BUILD-CHECK a PASS\n"
                          b"@@BUILD-CHECK a PASS\n"),
            "malformed": b"@@BUILD-CHECK a MAYBE\n",
            "crlf": b"@@BUILD-CHECK a PASS\r\n",
            "unterminated": b"@@BUILD-CHECK a PASS",
            "too_long_id": b"@@BUILD-CHECK " + b"x" * 33 + b" PASS\n",
        }
        for label, payload in cases.items():
            with self.subTest(label=label):
                code = (
                    "import os\n"
                    f"os.write(2, {payload!r})\n"
                    "raise SystemExit(9)\n"
                )
                outcomes = execute_guarded(
                    {"s": gstep(code)}, checks={"s": ("a",)})
                result = outcomes["s"].outcome
                self.assertEqual(
                    result.status, "check_protocol_error", result.detail)
                self.assertIsNone(result.returncode)
                if label == "undeclared":
                    self.assertIn("ghost", result.detail)
                if label == "duplicate":
                    self.assertIn("a", result.detail)

    def test_partial_prefix_at_eof_is_ordinary_log(self):
        outcomes = execute_guarded(
            {"s": gstep(
                "import os; os.write(2, b'@@BUILD-CHEC ordinary')")},
            checks={"s": ("a",)})
        self.assertEqual(
            outcomes["s"].outcome.status, "check_incomplete")

    def test_unlisted_step_does_not_parse_protocol(self):
        outcomes = execute_guarded(
            {"s": gstep(
                "import sys; sys.stderr.write('not a declared check\\n')")},
            checks={})
        self.assertEqual(outcomes["s"].outcome.status, "ok")

    def test_launch_failure_has_empty_tails_and_skips_dependency(self):
        with TemporaryDirectory() as tmp:
            missing_exe = Path(tmp) / "does-not-exist"
            outcomes = execute_guarded({
                "missing": Step((str(missing_exe), "arg")),
                "child": gstep("pass", ("missing",)),
            }, cwd=tmp)
            guarded = outcomes["missing"]
            self.assertEqual(guarded.outcome.status, "launch_error")
            self.assertIsNone(guarded.outcome.returncode)
            self.assertTrue(guarded.outcome.detail)
            self.assertEqual(guarded.stdout_tail, b"")
            self.assertEqual(guarded.stderr_tail, b"")
            self.assertEqual(
                outcomes["child"].outcome.status, "skipped")


class GuardedTailsTest(unittest.TestCase):
    def test_tails_are_exact_suffixes(self):
        with TemporaryDirectory() as tmp:
            out_path = Path(tmp) / "out.bin"
            err_path = Path(tmp) / "err.bin"
            out_bytes = bytes((i * 7 + 1) % 256 for i in range(5000))
            err_bytes = bytes((i * 11 + 3) % 256 for i in range(6000))
            out_path.write_bytes(out_bytes)
            err_path.write_bytes(err_bytes)
            code = (
                "import pathlib, sys\n"
                f"sys.stdout.buffer.write(pathlib.Path({str(out_path)!r})"
                ".read_bytes())\n"
                f"sys.stderr.buffer.write(pathlib.Path({str(err_path)!r})"
                ".read_bytes())\n"
            )
            outcomes = execute_guarded(
                {"s": gstep(code)}, tail_bytes=123, cwd=tmp)
            guarded = outcomes["s"]
            self.assertEqual(guarded.outcome.status, "ok")
            self.assertEqual(guarded.stdout_tail, out_bytes[-123:])
            self.assertEqual(guarded.stderr_tail, err_bytes[-123:])

    def test_tail_zero_still_reads_and_enforces_protocol(self):
        outcomes = execute_guarded(
            {"s": gstep(
                "import os\n"
                "for _ in range(100): os.write(2, b'x' * 4096)\n"
                "os.write(2, b'\\n@@BUILD-CHECK a PASS\\n')\n")},
            checks={"s": ("a",)}, tail_bytes=0)
        guarded = outcomes["s"]
        self.assertEqual(guarded.outcome.status, "ok", guarded.outcome.detail)
        self.assertEqual(guarded.stdout_tail, b"")
        self.assertEqual(guarded.stderr_tail, b"")

    def test_non_utf8_diagnostic_bytes_are_preserved(self):
        payload = bytes(range(256)) * 4
        code = (
            "import os\n"
            f"os.write(2, {payload!r})\n"
            "os.write(2, b'\\n@@BUILD-CHECK a PASS\\n')\n"
        )
        outcomes = execute_guarded(
            {"s": gstep(code)}, checks={"s": ("a",)}, tail_bytes=16384)
        guarded = outcomes["s"]
        self.assertEqual(guarded.outcome.status, "ok", guarded.outcome.detail)
        self.assertTrue(guarded.stderr_tail.endswith(
            payload[-200:] + b"\n@@BUILD-CHECK a PASS\n"))

    def test_stdin_is_devnull_and_cwd_is_used(self):
        with TemporaryDirectory() as tmp:
            marker = Path(tmp) / "cwd-ok"
            code = (
                "import sys\n"
                "from pathlib import Path\n"
                "assert sys.stdin.read() == ''\n"
                f"Path({marker.name!r}).touch()\n"
            )
            outcomes = execute_guarded(
                {"s": Step((PY, "-c", code))}, cwd=tmp)
            self.assertEqual(outcomes["s"].outcome.status, "ok",
                             outcomes["s"].outcome.detail)
            self.assertTrue(marker.exists())


class GuardedProcessSupervisionTest(unittest.TestCase):
    def test_segmented_fail_kills_helper_and_admission(self):
        with TemporaryDirectory() as tmp:
            ready = Path(tmp) / "helper-ready"
            release = Path(tmp) / "release-helper"
            continued = Path(tmp) / "helper-continued"
            package_marker = Path(tmp) / "package-ran"
            independent_marker = Path(tmp) / "independent-ran"

            helper_code = (
                "import time\n"
                "from pathlib import Path\n"
                f"ready = Path({str(ready)!r})\n"
                f"release = Path({str(release)!r})\n"
                f"continued = Path({str(continued)!r})\n"
                "ready.touch()\n"
                "while not release.exists():\n"
                "    time.sleep(0.01)\n"
                "continued.touch()\n"
            )
            main_code = (
                "import json, os, subprocess, sys, time\n"
                "from pathlib import Path\n"
                f"helper = subprocess.Popen([sys.executable, '-c', "
                f"{helper_code!r}])\n"
                f"Path({str(Path(tmp) / 'pids.json')!r}).write_text("
                "json.dumps({'main': os.getpid(), 'helper': helper.pid}))\n"
                f"deadline = time.monotonic() + 10\n"
                f"while not Path({str(ready)!r}).exists():\n"
                "    assert time.monotonic() < deadline\n"
                "    time.sleep(0.01)\n"
                # Deliberately split across prefix, ID, and verdict.
                "for segment in (b'@@BUILD-CHEC', b'K fail-id F', b'AIL\\n'):\n"
                "    os.write(2, segment)\n"
                "    time.sleep(0.1)\n"
                f"while not Path({str(release)!r}).exists():\n"
                "    time.sleep(0.02)\n"
                f"Path({str(Path(tmp) / 'main-continued')!r}).touch()\n"
            )

            started = time.monotonic()
            outcomes = execute_guarded({
                "abi": Step((PY, "-c", main_code), timeout=8),
                "package": touch_step(package_marker, ("abi",)),
                "independent": touch_step(independent_marker),
            }, checks={"abi": ("fail-id",)}, workers=2, cwd=tmp)
            elapsed = time.monotonic() - started

            abi = outcomes["abi"]
            self.assertEqual(abi.outcome.status, "check_failed",
                             abi.outcome.detail)
            self.assertIsNone(abi.outcome.returncode)
            self.assertIn("fail-id", abi.outcome.detail)
            self.assertIn(b"@@BUILD-CHECK fail-id FAIL\n", abi.stderr_tail)
            self.assertEqual(
                outcomes["package"].outcome.status, "skipped")
            self.assertEqual(
                outcomes["independent"].outcome.status, "ok")
            self.assertFalse(package_marker.exists())
            self.assertTrue(independent_marker.exists())
            self.assertLess(elapsed, 4.0)

            pids = json.loads((Path(tmp) / "pids.json").read_text())
            self.assertGreater(pids["helper"], 0)
            release.touch()
            assert_never_appears(self, continued)
            self.assertFalse((Path(tmp) / "main-continued").exists())

    def test_helper_holding_stderr_forces_timeout_group_kill(self):
        with TemporaryDirectory() as tmp:
            ready = Path(tmp) / "helper-ready"
            release = Path(tmp) / "release-helper"
            continued = Path(tmp) / "helper-continued"
            downstream_marker = Path(tmp) / "downstream-ran"

            helper_code = (
                "import time\n"
                "from pathlib import Path\n"
                f"ready = Path({str(ready)!r})\n"
                f"release = Path({str(release)!r})\n"
                f"continued = Path({str(continued)!r})\n"
                "ready.touch()\n"
                "while not release.exists():\n"
                "    time.sleep(0.01)\n"
                "continued.touch()\n"
            )
            main_code = (
                "import json, os, subprocess, sys, time\n"
                "from pathlib import Path\n"
                f"helper = subprocess.Popen([sys.executable, '-c', "
                f"{helper_code!r}])\n"
                f"Path({str(Path(tmp) / 'pids.json')!r}).write_text("
                "json.dumps({'main': os.getpid(), 'helper': helper.pid}))\n"
                "deadline = time.monotonic() + 10\n"
                f"while not Path({str(ready)!r}).exists():\n"
                "    assert time.monotonic() < deadline\n"
                "    time.sleep(0.01)\n"
                "os.write(2, b'@@BUILD-CHECK a PASS\\n')\n"
                # Exit main while the helper keeps the inherited stderr pipe.
            )

            started = time.monotonic()
            outcomes = execute_guarded({
                "held": Step((PY, "-c", main_code), timeout=0.7),
                "downstream": touch_step(downstream_marker, ("held",)),
            }, checks={"held": ("a",)}, workers=1, cwd=tmp)
            elapsed = time.monotonic() - started

            held = outcomes["held"]
            self.assertEqual(held.outcome.status, "timeout", held.outcome)
            self.assertIsNone(held.outcome.returncode)
            self.assertIn(b"@@BUILD-CHECK a PASS\n", held.stderr_tail)
            self.assertEqual(
                outcomes["downstream"].outcome.status, "skipped")
            self.assertFalse(downstream_marker.exists())
            self.assertGreaterEqual(elapsed, 0.65)
            self.assertLess(elapsed, 4.0)

            release.touch()
            assert_never_appears(self, continued)

    @unittest.skipUnless(sys.platform.startswith("linux"),
                         "RLIMIT_AS acceptance is Linux-specific")
    def test_large_stream_under_160_mib_address_limit(self):
        project_root = Path(__file__).resolve().parent
        driver = r"""
import sys
import time
sys.path.insert(0, sys.argv[1])
from executor import Step, execute_guarded

writer = r'''
import os
chunk = b"s" * 65536
for _ in range(3072):  # 192 MiB
    os.write(2, chunk)
check = b"@@BUILD-CHECK big PASS\n"
os.write(2, b"\n" + check)
out_chunk = b"o" * 65536
for _ in range(64):  # 4 MiB
    os.write(1, out_chunk)
'''
started = time.monotonic()
result = execute_guarded(
    {"big": Step((sys.executable, "-c", writer), timeout=20)},
    checks={"big": ("big",)},
    tail_bytes=4096,
    workers=1,
)
guarded = result["big"]
assert guarded.outcome.status == "ok", guarded.outcome
assert guarded.outcome.returncode == 0
check = b"@@BUILD-CHECK big PASS\n"
expected_stderr = b"s" * (4096 - len(check) - 1) + b"\n" + check
assert guarded.stderr_tail == expected_stderr
assert guarded.stdout_tail == b"o" * 4096
assert time.monotonic() - started < 20
print("LARGE_STREAM_OK")
"""

        def limit_address_space():
            import resource
            resource.setrlimit(
                resource.RLIMIT_AS, (160 * 1024 * 1024, 160 * 1024 * 1024))

        completed = subprocess.run(
            [PY, "-I", "-c", driver, str(project_root)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=60,
            preexec_fn=limit_address_space,
            check=False,
        )
        self.assertEqual(
            completed.returncode, 0,
            completed.stdout.decode("utf-8", "replace")
            + completed.stderr.decode("utf-8", "replace"))
        self.assertIn(b"LARGE_STREAM_OK", completed.stdout)


if __name__ == "__main__":
    unittest.main(verbosity=2)
