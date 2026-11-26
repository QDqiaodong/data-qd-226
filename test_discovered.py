"""Offline real-subprocess acceptance tests for execute_discovered."""
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest

from executor import Step, execute_discovered


PY = sys.executable
BOUNDED_WAIT_SECONDS = 8.0


def wait_for_path(path, timeout=BOUNDED_WAIT_SECONDS):
    deadline = time.monotonic() + timeout
    path = Path(path)
    while time.monotonic() < deadline:
        if path.exists():
            return
        time.sleep(0.01)
    raise AssertionError(f"timed out waiting for {path}")


def launch_count(path):
    path = Path(path)
    if not path.exists():
        return 0
    return sum(1 for line in path.read_text().splitlines() if line.strip())


def marker_step(body, marker, *args, after=(), timeout=10):
    code = (
        "import sys\n"
        "from pathlib import Path\n"
        "_marker = sys.argv[1]\n"
        "with open(_marker, 'a', encoding='utf-8') as _handle:\n"
        "    _handle.write(str(__import__('os').getpid()) + '\\n')\n"
        + body
        + "\n"
    )
    return Step(
        (PY, "-I", "-c", code, str(marker), *(str(arg) for arg in args)),
        tuple(after),
        timeout,
    )


def manifest_scan(dependencies, marker="", timeout=10):
    code = (
        "import json, os, sys\n"
        "from pathlib import Path\n"
        "marker, payload = sys.argv[1], sys.argv[2]\n"
        "if marker:\n"
        "    with open(marker, 'a', encoding='utf-8') as handle:\n"
        "        handle.write(str(os.getpid()) + '\\n')\n"
        "Path(os.environ['BUILD_DEPS_FILE']).write_text(payload, "
        "encoding='utf-8')\n"
    )
    return Step(
        (PY, "-I", "-c", code, str(marker), json.dumps(list(dependencies))),
        (),
        timeout,
    )


PLATFORM_SCAN_CODE = r"""
import json, os, sys
from pathlib import Path
marker, config_path = sys.argv[1], sys.argv[2]
if marker:
    with open(marker, 'a', encoding='utf-8') as handle:
        handle.write(str(os.getpid()) + '\n')
platform = Path(config_path).read_text(encoding='utf-8').strip()
dependencies = {
    'unix': ['gen_unix'],
    'win': ['gen_win'],
}[platform]
Path(os.environ['BUILD_DEPS_FILE']).write_text(json.dumps(dependencies))
"""


def platform_scan(marker, config_path):
    return Step(
        (PY, "-I", "-c", PLATFORM_SCAN_CODE, str(marker), str(config_path)),
        (),
    )


class DiscoveryValidationTest(unittest.TestCase):
    def test_bad_arguments_declarations_and_static_cycles_start_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            marker = root / "started"
            good = marker_step("pass", marker)

            bad_workers = [
                lambda: execute_discovered(
                    {"g": good}, targets=("g",), workers=True),
                lambda: execute_discovered(
                    {"g": good}, targets=("g",), workers=0),
                lambda: execute_discovered(
                    {"g": good}, targets=("g",), workers=9),
            ]
            for call in bad_workers:
                with self.assertRaises(ValueError):
                    call()

            with self.assertRaises(ValueError):
                execute_discovered(
                    [], targets=("g",))  # type: ignore[arg-type]

            with self.assertRaises(ValueError):
                execute_discovered(
                    {"g": good}, targets=["g"])  # type: ignore[arg-type]
            with self.assertRaises(ValueError):
                execute_discovered({"g": good}, targets=())
            with self.assertRaises(ValueError):
                execute_discovered(
                    {"g": good}, targets=("g", "g"))
            with self.assertRaises(ValueError):
                execute_discovered(
                    {"g": good}, targets=("missing",))

            long_name = "n" * 129
            with self.assertRaises(ValueError):
                execute_discovered(
                    {long_name: good}, targets=(long_name,))
            with self.assertRaises(ValueError):
                execute_discovered(
                    {"bad\0name": good}, targets=("bad\0name",))

            unknown_after = {
                "ok": good,
                "bad": marker_step("pass", root / "bad-started",
                                   after=("absent",)),
            }
            with self.assertRaises(ValueError):
                execute_discovered(
                    unknown_after, targets=("ok",), cwd=root)

            static_cycle = {
                "a": marker_step("pass", root / "a-started", after=("b",)),
                "b": marker_step("pass", root / "b-started", after=("a",)),
            }
            with self.assertRaises(ValueError):
                execute_discovered(
                    static_cycle, targets=("a",), cwd=root)

            with self.assertRaises(ValueError):
                execute_discovered(
                    {"g": good},
                    targets=("g",),
                    scans={"g": Step((PY, "-c", "pass"), after=("x",))},
                )

            self.assertFalse(marker.exists())
            self.assertFalse((root / "bad-started").exists())
            self.assertFalse((root / "a-started").exists())
            self.assertFalse((root / "b-started").exists())

    def test_full_plan_limit_is_validated_before_launch(self):
        with tempfile.TemporaryDirectory() as tmp:
            marker = Path(tmp) / "started"
            plan = {
                f"step_{i:03d}": marker_step("pass", marker)
                for i in range(257)
            }
            with self.assertRaises(ValueError):
                execute_discovered(plan, targets=("step_000",))
            self.assertFalse(marker.exists())

    def test_raw_after_limit_is_validated(self):
        plan = {
            "d": Step((PY, "-c", "pass")),
            "main": Step((PY, "-c", "pass"), ("d",) * 257),
        }
        with self.assertRaises(ValueError):
            execute_discovered(plan, targets=("main",))


class DiscoveryAcceptanceTest(unittest.TestCase):
    def _platform_fixture(self, root, platform):
        markers = root / "markers"
        markers.mkdir(exist_ok=True)
        config = root / "platform.cfg"
        config.write_text(platform, encoding="utf-8")

        def mark(name):
            return markers / name

        plan = {
            "prep": marker_step(
                "Path(sys.argv[2]).write_text('READY', encoding='utf-8')",
                mark("build-prep"),
                root / "prep.stamp",
            ),
            "gen_unix": marker_step(
                "stamp, header = Path(sys.argv[2]), Path(sys.argv[3])\n"
                "header.write_text('UNIX:' + stamp.read_text(), "
                "encoding='utf-8')",
                mark("build-gen_unix"),
                root / "prep.stamp",
                root / "generated.h",
            ),
            "gen_win": marker_step(
                "Path(sys.argv[2]).write_text('WINDOWS', encoding='utf-8')",
                mark("build-gen_win"),
                root / "generated.h",
            ),
            "consume_a": marker_step(
                "header, output = Path(sys.argv[2]), Path(sys.argv[3])\n"
                "output.write_text('A:' + header.read_text(), "
                "encoding='utf-8')",
                mark("build-consume_a"),
                root / "generated.h",
                root / "a.out",
            ),
            "consume_b": marker_step(
                "header, output = Path(sys.argv[2]), Path(sys.argv[3])\n"
                "output.write_text('B:' + header.read_text(), "
                "encoding='utf-8')",
                mark("build-consume_b"),
                root / "generated.h",
                root / "b.out",
            ),
        }
        scans = {
            "consume_a": platform_scan(
                mark("scan-consume_a"), config),
            "consume_b": platform_scan(
                mark("scan-consume_b"), config),
            "gen_unix": manifest_scan(
                ["prep"], mark("scan-gen_unix")),
            "gen_win": manifest_scan(
                [], mark("scan-gen_win")),
            "prep": manifest_scan(
                [], mark("scan-prep")),
        }
        return plan, scans, markers

    def test_platform_scans_select_different_shared_generators(self):
        for platform in ("unix", "win"):
            with self.subTest(platform=platform):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    plan, scans, markers = self._platform_fixture(
                        root, platform)
                    outcomes = execute_discovered(
                        plan,
                        targets=("consume_a", "consume_b"),
                        scans=scans,
                        workers=2,
                        cwd=root,
                    )

                    if platform == "unix":
                        expected = [
                            "prep", "gen_unix", "consume_a", "consume_b"]
                        self.assertEqual(
                            [n for n in plan if n in outcomes], expected)
                        self.assertEqual(
                            {outcomes[n].status for n in expected}, {"ok"})
                        self.assertEqual(
                            (root / "generated.h").read_text(), "UNIX:READY")
                        self.assertEqual(
                            (root / "a.out").read_text(),
                            "A:UNIX:READY")
                        self.assertEqual(
                            (root / "b.out").read_text(),
                            "B:UNIX:READY")
                        for name in (
                                "scan-consume_a", "scan-consume_b",
                                "scan-gen_unix", "scan-prep",
                                "build-consume_a", "build-consume_b",
                                "build-gen_unix", "build-prep"):
                            self.assertEqual(
                                launch_count(markers / name), 1, name)
                        self.assertEqual(
                            launch_count(markers / "scan-gen_win"), 0)
                        self.assertEqual(
                            launch_count(markers / "build-gen_win"), 0)
                    else:
                        expected = [
                            "gen_win", "consume_a", "consume_b"]
                        self.assertEqual(
                            [n for n in plan if n in outcomes], expected)
                        self.assertEqual(
                            {outcomes[n].status for n in expected}, {"ok"})
                        self.assertEqual(
                            (root / "generated.h").read_text(), "WINDOWS")
                        self.assertEqual(
                            launch_count(markers / "scan-gen_win"), 1)
                        self.assertEqual(
                            launch_count(markers / "build-gen_win"), 1)
                        for name in (
                                "scan-gen_unix", "build-gen_unix",
                                "scan-prep", "build-prep"):
                            self.assertEqual(
                                launch_count(markers / name), 0, name)

    def test_invalid_manifest_prefix_does_not_select_producer(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            markers = root / "markers"
            markers.mkdir()
            plan = {
                "bad": marker_step(
                    "pass", markers / "build-bad"),
                "legal": marker_step(
                    "pass", markers / "build-legal"),
                "independent": marker_step(
                    "Path(sys.argv[2]).write_text('ok', encoding='utf-8')",
                    markers / "build-independent",
                    root / "independent.out",
                ),
            }
            scans = {
                "bad": manifest_scan(
                    ["legal", "unknown"], markers / "scan-bad"),
            }

            outcomes = execute_discovered(
                plan,
                targets=("bad", "independent"),
                scans=scans,
                workers=2,
                cwd=root,
            )

            self.assertEqual(outcomes["bad"].status, "scan_error")
            self.assertIn("unknown_name", outcomes["bad"].detail)
            self.assertEqual(outcomes["bad"].returncode, 0)
            self.assertEqual(outcomes["independent"].status, "ok")
            self.assertNotIn("legal", outcomes)
            self.assertEqual(launch_count(markers / "build-legal"), 0)
            self.assertEqual(launch_count(markers / "build-bad"), 0)
            self.assertTrue((root / "independent.out").exists())

    def test_dynamic_chain_converges_with_one_worker(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            markers = root / "markers"
            markers.mkdir()
            plan = {
                "t": marker_step(
                    "Path(sys.argv[2]).write_text('target', encoding='utf-8')",
                    markers / "build-t", root / "t.out"),
                "g": marker_step(
                    "ready = Path(sys.argv[2]).read_text()\n"
                    "Path(sys.argv[3]).write_text('gen:' + ready,\n"
                    "                             encoding='utf-8')",
                    markers / "build-g",
                    root / "p.out", root / "g.out"),
                "p": marker_step(
                    "Path(sys.argv[2]).write_text('ready', encoding='utf-8')",
                    markers / "build-p", root / "p.out"),
            }
            scans = {
                "t": manifest_scan(["g"], markers / "scan-t"),
                "g": manifest_scan(["p"], markers / "scan-g"),
            }
            outcomes = execute_discovered(
                plan, targets=("t",), scans=scans, workers=1, cwd=root)
            self.assertEqual(
                {name: outcome.status for name, outcome in outcomes.items()},
                {"t": "ok", "g": "ok", "p": "ok"})
            self.assertEqual((root / "g.out").read_text(), "gen:ready")
            for name in ("scan-t", "scan-g", "build-t", "build-g", "build-p"):
                self.assertEqual(launch_count(markers / name), 1, name)

    def test_static_and_scan_edges_form_all_cycles_and_blocked_downstream(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            markers = root / "markers"
            markers.mkdir()

            plan = {
                "a": marker_step("pass", markers / "build-a",
                                  after=("b",)),
                "b": marker_step("pass", markers / "build-b"),
                "check": marker_step(
                    "raise SystemExit(7)", markers / "build-check"),
                "down": marker_step(
                    "pass", markers / "build-down",
                    after=("b", "check")),
                "selfcyc": marker_step(
                    "pass", markers / "build-self"),
            }
            scans = {
                "b": manifest_scan(["a"], markers / "scan-b"),
                "selfcyc": manifest_scan(
                    ["selfcyc"], markers / "scan-self"),
            }

            outcomes = execute_discovered(
                plan,
                targets=("a", "down", "selfcyc"),
                scans=scans,
                workers=3,
                cwd=root,
            )

            for name in ("a", "b", "selfcyc"):
                self.assertEqual(outcomes[name].status, "cycle", name)
                self.assertEqual(outcomes[name].blocked_by, ())
            self.assertEqual(outcomes["check"].status, "failed")
            self.assertEqual(outcomes["check"].returncode, 7)
            self.assertEqual(outcomes["down"].status, "skipped")
            self.assertEqual(
                outcomes["down"].blocked_by, ("b", "check"))

            self.assertEqual(launch_count(markers / "scan-b"), 1)
            self.assertEqual(launch_count(markers / "scan-self"), 1)
            for name in ("build-a", "build-b", "build-down", "build-self"):
                self.assertEqual(launch_count(markers / name), 0, name)
            self.assertEqual(launch_count(markers / "build-check"), 1)


class DiscoverySchedulingTest(unittest.TestCase):
    def test_two_slot_handshake_allows_fast_branch_then_slow_target(self):
        wait_code = (
            "deadline = __import__('time').monotonic() + 8\n"
            "while not __import__('pathlib').Path(sys.argv[2]).exists():\n"
            "    if __import__('time').monotonic() > deadline:\n"
            "        raise SystemExit('bounded wait failed')\n"
            "    __import__('time').sleep(0.01)\n"
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            order = root / "order.log"
            slow_start = root / "slow-start.flag"
            fast_done = root / "fast-build.done"

            def event(name):
                return (
                    "with open(sys.argv[2], 'a', encoding='utf-8') as h:\n"
                    f"    h.write({name!r} + '\\n')\n"
                )

            slow_scan_code = (
                "import json, os, sys, time\n"
                "from pathlib import Path\n"
                "order = Path(sys.argv[1])\n"
                "start_flag = Path(sys.argv[2])\n"
                "fast_done = Path(sys.argv[3])\n"
                "with order.open('a', encoding='utf-8') as h:\n"
                "    h.write('slow-scan-start\\n')\n"
                "start_flag.write_text('started', encoding='utf-8')\n"
                "deadline = time.monotonic() + 8\n"
                "while not fast_done.exists():\n"
                "    if time.monotonic() > deadline:\n"
                "        raise SystemExit('slow scan bounded wait failed')\n"
                "    time.sleep(0.01)\n"
                "with order.open('a', encoding='utf-8') as h:\n"
                "    h.write('slow-scan-finish\\n')\n"
                "Path(os.environ['BUILD_DEPS_FILE']).write_text('[]')\n"
            )
            fast_scan_code = (
                "import json, os, sys, time\n"
                "from pathlib import Path\n"
                "order = Path(sys.argv[1])\n"
                "start_flag = Path(sys.argv[2])\n"
                "deadline = time.monotonic() + 8\n"
                "while not start_flag.exists():\n"
                "    if time.monotonic() > deadline:\n"
                "        raise SystemExit('fast scan bounded wait failed')\n"
                "    time.sleep(0.01)\n"
                "with order.open('a', encoding='utf-8') as h:\n"
                "    h.write('fast-scan-saw-start\\n')\n"
                "Path(os.environ['BUILD_DEPS_FILE']).write_text("
                "json.dumps(['generator']))\n"
            )

            plan = {
                "slow": marker_step(
                    event("slow-build")
                    + "Path(sys.argv[3]).write_text('slow-ok')",
                    root / "mark-slow-build",
                    order,
                    root / "slow.out",
                ),
                "fast": marker_step(
                    event("fast-build")
                    + "generated = Path(sys.argv[3]).read_text()\n"
                    "Path(sys.argv[4]).write_text('fast:' + generated)\n"
                    "Path(sys.argv[5]).write_text('done')\n",
                    root / "mark-fast-build",
                    order,
                    root / "generated.txt",
                    root / "fast.out",
                    fast_done,
                ),
                "generator": marker_step(
                    event("generator-build")
                    + "Path(sys.argv[3]).write_text('header-v1')\n",
                    root / "mark-generator",
                    order,
                    root / "generated.txt",
                ),
            }
            scans = {
                "slow": Step(
                    (PY, "-I", "-c", slow_scan_code,
                     str(order), str(slow_start), str(fast_done)),
                    (),
                    10,
                ),
                "fast": Step(
                    (PY, "-I", "-c", fast_scan_code,
                     str(order), str(slow_start)),
                    (),
                    10,
                ),
            }

            outcomes = execute_discovered(
                plan,
                targets=("slow", "fast"),
                scans=scans,
                workers=2,
                cwd=root,
            )

            self.assertEqual(
                {name: outcomes[name].status
                 for name in ("slow", "fast", "generator")},
                {"slow": "ok", "fast": "ok", "generator": "ok"},
            )
            events = order.read_text().splitlines()
            self.assertLess(
                events.index("slow-scan-start"),
                events.index("fast-scan-saw-start"))
            self.assertLess(
                events.index("fast-scan-saw-start"),
                events.index("generator-build"))
            self.assertLess(
                events.index("generator-build"),
                events.index("fast-build"))
            self.assertLess(
                events.index("fast-build"),
                events.index("slow-scan-finish"))
            self.assertLess(
                events.index("slow-scan-finish"),
                events.index("slow-build"))
            self.assertEqual(
                (root / "fast.out").read_text(), "fast:header-v1")
            self.assertEqual((root / "slow.out").read_text(), "slow-ok")

    def test_scans_and_builds_share_worker_limit(self):
        counter_code = r"""
import json, os, sys, time
from pathlib import Path
stats_path, hold = sys.argv[1], float(sys.argv[2])
lock_dir = stats_path + '.lock'
deadline = time.monotonic() + 8
while True:
    try:
        os.mkdir(lock_dir)
        break
    except FileExistsError:
        if time.monotonic() > deadline:
            raise SystemExit('counter lock timeout')
        time.sleep(0.005)
try:
    try:
        stats = json.loads(Path(stats_path).read_text())
    except FileNotFoundError:
        stats = {'current': 0, 'max': 0}
    stats['current'] += 1
    stats['max'] = max(stats['max'], stats['current'])
    tmp = stats_path + '.tmp'
    Path(tmp).write_text(json.dumps(stats))
    os.replace(tmp, stats_path)
    deps_file = os.environ.get('BUILD_DEPS_FILE')
    if deps_file:
        Path(deps_file).write_text('[]', encoding='utf-8')
finally:
    os.rmdir(lock_dir)
time.sleep(hold)
while True:
    try:
        os.mkdir(lock_dir)
        break
    except FileExistsError:
        time.sleep(0.005)
try:
    stats = json.loads(Path(stats_path).read_text())
    stats['current'] -= 1
    tmp = stats_path + '.tmp'
    Path(tmp).write_text(json.dumps(stats))
    os.replace(tmp, stats_path)
finally:
    os.rmdir(lock_dir)
"""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stats = root / "stats.json"

            def counting_step(name, hold):
                return Step(
                    (PY, "-I", "-c", counter_code, str(stats), str(hold)),
                    (),
                    10,
                )

            plan = {
                f"step{i}": counting_step(f"step{i}", 0.2)
                for i in range(4)
            }
            scans = {
                f"step{i}": counting_step(f"scan{i}", 0.2)
                for i in range(4)
            }
            outcomes = execute_discovered(
                plan,
                targets=tuple(plan),
                scans=scans,
                workers=2,
                cwd=root,
            )
            self.assertTrue(
                all(outcome.status == "ok" for outcome in outcomes.values()))
            stats_data = json.loads(stats.read_text())
            self.assertEqual(stats_data["current"], 0)
            self.assertLessEqual(stats_data["max"], 2)
            self.assertEqual(stats_data["max"], 2)


class DiscoveryErrorAndCleanupTest(unittest.TestCase):
    def test_runtime_manifest_errors_and_temp_cleanup(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            temp_parent = root / "temp-parent"
            temp_parent.mkdir()
            old_tempdir = tempfile.tempdir
            tempfile.tempdir = str(temp_parent)
            self.addCleanup(setattr, tempfile, "tempdir", old_tempdir)

            markers = root / "markers"
            markers.mkdir()
            deps_records = root / "deps-paths.txt"
            record_code = (
                "import os, sys\n"
                "from pathlib import Path\n"
                "with open(sys.argv[1], 'a', encoding='utf-8') as h:\n"
                "    h.write(os.environ['BUILD_DEPS_FILE'] + '\\n')\n"
            )

            def recording_scan(payload_code, *args, timeout=5):
                code = record_code + payload_code
                return Step(
                    (PY, "-I", "-c", code, str(deps_records), *args),
                    (),
                    timeout,
                )

            missing_scan = recording_scan("")
            unknown_scan = recording_scan(
                "import json, os\n"
                "from pathlib import Path\n"
                "Path(os.environ['BUILD_DEPS_FILE']).write_text("
                "'[\"nope\"]')\n"
            )
            bad_json_scan = recording_scan(
                "from pathlib import Path\n"
                "import os\n"
                "Path(os.environ['BUILD_DEPS_FILE']).write_text('{bad')\n"
            )
            nonzero_scan = Step(
                (PY, "-I", "-c",
                 record_code + "raise SystemExit(3)",
                 str(deps_records)),
                (),
            )
            timeout_scan = Step(
                (PY, "-I", "-c",
                 record_code + "import time; time.sleep(5)",
                 str(deps_records)),
                (),
                0.08,
            )
            launch_scan = Step(
                (str(root / "definitely-missing-scan-executable"),), ())

            plan = {
                "missing": marker_step("pass", markers / "build-missing"),
                "unknown": marker_step("pass", markers / "build-unknown"),
                "badjson": marker_step("pass", markers / "build-badjson"),
                "nonzero": marker_step("pass", markers / "build-nonzero"),
                "timeout": marker_step("pass", markers / "build-timeout"),
                "launch": marker_step("pass", markers / "build-launch"),
            }
            scans = {
                "missing": missing_scan,
                "unknown": unknown_scan,
                "badjson": bad_json_scan,
                "nonzero": nonzero_scan,
                "timeout": timeout_scan,
                "launch": launch_scan,
            }

            outcomes = execute_discovered(
                plan,
                targets=tuple(plan),
                scans=scans,
                workers=6,
                cwd=root,
            )

            self.assertEqual(outcomes["missing"].status, "scan_error")
            self.assertIn("manifest_missing", outcomes["missing"].detail)
            self.assertEqual(outcomes["unknown"].status, "scan_error")
            self.assertIn("unknown_name", outcomes["unknown"].detail)
            self.assertEqual(outcomes["badjson"].status, "scan_error")
            self.assertIn("invalid_json", outcomes["badjson"].detail)
            self.assertEqual(outcomes["nonzero"].status, "scan_error")
            self.assertEqual(outcomes["nonzero"].returncode, 3)
            self.assertIn("nonzero_exit", outcomes["nonzero"].detail)
            self.assertEqual(outcomes["timeout"].status, "scan_error")
            self.assertIn("timeout", outcomes["timeout"].detail)
            self.assertIsNone(outcomes["timeout"].returncode)
            self.assertEqual(outcomes["launch"].status, "scan_error")
            self.assertIn("launch_error", outcomes["launch"].detail)

            for name in plan:
                self.assertEqual(
                    launch_count(markers / f"build-{name}"), 0, name)

            recorded_paths = [
                Path(line)
                for line in deps_records.read_text().splitlines()
            ]
            self.assertTrue(recorded_paths)
            for deps_path in recorded_paths:
                self.assertFalse(deps_path.exists())
                self.assertFalse(deps_path.parent.exists())
            self.assertEqual(os.listdir(temp_parent), [])

    def test_build_statuses_and_blocked_by_order_are_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plan = {
                "failed": Step(
                    (PY, "-I", "-c", "raise SystemExit(4)"), (), 5),
                "timeout": Step(
                    (PY, "-I", "-c", "import time; time.sleep(5)"),
                    (), 0.08),
                "launch": Step((str(root / "missing-build"),), (), 5),
                "down": marker_step(
                    "pass", root / "down-started",
                    after=("failed", "timeout", "launch")),
            }
            outcomes = execute_discovered(
                plan,
                targets=("down",),
                workers=3,
                cwd=root,
            )
            self.assertEqual(outcomes["failed"].status, "failed")
            self.assertEqual(outcomes["failed"].returncode, 4)
            self.assertEqual(outcomes["timeout"].status, "timeout")
            self.assertEqual(outcomes["launch"].status, "launch_error")
            self.assertEqual(outcomes["down"].status, "skipped")
            self.assertEqual(
                outcomes["down"].blocked_by,
                ("failed", "timeout", "launch"))
            self.assertEqual(launch_count(root / "down-started"), 0)

    def test_build_inherits_environment_but_not_discovery_variable(self):
        old_deps = os.environ.get("BUILD_DEPS_FILE")
        old_custom = os.environ.get("DISCOVERY_TEST_CUSTOM")
        os.environ["BUILD_DEPS_FILE"] = "ambient-discovery-path"
        os.environ["DISCOVERY_TEST_CUSTOM"] = "sentinel"
        try:
            with tempfile.TemporaryDirectory() as tmp:
                output = Path(tmp) / "env.txt"
                plan = {
                    "build": Step((
                        PY, "-I", "-c",
                        "import json, os, sys\n"
                        "from pathlib import Path\n"
                        "Path(sys.argv[1]).write_text(json.dumps(("
                        "os.environ.get('BUILD_DEPS_FILE'), "
                        "os.environ.get('DISCOVERY_TEST_CUSTOM'))))\n",
                        str(output),
                    )),
                }
                outcomes = execute_discovered(
                    plan, targets=("build",), cwd=tmp)
                self.assertEqual(outcomes["build"].status, "ok")
                deps_value, custom_value = json.loads(output.read_text())
                self.assertIsNone(deps_value)
                self.assertEqual(custom_value, "sentinel")
        finally:
            if old_deps is None:
                os.environ.pop("BUILD_DEPS_FILE", None)
            else:
                os.environ["BUILD_DEPS_FILE"] = old_deps
            if old_custom is None:
                os.environ.pop("DISCOVERY_TEST_CUSTOM", None)
            else:
                os.environ["DISCOVERY_TEST_CUSTOM"] = old_custom

    def test_symlink_manifest_does_not_cause_external_cleanup(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            external = root / "workspace-manifest-target.json"
            external.write_text("[]", encoding="utf-8")
            scan = Step((
                PY, "-I", "-c",
                "import os, sys\n"
                "from pathlib import Path\n"
                "Path(os.environ['BUILD_DEPS_FILE']).symlink_to(sys.argv[1])\n",
                str(external),
            ))
            outcomes = execute_discovered(
                {"s": Step((PY, "-I", "-c", "pass"))},
                targets=("s",),
                scans={"s": scan},
                cwd=root,
            )
            self.assertEqual(outcomes["s"].status, "scan_error")
            self.assertIn("symlink", outcomes["s"].detail)
            self.assertTrue(external.exists())
            self.assertEqual(external.read_text(), "[]")

    def test_symlink_and_oversize_manifest_are_rejected(self):
        from executor import _read_discovery_manifest

        known = frozenset(("a",))
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)

            target = directory / "target"
            target.write_text("[]", encoding="utf-8")
            link = directory / "deps.json"
            link.symlink_to(target)
            deps, detail = _read_discovery_manifest(str(link), known)
            self.assertIsNone(deps)
            self.assertIn("symlink", detail)

            regular = directory / "regular.json"
            regular.write_bytes(b"[" + b'"a",' * 300 + b'"a"]')
            deps, detail = _read_discovery_manifest(str(regular), known)
            self.assertIsNone(deps)
            self.assertTrue(
                "too_many_items" in detail or "duplicate" in detail)

            oversized = directory / "oversized.json"
            oversized.write_bytes(b" " * (64 * 1024 + 1))
            deps, detail = _read_discovery_manifest(str(oversized), known)
            self.assertIsNone(deps)
            self.assertIn("too_large", detail)


if __name__ == "__main__":
    unittest.main()
