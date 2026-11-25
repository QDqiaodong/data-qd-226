"""Offline acceptance tests for content-addressed incremental execution.

Run from the project directory:

    python3 -I -m unittest -v test_incremental

The module also has small command-line driver modes used to exercise a fresh
Python process and a Linux RLIMIT_AS boundary.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from executor import Step, execute_incremental  # noqa: E402

PY = sys.executable
PROJECT = Path(__file__).resolve().parent
CONTEXT = "incremental-acceptance-v1"
LARGE_SIZE = 192 * 1024 * 1024


def counted_step(name, body, after=(), timeout=20):
    code = f"""
from pathlib import Path
_count_path = Path({f"count-{name}"!r})
_count_path.write_text(
    str(int(_count_path.read_text()) + 1) if _count_path.exists() else "1")
{body}
"""
    return Step((PY, "-c", code), tuple(after), timeout)


def count_value(root, name):
    path = Path(root) / f"count-{name}"
    return int(path.read_text()) if path.exists() else 0


GENERATOR_BODY = """
from pathlib import Path
mode = Path("gen-mode").read_text().strip()
doc = "doc:" + Path("input.txt").read_text()
if mode == "omit-bin":
    Path("gen-doc").write_text(doc)
else:
    Path("gen-bin").write_bytes(b"binary-v1\\n")
    Path("gen-doc").write_text(doc)
"""

BINARY_BODY = """
from pathlib import Path
data = Path("gen-bin").read_bytes()
assert data == b"binary-v1\\n"
Path("bin-delivered").write_bytes(data)
"""

DOC_BODY = """
from pathlib import Path
data = Path("gen-doc").read_text()
assert data.startswith("doc:")
Path("doc-delivered").write_text(data)
"""

CHECK_BODY = """
from pathlib import Path
if Path("check-control").read_text().strip() != "pass":
    raise SystemExit(7)
"""

ACCEPTANCE_SOURCES = {
    "gen": ("input.txt", "gen-mode"),
    "check": ("check-control",),
}
ACCEPTANCE_OUTPUTS = {
    "gen": ("gen-bin", "gen-doc"),
    "bin": ("bin-delivered",),
    "doc": ("doc-delivered",),
}
ACCEPTANCE_READS = {
    "bin": (("gen", "gen-bin"),),
    "doc": (("gen", "gen-doc"),),
}


def acceptance_plan(with_check=False):
    plan = {
        "gen": counted_step("gen", GENERATOR_BODY),
        "bin": counted_step(
            "bin", BINARY_BODY, after=("check",) if with_check else ()),
        "doc": counted_step("doc", DOC_BODY),
    }
    if with_check:
        plan["check"] = counted_step("check", CHECK_BODY)
    return plan


def seed_acceptance_workspace(root):
    root = Path(root)
    (root / "input.txt").write_text("input-v1")
    (root / "gen-mode").write_text("write")
    (root / "check-control").write_text("pass")


def run_acceptance(
        root, state, *, with_check=False, targets=("bin", "doc"),
        workers=2, context=CONTEXT):
    sources = {
        "gen": ACCEPTANCE_SOURCES["gen"],
    }
    if with_check:
        sources["check"] = ACCEPTANCE_SOURCES["check"]
    return execute_incremental(
        acceptance_plan(with_check=with_check),
        targets=targets,
        sources=sources,
        outputs=ACCEPTANCE_OUTPUTS,
        reads=ACCEPTANCE_READS,
        context=context,
        state_file=state,
        workers=workers,
        cwd=root,
    )


def warm_driver(root_text, state_text):
    result = run_acceptance(
        Path(root_text), Path(state_text), with_check=False,
        targets=("bin", "doc"))
    statuses = {name: outcome.status for name, outcome in result.outcomes.items()}
    if statuses != {"gen": "reused", "bin": "reused", "doc": "reused"}:
        raise AssertionError(statuses)
    print("WARM_DRIVER_OK", result.source_bytes_read)


def large_driver(root_text, state_text, size_text):
    root = Path(root_text)
    size = int(size_text)
    names = tuple(f"share-{i}" for i in range(4))
    plan = {name: Step((PY, "-c", "pass")) for name in names}
    result = execute_incremental(
        plan,
        targets=names,
        sources={name: ("big.dat",) for name in names},
        outputs={},
        reads={},
        context="large-source-boundary",
        state_file=Path(state_text),
        workers=4,
        cwd=root,
    )
    statuses = {name: outcome.status for name, outcome in result.outcomes.items()}
    if any(status != "ok" for status in statuses.values()):
        raise AssertionError(statuses)
    if result.source_bytes_read != size:
        raise AssertionError(
            f"read {result.source_bytes_read}, expected {size}")
    print("LARGE_DRIVER_OK", result.source_bytes_read)


class IncrementalAcceptanceTest(unittest.TestCase):
    def test_cold_warm_content_change_fresh_process_and_after_gate(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "work"
            state_dir = Path(tmp) / "state"
            root.mkdir()
            state_dir.mkdir()
            state = state_dir / "incremental.json"
            seed_acceptance_workspace(root)

            cold = run_acceptance(root, state)
            self.assertEqual(
                {n: o.status for n, o in cold.outcomes.items()},
                {"gen": "ok", "bin": "ok", "doc": "ok"})
            self.assertEqual(cold.source_bytes_read,
                             len("input-v1") + len("write"))
            self.assertEqual(count_value(root, "gen"), 1)
            self.assertEqual(count_value(root, "bin"), 1)
            self.assertEqual(count_value(root, "doc"), 1)
            self.assertEqual((root / "bin-delivered").read_bytes(),
                             b"binary-v1\n")
            self.assertEqual((root / "doc-delivered").read_text(),
                             "doc:input-v1")

            warm = run_acceptance(root, state)
            self.assertEqual(
                [o.status for o in warm.outcomes.values()],
                ["reused", "reused", "reused"])
            self.assertEqual((count_value(root, "gen"),
                              count_value(root, "bin"),
                              count_value(root, "doc")), (1, 1, 1))

            # Equal-length source edit, with timestamps restored.  Only the
            # document fingerprint changes.
            source = root / "input.txt"
            old_stat = source.stat()
            source.write_text("INPUT-v1")
            os.utime(source, ns=(old_stat.st_atime_ns, old_stat.st_mtime_ns))

            changed = run_acceptance(root, state)
            self.assertEqual(changed.outcomes["gen"].status, "ok")
            self.assertEqual(changed.outcomes["doc"].status, "ok")
            self.assertEqual(changed.outcomes["bin"].status, "reused")
            self.assertEqual((count_value(root, "gen"),
                              count_value(root, "bin"),
                              count_value(root, "doc")), (2, 1, 2))
            self.assertEqual((root / "doc-delivered").read_text(),
                             "doc:INPUT-v1")
            self.assertEqual((root / "bin-delivered").read_bytes(),
                             b"binary-v1\n")

            # A genuinely fresh Python process uses the committed state.
            fresh = subprocess.run(
                [PY, "-I", str(PROJECT / "test_incremental.py"),
                 "--warm-driver", str(root), str(state)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=30,
                check=False,
            )
            self.assertEqual(
                fresh.returncode, 0,
                fresh.stdout + "\n" + fresh.stderr)
            self.assertIn("WARM_DRIVER_OK", fresh.stdout)
            self.assertEqual((count_value(root, "gen"),
                              count_value(root, "bin"),
                              count_value(root, "doc")), (2, 1, 2))

            # Corrupt only the generator output on disk.  Its key remains
            # valid, so output byte verification must force regeneration.
            binary_output = root / "gen-bin"
            binary_output.write_bytes(b"Z" * binary_output.stat().st_size)
            repaired = run_acceptance(root, state)
            self.assertEqual(repaired.outcomes["gen"].status, "ok")
            self.assertEqual(repaired.outcomes["bin"].status, "reused")
            self.assertEqual(repaired.outcomes["doc"].status, "reused")
            self.assertEqual((count_value(root, "gen"),
                              count_value(root, "bin"),
                              count_value(root, "doc")), (3, 1, 2))
            self.assertEqual(binary_output.read_bytes(), b"binary-v1\n")

            # Add an output-less after-only check.
            gated = run_acceptance(root, state, with_check=True)
            self.assertEqual(gated.outcomes["check"].status, "ok")
            self.assertEqual(gated.outcomes["gen"].status, "reused")
            self.assertEqual(gated.outcomes["bin"].status, "reused")
            self.assertEqual(gated.outcomes["doc"].status, "reused")
            self.assertEqual(count_value(root, "check"), 1)

            (root / "check-control").write_text("fail")
            failed_check = run_acceptance(root, state, with_check=True)
            self.assertEqual(failed_check.outcomes["check"].status, "failed")
            self.assertEqual(failed_check.outcomes["check"].returncode, 7)
            self.assertEqual(
                failed_check.outcomes["bin"].status, "skipped")
            self.assertEqual(
                failed_check.outcomes["bin"].blocked_by, ("check",))
            self.assertEqual(failed_check.outcomes["doc"].status, "reused")
            self.assertEqual(count_value(root, "bin"), 1)

            (root / "check-control").write_text("pass")
            restored_check = run_acceptance(root, state, with_check=True)
            self.assertEqual(restored_check.outcomes["check"].status, "ok")
            self.assertEqual(restored_check.outcomes["bin"].status, "reused")
            self.assertEqual(count_value(root, "bin"), 1)

            # Zero exit while omitting one old output cannot succeed.  The
            # executor removed the old path before launching the command.
            (root / "gen-mode").write_text("omit-bin")
            missing_output = run_acceptance(root, state, with_check=True)
            self.assertEqual(
                missing_output.outcomes["gen"].status, "output_error")
            self.assertEqual(
                missing_output.outcomes["gen"].returncode, 0)
            self.assertIn("gen-bin",
                          missing_output.outcomes["gen"].detail)
            self.assertFalse((root / "gen-bin").exists())
            self.assertEqual(
                missing_output.outcomes["bin"].status, "skipped")
            self.assertEqual(
                missing_output.outcomes["bin"].blocked_by, ("gen",))
            self.assertEqual(
                missing_output.outcomes["doc"].status, "skipped")
            self.assertEqual(
                missing_output.outcomes["doc"].blocked_by, ("gen",))

            (root / "gen-mode").write_text("write")
            restored_producer = run_acceptance(root, state, with_check=True)
            self.assertEqual(restored_producer.outcomes["gen"].status, "ok")
            self.assertIn(
                restored_producer.outcomes["bin"].status, ("ok", "reused"))
            self.assertIn(
                restored_producer.outcomes["doc"].status, ("ok", "reused"))
            self.assertEqual((root / "bin-delivered").read_bytes(),
                             b"binary-v1\n")
            self.assertEqual((root / "doc-delivered").read_text(),
                             "doc:INPUT-v1")

    @unittest.skipUnless(sys.platform.startswith("linux"),
                         "RLIMIT_AS acceptance is Linux-specific")
    def test_shared_large_source_under_128_mib_address_limit(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "work"
            state_dir = Path(tmp) / "state"
            root.mkdir()
            state_dir.mkdir()
            # truncate creates a sparse file; reads still traverse all bytes.
            with open(root / "big.dat", "wb") as handle:
                handle.truncate(LARGE_SIZE)
            state = state_dir / "large-state.json"

            def limit_address_space():
                import resource
                resource.setrlimit(
                    resource.RLIMIT_AS,
                    (128 * 1024 * 1024, 128 * 1024 * 1024))

            completed = subprocess.run(
                [PY, "-I", str(PROJECT / "test_incremental.py"),
                 "--large-driver", str(root), str(state), str(LARGE_SIZE)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=120,
                preexec_fn=limit_address_space,
                check=False,
            )
            self.assertEqual(
                completed.returncode, 0,
                completed.stdout.decode("utf-8", "replace")
                + completed.stderr.decode("utf-8", "replace"))
            self.assertIn(b"LARGE_DRIVER_OK", completed.stdout)

    def test_independent_commands_can_rendezvous_in_parallel(self):
        handshake = """
from pathlib import Path
import time
mine, other = {mine!r}, {other!r}
Path(mine).touch()
deadline = time.monotonic() + 4
while not Path(other).exists():
    assert time.monotonic() < deadline, "handshake timed out"
    time.sleep(0.01)
Path({done!r}).touch()
"""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plan = {
                "a": Step((PY, "-c", handshake.format(
                    mine="a-start", other="b-start", done="a-done")),
                    timeout=8),
                "b": Step((PY, "-c", handshake.format(
                    mine="b-start", other="a-start", done="b-done")),
                    timeout=8),
            }
            result = execute_incremental(
                plan,
                targets=("a", "b"),
                sources={},
                outputs={},
                reads={},
                context="parallel-handshake",
                state_file=root / "state.json",
                workers=2,
                cwd=root,
            )
            self.assertEqual(
                [o.status for o in result.outcomes.values()], ["ok", "ok"])
            self.assertTrue((root / "a-done").exists())
            self.assertTrue((root / "b-done").exists())

    def test_unselected_branch_is_neither_run_nor_source_read(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = root / "state.json"
            (root / "unselected-source").write_bytes(b"abc")
            marker = root / "chosen-marker"
            forbidden = root / "forbidden-marker"
            plan = {
                "chosen": Step((PY, "-c",
                               "from pathlib import Path; Path('chosen-marker').touch()")),
                "branch": Step((PY, "-c",
                                "from pathlib import Path; Path('forbidden-marker').touch()")),
            }
            result = execute_incremental(
                plan,
                targets=("chosen",),
                sources={"branch": ("unselected-source",)},
                outputs={},
                reads={},
                context="unselected",
                state_file=state,
                cwd=root,
            )
            self.assertEqual(list(result.outcomes), ["chosen"])
            self.assertEqual(result.outcomes["chosen"].status, "ok")
            self.assertEqual(result.source_bytes_read, 0)
            self.assertTrue(marker.exists())
            self.assertFalse(forbidden.exists())

    def test_independent_branch_missing_source_is_input_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plan = {
                "missing": Step((PY, "-c", "pass")),
                "okay": Step((PY, "-c", "pass")),
            }
            result = execute_incremental(
                plan,
                targets=("missing", "okay"),
                sources={"missing": ("absent.dat",)},
                outputs={},
                reads={},
                context="missing-source",
                state_file=root / "state.json",
                cwd=root,
            )
            self.assertEqual(result.outcomes["missing"].status, "input_error")
            self.assertIn("absent.dat", result.outcomes["missing"].detail)
            self.assertEqual(result.outcomes["okay"].status, "ok")

    def test_reverse_plan_order_propagates_skips_without_scheduler_stall(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            # Deliberately put downstream names before their dependencies.
            plan = {
                "consumer": Step((PY, "-c", "pass")),
                "producer": Step((PY, "-c", "pass"), after=("check",)),
                "check": Step((PY, "-c", "raise SystemExit(4)")),
            }
            result = execute_incremental(
                plan,
                targets=("consumer",),
                sources={},
                outputs={"producer": ("p.out",)},
                reads={"consumer": (("producer", "p.out"),)},
                context="reverse-order",
                state_file=root / "state.json",
                workers=2,
                cwd=root,
            )
            self.assertEqual(list(result.outcomes),
                             ["consumer", "producer", "check"])
            self.assertEqual(result.outcomes["check"].status, "failed")
            self.assertEqual(result.outcomes["producer"].status, "skipped")
            self.assertEqual(result.outcomes["producer"].blocked_by, ("check",))
            self.assertEqual(result.outcomes["consumer"].status, "skipped")
            self.assertEqual(result.outcomes["consumer"].blocked_by,
                             ("producer",))

    def test_timeout_launch_error_and_independent_progress(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            missing_executable = root / "missing-executable"
            plan = {
                "slow": Step((PY, "-c", "import time; time.sleep(5)"),
                             timeout=0.05),
                "child": Step((PY, "-c", "pass"), after=("slow",)),
                "launch-bad": Step((str(missing_executable),)),
            }
            result = execute_incremental(
                plan,
                targets=("child", "launch-bad"),
                sources={},
                outputs={},
                reads={},
                context="process-statuses",
                state_file=root / "state.json",
                workers=2,
                cwd=root,
            )
            self.assertEqual(result.outcomes["slow"].status, "timeout")
            self.assertEqual(result.outcomes["child"].status, "skipped")
            self.assertEqual(result.outcomes["child"].blocked_by, ("slow",))
            self.assertEqual(
                result.outcomes["launch-bad"].status, "launch_error")

    def test_output_path_is_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "out-dir").mkdir()
            plan = {"p": Step((PY, "-c", "pass"))}
            result = execute_incremental(
                plan,
                targets=("p",),
                sources={},
                outputs={"p": ("out-dir",)},
                reads={},
                context="directory-output",
                state_file=root / "state.json",
                cwd=root,
            )
            self.assertEqual(result.outcomes["p"].status, "output_error")
            self.assertIn("out-dir", result.outcomes["p"].detail)

    def test_no_output_step_always_runs_even_with_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plan = {"check": counted_step("check", "pass")}
            kwargs = dict(
                targets=("check",), sources={}, outputs={}, reads={},
                context="no-output", state_file=root / "state.json", cwd=root)
            first = execute_incremental(plan, **kwargs)
            second = execute_incremental(plan, **kwargs)
            self.assertEqual(first.outcomes["check"].status, "ok")
            self.assertEqual(second.outcomes["check"].status, "ok")
            self.assertEqual(count_value(root, "check"), 2)
            state = json.loads((root / "state.json").read_text())
            self.assertNotIn("check", state["records"])


class IncrementalValidationAndStateTest(unittest.TestCase):
    def call_kwargs(self, root, plan=None, state_name="state.json"):
        if plan is None:
            plan = {"a": Step((PY, "-c", "pass"))}
        return dict(
            plan=plan,
            targets=("a",),
            sources={},
            outputs={},
            reads={},
            context="validation-context",
            state_file=Path(root) / state_name,
            workers=2,
            cwd=root,
        )

    def test_invalid_paths_targets_workers_and_context_raise_value_error(self):
        invalid_paths = (
            "/absolute", "", "trailing/", "double//segment", "./dot",
            "parent/../escape", "../escape", "a/./b", "x" * 257,
        )
        with tempfile.TemporaryDirectory() as tmp:
            for bad_path in invalid_paths:
                with self.subTest(bad_path=bad_path):
                    kwargs = self.call_kwargs(tmp)
                    kwargs["outputs"] = {"a": (bad_path,)}
                    with self.assertRaises(ValueError):
                        execute_incremental(**kwargs)

            for workers in (True, False, 0, 9, 1.5, "2"):
                with self.subTest(workers=workers):
                    kwargs = self.call_kwargs(tmp)
                    kwargs["workers"] = workers
                    with self.assertRaises(ValueError):
                        execute_incremental(**kwargs)

            for context in ("", "x" * 257):
                with self.subTest(context=len(context)):
                    kwargs = self.call_kwargs(tmp)
                    kwargs["context"] = context
                    with self.assertRaises(ValueError):
                        execute_incremental(**kwargs)

            with self.assertRaises(ValueError):
                execute_incremental(**{
                    **self.call_kwargs(tmp), "targets": ()})
            with self.assertRaises(ValueError):
                execute_incremental(**{
                    **self.call_kwargs(tmp), "targets": ("a", "a")})
            with self.assertRaises(ValueError):
                execute_incremental(**{
                    **self.call_kwargs(tmp), "targets": ("missing",)})

    def test_declaration_conflicts_and_bad_reads(self):
        with tempfile.TemporaryDirectory() as tmp:
            plan = {
                "a": Step((PY, "-c", "pass")),
                "b": Step((PY, "-c", "pass")),
            }

            cases = [
                dict(outputs={"a": ("x",), "b": ("x",)}),
                dict(sources={"b": ("x",)}, outputs={"a": ("x",)}),
                dict(outputs={"a": ("dir", "dir/file")}),
                dict(outputs={"a": ("a.out",)},
                     reads={"b": (("a", "missing.out"),)}),
                dict(outputs={"a": ("a.out",)},
                     reads={"b": (("missing", "a.out"),)}),
                dict(outputs={"a": ("a.out",)},
                     reads={"b": (("a", "a.out"), ("a", "a.out"))}),
                dict(sources={"unknown": ("x",)}),
                dict(outputs={"unknown": ("x",)}),
                dict(reads={"unknown": ()}),
            ]
            for changes in cases:
                with self.subTest(changes=changes):
                    kwargs = self.call_kwargs(tmp, plan=plan)
                    for key, value in changes.items():
                        kwargs[key] = value
                    with self.assertRaises(ValueError):
                        execute_incremental(**kwargs)

    def test_full_plan_and_cross_edge_cycle_reject_before_side_effects(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            marker = root / "started"
            state = root / "state.json"
            touch = (
                "from pathlib import Path\n"
                f"Path({str(marker)!r}).touch()\n")
            plan = {
                "a": Step((PY, "-c", touch), after=("b",)),
                "b": Step((PY, "-c", touch)),
                "unselected-bad": Step((PY, "-c", "pass")),
            }
            with self.assertRaises(ValueError):
                execute_incremental(
                    plan,
                    targets=("a",),
                    sources={},
                    outputs={"a": ("a.out",)},
                    reads={"b": (("a", "a.out"),)},
                    context="cycle",
                    state_file=state,
                    cwd=root,
                )
            self.assertFalse(marker.exists())
            self.assertFalse(state.exists())

            # An invalid declaration in an unselected step is still fatal.
            with self.assertRaises(ValueError):
                execute_incremental(
                    plan,
                    targets=("a",),
                    sources={"unselected-bad": ("../escape",)},
                    outputs={"a": ("a.out",)},
                    reads={"b": (("a", "a.out"),)},
                    context="cycle",
                    state_file=state,
                    cwd=root,
                )

    def test_corrupt_version_or_wrong_workspace_state_is_value_error(self):
        state_payloads = [
            "{not json",
            {"version": 2, "workspace": "/work", "records": {}},
            {"version": "1", "workspace": "/work", "records": {}},
            {"version": 1, "workspace": "/different/workspace",
             "records": {}},
            {"version": 1, "workspace": None, "records": {}},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "work"
            state_dir = Path(tmp) / "state"
            root.mkdir()
            state_dir.mkdir()
            marker = root / "started"
            plan = {"a": Step((
                PY, "-c",
                "from pathlib import Path\n"
                f"Path({str(marker)!r}).touch()\n"))}

            for index, payload in enumerate(state_payloads):
                state = state_dir / f"state-{index}.json"
                if isinstance(payload, str):
                    state.write_text(payload)
                else:
                    state.write_text(json.dumps(payload))
                with self.subTest(payload=payload):
                    with self.assertRaises(ValueError):
                        execute_incremental(
                            plan,
                            targets=("a",),
                            sources={},
                            outputs={},
                            reads={},
                            context="bad-state",
                            state_file=state,
                            cwd=root,
                        )
                    self.assertFalse(marker.exists())

    def test_failure_clears_failed_record_but_skipped_record_survives(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = root / "state.json"
            producer = Step((PY, "-c",
                             "from pathlib import Path\n"
                             "Path('p.out').write_bytes(b'v')\n"))
            consumer = Step((PY, "-c",
                             "from pathlib import Path\n"
                             "Path('c.out').write_bytes(Path('p.out').read_bytes())\n"))
            kwargs = dict(
                targets=("p", "c"),
                sources={},
                outputs={"p": ("p.out",), "c": ("c.out",)},
                reads={"c": (("p", "p.out"),)},
                context="retention",
                state_file=state,
                cwd=root,
            )
            first = execute_incremental(
                {"p": producer, "c": consumer}, **kwargs)
            self.assertEqual(first.outcomes["p"].status, "ok")
            self.assertEqual(first.outcomes["c"].status, "ok")

            failing = Step((PY, "-c", "raise SystemExit(3)"))
            second = execute_incremental(
                {"p": failing, "c": consumer}, **kwargs)
            self.assertEqual(second.outcomes["p"].status, "failed")
            self.assertEqual(second.outcomes["c"].status, "skipped")
            persisted = json.loads(state.read_text())["records"]
            self.assertNotIn("p", persisted)
            self.assertIn("c", persisted)

    def test_unselected_and_removed_plan_records_are_cleaned_as_specified(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = root / "state.json"
            old = Step((PY, "-c",
                        "from pathlib import Path; Path('old.out').write_text('x')"))
            keep = Step((PY, "-c",
                         "from pathlib import Path; Path('keep.out').write_text('y')"))
            base = dict(
                targets=("old", "keep"),
                sources={},
                outputs={"old": ("old.out",), "keep": ("keep.out",)},
                reads={},
                context="cleanup",
                state_file=state,
                cwd=root,
            )
            execute_incremental({"old": old, "keep": keep}, **base)

            # Selecting only keep leaves old's record on disk.
            execute_incremental(
                {"old": old, "keep": keep}, **{**base, "targets": ("keep",)})
            records = json.loads(state.read_text())["records"]
            self.assertIn("old", records)
            self.assertIn("keep", records)

            # A later plan without old may clean its stale name.
            execute_incremental(
                {"keep": keep},
                targets=("keep",),
                sources={},
                outputs={"keep": ("keep.out",)},
                reads={},
                context="cleanup",
                state_file=state,
                cwd=root,
            )
            records = json.loads(state.read_text())["records"]
            self.assertNotIn("old", records)
            self.assertIn("keep", records)


if __name__ == "__main__":
    if len(sys.argv) >= 2 and sys.argv[1] == "--warm-driver":
        warm_driver(sys.argv[2], sys.argv[3])
    elif len(sys.argv) >= 2 and sys.argv[1] == "--large-driver":
        large_driver(sys.argv[2], sys.argv[3], sys.argv[4])
    else:
        unittest.main(verbosity=2)
