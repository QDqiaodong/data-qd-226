"""Offline tests for execute_with_artifacts. Stdlib only; run with -I."""
import json
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import time
import unittest

from executor import Step, execute, execute_with_artifacts

PY = sys.executable


def step(code, after=(), timeout=15):
    return Step((PY, "-c", code), after, timeout)


def producer_code(text):
    return (
        "import os\n"
        f"open(os.environ['BUILD_OUTPUT'], 'w').write({text!r})\n"
    )


READER = (
    "import json, os\n"
    "from pathlib import Path\n"
    "inputs = json.loads(os.environ['BUILD_INPUTS'])\n"
    "data = {k: Path(v).read_bytes().decode() for k, v in inputs.items()}\n"
)


def reader_code(marker):
    return READER + f"Path({marker!r}).write_text(json.dumps(data, sort_keys=True))\n"


class ArtifactPlanTest(unittest.TestCase):
    def test_capacity_8_two_producers_one_reader_rejected_before_launch(self):
        with TemporaryDirectory() as root, TemporaryDirectory() as scratch:
            marker = os.path.join(root, "launched")
            touch = (
                "from pathlib import Path\n"
                f"Path({marker!r}).touch()\n"
            )
            plan = {
                "p1": Step((PY, "-c", touch)),
                "p2": Step((PY, "-c", touch)),
                "reader": Step((PY, "-c", touch), ("p1", "p2")),
            }
            with self.assertRaises(ValueError) as ctx:
                execute_with_artifacts(
                    plan,
                    outputs={"p1": 4, "p2": 4, "reader": 1},
                    uses={"reader": ("p1", "p2")},
                    capacity=8, scratch_dir=scratch, workers=2, cwd=root)
            self.assertTrue(str(ctx.exception).startswith("capacity infeasible"))
            self.assertFalse(os.path.exists(marker))
            # Scratch root is left untouched apart from cleaning our subdir.
            self.assertEqual(os.listdir(scratch), [])

    def test_capacity_7_requires_ordering_that_unlocks_blocking_chain(self):
        # A(3) -> CA; B(4) -> CB(2); CA waits for CB. Feasible order starts
        # with B, although A is first in the input mapping.
        with TemporaryDirectory() as root, TemporaryDirectory() as scratch:
            marker = os.path.join(root, "marker")
            plan = {
                "A": step(producer_code("a")),
                "CA": step(reader_code(marker), ("CB",)),
                "B": step(producer_code("b")),
                "CB": step(producer_code("c")),
            }
            outcomes = execute_with_artifacts(
                plan,
                outputs={"A": 3, "B": 4, "CB": 2},
                uses={"CA": ("A",), "CB": ("B",)},
                capacity=7, scratch_dir=scratch, workers=2, cwd=root)
            for name in plan:
                self.assertEqual(outcomes[name].status, "ok", (name, outcomes[name]))
            self.assertEqual(Path(marker).read_text(), '{"A": "a"}')
            self.assertEqual(os.listdir(scratch), [])

    def test_capacity_9_parallel_producers_rendezvous(self):
        with TemporaryDirectory() as root, TemporaryDirectory() as scratch:
            a_go = os.path.join(root, "a_go")
            b_go = os.path.join(root, "b_go")
            a_seen = os.path.join(root, "a_seen")
            b_seen = os.path.join(root, "b_seen")
            handshake = (
                "import os, time\n"
                "from pathlib import Path\n"
                "mine, other, seen = os.environ['MINE'], os.environ['OTHER'], os.environ['SEEN']\n"
                "Path(mine).touch()\n"
                "deadline = time.monotonic() + 10\n"
                "while not Path(other).exists():\n"
                "    assert time.monotonic() < deadline\n"
                "    time.sleep(0.01)\n"
                "Path(seen).touch()\n"
                "Path(os.environ['BUILD_OUTPUT']).write_text('x')\n"
            )

            def hs(mine, other, seen):
                code = (
                    f"import os; os.environ.update(MINE={mine!r}, OTHER={other!r}, "
                    f"SEEN={seen!r}); exec({handshake!r})")
                return Step((PY, "-c", code), (), 15)

            plan = {
                "A": hs(a_go, b_go, a_seen),
                "B": hs(b_go, a_go, b_seen),
                "CA": step(reader_code(os.path.join(root, "ca.json")), ("A",)),
                "CB": step(producer_code("cb"), ("CA",)),
            }
            outcomes = execute_with_artifacts(
                plan,
                outputs={"A": 3, "B": 4, "CB": 2},
                uses={"CA": ("A",), "CB": ("B",)},
                capacity=9, scratch_dir=scratch, workers=2, cwd=root)
            for name in plan:
                self.assertEqual(outcomes[name].status, "ok", (name, outcomes[name]))
            self.assertTrue(os.path.exists(a_seen) and os.path.exists(b_seen))

    def test_chained_readers_keep_original_bytes(self):
        with TemporaryDirectory() as root, TemporaryDirectory() as scratch:
            m1, m2 = os.path.join(root, "r1"), os.path.join(root, "r2")
            plan = {
                "P": step("import os; open(os.environ['BUILD_OUTPUT'],'wb').write(b'payload-42')"),
                "R1": step(READER + f"Path({m1!r}).write_text(data['P'])\n", ("P",)),
                "R2": step(READER + f"Path({m2!r}).write_text(data['P'])\n", ("R1",)),
            }
            outcomes = execute_with_artifacts(
                plan, outputs={"P": 16},
                uses={"R1": ("P",), "R2": ("P",)},
                capacity=16, scratch_dir=scratch, workers=2, cwd=root)
            self.assertEqual([o.status for o in outcomes.values()],
                             ["ok", "ok", "ok"])
            self.assertEqual(Path(m1).read_text(), "payload-42")
            self.assertEqual(Path(m2).read_text(), "payload-42")
            self.assertEqual(os.listdir(scratch), [])

    def test_parallel_readers_failure_skips_and_releases_to_independent_step(self):
        # Two readers of P rendezvous in the same batch; one fails. A third,
        # not-yet-started consumer gets skipped. Independent producer I (which
        # only waits on P via after, never reads P) may only start once P's
        # reservation is released after every consumer is terminal.
        with TemporaryDirectory() as root, TemporaryDirectory() as scratch:
            r1_flag = os.path.join(root, "r1_here")
            r2_flag = os.path.join(root, "r2_here")
            ind_marker = os.path.join(root, "independent")
            reader = (
                "import json, os, time\n"
                "from pathlib import Path\n"
                "data = json.loads(os.environ['BUILD_INPUTS'])\n"
                "assert Path(data['P']).read_text() == 'z'\n"
                "Path(os.environ['FLAG']).touch()\n"
                "deadline = time.monotonic() + 10\n"
                "while not Path(os.environ['PEER']).exists():\n"
                "    assert time.monotonic() < deadline\n"
                "    time.sleep(0.01)\n"
                "raise SystemExit(int(os.environ['FAIL']))\n"
            )

            def reader_step(flag, peer, fail, after=()):
                code = (
                    f"import os; os.environ.update(FLAG={flag!r}, PEER={peer!r}, "
                    f"FAIL={fail!r}); exec({reader!r})")
                return Step((PY, "-c", code), after, 15)

            plan = {
                "P": step(
                    "import os; from pathlib import Path\n"
                    "Path(os.environ['BUILD_OUTPUT']).write_text('z')"),
                "R1": reader_step(r1_flag, r2_flag, "0"),
                "R2": reader_step(r2_flag, r1_flag, "1"),
                "R3": Step((PY, "-c", "pass"), ("R1", "R2"), 15),
                "I": step(
                    "import os\n"
                    f"from pathlib import Path; Path({ind_marker!r}).touch()\n"
                    "open(os.environ['BUILD_OUTPUT'],'w').write('i')",
                    ("P",)),
            }
            outcomes = execute_with_artifacts(
                plan, outputs={"P": 4, "I": 4},
                uses={"R1": ("P",), "R2": ("P",), "R3": ("P",)},
                capacity=4, scratch_dir=scratch, workers=2, cwd=root)
            self.assertEqual(outcomes["P"].status, "ok")
            self.assertEqual(outcomes["R1"].status, "ok")
            self.assertEqual(outcomes["R2"].status, "failed")
            self.assertEqual(outcomes["R2"].returncode, 1)
            self.assertEqual(outcomes["R3"].status, "skipped")
            self.assertEqual(outcomes["R3"].blocked_by, ("R2",))
            self.assertEqual(outcomes["I"].status, "ok")
            self.assertTrue(os.path.exists(ind_marker))
            self.assertEqual(os.listdir(scratch), [])

    def test_implicit_read_dependency_cycle_rejected(self):
        with TemporaryDirectory() as scratch:
            plan = {
                "X": step("pass"),
                "Y": step("pass"),
            }
            with self.assertRaises(ValueError) as ctx:
                execute_with_artifacts(
                    plan, outputs={"X": 1, "Y": 1},
                    uses={"X": ("Y",), "Y": ("X",)},
                    capacity=10, scratch_dir=scratch)
            self.assertIn("cycle", str(ctx.exception))

    def test_missing_artifact_is_artifact_error_and_blocks_consumers(self):
        with TemporaryDirectory() as root, TemporaryDirectory() as scratch:
            plan = {
                "P": step("pass"),
                "C": step(READER, ("P",)),
            }
            outcomes = execute_with_artifacts(
                plan, outputs={"P": 8}, uses={"C": ("P",)},
                capacity=8, scratch_dir=scratch, cwd=root)
            self.assertEqual(outcomes["P"].status, "artifact_error")
            self.assertIn("no output file", outcomes["P"].detail)
            self.assertEqual(outcomes["C"].status, "skipped")
            self.assertEqual(outcomes["C"].blocked_by, ("P",))
            self.assertEqual(os.listdir(scratch), [])

    def test_oversized_artifact_rejected_and_reservation_released(self):
        with TemporaryDirectory() as root, TemporaryDirectory() as scratch:
            done = os.path.join(root, "done")
            plan = {
                "P": step(
                    "import os; open(os.environ['BUILD_OUTPUT'],'wb').write(b'x' * 10)"),
                "late": step(
                    "import os\n"
                    f"from pathlib import Path; Path({done!r}).touch()\n"
                    "open(os.environ['BUILD_OUTPUT'],'w').write('q')"),
            }
            outcomes = execute_with_artifacts(
                plan, outputs={"P": 4, "late": 6},
                uses={}, capacity=6, scratch_dir=scratch, workers=1, cwd=root)
            self.assertEqual(outcomes["P"].status, "artifact_error")
            self.assertIn("reservation was 4", outcomes["P"].detail)
            self.assertEqual(outcomes["late"].status, "ok")
            self.assertTrue(os.path.exists(done))
            self.assertEqual(os.listdir(scratch), [])

    def test_symlink_artifact_rejected(self):
        with TemporaryDirectory() as scratch:
            plan = {
                "P": step(
                    "import os; os.symlink('/etc/hostname', os.environ['BUILD_OUTPUT'])"),
            }
            outcomes = execute_with_artifacts(
                plan, outputs={"P": 100}, uses={},
                capacity=100, scratch_dir=scratch)
            self.assertEqual(outcomes["P"].status, "artifact_error")
            self.assertIn("symbolic link", outcomes["P"].detail)
            self.assertEqual(os.listdir(scratch), [])

    def test_failed_producer_cleans_half_file_and_releases_capacity(self):
        with TemporaryDirectory() as root, TemporaryDirectory() as scratch:
            marker = os.path.join(root, "ran")
            plan = {
                "P": step(
                    "import os; open(os.environ['BUILD_OUTPUT'],'w').write('half')\n"
                    "raise SystemExit(3)"),
                "Q": step(
                    "import os\n"
                    f"from pathlib import Path; Path({marker!r}).touch()\n"
                    "open(os.environ['BUILD_OUTPUT'],'w').write('ok')"),
            }
            outcomes = execute_with_artifacts(
                plan, outputs={"P": 5, "Q": 5}, uses={},
                capacity=5, scratch_dir=scratch, workers=1, cwd=root)
            self.assertEqual(outcomes["P"].status, "failed")
            self.assertEqual(outcomes["P"].returncode, 3)
            self.assertEqual(outcomes["Q"].status, "ok")
            self.assertTrue(os.path.exists(marker))
            self.assertEqual(os.listdir(scratch), [])

    def test_environment_contract_inputs_output_and_inheritance(self):
        with TemporaryDirectory() as root, TemporaryDirectory() as scratch:
            seen = os.path.join(root, "env.json")
            os.environ["INHERITED_MARKER"] = "yes"
            try:
                plan = {
                    "P": step(
                        "import os; open(os.environ['BUILD_OUTPUT'],'w').write('v')"),
                    "plain": step(
                        "import os, json\n"
                        "assert 'BUILD_OUTPUT' not in os.environ\n"
                        "assert json.loads(os.environ['BUILD_INPUTS']) == {}\n"
                        "assert os.environ['INHERITED_MARKER'] == 'yes'\n"
                        f"open({seen!r},'w').write('ok')"),
                    "C": step(
                        "import json, os\n"
                        "from pathlib import Path\n"
                        "payload = json.loads(os.environ['BUILD_INPUTS'])\n"
                        "assert set(payload) == {'P'} and os.path.isabs(payload['P'])\n"
                        "assert Path(payload['P']).read_text() == 'v'\n"
                        f"open({seen + '.c'!r},'w').write(json.dumps(payload, sort_keys=True))\n",
                        ("P",)),
                }
                outcomes = execute_with_artifacts(
                    plan, outputs={"P": 2},
                    uses={"C": ("P",)},
                    capacity=2, scratch_dir=scratch, cwd=root)
            finally:
                os.environ.pop("INHERITED_MARKER", None)
            self.assertEqual([o.status for o in outcomes.values()],
                             ["ok", "ok", "ok"], outcomes)
            payload = json.loads(Path(seen + ".c").read_text())
            self.assertEqual(set(payload), {"P"})
            self.assertTrue(os.path.isabs(payload["P"]))
            self.assertEqual(Path(seen).read_text(), "ok")

    def test_parent_build_variables_do_not_leak(self):
        with TemporaryDirectory() as root, TemporaryDirectory() as scratch:
            probe = os.path.join(root, "probe")
            os.environ["BUILD_OUTPUT"] = "/should/not/leak"
            os.environ["BUILD_INPUTS"] = '{"stale": "/old"}'
            try:
                plan = {
                    "P": step(
                        "import os\n"
                        "assert os.environ['BUILD_OUTPUT'] != '/should/not/leak'\n"
                        "open(os.environ['BUILD_OUTPUT'], 'w').write('v')"),
                    "plain": step(
                        "import os, json\n"
                        "assert 'BUILD_OUTPUT' not in os.environ\n"
                        "assert json.loads(os.environ['BUILD_INPUTS']) == {}\n"
                        f"open({probe!r}, 'w').write('clean')"),
                }
                outcomes = execute_with_artifacts(
                    plan, outputs={"P": 2}, uses={},
                    capacity=2, scratch_dir=scratch, cwd=root)
            finally:
                os.environ.pop("BUILD_OUTPUT", None)
                os.environ.pop("BUILD_INPUTS", None)
            self.assertEqual([o.status for o in outcomes.values()],
                             ["ok", "ok"], outcomes)
            self.assertEqual(Path(probe).read_text(), "clean")

    def test_timeout_and_skip_branch_with_artifacts(self):
        with TemporaryDirectory() as scratch:
            plan = {
                "slow": step("import time; time.sleep(10)", timeout=0.06),
                "child": step("pass", ("slow",)),
                "free": step("pass"),
            }
            outcomes = execute_with_artifacts(
                plan, outputs={}, uses={},
                capacity=0, scratch_dir=scratch)
            self.assertEqual(outcomes["slow"].status, "timeout")
            self.assertEqual(outcomes["child"].status, "skipped")
            self.assertEqual(outcomes["child"].blocked_by, ("slow",))
            self.assertEqual(outcomes["free"].status, "ok")
            self.assertEqual(os.listdir(scratch), [])

    def test_blocked_by_order_after_then_uses_deduped(self):
        with TemporaryDirectory() as scratch:
            plan = {
                "P": step("raise SystemExit(1)"),
                "D": step("raise SystemExit(2)"),
                "C": step("pass", ("D", "P")),
            }
            outcomes = execute_with_artifacts(
                plan, outputs={"P": 1}, uses={"C": ("P",)},
                capacity=1, scratch_dir=scratch)
            self.assertEqual(outcomes["C"].status, "skipped")
            self.assertEqual(outcomes["C"].blocked_by, ("D", "P"))

    def test_unconsumed_artifact_removed_when_producer_finishes(self):
        with TemporaryDirectory() as scratch:
            marker = os.path.join(scratch, "marker_should_not_exist_in_root")
            plan = {
                "P": step(
                    "import os; open(os.environ['BUILD_OUTPUT'],'w').write('x')"),
            }
            outcomes = execute_with_artifacts(
                plan, outputs={"P": 4}, uses={},
                capacity=4, scratch_dir=scratch)
            self.assertEqual(outcomes["P"].status, "ok")
            self.assertEqual(os.listdir(scratch), [])
            self.assertFalse(os.path.exists(marker))

    def test_validation_errors(self):
        good = step("pass")
        with TemporaryDirectory() as scratch:
            base = dict(plan={"s": good}, outputs={}, uses={},
                        capacity=1, scratch_dir=scratch)
            with self.assertRaises(ValueError):
                execute_with_artifacts(
                    {f"s{i}": good for i in range(19)},
                    outputs={}, uses={}, capacity=1, scratch_dir=scratch)
            with self.assertRaises(ValueError):
                execute_with_artifacts(**{**base, "capacity": True})
            with self.assertRaises(ValueError):
                execute_with_artifacts(**{**base, "outputs": {"s": True}})
            with self.assertRaises(ValueError):
                execute_with_artifacts(**{**base, "outputs": {"s": 0}})
            with self.assertRaises(ValueError):
                execute_with_artifacts(**{**base, "outputs": {"nope": 1}})
            with self.assertRaises(ValueError):
                execute_with_artifacts(**{**base, "uses": {"s": ("s",)}})
            with self.assertRaises(ValueError):
                execute_with_artifacts(
                    {"s": good, "t": good}, outputs={"t": 1},
                    uses={"s": ("t",), "t": ("t",)},
                    capacity=1, scratch_dir=scratch)
            with self.assertRaises(ValueError):
                execute_with_artifacts(
                    {"s": good}, outputs={"s": 1},
                    uses={"s": ("s",)}, capacity=1, scratch_dir=scratch)
            with self.assertRaises(ValueError):
                execute_with_artifacts(
                    {"s": good}, outputs={"s": 1}, uses={},
                    capacity=1, scratch_dir=scratch, workers=0)
            with self.assertRaises(ValueError):
                execute_with_artifacts(
                    {"s": good}, outputs={"s": 1}, uses={},
                    capacity=1, scratch_dir=os.path.join(scratch, "missing"))

    def test_zero_capacity_runs_plan_without_producers(self):
        with TemporaryDirectory() as scratch:
            outcomes = execute_with_artifacts(
                {"s": step("pass")}, outputs={}, uses={},
                capacity=0, scratch_dir=scratch)
            self.assertEqual(outcomes["s"].status, "ok")
            self.assertEqual(os.listdir(scratch), [])

    def test_concurrent_producers_keep_their_own_artifacts(self):
        # Regression: finishing one concurrent producer must never remove or
        # invalidate another producer's scratch file.
        producer = (
            "import os\n"
            "from pathlib import Path\n"
            "Path(os.environ['BUILD_OUTPUT']).write_text('x')\n"
        )
        for _ in range(25):
            with TemporaryDirectory() as root, TemporaryDirectory() as scratch:
                plan = {
                    f"p{i}": Step((PY, "-c", f"import os; exec({producer!r})"),
                                  (), 10)
                    for i in range(3)}
                outcomes = execute_with_artifacts(
                    plan, outputs={f"p{i}": 1 for i in range(3)},
                    uses={}, capacity=3, scratch_dir=scratch,
                    workers=3, cwd=root)
                self.assertEqual(
                    {n: o.status for n, o in outcomes.items()},
                    {f"p{i}": "ok" for i in range(3)})
                self.assertEqual(os.listdir(scratch), [])

    def test_failure_branch_does_not_deadlock_unrelated_chain(self):
        with TemporaryDirectory() as root, TemporaryDirectory() as scratch:
            marker = os.path.join(root, "survivor")
            plan = {
                "bad": step(
                    "import os; open(os.environ['BUILD_OUTPUT'],'w').write('x')\n"
                    "raise SystemExit(4)"),
                "orphan": step("raise SystemExit(5)", ("bad",)),
                "good": step(
                    "import os\n"
                    f"from pathlib import Path; Path({marker!r}).touch()\n"
                    "open(os.environ['BUILD_OUTPUT'],'w').write('g')"),
            }
            outcomes = execute_with_artifacts(
                plan, outputs={"bad": 2, "good": 2}, uses={},
                capacity=2, scratch_dir=scratch, workers=2, cwd=root)
            self.assertEqual(outcomes["bad"].status, "failed")
            self.assertEqual(outcomes["orphan"].status, "skipped")
            self.assertEqual(outcomes["orphan"].blocked_by, ("bad",))
            self.assertEqual(outcomes["good"].status, "ok")
            self.assertTrue(os.path.exists(marker))
            self.assertEqual(os.listdir(scratch), [])


class OriginalEntryStillWorks(unittest.TestCase):
    def test_execute_behavior_unchanged(self):
        with TemporaryDirectory() as root:
            outcomes = execute({
                "slow": Step((PY, "-c", "import time; time.sleep(10)"), (), 0.06),
                "child": Step((PY, "-c", "pass"), ("slow",)),
            }, cwd=root)
            self.assertEqual(outcomes["slow"].status, "timeout")
            self.assertEqual(outcomes["child"].status, "skipped")


if __name__ == "__main__":
    unittest.main(verbosity=2)
