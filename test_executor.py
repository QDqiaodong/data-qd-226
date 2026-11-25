from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

from executor import Step, execute


def cmd(code, after=(), timeout=5):
    return Step((sys.executable, "-c", code), after, timeout)


class ExecutorTest(unittest.TestCase):
    def test_dependencies_and_independent_progress(self):
        with TemporaryDirectory() as root:
            outcomes = execute({
                "package": cmd("from pathlib import Path; assert Path('binary').read_bytes() == b'v1'",
                               ("compile",)),
                "stage": cmd("from pathlib import Path; Path('staged').touch()", ("package", "lint")),
                "compile": cmd("from pathlib import Path; Path('binary').write_bytes(b'v1')"),
                "lint": cmd("raise SystemExit(9)"),
                "deploy": cmd("raise SystemExit(99)", ("stage",)),
            }, cwd=root)
            self.assertEqual(outcomes["package"].status, "ok")
            self.assertEqual(outcomes["lint"].returncode, 9)
            self.assertEqual(outcomes["stage"].blocked_by, ("lint",))
            self.assertEqual(outcomes["deploy"].blocked_by, ("stage",))
            self.assertFalse((Path(root) / "staged").exists())

    def test_parallel_steps_can_rendezvous(self):
        code = ("import sys, time\nfrom pathlib import Path\n"
                "mine, other = map(Path, sys.argv[1:])\nmine.touch()\n"
                "while not other.exists(): time.sleep(.01)\n")
        with TemporaryDirectory() as root:
            outcomes = execute({a: Step((sys.executable, "-c", code, a, b), timeout=5)
                                for a, b in [("a", "b"), ("b", "a")]}, workers=2, cwd=root)
            self.assertEqual([v.status for v in outcomes.values()], ["ok", "ok"])

    def test_timeout_and_launch_failure(self):
        with TemporaryDirectory() as root:
            outcomes = execute({
                "slow": cmd("import time; time.sleep(10)", timeout=.06),
                "missing": Step((str(Path(root) / "missing"),)),
                "child": cmd("raise SystemExit(99)", ("slow",)),
            })
        self.assertEqual(outcomes["slow"].status, "timeout")
        self.assertEqual(outcomes["missing"].status, "launch_error")
        self.assertEqual(outcomes["child"].status, "skipped")

    def test_bad_graph_has_no_side_effects(self):
        with TemporaryDirectory() as root:
            good = cmd("from pathlib import Path; Path('started').touch()")
            for bad in (cmd("pass", ("absent",)), cmd("pass", ("bad",))):
                with self.assertRaises(ValueError):
                    execute({"good": good, "bad": bad}, cwd=root)
            self.assertFalse((Path(root) / "started").exists())
