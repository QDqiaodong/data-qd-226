from pathlib import Path
import sys
from tempfile import TemporaryDirectory

from executor import Step, execute


def step(code, *after):
    return Step((sys.executable, "-c", code), after)


if __name__ == "__main__":
    with TemporaryDirectory() as directory:
        plan = {
            "emit": step("from pathlib import Path; Path('manifest').write_text('ready')"),
            "check": step("from pathlib import Path; assert Path('manifest').read_text() == 'ready'", "emit"),
            "lint": step("raise SystemExit(7)"),
            "publish": step("from pathlib import Path; Path('released').touch()", "check", "lint"),
        }
        report = execute(plan, cwd=directory)
        expected = ("ok", "ok", "failed", "skipped")
        actual = tuple(row.status for row in report.values())
        assert actual == expected and report["lint"].returncode == 7
        assert report["publish"].blocked_by == ("lint",)
        assert not (Path(directory) / "released").exists()
        for name, outcome in report.items():
            print(name, outcome.status, outcome.returncode, outcome.blocked_by)
