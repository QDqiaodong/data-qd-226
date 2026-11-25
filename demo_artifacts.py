"""Demo for execute_with_artifacts: producers, readers and scratch capacity."""
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory

from executor import Step, execute_with_artifacts


def producer(text):
    return Step((sys.executable, "-c",
                 "import os\n"
                 f"open(os.environ['BUILD_OUTPUT'], 'w').write({text!r})"))


def reader(marker):
    return Step((sys.executable, "-c",
                 "import json, os\n"
                 "from pathlib import Path\n"
                 "inputs = json.loads(os.environ['BUILD_INPUTS'])\n"
                 "data = {k: Path(v).read_text() for k, v in inputs.items()}\n"
                 f"Path({marker!r}).write_text(json.dumps(data, sort_keys=True))"))


if __name__ == "__main__":
    with TemporaryDirectory() as work, TemporaryDirectory() as scratch:
        marker = os.path.join(work, "reader-result.json")
        plan = {
            # Two independent producers, each reserving scratch space.
            "compile": producer("binary-bytes"),
            "docs": producer("docs-bytes"),
            # One reader consumes both artifacts via BUILD_INPUTS.
            "publish": reader(marker),
            # An unrelated failing step only blocks its own branch.
            "lint": Step((sys.executable, "-c", "raise SystemExit(7)")),
            "notify": Step((sys.executable, "-c", "pass"), ("lint",)),
        }
        report = execute_with_artifacts(
            plan,
            outputs={"compile": 64, "docs": 64},
            uses={"publish": ("compile", "docs")},
            capacity=128,
            scratch_dir=scratch,
            cwd=work,
        )
        for name, outcome in report.items():
            print(name, outcome.status, outcome.returncode,
                  outcome.blocked_by, outcome.detail)

        assert report["compile"].status == "ok"
        assert report["docs"].status == "ok"
        assert report["publish"].status == "ok"
        assert report["lint"].returncode == 7
        assert report["notify"].status == "skipped"
        assert report["notify"].blocked_by == ("lint",)
        # The reader saw the real bytes; the scratch subdir is gone.
        assert Path(marker).read_text() == '{"compile": "binary-bytes", "docs": "docs-bytes"}'
        assert os.listdir(scratch) == []
        print("demo ok; scratch root left empty:", os.listdir(scratch) == [])
