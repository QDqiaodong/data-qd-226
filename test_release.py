"""Offline real-subprocess acceptance tests for execute_release."""
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest

from executor import (
    ReleaseCohort,
    Step,
    execute_release,
)

PY = sys.executable
BOUNDED_WAIT = 10.0


def wait_for(predicate, timeout=BOUNDED_WAIT, message="condition"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError(f"timed out waiting for {message}")


def wait_for_path(path, timeout=BOUNDED_WAIT):
    path = Path(path)
    wait_for(lambda: path.exists(), timeout, f"path {path}")


def marker_step(marker, body="", after=(), timeout=15):
    """A step that appends its pid to marker, then runs body."""
    code = (
        "import os, sys\n"
        "from pathlib import Path\n"
        f"marker = {str(marker)!r}\n"
        "Path(marker).parent.mkdir(parents=True, exist_ok=True)\n"
        "with open(marker, 'a', encoding='utf-8') as handle:\n"
        "    handle.write(str(os.getpid()) + '\\n')\n"
        + body
    )
    return Step((PY, "-I", "-c", code), tuple(after), timeout)


def probe_step(verdict_file, marker, after=(), timeout=15):
    """Read a verdict file: 42 -> incompatible, 0 -> approve, other -> fail."""
    code = (
        "import os, sys\n"
        "from pathlib import Path\n"
        f"verdict_file, marker = {str(verdict_file)!r}, {str(marker)!r}\n"
        "Path(marker).parent.mkdir(parents=True, exist_ok=True)\n"
        "with open(marker, 'a', encoding='utf-8') as handle:\n"
        "    handle.write(str(os.getpid()) + '\\n')\n"
        "verdict = int(Path(verdict_file).read_text(encoding='utf-8').strip())\n"
        "raise SystemExit(verdict)\n"
    )
    return Step((PY, "-I", "-c", code), tuple(after), timeout)


def handshake_probe(marker, wait_file, signal_file, verdict_file, after=(),
                    timeout=20):
    """Signal started, wait until wait_file exists, then apply the verdict."""
    code = (
        "import os, time\n"
        "from pathlib import Path\n"
        f"marker = {str(marker)!r}\n"
        f"wait_file = {str(wait_file)!r}\n"
        f"signal_file = {str(signal_file)!r}\n"
        f"verdict_file = {str(verdict_file)!r}\n"
        "with open(marker, 'a', encoding='utf-8') as handle:\n"
        "    handle.write(str(os.getpid()) + '\\n')\n"
        "Path(signal_file).touch()\n"
        "deadline = time.monotonic() + 20\n"
        "while not Path(wait_file).exists():\n"
        "    if time.monotonic() > deadline:\n"
        "        raise SystemExit(99)\n"
        "    time.sleep(0.01)\n"
        "verdict = int(Path(verdict_file).read_text(encoding='utf-8').strip())\n"
        "raise SystemExit(verdict)\n"
    )
    return Step((PY, "-I", "-c", code), tuple(after), timeout)


def package_step(bundle_bytes, marker, after=(), timeout=15, extra_code="",
                 exit_code=0):
    """Write bundle_bytes to ./bundle and record start/end/launch markers."""
    code = (
        "import os, sys, time\n"
        "from pathlib import Path\n"
        f"base_marker = {str(marker)!r}\n"
        f"start_marker, end_marker = {str(Path(str(marker) + '.start'))!r}, {str(Path(str(marker) + '.end'))!r}\n"
        "for path in (base_marker, start_marker, end_marker):\n"
        "    Path(path).parent.mkdir(parents=True, exist_ok=True)\n"
        "Path(start_marker).touch()\n"
        "with open(base_marker, 'a', encoding='utf-8') as handle:\n"
        "    handle.write(str(os.getpid()) + '\\n')\n"
        f"data = {bytes(bundle_bytes)!r}\n"
        "Path('bundle').write_bytes(data)\n"
        + extra_code
        + f"\nPath(end_marker).touch()\n"
        f"raise SystemExit({int(exit_code)})\n"
    )
    return Step((PY, "-I", "-c", code), tuple(after), timeout)


def count_launches(marker):
    path = Path(marker)
    if not path.exists():
        return 0
    return len([line for line in path.read_text().splitlines() if line.strip()])


def make_simple_cohort(root, *, verdict, fallback_verdict=0,
                       bundle_primary=b"PRIMARY",
                       bundle_fallback=b"FALLBACK", shared_prep=None,
                       fallback_shared=None):
    """Build a standard cohort whose recipes share step/intermediate names."""
    primary_verdict_file = root / "verdict-primary"
    fallback_verdict_file = root / "verdict-fallback"
    primary_verdict_file.write_text(str(verdict), encoding="utf-8")
    fallback_verdict_file.write_text(str(fallback_verdict), encoding="utf-8")

    compile_marker = root / "compile.log"
    primary_probe_marker = root / "probe-primary.log"
    fallback_probe_marker = root / "probe-fallback.log"
    primary_package_marker = root / "package-primary.log"
    fallback_package_marker = root / "package-fallback.log"
    fb_shared_marker = root / "fallback-shared.log"

    # Both recipes use the same ordinary step name "compile" and the same
    # intermediate file "object.bin".
    compile_body = (
        "from pathlib import Path\n"
        "import os\n"
        "# An empty, per-recipe working directory must contain no leftovers.\n"
        "assert sorted(os.listdir('.')) == [], 'workdir not empty: ' + repr(os.listdir('.'))\n"
        "shared = os.environ['BUILD_RELEASE_SHARED_DIR']\n"
        "assert Path(shared).is_dir()\n"
        "Path('object.bin').write_bytes(b'obj')\n"
    )
    primary_after = ("prep",) if shared_prep is not None else ()
    fallback_after = tuple(primary_after)
    if fallback_shared is not None:
        fallback_after = fallback_after + ("fb_only",)
    compile_step_primary = marker_step(
        compile_marker, compile_body, after=primary_after)
    compile_step_fallback = marker_step(
        compile_marker, compile_body, after=fallback_after)

    primary_recipe = {
        "compile": compile_step_primary,
        "probe": probe_step(primary_verdict_file, primary_probe_marker,
                            after=("compile",)),
        "package": package_step(
            bundle_primary, primary_package_marker, after=("compile",)),
    }
    fallback_recipe = {
        "compile": compile_step_fallback,
        "probe": probe_step(fallback_verdict_file, fallback_probe_marker,
                            after=("compile",)),
        "package": package_step(
            bundle_fallback, fallback_package_marker, after=("compile",)),
    }

    cohort = ReleaseCohort(
        primary=primary_recipe,
        fallback=fallback_recipe,
        probe="probe",
        package="package",
    )
    shared = {}
    if shared_prep is not None:
        shared_marker, shared_body = shared_prep
        shared["prep"] = marker_step(shared_marker, shared_body)
    if fallback_shared is not None:
        marker, body = fallback_shared
        shared["fb_only"] = marker_step(marker, body)
    return cohort, shared, {
        "compile": compile_marker,
        "probe_primary": primary_probe_marker,
        "probe_fallback": fallback_probe_marker,
        "package_primary": primary_package_marker,
        "package_fallback": fallback_package_marker,
        "fb_shared": fb_shared_marker,
    }


class ReleaseValidationTest(unittest.TestCase):
    def _good_cohort(self):
        good_probe = Step((PY, "-I", "-c", "raise SystemExit(0)"))
        good_package = Step((PY, "-I", "-c",
                             "from pathlib import Path; Path('bundle').write_bytes(b'x')"))
        recipe = {"probe": good_probe, "package": good_package}
        return ReleaseCohort(primary=dict(recipe), fallback=dict(recipe),
                             probe="probe", package="package")

    def test_bad_arguments_start_nothing_and_keep_release_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            release = root / "release"
            release.mkdir()
            started = root / "started"
            cohort = self._good_cohort()
            # Add a side-effect marker to a shared step to prove nothing runs.
            shared = {"prep": marker_step(started)}
            sentinel = release / "sentinel.txt"
            sentinel.write_text("keep", encoding="utf-8")

            def call(**kwargs):
                execute_release(shared, {"plat": cohort},
                                release_dir=str(release), **kwargs)

            for workers in (True, False, 0, 9, 1.5, "2"):
                with self.subTest(workers=workers):
                    with self.assertRaises(ValueError):
                        call(workers=workers)

            bad_cohort_names = ["", "a" * 33, "has space", "slash/name",
                                "dot.name", "unicode-\u00e9"]
            for name in bad_cohort_names:
                with self.subTest(name=name):
                    with self.assertRaises(ValueError):
                        execute_release(
                            shared, {name: cohort}, release_dir=str(release))

            with self.assertRaises(ValueError):
                execute_release(shared, {}, release_dir=str(release))
            with self.assertRaises(ValueError):
                execute_release(shared, {"plat": object()},
                                release_dir=str(release))
            with self.assertRaises(ValueError):
                execute_release(shared, {"plat": cohort},
                                release_dir=str(root / "missing-dir"))

            # 9 cohorts exceeds the cap of 8.
            with self.assertRaises(ValueError):
                execute_release(
                    shared,
                    {f"p{i}": self._good_cohort() for i in range(9)},
                    release_dir=str(release))

            self.assertFalse(started.exists())
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "keep")
            self.assertEqual(sorted(p.name for p in release.iterdir()),
                             ["sentinel.txt"])

    def test_bad_step_and_graph_declarations_rejected_before_launch(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            release = root / "release"
            release.mkdir()
            started = root / "started"
            probe = Step((PY, "-I", "-c", "raise SystemExit(0)"))
            package = Step((PY, "-I", "-c",
                            "from pathlib import Path; Path('bundle').write_bytes(b'x')"))
            compile_ok = marker_step(started, "from pathlib import Path; Path('o').touch()")

            def cohort_with(primary, fallback, probe_name="probe",
                            package_name="package"):
                return ReleaseCohort(primary, fallback, probe_name, package_name)

            # probe == package
            with self.assertRaises(ValueError):
                recipe = {"probe": probe}
                execute_release({}, {"p": cohort_with(recipe, dict(recipe),
                                                      "probe", "probe")},
                                release_dir=str(release))
            # probe missing from fallback
            with self.assertRaises(ValueError):
                execute_release(
                    {},
                    {"p": cohort_with(
                        {"probe": probe, "package": package},
                        {"package": package, "probe2": probe},
                        "probe", "package")},
                    release_dir=str(release))
            # unknown dependency edge in the *unused* fallback recipe
            bad_fallback = {
                "probe": Step((PY, "-I", "-c", "raise SystemExit(0)"),
                              after=("ghost",)),
                "package": package,
            }
            with self.assertRaises(ValueError):
                execute_release(
                    {},
                    {"p": cohort_with(
                        {"probe": probe, "package": package}, bad_fallback)},
                    release_dir=str(release))
            # cycle inside the fallback recipe
            cyclic = {
                "a": marker_step(started, after=("b",)),
                "b": marker_step(started, after=("a",)),
                "probe": Step((PY, "-I", "-c", "raise SystemExit(0)"),
                              after=("a",)),
                "package": Step((PY, "-I", "-c",
                                 "from pathlib import Path; Path('bundle').write_bytes(b'x')",
                                 ("b",))),
            }
            with self.assertRaises(ValueError):
                execute_release(
                    {},
                    {"p": cohort_with(
                        {"probe": probe, "package": package}, cyclic)},
                    release_dir=str(release))
            # a step downstream of the probe
            with self.assertRaises(ValueError):
                recipe = {
                    "probe": probe,
                    "package": package,
                    "late": marker_step(started, after=("probe",)),
                }
                execute_release(
                    {}, {"p": cohort_with(recipe, dict(recipe))},
                    release_dir=str(release))
            # an ordinary step that is not an ancestor of probe/package
            with self.assertRaises(ValueError):
                recipe = {
                    "probe": probe,
                    "package": package,
                    "orphan": marker_step(started),
                }
                execute_release(
                    {}, {"p": cohort_with(recipe, dict(recipe))},
                    release_dir=str(release))
            # cohort step sharing a shared step name
            shared = {"compile": compile_ok}
            recipe = {"compile": compile_ok, "probe": probe, "package": package}
            with self.assertRaises(ValueError):
                execute_release(
                    shared, {"p": cohort_with(recipe, dict(recipe))},
                    release_dir=str(release))
            # shared step depending on a cohort step
            bad_shared = {"prep": marker_step(started, after=("probe",))}
            with self.assertRaises(ValueError):
                execute_release(
                    bad_shared,
                    {"p": cohort_with(
                        {"probe": probe, "package": package},
                        {"probe": probe, "package": package})},
                    release_dir=str(release))
            # invalid argv / timeout
            with self.assertRaises(ValueError):
                recipe = {"probe": Step(("",)), "package": package}
                execute_release(
                    {}, {"p": cohort_with(recipe, recipe)},
                    release_dir=str(release))
            with self.assertRaises(ValueError):
                recipe = {"probe": Step((PY,), timeout=0),
                          "package": package}
                execute_release(
                    {}, {"p": cohort_with(recipe, recipe)},
                    release_dir=str(release))
            # name with NUL / too many UTF-8 bytes
            with self.assertRaises(ValueError):
                recipe = {"pr\x00obe": probe, "package": package}
                execute_release(
                    {}, {"p": cohort_with(recipe, recipe, "pr\x00obe",
                                          "package")},
                    release_dir=str(release))
            long_name = "\u00e9" * 65  # 130 UTF-8 bytes
            with self.assertRaises(ValueError):
                recipe = {long_name: probe, "package": package}
                execute_release(
                    {}, {"p": cohort_with(recipe, recipe, long_name,
                                          "package")},
                    release_dir=str(release))
            self.assertFalse(started.exists())

    def test_step_count_cap_across_shared_and_both_recipes(self):
        with tempfile.TemporaryDirectory() as tmp:
            release = Path(tmp) / "release"
            release.mkdir()
            probe = Step((PY, "-I", "-c", "raise SystemExit(0)"))
            package = Step((PY, "-I", "-c",
                            "from pathlib import Path; Path('bundle').write_bytes(b'x')"))
            # 250 shared + 4 primary + 4 fallback = 258 > 256.
            shared = {f"s{i}": Step((PY, "-I", "-c", "pass"),
                                    after=(f"s{i-1}",) if i else ())
                      for i in range(250)}
            ordinary = {f"o{i}": Step((PY, "-I", "-c", "pass")
                                      ) for i in range(2)}
            recipe = dict(ordinary)
            recipe.update({"probe": probe, "package": package})
            with self.assertRaises(ValueError):
                execute_release(
                    shared,
                    {"p": ReleaseCohort(recipe, dict(recipe),
                                        "probe", "package")},
                    release_dir=str(release))


class PrimaryApprovedTest(unittest.TestCase):
    def test_primary_approved_publishes_primary_and_skips_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            release = root / "release"
            release.mkdir()
            shared_marker = root / "shared.log"
            fb_shared_marker = root / "fb-shared.log"
            cohort, shared, markers = make_simple_cohort(
                root,
                verdict=0,
                shared_prep=(shared_marker,
                             "from pathlib import Path; Path('shared.txt').touch()"),
                fallback_shared=(fb_shared_marker, "pass"),
            )

            results = execute_release(
                shared, {"plat-a": cohort},
                release_dir=str(release), workers=2)

            result = results["plat-a"]
            self.assertEqual(result.status, "published")
            self.assertEqual(result.published_profile, "primary")
            self.assertEqual(set(result.probes), {"primary"})
            self.assertEqual(result.probes["primary"].status, "ok")
            self.assertEqual(result.probes["primary"].returncode, 0)
            self.assertEqual(result.detail, "")

            bundle = release / "plat-a.bundle"
            self.assertTrue(bundle.is_file())
            self.assertFalse(bundle.is_symlink())
            self.assertEqual(bundle.read_bytes(), b"PRIMARY")

            # Primary-only execution; fallback steps and fallback-only shared
            # steps never launch; the common shared prep runs exactly once.
            self.assertEqual(count_launches(markers["compile"]), 1)
            self.assertEqual(count_launches(markers["probe_primary"]), 1)
            self.assertEqual(count_launches(markers["probe_fallback"]), 0)
            self.assertEqual(count_launches(markers["package_primary"]), 1)
            self.assertEqual(count_launches(markers["package_fallback"]), 0)
            self.assertEqual(count_launches(shared_marker), 1)
            self.assertEqual(count_launches(fb_shared_marker), 0)

            # Temp files are collected; only the formal bundle remains.
            leftovers = [p.name for p in release.iterdir()
                         if p.name != "plat-a.bundle"]
            self.assertEqual(
                [name for name in leftovers
                 if not name.startswith(".executor-release")], [])


class SwitchRecipeTest(unittest.TestCase):
    def test_exit_42_switches_to_fallback_and_publishes_fallback_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            release = root / "release"
            release.mkdir()
            shared_marker = root / "shared.log"
            fb_shared_marker = root / "fb-shared.log"
            cohort, shared, markers = make_simple_cohort(
                root,
                verdict=42,
                shared_prep=(shared_marker,
                             "from pathlib import Path; Path('shared.txt').touch()"),
                fallback_shared=(fb_shared_marker, "pass"),
            )

            results = execute_release(
                shared, {"plat-a": cohort},
                release_dir=str(release), workers=2)
            result = results["plat-a"]

            self.assertEqual(result.status, "published")
            self.assertEqual(result.published_profile, "fallback")
            self.assertEqual(set(result.probes), {"primary", "fallback"})
            primary_probe = result.probes["primary"]
            fallback_probe = result.probes["fallback"]
            self.assertEqual(primary_probe.status, "failed")
            self.assertEqual(primary_probe.returncode, 42)
            self.assertEqual(fallback_probe.status, "ok")
            self.assertEqual(fallback_probe.returncode, 0)

            bundle = release / "plat-a.bundle"
            self.assertEqual(bundle.read_bytes(), b"FALLBACK")

            # The same-named compile step runs once per recipe; the fallback
            # run asserted its own empty workdir inside the command.
            self.assertEqual(count_launches(markers["compile"]), 2)
            self.assertEqual(count_launches(markers["probe_primary"]), 1)
            self.assertEqual(count_launches(markers["probe_fallback"]), 1)
            self.assertEqual(count_launches(markers["package_primary"]), 1)
            self.assertEqual(count_launches(markers["package_fallback"]), 1)
            # The fallback-only shared step is needed now; common prep once.
            self.assertEqual(count_launches(shared_marker), 1)
            self.assertEqual(count_launches(fb_shared_marker), 1)

    def test_failure_does_not_inherit_from_same_named_primary_step(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            release = root / "release"
            release.mkdir()
            primary_verdict = root / "verdict-primary"
            fallback_verdict = root / "verdict-fallback"
            primary_verdict.write_text("42", encoding="utf-8")
            fallback_verdict.write_text("0", encoding="utf-8")
            compile_marker = root / "compile.log"

            # Both recipes declare the same step name "compile" and the same
            # intermediate "object.bin", but they are distinct executions: the
            # fallback must run its own compile in a fresh empty workdir and
            # must not inherit the primary's execution or its files.
            compile_primary = marker_step(
                compile_marker,
                "from pathlib import Path; "
                "Path('object.bin').write_bytes(b'primary-obj')")
            compile_fallback = marker_step(
                compile_marker,
                "import os\n"
                "assert sorted(os.listdir('.')) == [], repr(os.listdir('.'))\n"
                "from pathlib import Path; "
                "Path('object.bin').write_bytes(b'fallback-obj')")
            probe_primary = probe_step(
                primary_verdict, root / "probe-primary.log",
                after=("compile",))
            probe_fallback = probe_step(
                fallback_verdict, root / "probe-fallback.log",
                after=("compile",))
            package = package_step(b"FALLBACK", root / "package.log",
                                   after=("compile",))
            cohort = ReleaseCohort(
                primary={"compile": compile_primary,
                         "probe": probe_primary, "package": package},
                fallback={"compile": compile_fallback,
                          "probe": probe_fallback, "package": package},
                probe="probe", package="package")

            results = execute_release({}, {"p": cohort},
                                      release_dir=str(release))
            result = results["p"]
            self.assertEqual(result.status, "published")
            self.assertEqual(result.published_profile, "fallback")
            self.assertEqual(
                (release / "p.bundle").read_bytes(), b"FALLBACK")
            # The same-named compile executed once per recipe, independently.
            self.assertEqual(count_launches(compile_marker), 2)
            self.assertEqual(result.probes["primary"].returncode, 42)
            self.assertEqual(result.probes["fallback"].status, "ok")

    def test_primary_prerequisite_failure_does_not_activate_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            release = root / "release"
            release.mkdir()
            verdict = root / "verdict-primary"
            verdict.write_text("0", encoding="utf-8")
            # A failing primary compile means the probe never runs; that is a
            # build error and must not switch to the fallback recipe.
            compile_primary = marker_step(
                root / "compile-primary.log", "raise SystemExit(7)")
            compile_fallback = marker_step(
                root / "compile-fallback.log", "pass")
            probe_primary = probe_step(
                verdict, root / "probe-primary.log", after=("compile",))
            probe_fallback = probe_step(
                verdict, root / "probe-fallback.log", after=("compile",))
            package = package_step(b"X", root / "package.log",
                                   after=("compile",))
            cohort = ReleaseCohort(
                primary={"compile": compile_primary,
                         "probe": probe_primary, "package": package},
                fallback={"compile": compile_fallback,
                          "probe": probe_fallback, "package": package},
                probe="probe", package="package")
            results = execute_release({}, {"p": cohort},
                                      release_dir=str(release))
            result = results["p"]
            self.assertEqual(result.status, "failed")
            self.assertIsNone(result.published_profile)
            self.assertEqual(
                result.probes["primary"].status, "skipped")
            self.assertEqual(
                result.probes["primary"].blocked_by, ("compile",))
            self.assertNotIn("fallback", result.probes)
            self.assertEqual(count_launches(root / "compile-fallback.log"), 0)
            self.assertFalse((release / "p.bundle").exists())


class FailureSemanticsTest(unittest.TestCase):
    def test_ordinary_probe_error_does_not_start_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            release = root / "release"
            release.mkdir()
            cohort, shared, markers = make_simple_cohort(
                root, verdict=1)  # ordinary nonzero, not 42

            results = execute_release(shared, {"p": cohort},
                                      release_dir=str(release))
            result = results["p"]
            self.assertEqual(result.status, "failed")
            self.assertIsNone(result.published_profile)
            self.assertEqual(set(result.probes), {"primary"})
            self.assertEqual(result.probes["primary"].status, "failed")
            self.assertEqual(result.probes["primary"].returncode, 1)
            self.assertNotIn("fallback", result.probes)
            self.assertFalse((release / "p.bundle").exists())
            self.assertEqual(count_launches(markers["compile"]), 1)

    def test_probe_timeout_is_a_build_error_without_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            release = root / "release"
            release.mkdir()
            slow_probe = Step(
                (PY, "-I", "-c", "import time; time.sleep(1)"),
                timeout=0.05)
            package = package_step(b"X", root / "package.log")
            recipe = {"probe": slow_probe, "package": package}
            cohort = ReleaseCohort(dict(recipe), dict(recipe),
                                   "probe", "package")
            results = execute_release({}, {"p": cohort},
                                      release_dir=str(release))
            result = results["p"]
            self.assertEqual(result.status, "failed")
            self.assertIsNone(result.published_profile)
            self.assertEqual(result.probes["primary"].status, "timeout")
            self.assertNotIn("fallback", result.probes)
            self.assertIn("timeout", result.detail)
            self.assertFalse((release / "p.bundle").exists())

    def test_skipped_probe_after_shared_failure_does_not_switch(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            release = root / "release"
            release.mkdir()
            primary_verdict = root / "verdict-primary"
            fallback_verdict = root / "verdict-fallback"
            primary_verdict.write_text("42", encoding="utf-8")
            fallback_verdict.write_text("0", encoding="utf-8")
            shared = {"prep": marker_step(
                root / "prep.log", "raise SystemExit(3)")}
            compile_p = marker_step(
                root / "compile-primary.log", "pass", after=("prep",))
            compile_f = marker_step(
                root / "compile-fallback.log", "pass", after=("prep",))
            probe_p = probe_step(primary_verdict,
                                 root / "probe-primary.log",
                                 after=("compile",))
            probe_f = probe_step(fallback_verdict,
                                 root / "probe-fallback.log",
                                 after=("compile",))
            package = package_step(b"X", root / "package.log",
                                   after=("compile",))
            cohort = ReleaseCohort(
                primary={"compile": compile_p, "probe": probe_p,
                         "package": package},
                fallback={"compile": compile_f, "probe": probe_f,
                          "package": package},
                probe="probe", package="package")
            results = execute_release(shared, {"p": cohort},
                                      release_dir=str(release))
            result = results["p"]
            self.assertEqual(result.status, "failed")
            self.assertIsNone(result.published_profile)
            self.assertEqual(result.probes["primary"].status, "skipped")
            self.assertNotIn("fallback", result.probes)
            self.assertEqual(
                count_launches(root / "compile-fallback.log"), 0)
            self.assertFalse((release / "p.bundle").exists())

    def test_both_recipes_incompatible_publishes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            release = root / "release"
            release.mkdir()
            cohort, shared, markers = make_simple_cohort(
                root, verdict=42, fallback_verdict=42,
                bundle_primary=b"P", bundle_fallback=b"F")

            results = execute_release(shared, {"p": cohort},
                                      release_dir=str(release))
            result = results["p"]
            self.assertEqual(result.status, "incompatible")
            self.assertIsNone(result.published_profile)
            self.assertEqual(
                result.probes["primary"].returncode, 42)
            self.assertEqual(
                result.probes["fallback"].returncode, 42)
            self.assertIn("42", result.detail)
            self.assertFalse((release / "p.bundle").exists())
            # Both recipes were genuinely built and probed.
            self.assertEqual(count_launches(markers["probe_primary"]), 1)
            self.assertEqual(count_launches(markers["probe_fallback"]), 1)

    def test_fallback_approved_but_broken_package_keeps_old_bundle(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            release = root / "release"
            release.mkdir()
            old_bundle = release / "p.bundle"
            old_bundle.write_bytes(b"OLD-BYTES")

            primary_verdict = root / "verdict-primary"
            fallback_verdict = root / "verdict-fallback"
            primary_verdict.write_text("42", encoding="utf-8")
            fallback_verdict.write_text("0", encoding="utf-8")
            compile_primary = marker_step(
                root / "compile-primary.log",
                "from pathlib import Path; Path('object.bin').touch()")
            compile_fallback = marker_step(
                root / "compile-fallback.log",
                "from pathlib import Path; Path('object.bin').touch()")
            probe_primary = probe_step(
                primary_verdict, root / "probe-primary.log",
                after=("compile",))
            probe_fallback = probe_step(
                fallback_verdict, root / "probe-fallback.log",
                after=("compile",))
            # Writes a partial bundle then exits nonzero.
            broken_package = package_step(
                b"HALF", root / "package-fallback.log",
                after=("compile",), exit_code=8)
            good_package = package_step(
                b"PRIMARY", root / "package-primary.log",
                after=("compile",))
            cohort = ReleaseCohort(
                primary={"compile": compile_primary,
                         "probe": probe_primary,
                         "package": good_package},
                fallback={"compile": compile_fallback,
                          "probe": probe_fallback,
                          "package": broken_package},
                probe="probe", package="package")

            results = execute_release({}, {"p": cohort},
                                      release_dir=str(release))
            result = results["p"]
            self.assertEqual(result.status, "failed")
            self.assertIsNone(result.published_profile)
            self.assertEqual(result.probes["fallback"].status, "ok")
            self.assertIn("package", result.detail)
            # The old formal bundle is untouched, candidate bytes invisible.
            self.assertEqual(old_bundle.read_bytes(), b"OLD-BYTES")
            stage_leftovers = [p.name for p in release.iterdir()
                               if p.name.startswith(".bundle-stage-")]
            self.assertEqual(stage_leftovers, [])

    def test_non_compliant_bundle_symlink_is_not_published(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            release = root / "release"
            release.mkdir()
            old_bundle = release / "p.bundle"
            old_bundle.write_bytes(b"OLD")
            target = root / "elsewhere.bin"
            target.write_bytes(b"OUTSIDE")
            package_code = (
                "import os\n"
                f"os.symlink({str(target)!r}, 'bundle')\n"
            )
            probe = Step((PY, "-I", "-c", "raise SystemExit(0)"))
            package = Step((PY, "-I", "-c", package_code))
            recipe = {"probe": probe, "package": package}
            cohort = ReleaseCohort(dict(recipe), dict(recipe),
                                   "probe", "package")
            results = execute_release({}, {"p": cohort},
                                      release_dir=str(release))
            self.assertEqual(results["p"].status, "failed")
            self.assertIn("symbolic link", results["p"].detail)
            self.assertEqual(old_bundle.read_bytes(), b"OLD")

    def test_oversized_bundle_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            release = root / "release"
            release.mkdir()
            # Sparse file: cheap to create, but st_size exceeds the cap.
            package_code = (
                "import os\n"
                "fd = os.open('bundle', os.O_WRONLY | os.O_CREAT, 0o600)\n"
                f"os.ftruncate(fd, {512 * 1024 * 1024 + 1})\n"
                "os.close(fd)\n"
            )
            probe = Step((PY, "-I", "-c", "raise SystemExit(0)"))
            package = Step((PY, "-I", "-c", package_code), timeout=20)
            recipe = {"probe": probe, "package": package}
            cohort = ReleaseCohort(dict(recipe), dict(recipe),
                                   "probe", "package")
            results = execute_release({}, {"p": cohort},
                                      release_dir=str(release))
            self.assertEqual(results["p"].status, "failed")
            self.assertIn("limit is 536870912", results["p"].detail)
            self.assertFalse((release / "p.bundle").exists())

    def test_prerequisite_failure_skips_probe_and_package(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            release = root / "release"
            release.mkdir()
            broken_compile = marker_step(
                root / "compile.log", "raise SystemExit(5)")
            probe = Step((PY, "-I", "-c", "raise SystemExit(0)"),
                         after=("compile",))
            package = package_step(b"X", root / "package.log",
                                   after=("compile",))
            recipe = {"compile": broken_compile, "probe": probe,
                      "package": package}
            cohort = ReleaseCohort(dict(recipe), dict(recipe),
                                   "probe", "package")
            results = execute_release({}, {"p": cohort},
                                      release_dir=str(release))
            result = results["p"]
            self.assertEqual(result.status, "failed")
            self.assertEqual(result.probes["primary"].status, "skipped")
            self.assertEqual(result.probes["primary"].blocked_by,
                             ("compile",))
            self.assertFalse((release / "p.bundle").exists())
            self.assertEqual(count_launches(root / "package.log"), 0)

    def test_shared_failure_only_blocks_dependent_cohort(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            release = root / "release"
            release.mkdir()
            shared = {
                "prep": marker_step(root / "shared.log",
                                    "raise SystemExit(3)"),
            }
            good_probe = Step((PY, "-I", "-c", "raise SystemExit(0)"))
            package = package_step(b"OK", root / "package.log")
            blocked_recipe = {
                "probe": Step((PY, "-I", "-c", "raise SystemExit(0)"),
                              after=("prep",)),
                "package": Step(
                    (PY, "-I", "-c",
                     "from pathlib import Path; Path('bundle').write_bytes(b'X')"),
                    after=("prep",)),
            }
            independent_recipe = {"probe": good_probe, "package": package}
            cohorts = {
                "blocked": ReleaseCohort(dict(blocked_recipe),
                                         dict(blocked_recipe),
                                         "probe", "package"),
                "free": ReleaseCohort(dict(independent_recipe),
                                      dict(independent_recipe),
                                      "probe", "package"),
            }
            results = execute_release(shared, cohorts,
                                      release_dir=str(release), workers=2)
            self.assertEqual(results["blocked"].status, "failed")
            self.assertEqual(
                results["blocked"].probes["primary"].status, "skipped")
            self.assertEqual(
                results["blocked"].probes["primary"].blocked_by,
                ("prep",))
            self.assertEqual(results["free"].status, "published")
            self.assertEqual(
                (release / "free.bundle").read_bytes(), b"OK")
            self.assertFalse((release / "blocked.bundle").exists())


class ConcurrencyAndAtomicityTest(unittest.TestCase):
    def test_two_cohorts_share_slots_and_switch_is_atomic_for_readers(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            release = root / "release"
            release.mkdir()

            # Pre-existing formal bundle for the slow cohort.
            slow_bundle = release / "slow.bundle"
            slow_bundle.write_bytes(b"OLD-SLOW")
            old_handle = open(slow_bundle, "rb")
            self.addCleanup(old_handle.close)
            self.assertEqual(old_handle.read(), b"OLD-SLOW")

            handshake = root / "handshake"
            handshake.mkdir()
            # Fast cohort: probe waits until the slow probe has started.
            fast_probe_started = handshake / "fast-probe-started"
            slow_probe_started = handshake / "slow-probe-started"
            fast_release_done = handshake / "fast-release-done"
            fast_verdict = root / "fast-verdict"
            fast_verdict.write_text("0", encoding="utf-8")
            slow_primary_verdict = root / "slow-verdict-primary"
            slow_fallback_verdict = root / "slow-verdict-fallback"
            slow_primary_verdict.write_text("42", encoding="utf-8")
            slow_fallback_verdict.write_text("0", encoding="utf-8")

            fast_probe = handshake_probe(
                root / "fast-probe.log", slow_probe_started,
                fast_probe_started, fast_verdict)
            fast_package = package_step(
                b"FAST", root / "fast-package.log")
            fast_recipe = {"probe": fast_probe, "package": fast_package}
            fast_cohort = ReleaseCohort(
                dict(fast_recipe), dict(fast_recipe),
                "probe", "package")

            # The slow primary probe blocks until the fast formal bundle is
            # readable, then reports ABI incompatibility (42).
            slow_primary_probe = handshake_probe(
                root / "slow-probe-primary.log",
                fast_release_done, slow_probe_started, slow_primary_verdict)
            # The fallback probe is independent and simply approves.
            slow_fallback_probe = probe_step(
                slow_fallback_verdict, root / "slow-probe-fallback.log")
            # Primary package has no unfinished build dependency and may finish
            # early, writing recognizable primary candidate bytes.
            slow_primary_package = package_step(
                b"PRIMARY-CANDIDATE", root / "slow-primary-package.log")
            slow_fallback_package = package_step(
                b"NEW-SLOW", root / "slow-fallback-package.log")
            slow_primary = {
                "probe": slow_primary_probe,
                "package": slow_primary_package,
            }
            slow_fallback = {
                "probe": slow_fallback_probe,
                "package": slow_fallback_package,
            }
            slow_cohort = ReleaseCohort(
                slow_primary, slow_fallback, "probe", "package")

            thread_result = {}

            def run_release():
                thread_result["value"] = execute_release(
                    {}, {"slow": slow_cohort, "fast": fast_cohort},
                    release_dir=str(release), workers=2)

            import threading
            thread = threading.Thread(target=run_release)
            thread.start()

            # The fast probe returns only after the slow probe has started.
            wait_for_path(slow_probe_started)
            wait_for_path(fast_probe_started)
            # The fast cohort publishes while the slow probe is still blocked.
            fast_bundle = release / "fast.bundle"
            wait_for(lambda: fast_bundle.exists(),
                     message="fast cohort publication")
            self.assertEqual(fast_bundle.read_bytes(), b"FAST")
            # Unblock the slow probe.
            fast_release_done.touch()

            thread.join(timeout=BOUNDED_WAIT)
            self.assertFalse(thread.is_alive())
            results = thread_result["value"]

            # Input order is preserved in the returned dict.
            self.assertEqual(list(results), ["slow", "fast"])
            self.assertEqual(results["fast"].status, "published")
            self.assertEqual(results["fast"].published_profile, "primary")
            self.assertEqual(results["slow"].status, "published")
            self.assertEqual(results["slow"].published_profile,
                             "fallback")

            # The handle opened before the switch keeps reading old bytes.
            old_handle.seek(0)
            self.assertEqual(old_handle.read(), b"OLD-SLOW")
            # A newly opened handle only ever sees the complete new bundle.
            with open(slow_bundle, "rb") as new_handle:
                self.assertEqual(new_handle.read(), b"NEW-SLOW")
            self.assertNotIn(b"PRIMARY-CANDIDATE",
                             slow_bundle.read_bytes())

            # If the implementation packages early, the rejected primary
            # package must never overlap any fallback command of the cohort.
            primary_start = root / "slow-primary-package.log.start"
            primary_end = root / "slow-fallback-package.log.start"
            if primary_start.exists():
                wait_for_path(root / "slow-primary-package.log.end")
                # Fallback package cannot start before the primary package
                # process was reaped; compare bounded marker ordering.
                self.assertLess(
                    os.path.getmtime(root / "slow-primary-package.log.end"),
                    os.path.getmtime(primary_end))
                # Rejected candidate bytes never appear at the formal path.
                self.assertEqual(
                    slow_bundle.read_bytes(), b"NEW-SLOW")

    def test_no_cohort_hoards_the_process_slots(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            release = root / "release"
            release.mkdir()
            gate = root / "gate"
            # One long blocked step per cohort; with workers=2 both cohorts'
            # steps must run concurrently rather than one cohort owning slots.
            markers = {}

            def blocking_recipe(name):
                started = root / f"{name}-started"
                markers[name] = started
                code = (
                    "from pathlib import Path\n"
                    "import time\n"
                    f"Path({str(started)!r}).touch()\n"
                    f"deadline = time.monotonic() + {BOUNDED_WAIT}\n"
                    f"while not Path({str(gate)!r}).exists():\n"
                    "    if time.monotonic() > deadline:\n"
                    "        raise SystemExit(9)\n"
                    "    time.sleep(0.01)\n"
                )
                long_step = Step((PY, "-I", "-c", code))
                probe = Step((PY, "-I", "-c", "raise SystemExit(0)"),
                             after=("long",))
                package = package_step(
                    name.encode(), root / f"{name}-package.log",
                    after=("long",))
                recipe = {"long": long_step, "probe": probe,
                          "package": package}
                return ReleaseCohort(dict(recipe), dict(recipe),
                                     "probe", "package")

            import threading
            outcome = {}

            def run():
                outcome["results"] = execute_release(
                    {}, {"a": blocking_recipe("a"),
                         "b": blocking_recipe("b")},
                    release_dir=str(release), workers=2)

            thread = threading.Thread(target=run)
            thread.start()
            wait_for_path(markers["a"])
            wait_for_path(markers["b"])  # both cohorts got a slot
            gate.touch()
            thread.join(timeout=BOUNDED_WAIT)
            self.assertFalse(thread.is_alive())
            self.assertEqual(outcome["results"]["a"].status, "published")
            self.assertEqual(outcome["results"]["b"].status, "published")


class EnvironmentAndLockingTest(unittest.TestCase):
    def test_workdirs_are_isolated_and_shared_dir_is_exposed_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            release = root / "release"
            release.mkdir()
            seen_paths = root / "seen-paths.log"
            compile_body = (
                "import os\n"
                f"with open({str(seen_paths)!r}, 'a') as handle:\n"
                "    handle.write(os.getcwd() + '\\n')\n"
                "    handle.write(os.environ['BUILD_RELEASE_SHARED_DIR'] + '\\n')\n"
                "assert sorted(os.listdir('.')) == []\n"
            )
            cohort, shared, _ = make_simple_cohort(
                root, verdict=42,
                shared_prep=(root / "prep.log",
                             "from pathlib import Path; Path('r').touch()"))
            # Replace the compile step body with the recording variant while
            # keeping identical names in both recipes.
            for recipe in (cohort.primary, cohort.fallback):
                recipe["compile"] = marker_step(root / "compile.log",
                                                compile_body)
            execute_release(shared, {"p": cohort},
                            release_dir=str(release), workers=1)
            lines = seen_paths.read_text().splitlines()
            cwds = lines[0::2]
            shared_dirs = lines[1::2]
            self.assertEqual(len(cwds), 2)
            self.assertNotEqual(cwds[0], cwds[1])
            self.assertEqual(len(set(shared_dirs)), 1)
            self.assertTrue(Path(shared_dirs[0]).is_absolute())
            # The shared dir is never one of the recipe workdirs.
            self.assertNotIn(shared_dirs[0], cwds)

    def test_concurrent_release_against_same_dir_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            release = Path(tmp) / "release"
            release.mkdir()
            gate = Path(tmp) / "gate"
            started = Path(tmp) / "started"
            code = (
                "from pathlib import Path\n"
                f"Path({str(started)!r}).touch()\n"
                f"deadline = __import__('time').monotonic() + {BOUNDED_WAIT}\n"
                f"while not Path({str(gate)!r}).exists():\n"
                "    if __import__('time').monotonic() > deadline:\n"
                "        raise SystemExit(9)\n"
                "    __import__('time').sleep(0.01)\n"
            )
            probe = Step((PY, "-I", "-c", code))
            package = package_step(b"X", Path(tmp) / "package.log")
            recipe = {"probe": probe, "package": package}
            cohort = ReleaseCohort(dict(recipe), dict(recipe),
                                   "probe", "package")
            import threading
            errors = []

            def first():
                execute_release({}, {"p": cohort},
                                release_dir=str(release), workers=1)

            thread = threading.Thread(target=first)
            thread.start()
            wait_for_path(started)
            try:
                with self.assertRaises(ValueError):
                    execute_release({}, {"p": cohort},
                                    release_dir=str(release))
            finally:
                gate.touch()
                thread.join(timeout=BOUNDED_WAIT)
            self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [])

    def test_unrelated_release_files_are_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            release = root / "release"
            release.mkdir()
            other = release / "other.bundle"
            other.write_bytes(b"UNRELATED")
            notes = release / "notes.txt"
            notes.write_text("read-only consumer data", encoding="utf-8")
            cohort, _, _ = make_simple_cohort(root, verdict=0)
            execute_release({}, {"mine": cohort},
                            release_dir=str(release))
            self.assertEqual(other.read_bytes(), b"UNRELATED")
            self.assertEqual(
                notes.read_text(encoding="utf-8"),
                "read-only consumer data")
            self.assertEqual(
                (release / "mine.bundle").read_bytes(), b"PRIMARY")


if __name__ == "__main__":
    unittest.main()
