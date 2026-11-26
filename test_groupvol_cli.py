"""storage_ctl.py 的可重复入口、退出码、真实进程崩溃恢复与并发竞争测试。"""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

os.environ["GROUPVOL_FSYNC"] = "0"

REPO = Path(__file__).resolve().parent
CTL = REPO / "storage_ctl.py"
EXAMPLES = REPO / "examples"


def run_cli(root, *args, crash_point=None):
    env = dict(os.environ)
    env["PYTHONPATH"] = str(REPO) + os.pathsep + env.get("PYTHONPATH", "")
    if crash_point:
        env["GROUPVOL_CRASH"] = crash_point
    elif "GROUPVOL_CRASH" in env:
        del env["GROUPVOL_CRASH"]
    return subprocess.run(
        [sys.executable, str(CTL), "--root", str(root), *args],
        cwd=str(REPO),
        env=env,
        capture_output=True,
        text=True,
    )


def setup_snapshot_via_api(root):
    """用进程内 API 快速准备一个健康 s1（CLI 本身另有完整链路测试）。"""
    sys.path.insert(0, str(REPO))
    from groupvol import Coordinator

    c = Coordinator(root)

    def m(at, kind, record):
        return {"at": at, "kind": kind, "record": record}

    c.handle_message(m(0, "volume", {"group": "g", "volume": "a", "blocks": 2}))
    c.handle_message(m(0, "volume", {"group": "g", "volume": "b", "blocks": 2}))
    c.handle_message(m(1, "write", {"group": "g", "volume": "a", "batch": "x", "seq": 1, "block": 0, "data": "A"}))
    c.handle_message(m(1, "write", {"group": "g", "volume": "b", "batch": "x", "seq": 2, "block": 1, "data": "B"}))
    c.handle_message(m(2, "flush", {"group": "g", "volume": "a", "through": 1}))
    c.handle_message(m(2, "flush", {"group": "g", "volume": "b", "through": 2}))
    c.handle_message(m(3, "commit", {"group": "g", "batch": "x", "requires": [1, 2]}))
    c.handle_message(m(4, "freeze", {"group": "g", "id": "s1", "deadline": 9}))
    c.handle_message(m(5, "sealed", {"id": "s1", "volume": "a", "through": 1}))
    c.handle_message(m(5, "sealed", {"id": "s1", "volume": "b", "through": 2}))


CLI_SETUP_STEPS = [
    ("volume", "--group", "g", "--volume", "a", "--blocks", "2", "--at", "0"),
    ("volume", "--group", "g", "--volume", "b", "--blocks", "2", "--at", "0"),
    ("write", "--group", "g", "--volume", "a", "--seq", "1", "--block", "0", "--data", "A", "--at", "1"),
    ("write", "--group", "g", "--volume", "b", "--seq", "2", "--block", "1", "--data", "B", "--at", "1"),
    ("flush", "--group", "g", "--volume", "a", "--through", "1", "--at", "2"),
    ("flush", "--group", "g", "--volume", "b", "--through", "2", "--at", "2"),
    ("commit", "--group", "g", "--batch", "x", "--requires", "1,2", "--at", "3"),
    ("freeze", "--group", "g", "--id", "s1", "--deadline", "9", "--at", "4"),
    ("sealed", "--id", "s1", "--volume", "a", "--through", "1", "--at", "5"),
    ("sealed", "--id", "s1", "--volume", "b", "--through", "2", "--at", "5"),
]


def setup_snapshot_via_cli(root):
    for step in CLI_SETUP_STEPS:
        proc = run_cli(root, *step)
        assert proc.returncode == 0, proc.stderr + proc.stdout


class CliFlowTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "lab"

    def tearDown(self):
        self.tmp.cleanup()

    def test_raw_cli_write_freeze_seal_chain(self):
        setup_snapshot_via_cli(self.root)
        proc = run_cli(self.root, "verify", "--id", "s1")
        self.assertEqual(proc.returncode, 0, proc.stdout)

    def test_full_flow_verify_restore_inspect(self):
        setup_snapshot_via_api(self.root)

        proc = run_cli(self.root, "verify", "--id", "s1")
        self.assertEqual(proc.returncode, 0, proc.stdout)
        self.assertTrue(json.loads(proc.stdout)["report"]["restorable"])

        proc = run_cli(
            self.root, "restore", "--id", "s1", "--target", "ws",
            "--map", "a=aa", "--map", "b=bb", "--at", "6",
        )
        self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
        self.assertEqual(json.loads(proc.stdout)["result"]["phase"], "switched")

        proc = run_cli(self.root, "inspect", "--target", "ws")
        report = json.loads(proc.stdout)["report"]
        self.assertEqual(report["state"], "switched")
        self.assertEqual(report["mounted_version"]["snapshot_id"], "s1")
        self.assertEqual(report["mounted_version"]["mapping"], {"a": "aa", "b": "bb"})
        self.assertEqual(report["mounted_version"]["covered_batches"], ["x"])

    def test_unknown_id_returns_exit_2(self):
        proc = run_cli(self.root, "inspect", "--id", "nope")
        self.assertEqual(proc.returncode, 2)
        self.assertFalse(json.loads(proc.stdout)["ok"])

    def test_replay_example_compact(self):
        root = self.root / "ex"
        proc = subprocess.run(
            [
                sys.executable, str(CTL), "replay",
                str(EXAMPLES / "incremental-missing-volume.jsonl"),
                "--root", str(root), "--fresh", "--compact",
            ],
            cwd=str(REPO), capture_output=True, text=True,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        lines = [json.loads(line) for line in proc.stdout.splitlines() if line.strip()]
        self.assertEqual(len(lines), 22)

        proc = run_cli(root, "verify", "--id", "s2")
        self.assertEqual(proc.returncode, 1)
        self.assertFalse(json.loads(proc.stdout)["report"]["restorable"])

    def test_rebuild_into_standalone_directory(self):
        setup_snapshot_via_api(self.root)
        into = Path(self.tmp.name) / "brand-new-workspace"
        proc = run_cli(self.root, "rebuild", "--id", "s1", "--into", str(into), "--map", "a=disk1")
        self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
        rp = into / "RECOVERY_POINT.json"
        self.assertTrue(rp.is_file())
        data = json.loads(rp.read_text(encoding="utf-8"))
        self.assertEqual(data["snapshot_id"], "s1")
        self.assertEqual(data["covered_batches"], ["x"])
        self.assertTrue((into / "volumes" / "disk1" / "blocks" / "blk-00000000.json").is_file())

        # 非空目录再次 rebuild 不带 --clean 必须失败，带 --clean 可重复执行。
        proc = run_cli(self.root, "rebuild", "--id", "s1", "--into", str(into))
        self.assertNotEqual(proc.returncode, 0)
        proc = run_cli(self.root, "rebuild", "--id", "s1", "--into", str(into), "--clean")
        self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)


class CliCrashRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "lab"
        setup_snapshot_via_api(self.root)

    def tearDown(self):
        self.tmp.cleanup()

    def test_hard_crash_after_prepare_shows_preparing_then_resume_switches(self):
        proc = run_cli(
            self.root, "restore", "--id", "s1", "--target", "ws",
            "--rid", "rr1", "--at", "6", crash_point="restore_prepared",
        )
        self.assertEqual(proc.returncode, 86)

        proc = run_cli(self.root, "inspect", "--target", "ws")
        report = json.loads(proc.stdout)["report"]
        self.assertEqual(report["state"], "preparing")
        self.assertEqual(report["active_restore"]["phase"], "prepared")
        self.assertIsNone(report["mounted_version"])

        proc = run_cli(self.root, "resume", "--rid", "rr1")
        self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
        self.assertEqual(json.loads(proc.stdout)["result"]["phase"], "switched")

        proc = run_cli(self.root, "inspect", "--target", "ws")
        report = json.loads(proc.stdout)["report"]
        self.assertEqual(report["state"], "switched")

    def test_hard_crash_after_mount_rename_reconciles_to_switched(self):
        proc = run_cli(
            self.root, "restore", "--id", "s1", "--target", "ws",
            "--rid", "rr2", "--at", "6", crash_point="mount_renamed",
        )
        self.assertEqual(proc.returncode, 86)
        # 全新进程仅 inspect：mount.json 原子指针已经是整组真相。
        proc = run_cli(self.root, "inspect", "--target", "ws")
        report = json.loads(proc.stdout)["report"]
        self.assertEqual(report["state"], "switched")
        self.assertEqual(report["mounted_version"]["restore_id"], "rr2")

    def test_concurrent_processes_same_target_single_winner(self):
        procs = []
        for i in range(4):
            procs.append(
                subprocess.Popen(
                    [
                        sys.executable, str(CTL), "--root", str(self.root),
                        "restore", "--id", "s1", "--target", "race", "--rid", f"p{i}",
                    ],
                    cwd=str(REPO),
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                )
            )
        phases = []
        for p in procs:
            out, _ = p.communicate(timeout=60)
            try:
                phases.append(json.loads(out)["result"]["phase"])
            except json.JSONDecodeError:
                phases.append(f"exit{p.returncode}")
        self.assertEqual(phases.count("switched"), 1, phases)
        self.assertEqual(phases.count("conflict"), 3, phases)


if __name__ == "__main__":
    unittest.main()
