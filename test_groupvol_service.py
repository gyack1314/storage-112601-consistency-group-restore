"""覆盖题目其余硬约束：未提交批次、组隔离、取消释放、增量父链、目标单 winner、并发。"""

import os
import tempfile
import threading
import unittest
from pathlib import Path

os.environ["GROUPVOL_FSYNC"] = "0"

from groupvol.coordinator import Coordinator  # noqa: E402
from groupvol.util import ConflictError, ValidationError  # noqa: E402


def m(at, kind, record):
    return {"at": at, "kind": kind, "record": record}


def build_healthy_pair(coord, gid="g", sid="s1", deadline=9, at0=0):
    coord.handle_message(m(at0, "volume", {"group": gid, "volume": "a", "blocks": 2}))
    coord.handle_message(m(at0, "volume", {"group": gid, "volume": "b", "blocks": 2}))
    coord.handle_message(
        m(at0 + 1, "write", {"group": gid, "volume": "a", "batch": "x", "seq": 1, "block": 0, "data": "A"})
    )
    coord.handle_message(
        m(at0 + 1, "write", {"group": gid, "volume": "b", "batch": "x", "seq": 2, "block": 1, "data": "B"})
    )
    coord.handle_message(m(at0 + 2, "flush", {"group": gid, "volume": "a", "through": 1}))
    coord.handle_message(m(at0 + 2, "flush", {"group": gid, "volume": "b", "through": 2}))
    coord.handle_message(m(at0 + 3, "commit", {"group": gid, "batch": "x", "requires": [1, 2]}))
    coord.handle_message(m(at0 + 4, "freeze", {"group": gid, "id": sid, "deadline": deadline}))
    coord.handle_message(m(at0 + 5, "sealed", {"id": sid, "volume": "a", "through": 1}))
    coord.handle_message(m(at0 + 5, "sealed", {"id": sid, "volume": "b", "through": 2}))


class UncommittedBatchTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.coord = Coordinator(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_attachment_present_without_commit_is_not_declared_committed(self):
        c = self.coord
        c.handle_message(m(0, "volume", {"group": "g", "volume": "blobs", "blocks": 4}))
        c.handle_message(m(0, "volume", {"group": "g", "volume": "journal", "blocks": 2}))
        c.handle_message(
            m(1, "write", {"group": "g", "volume": "blobs", "batch": "b9", "seq": 1, "block": 1, "data": "att"})
        )
        c.handle_message(m(2, "flush", {"group": "g", "volume": "blobs", "through": 1}))
        c.handle_message(m(3, "freeze", {"group": "g", "id": "s9", "deadline": 9}))
        c.handle_message(m(4, "sealed", {"id": "s9", "volume": "blobs", "through": 1}))
        c.handle_message(m(4, "sealed", {"id": "s9", "volume": "journal", "through": 0}))

        rep = c.inspect_snapshot("s9")
        self.assertEqual(rep["status"], "complete")
        self.assertTrue(rep["restorable"])  # 崩溃一致点仍然可用
        self.assertEqual(rep["covered_batches"], [])
        batch = rep["batches"][0]
        self.assertEqual(batch["verdict"], "writes_present_but_not_committed")

        result = c.restore_snapshot("s9", "ws")
        self.assertEqual(result["phase"], "switched")
        import json

        rp = json.loads(
            (Path(self.tmp.name) / result["generation_dir"] / "RECOVERY_POINT.json").read_text()
        )
        self.assertEqual(rp["commits"], [])
        self.assertEqual(rp["covered_batches"], [])


class GroupIsolationAndCancelTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.c = Coordinator(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_one_group_timeout_does_not_block_others(self):
        c = self.c
        c.handle_message(m(0, "volume", {"group": "ga", "volume": "v", "blocks": 1}))
        c.handle_message(m(0, "volume", {"group": "gb", "volume": "v", "blocks": 1}))
        c.handle_message(m(1, "freeze", {"group": "ga", "id": "sa", "deadline": 2}))
        c.handle_message(m(1, "freeze", {"group": "gb", "id": "sb", "deadline": 30}))
        # 在 deadline 当秒 sealed => ga 超时。
        c.handle_message(m(2, "sealed", {"id": "sa", "volume": "v", "through": 0}))
        c.handle_message(m(3, "sealed", {"id": "sb", "volume": "v", "through": 0}))
        self.assertEqual(c.inspect_snapshot("sa")["status"], "timed_out")
        sb = c.inspect_snapshot("sb")
        self.assertEqual(sb["status"], "complete")
        self.assertTrue(sb["restorable"])
        group_b = c.inspect_group("gb")
        self.assertEqual(group_b["latest_complete_recovery_point"], "sb")

    def test_cancel_releases_freeze_and_allows_writes_again(self):
        c = self.c
        c.handle_message(m(0, "volume", {"group": "g", "volume": "v", "blocks": 2}))
        c.handle_message(m(1, "freeze", {"group": "g", "id": "sc", "deadline": 99}))
        result = c.cancel("sc", at=2)
        self.assertEqual(result["event"], "freeze_cancelled")
        write = c.handle_message(
            m(3, "write", {"group": "g", "volume": "v", "seq": 7, "block": 0, "data": "again"})
        )
        self.assertEqual(write["event"], "write_accepted")
        self.assertFalse(c.driver.is_frozen("g", "v"))

    def test_cancel_completed_snapshot_is_rejected(self):
        build_healthy_pair(self.c)
        with self.assertRaises(ConflictError):
            self.c.cancel("s1")

    def test_new_timeout_keeps_previous_complete_recovery_point(self):
        c = self.c
        c.handle_message(m(0, "volume", {"group": "g", "volume": "v", "blocks": 1}))
        c.handle_message(m(1, "write", {"group": "g", "volume": "v", "seq": 1, "block": 0, "data": "a"}))
        c.handle_message(m(2, "flush", {"group": "g", "volume": "v", "through": 1}))
        c.handle_message(m(3, "freeze", {"group": "g", "id": "s1", "deadline": 9}))
        c.handle_message(m(4, "sealed", {"id": "s1", "volume": "v", "through": 1}))
        # 第二个冻结超时且无 sealed。
        c.handle_message(m(10, "freeze", {"group": "g", "id": "s2", "deadline": 12}))
        c.handle_message(m(13, "inspect", {"group": "g"}))
        group = c.inspect_group("g")
        self.assertEqual(group["latest_complete_recovery_point"], "s1")
        self.assertTrue(c.inspect_snapshot("s1")["restorable"])
        self.assertEqual(c.inspect_snapshot("s2")["status"], "timed_out")


class IncrementalAndParentLossTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.c = Coordinator(self.root)

    def tearDown(self):
        self.tmp.cleanup()

    def _build_incremental_with_rewrite_and_clear(self):
        c = self.c
        build_healthy_pair(c, sid="s1")
        # 第二批：改写 a#0，并清空 b#1。
        c.handle_message(
            m(6, "write", {"group": "g", "volume": "a", "batch": "y", "seq": 3, "block": 0, "data": "A2"})
        )
        c.handle_message(
            m(6, "write", {"group": "g", "volume": "b", "batch": "y", "seq": 4, "block": 1, "data": None})
        )
        c.handle_message(m(7, "flush", {"group": "g", "volume": "a", "through": 3}))
        c.handle_message(m(7, "flush", {"group": "g", "volume": "b", "through": 4}))
        c.handle_message(m(8, "commit", {"group": "g", "batch": "y", "requires": [3, 4]}))
        c.handle_message(m(9, "freeze", {"group": "g", "id": "s2", "parent": "s1", "deadline": 19}))
        c.handle_message(m(10, "sealed", {"id": "s2", "volume": "a", "through": 3}))
        c.handle_message(m(10, "sealed", {"id": "s2", "volume": "b", "through": 4}))

    def test_incremental_restore_has_new_value_and_explicit_empty(self):
        import json

        self._build_incremental_with_rewrite_and_clear()
        s2 = self.c.inspect_snapshot("s2")
        self.assertTrue(s2["restorable"], s2["problems"])
        self.assertEqual(s2["covered_batches"], ["x", "y"])
        result = self.c.restore_snapshot("s2", "ws")
        self.assertEqual(result["phase"], "switched")
        gen = self.root / result["generation_dir"]
        a0 = json.loads((gen / "volumes" / "a" / "blocks" / "blk-00000000.json").read_text())["data"]
        b1 = json.loads((gen / "volumes" / "b" / "blocks" / "blk-00000001.json").read_text())["data"]
        self.assertEqual(a0, "A2")
        self.assertIsNone(b1)  # 清空块恢复后必须仍是 null，不能从 s1 补回 "B"

    def test_missing_parent_manifest_is_explicit_obstruction(self):
        self._build_incremental_with_rewrite_and_clear()
        # 整个父快照清单不可读。
        (self.root / "snapshots" / "s1" / "manifest.json").unlink()
        report = self.c.verify_snapshot("s2")
        self.assertFalse(report["ok"])
        self.assertIn("s1", report["missing_parent_snapshots"])
        result = self.c.restore_snapshot("s2", "ws")
        self.assertEqual(result["phase"], "blocked")
        self.assertFalse((self.root / "workspaces" / "ws" / "mount.json").exists())

    def test_missing_parent_block_reports_chain_and_blocks_restore(self):
        self._build_incremental_with_rewrite_and_clear()
        # b#0 在 s2 中是指向 s1 的 ref；让 s1 的 b 卷对象丢失。
        self.c.handle_message(m(11, "lost", {"id": "s1", "volume": "b"}))
        report = self.c.verify_snapshot("s2")
        self.assertFalse(report["ok"])
        codes = {p["code"] for p in report["problems"]}
        self.assertTrue(
            {"block_file_missing", "volume_object_missing", "parent_volume_missing"} & codes
        )
        result = self.c.restore_snapshot("s2", "ws")
        self.assertEqual(result["phase"], "blocked")

    def test_production_live_volume_tree_unchanged_by_restore(self):
        import hashlib

        self._build_incremental_with_rewrite_and_clear()
        live = self.root / "live"

        def digest_tree(base: Path):
            out = {}
            for p in sorted(base.rglob("*")):
                if p.is_file():
                    out[str(p.relative_to(base))] = hashlib.sha256(p.read_bytes()).hexdigest()
            return out

        before = digest_tree(live)
        self.c.restore_snapshot("s2", "ws")
        self.c.restore_snapshot("s2", "other", {"a": "aa", "b": "bb"})
        after = digest_tree(live)
        self.assertEqual(before, after)


class SameTargetSingleWinnerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.c = Coordinator(self.tmp.name)
        build_healthy_pair(self.c)

    def tearDown(self):
        self.tmp.cleanup()

    def test_two_distinct_requests_only_one_switches(self):
        r1 = self.c.restore_snapshot("s1", "t", rid="r1")
        r2 = self.c.restore_snapshot("s1", "t", rid="r2")
        self.assertEqual(r1["phase"], "switched")
        self.assertEqual(r2["phase"], "conflict")
        target = self.c.inspect_target("t")
        self.assertEqual(target["winner_restore_id"], "r1")
        self.assertEqual(target["state"], "switched")

    def test_identical_request_is_idempotent(self):
        kwargs = dict(snapshot_id="s1", target="t", mapping={"a": "a2"})
        r1 = self.c.restore_snapshot(**kwargs)
        r2 = self.c.restore_snapshot(**kwargs)
        self.assertEqual(r1["rid"], r2["rid"])
        self.assertEqual(r2["phase"], "switched")
        gens = list((Path(self.tmp.name) / "workspaces" / "t" / "generations").iterdir())
        self.assertEqual(len(gens), 1)

    def test_concurrent_threads_exactly_one_winner(self):
        errors = []
        phases = []
        lock = threading.Lock()
        barrier = threading.Barrier(8)

        def worker(i):
            try:
                coord = Coordinator(self.tmp.name)
                barrier.wait()
                result = coord.restore_snapshot("s1", "race", rid=f"rt{i}")
                with lock:
                    phases.append(result["phase"])
            except Exception as exc:  # pragma: no cover - 诊断用
                with lock:
                    errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertFalse(errors, errors)
        self.assertEqual(phases.count("switched"), 1, phases)
        self.assertEqual(phases.count("conflict"), 7, phases)


class NameSafetyAndUnicodeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.c = Coordinator(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_unicode_group_volume_target_names_supported(self):
        c = self.c
        c.handle_message(m(0, "volume", {"group": "实验组", "volume": "记录卷", "blocks": 1}))
        c.handle_message(m(1, "write", {"group": "实验组", "volume": "记录卷", "seq": 1, "block": 0, "data": "甲"}))
        c.handle_message(m(2, "flush", {"group": "实验组", "volume": "记录卷", "through": 1}))
        c.handle_message(m(3, "freeze", {"group": "实验组", "id": "快照一", "deadline": 9}))
        c.handle_message(m(4, "sealed", {"id": "快照一", "volume": "记录卷", "through": 1}))
        rep = c.inspect_snapshot("快照一")
        self.assertEqual(rep["status"], "complete")
        result = c.restore_snapshot("快照一", "目标工作区", {"记录卷": "新记录卷"})
        self.assertEqual(result["phase"], "switched")
        self.assertTrue((self.c.root / "workspaces" / "目标工作区").exists())

    def test_path_traversal_names_rejected(self):
        for bad in ["../etc", "a/b", "a\\b", "..", "."]:
            with self.assertRaises(ValidationError):
                self.c.restore_snapshot("whatever", bad)
            with self.assertRaises(ValidationError):
                self.c.inspect_target(bad)


if __name__ == "__main__":
    unittest.main()
