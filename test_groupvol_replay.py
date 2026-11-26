"""回放仓库自带的两个演练场景，校验题目明确要求的各项结论。"""

import json
import os
import tempfile
import unittest
from pathlib import Path

os.environ["GROUPVOL_FSYNC"] = "0"

from groupvol.coordinator import Coordinator  # noqa: E402
from groupvol.replay import run_file  # noqa: E402
from groupvol.snapshot import resolve_block  # noqa: E402

REPO = Path(__file__).resolve().parent
EXAMPLES = REPO / "examples"


def events_by_kind(events, kind):
    return [e for e in events if e["kind"] == kind]


class AttachmentAheadScenarioTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "lab"
        self.events = run_file(
            EXAMPLES / "attachment-ahead-of-commit.jsonl", self.root, fresh=True
        )
        self.coord = Coordinator(self.root)

    def tearDown(self):
        self.tmp.cleanup()

    def test_deadline_second_seal_is_timeout_and_releases_all_volumes(self):
        s1 = self.coord.inspect_snapshot("s1")
        self.assertEqual(s1["status"], "timed_out")
        self.assertFalse(s1["restorable"])
        self.assertEqual(s1["expired_at"], 7)
        # journal 的 sealed 恰在 deadline=7 当秒，记为迟到。
        self.assertTrue(s1["seals"]["journal"]["late"])
        self.assertFalse(s1["seals"]["blobs"]["late"])
        # 超时后对每个卷都请求释放冻结。
        released = {a["volume"] for a in s1["driver_releases"]}
        self.assertEqual(released, {"journal", "blobs"})

    def test_single_volume_artifacts_do_not_make_group_snapshot(self):
        s1 = self.coord.inspect_snapshot("s1")
        # 两个单卷制品文件本身完整……
        self.assertTrue(s1["artifact_ok"])
        # ……但整组绝不可恢复。
        self.assertFalse(s1["restorable"])

    def test_commit_cannot_be_declared_covered(self):
        s1 = self.coord.inspect_snapshot("s1")
        self.assertEqual(s1["covered_batches"], [])
        batch = next(b for b in s1["batches"] if b["batch"] == "b1")
        self.assertEqual(batch["verdict"], "committed_but_incomplete_at_point")
        self.assertEqual(batch["contained_seqs"], [1])
        self.assertEqual(batch["missing_seqs"], [2])
        uncovered = s1["declared_but_uncovered_commits"]
        self.assertEqual(len(uncovered), 1)
        self.assertEqual([m["seq"] for m in uncovered[0]["missing"]], [2])

    def test_restore_of_timed_out_snapshot_blocked_and_target_not_switched(self):
        target = self.coord.inspect_target("replay")
        self.assertEqual(target["state"], "original_in_use")
        self.assertIsNone(target["mounted_version"])
        self.assertEqual(len(target["restore_requests"]), 1)
        self.assertEqual(target["restore_requests"][0]["phase"], "blocked")
        self.assertFalse((self.root / "workspaces" / "replay" / "mount.json").exists())


class IncrementalMissingVolumeScenarioTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "lab"
        self.events = run_file(
            EXAMPLES / "incremental-missing-volume.jsonl", self.root, fresh=True
        )
        self.coord = Coordinator(self.root)

    def tearDown(self):
        self.tmp.cleanup()

    def test_s1_is_complete_recovery_point_covering_b1(self):
        s1 = self.coord.inspect_snapshot("s1")
        self.assertEqual(s1["status"], "complete")
        self.assertTrue(s1["restorable"])
        self.assertEqual(s1["covered_batches"], ["b1"])

    def test_s2_covers_both_commits_but_lost_journal_blocks_verify(self):
        s2 = self.coord.inspect_snapshot("s2")
        self.assertEqual(s2["status"], "complete")
        self.assertEqual(s2["covered_batches"], ["b1", "b2"])
        self.assertFalse(s2["restorable"])
        missing = {(m["volume"], m["block"]) for m in s2["missing_blocks"]}
        self.assertIn(("journal", 0), missing)
        codes = {p["code"] for p in s2["problems"]}
        self.assertIn("volume_object_missing", codes)
        self.assertIn("block_file_missing", codes)

    def test_cleared_block_is_explicit_null_not_parent_value(self):
        # s1 中 blobs#2 = "xyz"；b2 的 seq3 把它清空，s2 必须解析成 None。
        v1, p1 = resolve_block(self.root, "s1", "blobs", 2)
        v2, p2 = resolve_block(self.root, "s2", "blobs", 2)
        self.assertEqual(v1, "xyz")
        self.assertIsNone(p1)
        self.assertIsNone(v2)
        self.assertIsNone(p2)
        # s2 的该条目必须是自有 full(null)，而不是 ref，否则丢失 full 文件后会回退父快照。
        manifest = json.loads(
            (self.root / "snapshots" / "s2" / "manifest.json").read_text(encoding="utf-8")
        )
        self.assertEqual(manifest["volumes"]["blobs"]["blocks"][2]["t"], "full")

    def test_restore_s2_blocked_and_after_restart_target_still_original(self):
        # 场景内已做一次 restore + restart + inspect target。
        target = self.coord.inspect_target("dest")
        self.assertEqual(target["state"], "original_in_use")
        self.assertIsNone(target["mounted_version"])
        rec = target["restore_requests"][-1]
        self.assertEqual(rec["phase"], "blocked")
        self.assertTrue(rec["problems"])
        # 协调器“重启”（新建实例）后结论不变，没有半套挂载。
        rebooted = Coordinator(self.root).inspect_target("dest")
        self.assertEqual(rebooted["state"], "original_in_use")
        self.assertIsNone(rebooted["mounted_version"])

    def test_s1_still_restorable_into_new_names(self):
        result = self.coord.restore_snapshot(
            "s1", "isolated", {"blobs": "data", "journal": "manifest"}, rid="r-ok"
        )
        self.assertEqual(result["phase"], "switched")
        gen = self.root / result["generation_dir"]
        self.assertEqual(
            json.loads((gen / "volumes" / "data" / "VOLUME.json").read_text())["blocks_count"], 8
        )
        self.assertEqual(
            json.loads((gen / "volumes" / "manifest" / "VOLUME.json").read_text())["blocks_count"],
            4,
        )
        self.assertEqual(
            json.loads((gen / "volumes" / "data" / "blocks" / "blk-00000002.json").read_text())[
                "data"
            ],
            "xyz",
        )


if __name__ == "__main__":
    unittest.main()
