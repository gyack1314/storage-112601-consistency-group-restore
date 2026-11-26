"""协调服务：批次/提交登记、组快照冻结状态机、覆盖提交判定、原子恢复与崩溃对账。

所有可变状态落在 ``<root>/coordinator-state.json``（原子替换写），块数据在
driver/snapshot/workspace 目录中。每个公共操作通过 ``operation()`` 取跨进程
排他锁、重载最新状态、做崩溃对账，再修改并原子保存。
"""

from __future__ import annotations

import contextlib
import os
import shutil
import threading
from pathlib import Path
from typing import Any, Iterator

from . import snapshot as snap_mod
from .driver import LocalBlockDriver, VolumeFrozenError
from .restore import materialize_workspace
from .util import (
    ConflictError,
    NotFoundError,
    ServiceError,
    ValidationError,
    atomic_write_json,
    canonical_mapping,
    check_int,
    check_name,
    file_lock,
    mapping_key,
    read_json,
)

STATE_FILE = "coordinator-state.json"

# 同进程多线程时的串行层；跨进程一致性由 fcntl 文件锁保证。
_PROCESS_LOCK = threading.RLock()


def _default_state() -> dict[str, Any]:
    return {
        "version": 1,
        "clock": -1,
        "groups": {},
        "volumes": {},
        "writes": {},
        "batches": {},
        "commits": [],
        "snapshots": {},
        "restores": {},
        "targets": {},
        "losses": [],
        "actions": [],
    }


def _crash_hook(point: str) -> None:
    """测试用故障注入：GROUPVOL_CRASH=<point> 时在指定持久化点硬退出。"""
    if os.environ.get("GROUPVOL_CRASH") == point:
        os._exit(86)


class Coordinator:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / "locks").mkdir(exist_ok=True)
        self.driver = LocalBlockDriver(self.root)
        self.state_path = self.root / STATE_FILE
        self.state: dict[str, Any] = self._load_state()
        self._reconcile_state()

    # ------------------------------------------------------------ plumbing

    def _load_state(self) -> dict[str, Any]:
        data = read_json(self.state_path) if self.state_path.exists() else None
        if not isinstance(data, dict):
            return _default_state()
        base = _default_state()
        base.update(data)
        return base

    def _save_state(self) -> None:
        atomic_write_json(self.state_path, self.state)

    @contextlib.contextmanager
    def operation(self) -> Iterator["Coordinator"]:
        lock = self.root / "locks" / "coordinator.lock"
        with _PROCESS_LOCK, file_lock(lock):
            self.state = self._load_state()
            self._reconcile_state()
            yield self
            self._save_state()

    def refresh(self) -> "Coordinator":
        """无锁重读并按 mount.json 对账，供 inspect/verify 使用。"""
        self.state = self._load_state()
        self._reconcile_state()
        return self

    def _resolve_at(self, at: int | None) -> int:
        clock = int(self.state.get("clock", -1))
        if at is None:
            at = max(clock, 0)
        at = check_int(at, "at", minimum=0)
        if at < clock:
            raise ValidationError(f"消息秒点不能倒退：收到 at={at}，当前时钟={clock}")
        self.state["clock"] = max(clock, at)
        return at

    def _record_action(self, at: int, action: str, **details: Any) -> None:
        self.state.setdefault("actions", []).append({"at": at, "action": action, **details})

    # ------------------------------------------------------------ crash reconcile

    def _mount_file(self, target: str) -> Path:
        return self.root / "workspaces" / target / "mount.json"

    def _read_mount(self, target: str) -> dict[str, Any] | None:
        from .util import try_read_json

        data = try_read_json(self._mount_file(target))
        return data if isinstance(data, dict) else None

    def _reconcile_state(self) -> list[dict[str, Any]]:
        """以 mount.json 原子指针为准，纠正崩溃窗口中滞后的恢复阶段。"""
        notes: list[dict[str, Any]] = []
        for target, tstate in self.state.get("targets", {}).items():
            mount = self._read_mount(target)
            if mount:
                tstate["mount"] = mount
                tstate["winner"] = mount.get("restore_id") or tstate.get("winner")
        for rid, rec in self.state.get("restores", {}).items():
            target = rec.get("target")
            if not target:
                continue
            mount = self._read_mount(target)
            if mount and mount.get("restore_id") == rid and rec.get("phase") != "switched":
                rec["phase"] = "switched"
                rec["switched_at"] = mount.get("mounted_at") or rec.get("switched_at")
                rec["reconciled"] = True
                tstate = self.state["targets"].setdefault(
                    target, {"winner": None, "generation_counter": 0, "mount": None}
                )
                tstate["winner"] = rid
                tstate["mount"] = mount
                notes.append({"restore_id": rid, "target": target, "phase": "switched"})
        return notes

    def recover(self, at: int | None = None) -> dict[str, Any]:
        """重启协调服务后调用：对账并持久化结果。"""
        with self.operation():
            now = self._resolve_at(at)
            expired = self._expire_due_locked(now)
            self._save_state()
            return {
                "event": "coordinator_recovered",
                "at": now,
                "expired_snapshots": expired,
                "targets": {
                    name: {
                        "winner": t.get("winner"),
                        "mounted_snapshot": (t.get("mount") or {}).get("snapshot_id"),
                    }
                    for name, t in self.state.get("targets", {}).items()
                },
                "pending_restores": [
                    {"rid": rid, "target": r.get("target"), "phase": r.get("phase")}
                    for rid, r in self.state.get("restores", {}).items()
                    if r.get("phase") in {"verifying", "preparing", "prepared", "switching"}
                ],
            }

    # ------------------------------------------------------------ clock / timeout

    def tick(self, at: int) -> dict[str, Any]:
        with self.operation():
            now = self._resolve_at(at)
            expired = self._expire_due_locked(now)
            return {"at": now, "expired": expired}

    def _expire_due_locked(self, now: int) -> list[dict[str, Any]]:
        expired: list[dict[str, Any]] = []
        for sid, snap in self.state.get("snapshots", {}).items():
            if snap.get("status") != "freezing":
                continue
            deadline = int(snap.get("deadline"))
            if deadline > now:
                continue
            snap["status"] = "timed_out"
            snap["expired_at"] = deadline
            snap["expiry_seen_at"] = now
            if snap.get("volume_specs"):
                # 让保留下来的单卷制品清单也自述为 timed_out，避免被误读为可恢复。
                self._write_manifest_locked(snap, "timed_out")
            released: list[str] = []
            for volume in snap.get("requested_volumes", []):
                try:
                    if self.driver.release(snap["group"], volume, sid):
                        released.append(volume)
                except ServiceError:
                    pass
                self._record_action(
                    now,
                    "release_freeze",
                    id=sid,
                    group=snap["group"],
                    volume=volume,
                    reason="deadline_timeout",
                )
            snap["released_volumes"] = sorted(set(released + snap.get("requested_volumes", [])))
            expired.append(
                {
                    "id": sid,
                    "status": "timed_out",
                    "deadline": deadline,
                    "at_deadline_second_is_timeout": True,
                    "release_requested_for": snap.get("requested_volumes", []),
                    "driver_released_tokens": released,
                    "sealed_volumes": list((snap.get("seals") or {}).keys()),
                    "note": "已有的单卷只读快照保留在磁盘上，但整组快照不可用、不可恢复",
                }
            )
        return expired

    # ------------------------------------------------------------ dispatch

    def handle_message(self, row: dict[str, Any]) -> dict[str, Any]:
        at = int(row["at"])
        kind = str(row["kind"])
        rec = row.get("record") or {}
        with self.operation():
            now = self._resolve_at(at)
            self._expire_due_locked(now)
            handler = {
                "volume": self._h_volume,
                "write": self._h_write,
                "flush": self._h_flush,
                "commit": self._h_commit,
                "freeze": self._h_freeze,
                "sealed": self._h_sealed,
                "cancel": self._h_cancel,
                "lost": self._h_lost,
            }.get(kind)
            if handler is None:
                if kind == "inspect":
                    return {"event": "inspect_report", "report": self._inspect(rec)}
                if kind == "restore":
                    return self._h_restore(rec, now)
                if kind == "restart":
                    # replay 会在外层重建 Coordinator；这里仅做对账。
                    return {"event": "restart_reconciled", "recovery": self._recover_report_locked()}
                raise ValidationError(f"未支持的消息类型：{kind}")
            return handler(rec, now)

    def _h_restore(self, rec: dict[str, Any], at: int) -> dict[str, Any]:
        snapshot_id = check_name(rec.get("id"), "快照 id")
        target = check_name(rec.get("target"), "恢复目标")
        mapping = canonical_mapping(rec.get("mapping"))
        rid = rec.get("rid") or rec.get("restore_id") or rec.get("request")
        if rid is not None:
            rid = check_name(rid, "恢复申请 id")
        force = bool(rec.get("force", False))
        return self._restore_locked(
            snapshot_id, target, mapping, rid=rid, at=at, force=force
        )

    def _recover_report_locked(self) -> dict[str, Any]:
        return {
            "pending_restores": [
                {"rid": rid, "target": r.get("target"), "phase": r.get("phase")}
                for rid, r in self.state.get("restores", {}).items()
                if r.get("phase") in {"verifying", "preparing", "prepared", "switching"}
            ]
        }

    # ------------------------------------------------------------ volumes / writes

    def _h_volume(self, rec: dict[str, Any], at: int) -> dict[str, Any]:
        group = check_name(rec.get("group"), "工作组")
        volume = check_name(rec.get("volume"), "卷名")
        blocks = check_int(rec.get("blocks"), "blocks", minimum=1)
        existed = self.driver.exists(group, volume)
        meta = self.driver.register_volume(group, volume, blocks, at)
        gstate = self.state["groups"].setdefault(group, {"volumes": []})
        if volume not in gstate["volumes"]:
            gstate["volumes"].append(volume)
        self.state["volumes"][f"{group}/{volume}"] = {
            "group": group,
            "volume": volume,
            "blocks": blocks,
            "registered_at": at,
            "flush_through": int(meta.get("flush_through", -1)),
        }
        return {
            "event": "volume_registered",
            "at": at,
            "group": group,
            "volume": volume,
            "blocks": blocks,
            "already_existed": existed,
        }

    def _require_volume(self, group: str, volume: str) -> dict[str, Any]:
        key = f"{group}/{volume}"
        v = self.state["volumes"].get(key)
        if v is None:
            raise NotFoundError(f"卷未登记：{key}")
        return v

    def _h_write(self, rec: dict[str, Any], at: int) -> dict[str, Any]:
        group = check_name(rec.get("group"), "工作组")
        volume = check_name(rec.get("volume"), "卷名")
        seq = check_int(rec.get("seq"), "seq", minimum=0)
        block = check_int(rec.get("block"), "block", minimum=0)
        if "data" not in rec:
            raise ValidationError("write 必须提供 data（字符串或 null 清空）")
        data = rec.get("data")
        if data is not None and not isinstance(data, str):
            raise ValidationError("data 必须是字符串或 null")
        batch = rec.get("batch")
        if batch is not None:
            batch = check_name(batch, "batch")
        self._require_volume(group, volume)

        key = str(seq)
        payload = {
            "at": at,
            "group": group,
            "volume": volume,
            "batch": batch,
            "seq": seq,
            "block": block,
            "data": data,
        }
        if key in self.state["writes"]:
            old = self.state["writes"][key]
            if all(old.get(k) == payload[k] for k in ("group", "volume", "block", "data", "batch")):
                return {"event": "write_idempotent", "at": at, "seq": seq}
            raise ConflictError(f"seq={seq} 已存在且内容不一致，拒绝覆盖")

        try:
            self.driver.write_block(group, volume, seq=seq, block=block, data=data, batch=batch)
        except VolumeFrozenError as error:
            raise ConflictError(str(error), code="volume_frozen") from error

        self.state["writes"][key] = payload
        if batch is not None:
            bstate = self.state["batches"].setdefault(group, {}).setdefault(
                batch, {"writes": [], "first_at": at}
            )
            if seq not in bstate["writes"]:
                bstate["writes"].append(seq)
        return {
            "event": "write_accepted",
            "at": at,
            "group": group,
            "volume": volume,
            "seq": seq,
            "block": block,
            "batch": batch,
            "cleared": data is None,
        }

    def _h_flush(self, rec: dict[str, Any], at: int) -> dict[str, Any]:
        group = check_name(rec.get("group"), "工作组")
        volume = check_name(rec.get("volume"), "卷名")
        through = check_int(rec.get("through"), "through", minimum=0)
        self._require_volume(group, volume)
        watermark = self.driver.flush(group, volume, through)
        self.state["volumes"][f"{group}/{volume}"]["flush_through"] = watermark
        self._record_action(at, "flush", group=group, volume=volume, through=through, watermark=watermark)
        return {
            "event": "flush_persisted",
            "at": at,
            "group": group,
            "volume": volume,
            "through": through,
            "watermark": watermark,
        }

    def _h_commit(self, rec: dict[str, Any], at: int) -> dict[str, Any]:
        group = check_name(rec.get("group"), "工作组")
        batch = check_name(rec.get("batch"), "batch")
        requires = rec.get("requires")
        if requires is None:
            requires = list(
                self.state["batches"].get(group, {}).get(batch, {}).get("writes", [])
            )
        if not isinstance(requires, list) or not all(isinstance(x, int) and not isinstance(x, bool) for x in requires):
            raise ValidationError("commit.requires 必须是整数序号数组")
        commit = {"at": at, "group": group, "batch": batch, "requires": list(requires)}
        # 完全相同的提交声明幂等。
        for old in self.state["commits"]:
            if (
                old["group"] == group
                and old["batch"] == batch
                and old["at"] == at
                and list(old["requires"]) == list(requires)
            ):
                return {"event": "commit_idempotent", "commit": commit}
        self.state["commits"].append(commit)
        self.state["batches"].setdefault(group, {}).setdefault(
            batch, {"writes": [], "first_at": at}
        ).setdefault("commits", []).append(commit)
        return {"event": "commit_recorded", "commit": commit}

    # ------------------------------------------------------------ freeze / seal

    def _h_freeze(self, rec: dict[str, Any], at: int) -> dict[str, Any]:
        group = check_name(rec.get("group"), "工作组")
        sid = check_name(rec.get("id"), "快照 id")
        deadline = check_int(rec.get("deadline"), "deadline", minimum=0)
        parent = rec.get("parent")
        if parent is not None:
            parent = check_name(parent, "parent 快照 id")

        if sid in self.state["snapshots"]:
            old = self.state["snapshots"][sid]
            if old.get("group") != group or int(old.get("deadline")) != deadline or old.get("parent") != parent:
                raise ConflictError(f"快照 id {sid} 已以不同参数存在")
            return {"event": "freeze_idempotent", "id": sid, "status": old.get("status")}

        gstate = self.state["groups"].get(group)
        volumes = list(gstate["volumes"]) if gstate else []
        if not volumes:
            raise ValidationError(f"工作组 {group} 还没有登记任何卷，无法冻结成组快照")

        record: dict[str, Any] = {
            "id": sid,
            "group": group,
            "parent": parent,
            "deadline": deadline,
            "created_at": at,
            "status": "freezing",
            "requested_volumes": volumes,
            "seals": {},
            "volume_specs": {},
            "released_volumes": [],
            "warnings": [],
        }

        if parent is not None:
            parent_snap = self.state["snapshots"].get(parent)
            if parent_snap is None:
                record["status"] = "invalid"
                record["invalid_reason"] = f"parent_not_found: 父快照 {parent} 不存在"
                record["warnings"].append(record["invalid_reason"])
            elif parent_snap.get("group") != group:
                record["status"] = "invalid"
                record["invalid_reason"] = "parent_group_mismatch: 父快照不属于同一工作组"
            elif parent_snap.get("status") != "complete":
                record["status"] = "invalid"
                record["invalid_reason"] = (
                    f"parent_not_complete: 父快照 {parent} 状态为 {parent_snap.get('status')}，"
                    "只有已完成的同组快照可作为增量父快照"
                )

        self.state["snapshots"][sid] = record
        self._record_action(at, "freeze_requested", id=sid, group=group, deadline=deadline, parent=parent)
        # deadline 当秒即超时：若创建时已处于 deadline 秒，立即过期。
        self._expire_due_locked(at)
        return {
            "event": "freeze_accepted",
            "at": at,
            "id": sid,
            "status": record["status"],
            "requested_volumes": volumes,
            "deadline": deadline,
            "parent": parent,
            "invalid_reason": record.get("invalid_reason"),
        }

    def _seal_values_locked(self, snap: dict[str, Any], volume: str, through: int) -> dict[str, Any]:
        group = snap["group"]
        vstate = self._require_volume(group, volume)
        blocks_count = int(vstate["blocks"])
        driver_wm = self.driver.watermark(group, volume)
        # 切快照只承认已落盘部分；从无 flush 回报时信任 sealed 水位，
        # 否则 sealed 超过 flush 水位则夹取到实际落盘点并告警。
        effective = through if driver_wm < 0 else min(through, driver_wm)
        values = self.driver.as_of_values(group, volume, effective)
        parent = snap.get("parent")
        parent_ok = bool(
            parent
            and self.state["snapshots"].get(parent, {}).get("status") == "complete"
        )
        spec = snap_mod.build_volume_spec(
            self.root,
            snap["id"],
            volume,
            blocks_count,
            values,
            parent_id=parent if parent_ok else None,
            watermark=through,
            durable_through=driver_wm,
            sealed_at=int(self.state["clock"]),
        )
        spec["effective_watermark"] = effective
        if driver_wm >= 0 and through > driver_wm:
            spec["warnings"].append(
                f"sealed 水位 through={through} 高于驱动落盘水位 {driver_wm}，"
                f"共同恢复点只取到 {effective}"
            )
        return spec

    def _h_sealed(self, rec: dict[str, Any], at: int) -> dict[str, Any]:
        sid = check_name(rec.get("id"), "快照 id")
        volume = check_name(rec.get("volume"), "卷名")
        through = check_int(rec.get("through"), "through", minimum=0)
        snap = self.state["snapshots"].get(sid)
        if snap is None:
            raise NotFoundError(f"sealed 指向未知快照 id：{sid}")
        if volume not in snap.get("requested_volumes", []):
            raise ValidationError(f"卷 {volume} 不属于快照 {sid} 的冻结卷集合")

        # 幂等重复 sealed。
        existing = snap.get("seals", {}).get(volume)
        if existing is not None:
            if int(existing["through"]) == through:
                return {"event": "sealed_idempotent", "id": sid, "volume": volume}
            raise ConflictError(f"卷 {volume} 对快照 {sid} 已报 through={existing['through']}，不能改为 {through}")

        status = snap.get("status")
        if status == "complete":
            return {"event": "sealed_ignored_completed", "id": sid, "volume": volume}

        if status in {"timed_out", "cancelled", "invalid"}:
            # 迟到的 sealed：仍切出单卷制品供检查，但绝不恢复整组资格，也不重新冻结。
            spec = self._seal_values_locked(snap, volume, through)
            snap.setdefault("volume_specs", {})[volume] = spec
            snap.setdefault("seals", {})[volume] = {
                "at": at,
                "through": through,
                "on_time": False,
                "late": True,
                "after_status": status,
            }
            self._write_manifest_locked(snap, str(status))
            self._record_action(at, "late_sealed_artifact", id=sid, volume=volume, through=through)
            return {
                "event": "sealed_after_window_closed",
                "id": sid,
                "volume": volume,
                "through": through,
                "snapshot_status": status,
                "group_snapshot_usable": False,
                "note": "已保留该卷的单卷快照制品，但整组快照不可用",
            }

        # status == freezing；能走到这里说明 deadline > at（当秒超时已先执行）。
        self.driver.stop_writes(snap["group"], volume, sid)
        self._record_action(at, "stop_writes", id=sid, group=snap["group"], volume=volume)
        spec = self._seal_values_locked(snap, volume, through)
        snap.setdefault("volume_specs", {})[volume] = spec
        snap.setdefault("seals", {})[volume] = {
            "at": at,
            "through": through,
            "on_time": True,
            "late": False,
        }

        sealed_volumes = set(snap["seals"].keys())
        finalized = sealed_volumes == set(snap["requested_volumes"])
        result: dict[str, Any] = {
            "event": "sealed_accepted",
            "id": sid,
            "volume": volume,
            "through": through,
            "sealed_count": len(sealed_volumes),
            "requested_count": len(snap["requested_volumes"]),
            "finalized": finalized,
        }
        if finalized:
            result["finalize"] = self._finalize_locked(snap, at)
        else:
            self._write_manifest_locked(snap, "freezing")
        return result

    def _commit_coverage_locked(self, snap: dict[str, Any], cutoff: int | None = None) -> dict[str, Any]:
        """计算快照共同恢复点上的提交覆盖与批次状态。"""
        seals = snap.get("seals", {})
        specs = snap.get("volume_specs", {})

        def seq_contained(seq: int) -> tuple[bool, str | None]:
            write = self.state["writes"].get(str(seq))
            if write is None:
                return False, f"requires 序号 {seq} 没有对应的 write 登记"
            volume = write["volume"]
            seal = seals.get(volume)
            if seal is None:
                return False, f"卷 {volume} 尚未 sealed，序号 {seq} 不在共同恢复点"
            # 以实际物化的有效水位为准（通常等于 sealed.through）。
            spec = specs.get(volume) or {}
            watermark = int(spec.get("effective_watermark", seal["through"]))
            if watermark < seq:
                return (
                    False,
                    f"卷 {volume} 有效水位 through={watermark} 未覆盖必需序号 {seq}"
                    "（附件可能已落盘而主记录写入/落盘未完成）",
                )
            return True, None

        scope_commits = [
            c
            for c in self.state["commits"]
            if c.get("group") == snap["group"] and (cutoff is None or int(c["at"]) <= cutoff)
        ]
        covered_commits: list[dict[str, Any]] = []
        uncovered_commits: list[dict[str, Any]] = []
        for commit in scope_commits:
            missing: list[dict[str, Any]] = []
            for seq in commit.get("requires", []):
                ok, reason = seq_contained(int(seq))
                if not ok:
                    write = self.state["writes"].get(str(seq)) or {}
                    missing.append(
                        {"seq": seq, "volume": write.get("volume"), "block": write.get("block"), "reason": reason}
                    )
            entry = dict(commit)
            if missing:
                entry["missing"] = missing
                uncovered_commits.append(entry)
            else:
                covered_commits.append(entry)

        covered_batches = sorted({c["batch"] for c in covered_commits})

        batch_reports: list[dict[str, Any]] = []
        for batch, bstate in self.state["batches"].get(snap["group"], {}).items():
            seqs = list(bstate.get("writes", []))
            contained: list[int] = []
            missing_seq: list[int] = []
            for seq in seqs:
                ok, _ = seq_contained(int(seq))
                (contained if ok else missing_seq).append(seq)
            batch_commits = [c for c in scope_commits if c["batch"] == batch]
            is_covered = batch in covered_batches
            if is_covered:
                verdict = "committed_in_recovery_point"
            elif batch_commits:
                verdict = "committed_but_incomplete_at_point"
            elif contained:
                verdict = "writes_present_but_not_committed"
            else:
                verdict = "not_at_recovery_point"
            batch_reports.append(
                {
                    "batch": batch,
                    "verdict": verdict,
                    "registered_seqs": seqs,
                    "contained_seqs": contained,
                    "missing_seqs": missing_seq,
                    "commit_declared": bool(batch_commits),
                    "covered": is_covered,
                }
            )

        return {
            "covered_commits": covered_commits,
            "uncovered_commits": uncovered_commits,
            "covered_batches": covered_batches,
            "batches": batch_reports,
        }

    def _write_manifest_locked(self, snap: dict[str, Any], status: str) -> Path:
        manifest = {
            "kind": "groupvol-manifest",
            "version": 1,
            "id": snap["id"],
            "group": snap["group"],
            "parent": snap.get("parent"),
            "status": status,
            "created_at": snap.get("created_at"),
            "completed_at": snap.get("completed_at"),
            "deadline": snap.get("deadline"),
            "volume_order": [
                v for v in snap["requested_volumes"] if v in snap.get("volume_specs", {})
            ],
            "volumes": snap.get("volume_specs", {}),
            "commits": snap.get("covered_commits", []),
            "covered_batches": snap.get("covered_batches", []),
        }
        return snap_mod.write_manifest(self.root, manifest)

    def _finalize_locked(self, snap: dict[str, Any], at: int) -> dict[str, Any]:
        sid = snap["id"]
        coverage = self._commit_coverage_locked(snap, cutoff=at)
        snap["covered_batches"] = coverage["covered_batches"]
        snap["covered_commits"] = coverage["covered_commits"]
        snap["status"] = "complete"
        snap["completed_at"] = at
        self._write_manifest_locked(snap, "complete")
        # 整组切出完成，释放所有卷的写冻结令牌；只读快照制品保留。
        for volume in snap["requested_volumes"]:
            try:
                self.driver.release(snap["group"], volume, sid)
            except ServiceError:
                pass
            self._record_action(at, "release_freeze", id=sid, group=snap["group"], volume=volume, reason="finalized")
        return {
            "id": sid,
            "status": "complete",
            "covered_batches": coverage["covered_batches"],
            "volumes": len(snap["requested_volumes"]),
        }

    # ------------------------------------------------------------ cancel / lost

    def cancel(self, target_id: str, at: int | None = None, reason: str = "operator_cancel") -> dict[str, Any]:
        with self.operation():
            now = self._resolve_at(at)
            return self._cancel_locked(target_id, now, reason)

    def _h_cancel(self, rec: dict[str, Any], at: int) -> dict[str, Any]:
        target_id = rec.get("id") or rec.get("snapshot") or rec.get("restore_id") or rec.get("rid")
        kind = rec.get("kind")
        if not target_id:
            raise ValidationError("cancel 需要 id（快照 id 或恢复申请 id）")
        if kind == "restore":
            return self._cancel_restore_locked(str(target_id), at, rec.get("reason") or "operator_cancel")
        if kind == "snapshot":
            return self._cancel_snapshot_locked(str(target_id), at, rec.get("reason") or "operator_cancel")
        return self._cancel_locked(str(target_id), at, rec.get("reason") or "operator_cancel")

    def _cancel_locked(self, target_id: str, at: int, reason: str) -> dict[str, Any]:
        if target_id in self.state["snapshots"]:
            return self._cancel_snapshot_locked(target_id, at, reason)
        if target_id in self.state["restores"]:
            return self._cancel_restore_locked(target_id, at, reason)
        raise NotFoundError(f"找不到可取消的快照冻结或恢复申请：{target_id}")

    def _cancel_snapshot_locked(self, sid: str, at: int, reason: str) -> dict[str, Any]:
        snap = self.state["snapshots"].get(sid)
        if snap is None:
            raise NotFoundError(f"快照 id 不存在：{sid}")
        if snap.get("status") == "cancelled":
            return {"event": "cancel_idempotent", "id": sid, "kind": "snapshot"}
        if snap.get("status") == "complete":
            raise ConflictError(f"快照 {sid} 已完成并成为恢复点，不能取消；可保留或在恢复时选择其他点")
        snap["status"] = "cancelled"
        snap["cancelled_at"] = at
        snap["cancel_reason"] = reason
        if snap.get("volume_specs"):
            self._write_manifest_locked(snap, "cancelled")
        released = []
        for volume in snap.get("requested_volumes", []):
            try:
                if self.driver.release(snap["group"], volume, sid):
                    released.append(volume)
            except ServiceError:
                pass
            self._record_action(at, "release_freeze", id=sid, volume=volume, reason="cancel")
        return {
            "event": "freeze_cancelled",
            "id": sid,
            "at": at,
            "release_requested_for": snap.get("requested_volumes", []),
            "driver_released_tokens": released,
            "note": "冻结已释放，卷可重新接写；已切出的单卷制品不构成整组快照",
        }

    def _cancel_restore_locked(self, rid: str, at: int, reason: str) -> dict[str, Any]:
        rec = self.state["restores"].get(rid)
        if rec is None:
            raise NotFoundError(f"恢复申请不存在：{rid}")
        phase = rec.get("phase")
        if phase == "cancelled":
            return {"event": "cancel_idempotent", "rid": rid, "kind": "restore"}
        if phase == "switched":
            raise ConflictError(f"恢复申请 {rid} 已完成整组切换，不能取消；可显式恢复另一个恢复点")
        if phase == "switching":
            raise ConflictError(
                f"恢复申请 {rid} 正处于切换提交点，请先 resume 完成原子切换，避免遗留不一致指针"
            )
        # 清理暂存/已准备但未挂载的生成目录。
        for key in ("staging_dir", "generation_dir"):
            rel = rec.get(key)
            if rel:
                path = self.root / rel
                if path.exists():
                    shutil.rmtree(path, ignore_errors=True)
        rec["phase"] = "cancelled"
        rec["cancelled_at"] = at
        rec["cancel_reason"] = reason
        tstate = self.state["targets"].get(rec["target"])
        if tstate and tstate.get("active_rid") == rid:
            tstate["active_rid"] = None
        return {"event": "restore_cancelled", "rid": rid, "target": rec["target"], "at": at}

    def _h_lost(self, rec: dict[str, Any], at: int) -> dict[str, Any]:
        sid = check_name(rec.get("id"), "快照 id")
        volume = check_name(rec.get("volume"), "卷名")
        block = rec.get("block")
        if block is not None:
            block = check_int(block, "block", minimum=0)
        result = snap_mod.apply_lost(self.root, sid, volume, block, at=at)
        loss = {"at": at, "id": sid, "volume": volume, "block": block, **result}
        self.state["losses"].append(loss)
        return {"event": "loss_applied", **loss}

    # ------------------------------------------------------------ verify / inspect

    def verify_snapshot(self, sid: str) -> dict[str, Any]:
        self.refresh()
        return self._verify_snapshot_report(sid)

    def _verify_snapshot_report(self, sid: str) -> dict[str, Any]:
        snap = self.state["snapshots"].get(sid)
        fallback = (snap or {}).get("volume_specs")
        requested = (snap or {}).get("requested_volumes")
        report = snap_mod.verify_snapshot_artifacts(
            self.root, sid, fallback_specs=fallback, requested_volumes=requested
        )
        status = (snap or {}).get("status", "unknown")
        report["status"] = status
        report["lifecycle_restorable"] = status == "complete"
        report["restorable"] = bool(status == "complete" and report["ok"])
        if status == "unknown":
            report["lifecycle_reason"] = "coordinator 状态中没有该快照 id"
        elif status == "timed_out":
            report["lifecycle_reason"] = "冻结等待已超时，单卷快照不能视为整组可用"
        elif status == "cancelled":
            report["lifecycle_reason"] = "冻结已被取消"
        elif status == "invalid":
            report["lifecycle_reason"] = (snap or {}).get("invalid_reason", "快照参数无效")
        elif status != "complete":
            report["lifecycle_reason"] = f"快照尚未完成（状态 {status}）"
        return report

    def inspect(self, query: dict[str, Any] | None = None) -> dict[str, Any]:
        self.refresh()
        return self._inspect(query)

    def _inspect(self, query: dict[str, Any] | None = None) -> dict[str, Any]:
        query = query or {}
        if query.get("id"):
            return self.inspect_snapshot(str(query["id"]))
        if query.get("target"):
            return self.inspect_target(str(query["target"]))
        if query.get("group"):
            return self.inspect_group(str(query["group"]))
        # 无具体条件：返回总览。
        return self.inspect_overview()

    def inspect_snapshot(self, sid: str) -> dict[str, Any]:
        snap = self.state["snapshots"].get(sid)
        if snap is None:
            raise NotFoundError(f"快照 id 不存在：{sid}")
        verify = self._verify_snapshot_report(sid)
        cutoff = snap.get("completed_at")
        if cutoff is None:
            cutoff = snap.get("expired_at") or snap.get("cancelled_at") or snap.get("deadline")
        coverage = self._commit_coverage_locked(snap, cutoff=cutoff)

        mounted_at: list[str] = []
        for target, tstate in self.state["targets"].items():
            mount = tstate.get("mount") or {}
            if mount.get("snapshot_id") == sid and tstate.get("winner"):
                mounted_at.append(target)

        seals_report = {
            v: {
                "at": s.get("at"),
                "through": s.get("through"),
                "on_time": s.get("on_time", not s.get("late", False)),
                "late": bool(s.get("late")),
            }
            for v, s in (snap.get("seals") or {}).items()
        }
        return {
            "kind": "snapshot-inspection",
            "id": sid,
            "group": snap.get("group"),
            "parent": snap.get("parent"),
            "status": snap.get("status"),
            "restorable": verify["restorable"],
            "deadline": snap.get("deadline"),
            "created_at": snap.get("created_at"),
            "completed_at": snap.get("completed_at"),
            "expired_at": snap.get("expired_at"),
            "cancelled_at": snap.get("cancelled_at"),
            "invalid_reason": snap.get("invalid_reason"),
            "requested_volumes": snap.get("requested_volumes"),
            "seals": seals_report,
            "covered_commits": coverage["covered_commits"],
            "covered_batches": coverage["covered_batches"],
            "declared_but_uncovered_commits": coverage["uncovered_commits"],
            "batches": coverage["batches"],
            "missing_blocks": verify["missing_blocks"],
            "missing_parent_snapshots": verify["missing_parent_snapshots"],
            "artifact_ok": verify["ok"],
            "problems": verify["problems"],
            "volume_verification": verify["volume_reports"],
            "mounted_at_targets": mounted_at,
            "driver_releases": [
                a for a in self.state["actions"] if a.get("id") == sid and a.get("action") == "release_freeze"
            ],
        }

    def inspect_group(self, group: str) -> dict[str, Any]:
        gstate = self.state["groups"].get(group)
        if gstate is None:
            raise NotFoundError(f"工作组不存在：{group}")
        snaps = []
        latest_complete = None
        latest_restorable = None
        active_freeze = None
        for sid, snap in self.state["snapshots"].items():
            if snap.get("group") != group:
                continue
            item = {
                "id": sid,
                "parent": snap.get("parent"),
                "status": snap.get("status"),
                "created_at": snap.get("created_at"),
                "completed_at": snap.get("completed_at"),
                "covered_batches": snap.get("covered_batches", []),
            }
            if snap.get("status") == "complete":
                report = self._verify_snapshot_report(sid)
                item["restorable"] = report["restorable"]
                item["missing_blocks"] = report["missing_blocks"]
                latest_complete = sid
                if report["restorable"]:
                    latest_restorable = sid
            if snap.get("status") == "freezing":
                active_freeze = sid
            snaps.append(item)
        mounts = []
        for target, tstate in self.state["targets"].items():
            mount = tstate.get("mount") or {}
            if mount.get("group") == group:
                mounts.append({"target": target, "snapshot_id": mount.get("snapshot_id"), **mount})
        return {
            "kind": "group-inspection",
            "group": group,
            "volumes": gstate.get("volumes", []),
            "snapshots": snaps,
            "latest_complete_recovery_point": latest_complete,
            "latest_restorable_recovery_point": latest_restorable,
            "active_freeze": active_freeze,
            "mounts": mounts,
        }

    def inspect_target(self, target: str) -> dict[str, Any]:
        check_name(target, "目标名")
        tstate = self.state["targets"].get(target, {})
        mount = self._read_mount(target) or tstate.get("mount")
        restores = [
            self._restore_summary(rid)
            for rid, r in self.state["restores"].items()
            if r.get("target") == target
        ]
        active_phases = {"verifying", "preparing", "prepared", "switching"}
        active = [r for r in restores if r["phase"] in active_phases]
        winner = tstate.get("winner")

        if active:
            coarse = "preparing"
        elif mount:
            coarse = "switched"
        else:
            coarse = "original_in_use"

        return {
            "kind": "target-inspection",
            "target": target,
            "state": coarse,
            "state_zh": {
                "original_in_use": "原卷仍在使用，没有任何恢复集完成切换",
                "preparing": "正在准备恢复（暂存整组卷，尚未原子切换）",
                "switched": "整组已切换到快照恢复点",
            }[coarse],
            "mount_state": "switched" if mount else "original_in_use",
            "mounted_version": mount,
            "winner_restore_id": winner,
            "active_restore": active[-1] if active else None,
            "restore_requests": restores,
            "production_store": str(self.root / "live"),
            "production_volumes_written_by_restore": False,
        }

    def inspect_overview(self) -> dict[str, Any]:
        return {
            "kind": "overview",
            "groups": sorted(self.state["groups"].keys()),
            "snapshots": [
                {"id": sid, "group": s.get("group"), "status": s.get("status")}
                for sid, s in self.state["snapshots"].items()
            ],
            "targets": {
                t: {
                    "state": "switched" if (ts.get("mount")) else "original_in_use",
                    "mounted_snapshot": (ts.get("mount") or {}).get("snapshot_id"),
                    "winner": ts.get("winner"),
                }
                for t, ts in self.state["targets"].items()
            },
        }

    # ------------------------------------------------------------ restore

    def restore_snapshot(
        self,
        snapshot_id: str,
        target: str,
        mapping: dict[str, str] | None = None,
        *,
        rid: str | None = None,
        at: int | None = None,
        force: bool = False,
    ) -> dict[str, Any]:
        mapping = canonical_mapping(mapping)
        with self.operation():
            now = self._resolve_at(at)
            return self._restore_locked(snapshot_id, target, mapping, rid=rid, at=now, force=force)

    def resume_restore(self, rid: str, at: int | None = None) -> dict[str, Any]:
        with self.operation():
            now = self._resolve_at(at)
            rec = self.state["restores"].get(rid)
            if rec is None:
                raise NotFoundError(f"恢复申请不存在：{rid}")
            if rec.get("phase") == "switched":
                return self._restore_report(rid)
            if rec.get("phase") in {"blocked", "conflict", "cancelled", "failed"}:
                return self._restore_report(rid)
            return self._execute_restore_locked(rec, now)

    def _default_rid(self, snapshot_id: str, target: str, mapping: dict[str, str]) -> str:
        import hashlib

        raw = f"{snapshot_id}|{target}|{mapping_key(mapping)}"
        return "r-" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]

    def _restore_locked(
        self,
        snapshot_id: str,
        target: str,
        mapping: dict[str, str],
        *,
        rid: str | None,
        at: int,
        force: bool,
    ) -> dict[str, Any]:
        check_name(snapshot_id, "快照 id")
        check_name(target, "恢复目标")
        if rid is None:
            rid = self._default_rid(snapshot_id, target, mapping)
        else:
            check_name(rid, "恢复申请 id")

        snap = self.state["snapshots"].get(snapshot_id)
        if snap is None:
            raise NotFoundError(f"快照 id 不存在：{snapshot_id}")

        # 完全相同的重复申请：幂等返回/续跑。
        existing = self.state["restores"].get(rid)
        if existing is not None:
            same = (
                existing.get("snapshot_id") == snapshot_id
                and existing.get("target") == target
                and existing.get("mapping") == mapping
            )
            if not same:
                raise ConflictError(f"恢复申请 id {rid} 已用于不同的恢复参数")
            if existing.get("phase") in {"verifying", "preparing", "prepared", "switching"}:
                return self._execute_restore_locked(existing, at)
            return self._restore_report(rid)

        tstate = self.state["targets"].setdefault(
            target, {"winner": None, "generation_counter": 0, "mount": None, "active_rid": None}
        )
        active_rid = tstate.get("active_rid")
        if active_rid and active_rid != rid:
            active = self.state["restores"].get(active_rid)
            if active and active.get("phase") in {"verifying", "preparing", "prepared", "switching"}:
                raise ConflictError(
                    f"目标 {target} 已有正在进行的恢复申请 {active_rid}；"
                    "请先 resume 完成它，或 cancel 后再提交，避免两套半恢复集竞争同一目标",
                    code="target_busy",
                    details={"active_rid": active_rid},
                )

        rec: dict[str, Any] = {
            "rid": rid,
            "snapshot_id": snapshot_id,
            "target": target,
            "mapping": mapping,
            "force": force,
            "submitted_at": at,
            "updated_at": at,
            "phase": "verifying",
            "generation": None,
            "generation_dir": None,
            "staging_dir": None,
            "problems": [],
        }
        self.state["restores"][rid] = rec
        tstate["active_rid"] = rid
        self._save_state()
        _crash_hook("restore_begin")
        return self._execute_restore_locked(rec, at)

    def _execute_restore_locked(self, rec: dict[str, Any], at: int) -> dict[str, Any]:
        rid = rec["rid"]
        sid = rec["snapshot_id"]
        target = rec["target"]
        mapping = rec.get("mapping") or {}
        rec["updated_at"] = at

        # 1) 验证：生命周期 + 父链/块完整性。阻碍明确落档，绝不碰生产卷。
        verification = self._verify_snapshot_report(sid)
        rec["verification"] = {
            "restorable": verification["restorable"],
            "status": verification["status"],
            "missing_blocks": verification["missing_blocks"],
            "missing_parent_snapshots": verification["missing_parent_snapshots"],
            "problems": verification["problems"],
            "lifecycle_reason": verification.get("lifecycle_reason"),
        }
        if not verification["restorable"]:
            rec["phase"] = "blocked"
            rec["problems"] = verification["problems"]
            tstate = self.state["targets"].setdefault(
                target, {"winner": None, "generation_counter": 0, "mount": None}
            )
            if tstate.get("active_rid") == rid:
                tstate["active_rid"] = None
            self._save_state()
            return self._restore_report(rid)

        manifest = snap_mod.load_manifest(self.root, sid)
        if manifest is None:
            rec["phase"] = "blocked"
            rec["problems"] = [{"code": "manifest_missing", "message": f"快照清单不存在：{sid}"}]
            self._save_state()
            return self._restore_report(rid)

        # 已有 winner（另一个申请完成切换）：默认严格保证同一目标只有一个 winner。
        tstate = self.state["targets"].setdefault(
            target, {"winner": None, "generation_counter": 0, "mount": None, "active_rid": None}
        )
        winner = tstate.get("winner")
        if winner and winner != rid and not rec.get("force"):
            rec["phase"] = "conflict"
            rec["problems"] = [
                {
                    "code": "target_already_switched",
                    "message": f"目标 {target} 已被恢复申请 {winner} 完成整组切换；"
                    "同一目标只允许一个申请完成切换。可 cancel 本申请，或显式 force 重挂新恢复点",
                }
            ]
            if tstate.get("active_rid") == rid:
                tstate["active_rid"] = None
            self._save_state()
            return self._restore_report(rid)

        # 2) 准备：整组卷写入独立暂存目录，再原子改名到 generation 目录。
        # 若崩溃前已经 prepared/switching 且生成目录完整，直接复用，不另起第二套。
        existing_gen_rel = rec.get("generation_dir")
        existing_gen_dir = self.root / existing_gen_rel if existing_gen_rel else None
        already_prepared = (
            rec.get("phase") in {"prepared", "switching"}
            and existing_gen_dir is not None
            and (existing_gen_dir / "PREPARED.json").is_file()
        )

        if already_prepared:
            gen_dir = existing_gen_dir
            gen_rel = existing_gen_rel
            gen_name = gen_dir.name
        else:
            rec["phase"] = "preparing"
            self._save_state()
            staging_rel = Path("workspaces") / target / ".staging" / rid
            staging = self.root / staging_rel
            if staging.exists():
                shutil.rmtree(staging, ignore_errors=True)
            rec["staging_dir"] = str(staging_rel)

            gen_no = int(tstate.get("generation_counter", 0)) + 1
            tstate["generation_counter"] = gen_no
            gen_name = f"gen-{gen_no:04d}"
            gen_rel_path = Path("workspaces") / target / "generations" / gen_name
            gen_dir = self.root / gen_rel_path
            gen_rel = str(gen_rel_path)
            if gen_dir.exists():
                shutil.rmtree(gen_dir, ignore_errors=True)

            try:
                materialize_workspace(
                    self.root, manifest, staging, mapping, restore_id=rid, at=at
                )
            except ServiceError as error:
                rec["phase"] = "blocked"
                rec["problems"] = [error.to_dict()]
                if tstate.get("active_rid") == rid:
                    tstate["active_rid"] = None
                self._save_state()
                return self._restore_report(rid)

            gen_dir.parent.mkdir(parents=True, exist_ok=True)
            os.replace(staging, gen_dir)
            rec["generation"] = gen_no
            rec["generation_dir"] = gen_rel
            rec["phase"] = "prepared"
            self._save_state()
            _crash_hook("restore_prepared")

        # 3) 切换提交：先持久化 switching + winner 独占声明，再原子替换 mount.json。
        if winner and winner != rid and rec.get("force"):
            rec["supersedes"] = winner
        tstate["winner"] = rid
        tstate["active_rid"] = rid
        rec["phase"] = "switching"
        self._save_state()
        _crash_hook("restore_switching")

        mount = {
            "kind": "groupvol-mount",
            "target": target,
            "restore_id": rid,
            "snapshot_id": sid,
            "group": manifest.get("group"),
            "parent": manifest.get("parent"),
            "generation": gen_name,
            "generation_dir": gen_rel,
            "mounted_at": at,
            "mapping": mapping,
            "covered_batches": manifest.get("covered_batches", []),
            "commits": manifest.get("commits", []),
            "block_count": (rec.get("verification") or {}).get("block_count"),
        }
        atomic_write_json(self._mount_file(target), mount)
        _crash_hook("mount_renamed")

        tstate["mount"] = mount
        tstate["winner"] = rid
        tstate["active_rid"] = None
        rec["phase"] = "switched"
        rec["switched_at"] = at
        rec["mount"] = mount
        self._save_state()
        return self._restore_report(rid)

    def _restore_summary(self, rid: str) -> dict[str, Any]:
        rec = self.state["restores"][rid]
        return {
            "rid": rid,
            "snapshot_id": rec.get("snapshot_id"),
            "target": rec.get("target"),
            "mapping": rec.get("mapping"),
            "phase": rec.get("phase"),
            "updated_at": rec.get("updated_at"),
            "generation": rec.get("generation"),
            "generation_dir": rec.get("generation_dir"),
            "switched": rec.get("phase") == "switched",
            "problems": rec.get("problems", []),
            "supersedes": rec.get("supersedes"),
            "reconciled": rec.get("reconciled", False),
        }

    def _restore_report(self, rid: str) -> dict[str, Any]:
        summary = self._restore_summary(rid)
        rec = self.state["restores"][rid]
        summary["verification"] = rec.get("verification")
        summary["mount"] = rec.get("mount") or self._read_mount(rec["target"])
        summary["generation_dir_abs"] = (
            str(self.root / rec["generation_dir"]) if rec.get("generation_dir") else None
        )
        summary["coarse_state"] = (
            "switched"
            if rec.get("phase") == "switched"
            else "preparing"
            if rec.get("phase") in {"verifying", "preparing", "prepared", "switching"}
            else "original_in_use"
        )
        return summary

    # ------------------------------------------------------------ standalone rebuild

    def rebuild_into(
        self,
        snapshot_id: str,
        destination: str | Path,
        mapping: dict[str, str] | None = None,
        *,
        clean: bool = False,
    ) -> dict[str, Any]:
        """只依赖快照制品（含父链）把工作区重建到任意新目录，不切换挂载。"""
        self.refresh()
        mapping = canonical_mapping(mapping)
        report = self._verify_snapshot_report(snapshot_id)
        if not report["ok"]:
            raise ConflictError(
                f"快照 {snapshot_id} 制品验证未通过，无法重建：{report['problems'][:3]}",
                code="snapshot_not_restorable",
                details={"missing_blocks": report["missing_blocks"]},
            )
        manifest = snap_mod.load_manifest(self.root, snapshot_id)
        if manifest is None:
            raise NotFoundError(f"快照清单不存在：{snapshot_id}")
        dest = Path(destination)
        if dest.exists() and any(dest.iterdir()):
            if not clean:
                raise ConflictError(f"目标目录非空（加 clean=True/--clean 可清空）：{dest}")
            shutil.rmtree(dest)
        dest.mkdir(parents=True, exist_ok=True)
        return materialize_workspace(
            self.root, manifest, dest, mapping, restore_id="rebuild", at=None
        )
