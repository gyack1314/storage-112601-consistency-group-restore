"""组快照制品：清单（manifest）、增量父引用、块解析与验证、lost 模拟。

快照目录::

    <root>/snapshots/<snapshot-id>/
        manifest.json                     # 仅在整组 finalize 后写出
        volumes/<volume>/blk-XXXXXXXX.json

清单中每个块有两种条目：

- ``{"t": "full", "file": ..., "sha256": ...}``：本快照自有的显式块，
  ``data`` 可以为 null（清空/空块）。**只要是 full，恢复时就只认这个文件，
  绝不回退父快照**——这是“空块不能从上一份补回”的关键。
- ``{"t": "ref", "snapshot": parent, "volume": ..., "block": i}``：
  未变化块，沿父快照链解析。
"""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Any

from .util import (
    atomic_write_json,
    check_name,
    read_json,
    sha256_file,
    try_read_json,
)

MANIFEST_NAME = "manifest.json"


# ---------------------------------------------------------------- paths

def snapshots_root(root: str | Path) -> Path:
    return Path(root) / "snapshots"


def snapshot_dir(root: str | Path, snapshot_id: str) -> Path:
    return snapshots_root(root) / check_name(snapshot_id, "快照 id")


def manifest_path(root: str | Path, snapshot_id: str) -> Path:
    return snapshot_dir(root, snapshot_id) / MANIFEST_NAME


def load_manifest(root: str | Path, snapshot_id: str) -> dict[str, Any] | None:
    path = manifest_path(root, snapshot_id)
    data = try_read_json(path)
    if isinstance(data, dict) and data.get("kind") == "groupvol-manifest":
        return data
    return None


def write_manifest(root: str | Path, manifest: dict[str, Any]) -> Path:
    sid = check_name(manifest["id"], "快照 id")
    path = manifest_path(root, sid)
    atomic_write_json(path, manifest)
    return path


# ---------------------------------------------------------------- building

def _block_relpath(volume: str, index: int) -> str:
    return (Path("volumes") / volume / f"blk-{index:08d}.json").as_posix()


def write_full_block(root: str | Path, snapshot_id: str, volume: str, index: int, value: Any) -> dict[str, Any]:
    rel = _block_relpath(volume, index)
    path = snapshot_dir(root, snapshot_id) / rel
    payload = {"block": index, "data": value}
    raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    from .util import atomic_write_bytes, sha256_bytes

    atomic_write_bytes(path, raw)
    # 只读快照：自有块落盘后置为只读（best-effort；POSIX 下不影响父目录移动/隔离）。
    try:
        os.chmod(path, 0o444)
    except OSError:
        pass
    return {"t": "full", "file": rel, "sha256": sha256_bytes(raw)}


def build_volume_spec(
    root: str | Path,
    snapshot_id: str,
    volume: str,
    blocks_count: int,
    values: list[Any],
    *,
    parent_id: str | None,
    watermark: int,
    durable_through: int,
    sealed_at: int,
) -> dict[str, Any]:
    """对比父快照解析值：相同则建 ref，不同（含改写/清空）则写 full 块。"""
    warnings: list[str] = []
    blocks: list[dict[str, Any]] = []
    # index -> (parent_value, problem)
    parent_cache: dict[int, tuple[Any, dict[str, Any] | None]] = {}
    resolver = Resolver(root)

    for index in range(blocks_count):
        current = values[index] if index < len(values) else None
        if parent_id:
            if index not in parent_cache:
                parent_cache[index] = resolver.resolve(parent_id, volume, index)
            parent_value, problem = parent_cache[index]
            if problem is None and parent_value == current:
                # 与父快照共同恢复点中的值完全一致（包括两边都是 null），引用即可。
                blocks.append(
                    {"t": "ref", "snapshot": parent_id, "volume": volume, "block": index}
                )
                continue
            if problem is not None and not any("父快照" in w for w in warnings):
                warnings.append(
                    f"父快照 {parent_id} 的部分块无法读取，相关块改为本快照全量保存"
                )
        # 值不同（改写/清空）或父块不可读：写本快照自有 full 条目，
        # 即使 data=null 也显式落文件，恢复时绝不回退父快照。
        blocks.append(write_full_block(root, snapshot_id, volume, index, current))

    return {
        "blocks_count": blocks_count,
        "watermark": watermark,
        "durable_through": durable_through,
        "sealed_at": sealed_at,
        "warnings": warnings,
        "blocks": blocks,
    }


# ---------------------------------------------------------------- resolution


class Resolver:
    """单次验证/物化过程内的带缓存块解析器（文件在该过程中不会变化）。"""

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self._manifest_cache: dict[str, dict[str, Any] | None] = {}

    def manifest(self, snapshot_id: str) -> dict[str, Any] | None:
        if snapshot_id not in self._manifest_cache:
            self._manifest_cache[snapshot_id] = load_manifest(self.root, snapshot_id)
        return self._manifest_cache[snapshot_id]

    def resolve(
        self,
        snapshot_id: str,
        volume: str,
        index: int,
        _seen: tuple[str, ...] = (),
    ) -> tuple[Any, dict[str, Any] | None]:
        if snapshot_id in _seen:
            return None, {
                "code": "reference_cycle",
                "snapshot": snapshot_id,
                "volume": volume,
                "block": index,
                "message": f"父快照引用存在环：{' -> '.join(_seen + (snapshot_id,))}",
            }
        next_seen = _seen + (snapshot_id,)

        manifest = self.manifest(snapshot_id)
        if manifest is None:
            return None, {
                "code": "parent_snapshot_missing",
                "snapshot": snapshot_id,
                "volume": volume,
                "block": index,
                "message": f"父快照对象缺失或不可读：{snapshot_id}",
            }
        volumes = manifest.get("volumes") or {}
        vspec = volumes.get(volume)
        if not isinstance(vspec, dict):
            return None, {
                "code": "parent_volume_missing",
                "snapshot": snapshot_id,
                "volume": volume,
                "block": index,
                "message": f"快照 {snapshot_id} 中找不到卷对象 {volume}",
            }
        entries = vspec.get("blocks") or []
        if not isinstance(entries, list) or index >= len(entries):
            return None, {
                "code": "block_index_missing",
                "snapshot": snapshot_id,
                "volume": volume,
                "block": index,
                "message": f"快照 {snapshot_id} 卷 {volume} 缺少块 {index} 的条目",
            }
        entry = entries[index]
        if not isinstance(entry, dict):
            return None, {
                "code": "block_entry_invalid",
                "snapshot": snapshot_id,
                "volume": volume,
                "block": index,
                "message": f"快照 {snapshot_id} 卷 {volume} 块 {index} 条目损坏",
            }

        kind = entry.get("t")
        if kind == "ref":
            return self.resolve(
                entry.get("snapshot"),
                entry.get("volume", volume),
                int(entry.get("block", index)),
                next_seen,
            )
        if kind != "full":
            return None, {
                "code": "block_entry_invalid",
                "snapshot": snapshot_id,
                "volume": volume,
                "block": index,
                "message": f"未知块条目类型 {kind!r}（{snapshot_id}/{volume}#{index}）",
            }

        file_rel = entry.get("file")
        file_path = snapshot_dir(self.root, snapshot_id) / str(file_rel)
        if not file_path.is_file():
            return None, {
                "code": "block_file_missing",
                "snapshot": snapshot_id,
                "volume": volume,
                "block": index,
                "path": str(file_path),
                "message": f"块文件缺失或不可读：{snapshot_id}/{volume} 块 {index}（{file_rel}）",
            }
        expected_hash = entry.get("sha256")
        if expected_hash:
            try:
                actual_hash = sha256_file(file_path)
            except OSError as error:
                return None, {
                    "code": "block_file_unreadable",
                    "snapshot": snapshot_id,
                    "volume": volume,
                    "block": index,
                    "path": str(file_path),
                    "message": f"块文件无法读取：{error}",
                }
            if actual_hash != expected_hash:
                return None, {
                    "code": "block_checksum_mismatch",
                    "snapshot": snapshot_id,
                    "volume": volume,
                    "block": index,
                    "message": f"块校验和不符：{snapshot_id}/{volume} 块 {index}",
                }
        payload = try_read_json(file_path)
        if not isinstance(payload, dict) or "data" not in payload:
            return None, {
                "code": "block_file_invalid",
                "snapshot": snapshot_id,
                "volume": volume,
                "block": index,
                "message": f"块文件内容不是合法的 {{block,data}} 对象：{file_rel}",
            }
        return payload["data"], None


def resolve_block(
    root: str | Path,
    snapshot_id: str,
    volume: str,
    index: int,
    _seen: tuple[str, ...] = (),
) -> tuple[Any, dict[str, Any] | None]:
    """沿 ref 链解析块的最终数据（便捷函数；批量场景请复用 :class:`Resolver`）。

    full 条目文件丢失/损坏时直接报错，**不会**继续找父快照，因为 full
    （尤其是 data=null 的清空块）代表该快照自己的确定状态。
    """
    return Resolver(root).resolve(snapshot_id, volume, index, _seen)


# ---------------------------------------------------------------- verification

def verify_volume_spec(
    root: str | Path,
    snapshot_id: str,
    volume: str,
    vspec: dict[str, Any],
    resolver: "Resolver | None" = None,
) -> dict[str, Any]:
    """验证一个卷规格中的全部块可解析、可读。"""
    resolver = resolver or Resolver(root)
    count = int(vspec.get("blocks_count", 0))
    entries = vspec.get("blocks") or []
    problems: list[dict[str, Any]] = []
    missing_blocks: list[int] = []
    full_count = 0
    ref_count = 0

    volume_dir = snapshot_dir(root, snapshot_id) / "volumes" / volume
    if not volume_dir.exists():
        problems.append(
            {
                "code": "volume_object_missing",
                "snapshot": snapshot_id,
                "volume": volume,
                "message": f"卷对象目录缺失或不可读：{snapshot_id}/{volume}",
            }
        )

    if not isinstance(entries, list) or len(entries) != count:
        problems.append(
            {
                "code": "volume_size_mismatch",
                "snapshot": snapshot_id,
                "volume": volume,
                "message": (
                    f"卷尺寸不一致：blocks_count={count}，块条目数="
                    f"{len(entries) if isinstance(entries, list) else '非列表'}"
                ),
            }
        )

    for index in range(min(count, len(entries) if isinstance(entries, list) else 0)):
        entry = entries[index]
        if isinstance(entry, dict) and entry.get("t") == "full":
            full_count += 1
        elif isinstance(entry, dict) and entry.get("t") == "ref":
            ref_count += 1
        value, problem = resolver.resolve(snapshot_id, volume, index)
        if problem is not None:
            problems.append(problem)
            if problem.get("code") in {
                "block_file_missing",
                "block_file_unreadable",
                "block_checksum_mismatch",
                "parent_snapshot_missing",
                "parent_volume_missing",
                "block_index_missing",
                "block_file_invalid",
            }:
                missing_blocks.append(index)

    return {
        "volume": volume,
        "blocks_count": count,
        "watermark": vspec.get("watermark"),
        "full_blocks": full_count,
        "referenced_blocks": ref_count,
        "missing_blocks": missing_blocks,
        "ok": not problems,
        "problems": problems,
    }


def verify_snapshot_artifacts(
    root: str | Path,
    snapshot_id: str,
    *,
    fallback_specs: dict[str, Any] | None = None,
    requested_volumes: list[str] | None = None,
) -> dict[str, Any]:
    """验证快照制品。complete 快照以 manifest 为准；未完成快照可验证已切出的部分规格。"""
    check_name(snapshot_id, "快照 id")
    manifest = load_manifest(root, snapshot_id)
    resolver = Resolver(root)
    problems: list[dict[str, Any]] = []
    volume_reports: list[dict[str, Any]] = []

    if manifest is not None:
        volumes = manifest.get("volumes") or {}
        for volume in manifest.get("volume_order") or list(volumes.keys()):
            vspec = volumes.get(volume)
            if not isinstance(vspec, dict):
                report = {
                    "volume": volume,
                    "ok": False,
                    "missing_blocks": [],
                    "problems": [{"code": "volume_spec_invalid", "message": "卷规格不是对象"}],
                }
            else:
                report = verify_volume_spec(root, snapshot_id, volume, vspec, resolver)
            volume_reports.append(report)
        if requested_volumes:
            for volume in requested_volumes:
                if volume not in volumes:
                    problems.append(
                        {
                            "code": "volume_not_sealed",
                            "snapshot": snapshot_id,
                            "volume": volume,
                            "message": f"卷 {volume} 尚未 sealed，整组共同恢复点不存在",
                        }
                    )
    elif fallback_specs:
        for volume in requested_volumes or list(fallback_specs.keys()):
            vspec = fallback_specs.get(volume)
            if not isinstance(vspec, dict):
                continue
            volume_reports.append(
                verify_volume_spec(root, snapshot_id, volume, vspec, resolver)
            )
        if requested_volumes:
            sealed = set(fallback_specs.keys())
            for volume in requested_volumes:
                if volume not in sealed:
                    problems.append(
                        {
                            "code": "volume_not_sealed",
                            "snapshot": snapshot_id,
                            "volume": volume,
                            "message": f"卷 {volume} 尚未 sealed，整组共同恢复点不存在",
                        }
                    )
    else:
        problems.append(
            {
                "code": "manifest_missing",
                "snapshot": snapshot_id,
                "message": f"快照清单不存在或不可读：{snapshot_id}",
            }
        )

    for report in volume_reports:
        problems.extend(report.get("problems", []))

    missing_blocks = [
        {"volume": report["volume"], "block": block}
        for report in volume_reports
        for block in report.get("missing_blocks", [])
    ]
    missing_parents = sorted(
        {
            p.get("snapshot")
            for p in problems
            if p.get("code") == "parent_snapshot_missing" and p.get("snapshot")
        }
    )
    ok = not problems and all(report.get("ok") for report in volume_reports)
    return {
        "id": snapshot_id,
        "manifest_present": manifest is not None,
        "ok": ok,
        "volume_reports": volume_reports,
        "missing_blocks": missing_blocks,
        "missing_parent_snapshots": missing_parents,
        "problems": problems,
    }


# ---------------------------------------------------------------- lost 模拟

def apply_lost(
    root: str | Path,
    snapshot_id: str,
    volume: str,
    block: int | None = None,
    *,
    at: int | None = None,
) -> dict[str, Any]:
    """让指定快照的卷对象或块不可读：移动到隔离区（等价于丢失，验证会报错）。

    若指定 block 且该块是父快照 ref，则沿链找到真正持有 full 文件的对象并移除，
    这样验证当前快照时会明确给出引用链阻碍。
    """
    check_name(snapshot_id, "快照 id")
    check_name(volume, "卷名")
    root = Path(root)
    quarantine = root / "quarantine" / (f"at{at}" if at is not None else "manual")

    owner_sid = snapshot_id
    owner_volume = volume
    entry: dict[str, Any] | None = None

    if block is not None:
        # 找到该块条目的直接归属（遇到 ref 跳到父快照的对应条目）。
        cur_sid, cur_vol, cur_idx = snapshot_id, volume, block
        seen: tuple[str, ...] = ()
        while True:
            manifest = load_manifest(root, cur_sid)
            if manifest is None:
                # 未 finalize 的快照可能没有 manifest，但 state 规格文件可能在；
                # lost 块在演练中通常针对 complete 快照。
                target = snapshot_dir(root, cur_sid) / _block_relpath(cur_vol, cur_idx)
                owner_sid, owner_volume = cur_sid, cur_vol
                break
            vspec = (manifest.get("volumes") or {}).get(cur_vol) or {}
            entries = vspec.get("blocks") or []
            if cur_idx >= len(entries):
                raise FileNotFoundError(f"{cur_sid}/{cur_vol} 块 {cur_idx} 条目不存在")
            entry = entries[cur_idx]
            if entry.get("t") == "ref":
                if cur_sid in seen:
                    raise FileNotFoundError("引用环")
                seen += (cur_sid,)
                cur_sid = entry.get("snapshot")
                cur_vol = entry.get("volume", cur_vol)
                cur_idx = int(entry.get("block", cur_idx))
                continue
            owner_sid, owner_volume = cur_sid, cur_vol
            target = snapshot_dir(root, cur_sid) / str(entry.get("file"))
            break

        if not target.exists():
            return {
                "already_lost": True,
                "target": str(target),
                "owner_snapshot": owner_sid,
                "owner_volume": owner_volume,
                "block": block,
            }
        dest = quarantine / owner_sid / owner_volume / target.name
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(target), str(dest))
        return {
            "lost": True,
            "kind": "block",
            "block": block,
            "requested_snapshot": snapshot_id,
            "owner_snapshot": owner_sid,
            "owner_volume": owner_volume,
            "moved_to": str(dest),
        }

    # 整卷对象：移走 volumes/<volume> 目录。
    target = snapshot_dir(root, snapshot_id) / "volumes" / volume
    if not target.exists():
        return {"already_lost": True, "target": str(target), "volume": volume}
    dest = quarantine / snapshot_id / volume
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        shutil.rmtree(dest)
    shutil.move(str(target), str(dest))
    return {
        "lost": True,
        "kind": "volume",
        "snapshot": snapshot_id,
        "volume": volume,
        "moved_to": str(dest),
    }
