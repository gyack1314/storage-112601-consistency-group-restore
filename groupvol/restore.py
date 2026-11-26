"""把同一快照清单中的全部卷物化为一个完整的新工作区。

物化过程只从快照制品（含父链解析）取数据，卷数、卷尺寸、块内容、提交事实
全部来自同一份 manifest，不读取 live 生产卷。先写到暂存目录，全部块写好并
自检后放 ``PREPARED`` 标记，再由 Coordinator 原子切换 mount.json。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .snapshot import Resolver, resolve_block
from .util import atomic_write_json, check_name, ensure_dir


def materialize_workspace(
    root: str | Path,
    manifest: dict[str, Any],
    destination: str | Path,
    mapping: dict[str, str],
    *,
    restore_id: str,
    at: int | None,
) -> dict[str, Any]:
    """在 destination（应为不存在或已清空的暂存/生成目录）重建整组卷。"""
    root = Path(root)
    destination = Path(destination)
    if destination.exists() and any(destination.iterdir()):
        raise FileExistsError(f"物化目标目录非空：{destination}")
    volumes_root = ensure_dir(destination / "volumes")

    source_volumes: dict[str, Any] = manifest.get("volumes") or {}
    volume_order = manifest.get("volume_order") or list(source_volumes.keys())
    resolved_mapping: dict[str, str] = {}
    total_blocks = 0
    written_blocks = 0
    resolver = Resolver(root)

    for source_volume in volume_order:
        vspec = source_volumes[source_volume]
        target_volume = mapping.get(source_volume, source_volume)
        check_name(target_volume, "目标卷名")
        if target_volume in resolved_mapping.values():
            from .util import ConflictError

            raise ConflictError(f"目标卷名冲突：{target_volume}")
        resolved_mapping[source_volume] = target_volume

        count = int(vspec["blocks_count"])
        target_dir = volumes_root / target_volume
        blocks_dir = ensure_dir(target_dir / "blocks")
        values: list[Any] = []
        for index in range(count):
            value, problem = resolver.resolve(manifest["id"], source_volume, index)
            if problem is not None:
                # 正常情况下 Coordinator 已先 verify；这里是最后一道闸。
                from .util import ServiceError

                raise ServiceError(
                    problem.get("message", f"块解析失败：{source_volume}#{index}"),
                    code="restore_block_unreadable",
                    details=problem,
                )
            values.append(value)
            payload = {"block": index, "data": value}
            block_path = blocks_dir / f"blk-{index:08d}.json"
            atomic_write_json(block_path, payload)
            written_blocks += 1

        atomic_write_json(
            target_dir / "VOLUME.json",
            {
                "kind": "groupvol-restored-volume",
                "source_snapshot": manifest["id"],
                "source_volume": source_volume,
                "name": target_volume,
                "blocks_count": count,
                "watermark": vspec.get("watermark"),
            },
        )
        total_blocks += count

    recovery_point = {
        "kind": "groupvol-recovery-point",
        "restore_id": restore_id,
        "restored_at": at,
        "snapshot_id": manifest["id"],
        "snapshot_created_at": manifest.get("created_at"),
        "group": manifest.get("group"),
        "parent": manifest.get("parent"),
        "mapping": resolved_mapping,
        "volume_order_target": [resolved_mapping[v] for v in volume_order],
        "volumes": {
            resolved_mapping[source]: {
                "source_volume": source,
                "blocks_count": int(source_volumes[source]["blocks_count"]),
                "watermark": source_volumes[source].get("watermark"),
            }
            for source in volume_order
        },
        "covered_batches": manifest.get("covered_batches", []),
        "commits": manifest.get("commits", []),
        "block_count": total_blocks,
    }
    atomic_write_json(destination / "RECOVERY_POINT.json", recovery_point)

    # 写完后从磁盘回读自检，保证落盘内容与解析结果逐块一致。
    for source_volume in volume_order:
        target_volume = resolved_mapping[source_volume]
        count = int(source_volumes[source_volume]["blocks_count"])
        for index in range(count):
            path = volumes_root / target_volume / "blocks" / f"blk-{index:08d}.json"
            row = json.loads(path.read_text(encoding="utf-8"))
            expected, problem = resolver.resolve(manifest["id"], source_volume, index)
            if problem is not None:
                from .util import ServiceError

                raise ServiceError(problem["message"], code="restore_block_unreadable", details=problem)
            if row.get("data") != expected:
                from .util import ServiceError

                raise ServiceError(
                    f"回读自检失败：{target_volume} 块 {index}", code="restore_selfcheck_failed"
                )

    atomic_write_json(destination / "PREPARED.json", {"restore_id": restore_id, "ready": True})
    return {
        "destination": str(destination),
        "mapping": resolved_mapping,
        "volumes": len(volume_order),
        "blocks": total_blocks,
        "written_blocks": written_blocks,
        "recovery_point": recovery_point,
    }
