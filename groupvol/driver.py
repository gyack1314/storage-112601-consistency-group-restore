"""本地模拟卷驱动。

明确对应题目给出的四种驱动能力：

- ``write_block``   写块（先进入 WAL，尚未声明落盘）
- ``flush``         卷级落盘，把截至 through 的写入物化到 blocks/
- ``watermark``     水位回报（已落盘写入序号）
- ``stop_writes``   为某个快照 id 停止接新写（sealed 时调用）
- ``release``       冻结结束/超时/取消后释放，卷重新可写
- ``as_of_values``  按 sealed 水位重放 WAL，得到切快照时刻的逐块内容

目录结构（root 由调用方给定）::

    <root>/live/<group>/<volume>/
        volume.json                 # 块数、落盘水位、冻结令牌
        wal/<seq:012d>.json         # 每次写块的持久意向
        blocks/blk-<index:08d>.json # flush 后物化的块（data 可为 null）
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .util import (
    atomic_write_json,
    check_int,
    check_name,
    read_json,
    try_read_json,
)

NO_WATERMARK = -1


class VolumeFrozenError(Exception):
    """卷已为某个快照停止接新写。"""


class LocalBlockDriver:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.live_root = self.root / "live"

    # ---------- 路径 ----------

    def volume_dir(self, group: str, volume: str) -> Path:
        return self.live_root / check_name(group, "工作组") / check_name(volume, "卷名")

    def _meta_path(self, group: str, volume: str) -> Path:
        return self.volume_dir(group, volume) / "volume.json"

    def _wal_path(self, group: str, volume: str, seq: int) -> Path:
        return self.volume_dir(group, volume) / "wal" / f"{seq:012d}.json"

    def _block_path(self, group: str, volume: str, block: int) -> Path:
        return self.volume_dir(group, volume) / "blocks" / f"blk-{block:08d}.json"

    # ---------- 登记 ----------

    def register_volume(self, group: str, volume: str, blocks_count: int, at: int) -> dict[str, Any]:
        check_name(group, "工作组")
        check_name(volume, "卷名")
        check_int(blocks_count, "blocks", minimum=1)
        vdir = self.volume_dir(group, volume)
        meta_path = self._meta_path(group, volume)
        existing = try_read_json(meta_path)
        if existing is not None:
            if existing.get("blocks_count") != blocks_count:
                from .util import ConflictError

                raise ConflictError(
                    f"卷 {group}/{volume} 已以 blocks={existing.get('blocks_count')} 登记，"
                    f"不能改为 {blocks_count}"
                )
            return existing
        vdir.mkdir(parents=True, exist_ok=True)
        (vdir / "wal").mkdir(exist_ok=True)
        (vdir / "blocks").mkdir(exist_ok=True)
        meta = {
            "group": group,
            "volume": volume,
            "blocks_count": blocks_count,
            "flush_through": NO_WATERMARK,
            "registered_at": at,
            "frozen_for": [],
        }
        atomic_write_json(meta_path, meta)
        return meta

    def meta(self, group: str, volume: str) -> dict[str, Any]:
        meta = try_read_json(self._meta_path(group, volume))
        if meta is None:
            from .util import NotFoundError

            raise NotFoundError(f"卷未登记：{group}/{volume}")
        return meta

    def exists(self, group: str, volume: str) -> bool:
        return self._meta_path(group, volume).exists()

    # ---------- 冻结 / 解封 ----------

    def stop_writes(self, group: str, volume: str, snapshot_id: str) -> list[str]:
        meta = self.meta(group, volume)
        frozen = list(meta.get("frozen_for", []))
        if snapshot_id not in frozen:
            frozen.append(snapshot_id)
            meta["frozen_for"] = frozen
            atomic_write_json(self._meta_path(group, volume), meta)
        return frozen

    def release(self, group: str, volume: str, snapshot_id: str) -> bool:
        """释放指定快照在该卷上的冻结令牌。返回是否真的释放了。"""
        meta = self.meta(group, volume)
        frozen = list(meta.get("frozen_for", []))
        if snapshot_id in frozen:
            frozen.remove(snapshot_id)
            meta["frozen_for"] = frozen
            atomic_write_json(self._meta_path(group, volume), meta)
            return True
        return False

    def is_frozen(self, group: str, volume: str) -> bool:
        return bool(self.meta(group, volume).get("frozen_for"))

    # ---------- 写块 / 落盘 / 水位 ----------

    def write_block(
        self, group: str, volume: str, *, seq: int, block: int, data: str | None, batch: str | None = None
    ) -> Path:
        meta = self.meta(group, volume)
        if meta.get("frozen_for"):
            raise VolumeFrozenError(
                f"卷 {group}/{volume} 已停止接新写（冻结快照：{', '.join(meta['frozen_for'])}）"
            )
        blocks_count = int(meta["blocks_count"])
        check_int(seq, "seq", minimum=0)
        check_int(block, "block", minimum=0)
        if block >= blocks_count:
            from .util import ValidationError

            raise ValidationError(f"block={block} 越界，卷 {volume} 只有 {blocks_count} 块")
        if data is not None and not isinstance(data, str):
            from .util import ValidationError

            raise ValidationError("data 必须是字符串或 null")
        path = self._wal_path(group, volume, seq)
        if path.exists():
            previous = read_json(path)
            if previous.get("block") != block or previous.get("data") != data:
                from .util import ConflictError

                raise ConflictError(f"写入序号 seq={seq} 已存在且内容不一致，拒绝覆盖")
            return path
        atomic_write_json(
            path,
            {"seq": seq, "block": block, "data": data, "batch": batch},
        )
        return path

    def flush(self, group: str, volume: str, through: int) -> int:
        """把该卷 seq<=through 的 WAL 按序物化到 blocks/，更新水位。"""
        meta = self.meta(group, volume)
        check_int(through, "through", minimum=0)
        vdir = self.volume_dir(group, volume)
        wal_dir = vdir / "wal"
        values = self.as_of_values(group, volume, through)
        for index, value in enumerate(values):
            # null 也写显式标记文件，区分“清空”与“从未有过”。
            atomic_write_json(self._block_path(group, volume, index), {"block": index, "data": value})
        new_watermark = max(int(meta.get("flush_through", NO_WATERMARK)), through)
        meta["flush_through"] = new_watermark
        atomic_write_json(self._meta_path(group, volume), meta)
        return new_watermark

    def watermark(self, group: str, volume: str) -> int:
        return int(self.meta(group, volume).get("flush_through", NO_WATERMARK))

    def as_of_values(self, group: str, volume: str, through: int) -> list[str | None]:
        """重放 WAL 中 seq<=through 的写入，返回逐块内容（未写过的块为 None）。"""
        meta = self.meta(group, volume)
        count = int(meta["blocks_count"])
        values: list[str | None] = [None] * count
        wal_dir = self.volume_dir(group, volume) / "wal"
        writes: list[dict[str, Any]] = []
        if wal_dir.exists():
            for path in wal_dir.glob("*.json"):
                row = try_read_json(path)
                if isinstance(row, dict) and isinstance(row.get("seq"), int):
                    writes.append(row)
        writes.sort(key=lambda row: row["seq"])
        for row in writes:
            if row["seq"] > through:
                break
            block = int(row["block"])
            if 0 <= block < count:
                values[block] = row.get("data")
        return values

    def live_block_value(self, group: str, volume: str, block: int) -> str | None:
        row = try_read_json(self._block_path(group, volume, block))
        if row is None:
            return None
        return row.get("data")
