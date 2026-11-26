#!/usr/bin/env python3
"""可重复执行的卷写入 / 快照 / 取消 / 验证 / 恢复入口。

示例：
    python3 storage_ctl.py replay examples/incremental-missing-volume.jsonl --root /tmp/lab --fresh
    python3 storage_ctl.py inspect --id s1 --root /tmp/lab
    python3 storage_ctl.py verify --id s2 --root /tmp/lab
    python3 storage_ctl.py restore --id s1 --target ws1 --map blobs=data --map journal=manifest --root /tmp/lab
"""

import sys
from pathlib import Path

# 即使以 `python3 -I /path/to/storage_ctl.py` 运行（-I/-P 不把脚本目录加入 sys.path），
# 也能找到同目录下的 groupvol 包。
sys.path.insert(0, str(Path(__file__).resolve().parent))

from groupvol.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
