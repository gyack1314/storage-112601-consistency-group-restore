#!/usr/bin/env python3
"""通用入口：等价于 ``python3 storage_ctl.py ...``。"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from groupvol.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
