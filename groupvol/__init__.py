"""成组卷一致快照与原子恢复服务（仅使用 Python 标准库）。"""

from .coordinator import Coordinator, ServiceError, NotFoundError, ConflictError
from .replay import run_file, run_paths

__all__ = [
    "Coordinator",
    "ServiceError",
    "NotFoundError",
    "ConflictError",
    "run_file",
    "run_paths",
]

__version__ = "1.0.0"
