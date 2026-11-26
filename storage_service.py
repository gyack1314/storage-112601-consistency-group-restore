"""兼容性别名：部分演练脚本可能以 ``storage_service`` 名称导入协调服务。"""

from groupvol.coordinator import Coordinator
from groupvol.replay import run_file, run_paths
from groupvol.util import ConflictError, NotFoundError, ServiceError, ValidationError


class StorageService(Coordinator):
    """Coordinator 的语义化别名。"""


__all__ = [
    "Coordinator",
    "StorageService",
    "run_file",
    "run_paths",
    "ServiceError",
    "NotFoundError",
    "ConflictError",
    "ValidationError",
]
