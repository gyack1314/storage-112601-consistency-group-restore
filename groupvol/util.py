"""通用工具：安全命名、原子 JSON 落盘、文件锁、摘要与错误类型。"""

from __future__ import annotations

import hashlib
import json
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator


class ServiceError(Exception):
    """可向值班员展示的业务错误，带稳定错误码。"""

    code = "service_error"

    def __init__(self, message: str, *, code: str | None = None, details: dict | None = None):
        super().__init__(message)
        self.message = message
        if code:
            self.code = code
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "details": self.details}


class NotFoundError(ServiceError):
    code = "not_found"


class ConflictError(ServiceError):
    code = "conflict"


class ValidationError(ServiceError):
    code = "validation_error"


def check_name(value: Any, what: str = "名称") -> str:
    """卷名/快照 id/目标名等会成为单级路径分量：接受中英文等 Unicode，
    但坚决拒绝路径分隔符、NUL、控制字符与 ``..``，杜绝路径逃逸。"""
    if not isinstance(value, str):
        raise ValidationError(f"{what}必须是字符串，收到 {value!r}")
    if not (1 <= len(value) <= 128):
        raise ValidationError(f"{what}长度需在 1-128 之间：{value!r}")
    if value in {".", ".."} or ".." in value:
        raise ValidationError(f"{what}不合法（禁止 . 与 ..）：{value!r}")
    if "/" in value or "\\" in value or "\x00" in value:
        raise ValidationError(f"{what}不合法（含路径分隔符或 NUL）：{value!r}")
    if any(ord(ch) < 32 for ch in value):
        raise ValidationError(f"{what}不合法（含控制字符）：{value!r}")
    return value


def check_int(value: Any, what: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"{what}必须是整数，收到 {value!r}")
    if value < minimum:
        raise ValidationError(f"{what}必须 >= {minimum}，收到 {value}")
    return value


def canonical_mapping(mapping: dict[str, str] | None) -> dict[str, str]:
    if mapping is None:
        return {}
    if not isinstance(mapping, dict):
        raise ValidationError("mapping 必须是对象")
    result: dict[str, str] = {}
    for src, dst in mapping.items():
        check_name(src, "源卷名")
        check_name(dst, "目标卷名")
        result[str(src)] = dst
    if len(set(result.values())) != len(result):
        raise ValidationError("mapping 中多个源卷映射到了同一个目标卷名")
    return dict(sorted(result.items()))


def mapping_key(mapping: dict[str, str]) -> str:
    return json.dumps(canonical_mapping(mapping), ensure_ascii=False, sort_keys=True)


def _fsync_enabled() -> bool:
    # 默认保留 fsync 的落盘语义；测试可设 GROUPVOL_FSYNC=0 加速。
    return os.environ.get("GROUPVOL_FSYNC", "1") not in {"0", "false", "False", ""}


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".tmp-{os.getpid()}")
    with open(tmp, "wb") as handle:
        handle.write(data)
        handle.flush()
        if _fsync_enabled():
            os.fsync(handle.fileno())
    os.replace(tmp, path)
    if _fsync_enabled():
        _fsync_directory(path.parent)


def atomic_write_json(path: Path, value: Any) -> None:
    text = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=False)
    atomic_write_bytes(path, text.encode("utf-8"))


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def try_read_json(path: Path) -> Any | None:
    try:
        return read_json(path)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None


def sha256_bytes(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _fsync_directory(directory: Path) -> None:
    try:
        fd = os.open(str(directory), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        # 某些文件系统（如部分网络/挂载卷）不支持目录 fsync，忽略即可。
        pass
    finally:
        os.close(fd)


@contextmanager
def file_lock(lock_path: Path, *, blocking: bool = True) -> Iterator[None]:
    """跨进程排他锁；进程崩溃时由内核自动释放。"""
    import fcntl

    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o644)
    try:
        flags = fcntl.LOCK_EX if blocking else fcntl.LOCK_EX | fcntl.LOCK_NB
        try:
            fcntl.flock(fd, flags)
        except BlockingIOError as error:
            raise ConflictError("另一个操作正持有该资源的锁，请稍后重试或先查询/取消既有申请") from error
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path
