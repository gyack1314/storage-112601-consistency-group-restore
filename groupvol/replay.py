"""按顺序回放演练 JSONL，复用仓库的 scenario_reader 契约校验。"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from .coordinator import Coordinator
from .util import ServiceError

# 与 examples/message_types.json 保持一致（读取器测试会校验登记文件本身）。
MESSAGE_TYPES = {
    "cancel",
    "commit",
    "flush",
    "freeze",
    "inspect",
    "lost",
    "restart",
    "restore",
    "sealed",
    "volume",
    "write",
}


def supported_types() -> set[str]:
    """优先读取仓库里的 message_types.json 登记表，便于场景扩展后直接回放。"""
    registry = Path(__file__).resolve().parent.parent / "examples" / "message_types.json"
    try:
        import json

        data = json.loads(registry.read_text(encoding="utf-8"))
        if isinstance(data, list) and data and all(isinstance(x, str) for x in data):
            return set(data)
    except OSError:
        pass
    return set(MESSAGE_TYPES)


def _import_iter_messages():
    try:
        from scenario_reader import iter_messages  # type: ignore

        return iter_messages
    except ImportError:
        repo_root = Path(__file__).resolve().parent.parent
        if str(repo_root) not in sys.path:
            sys.path.insert(0, str(repo_root))
        from scenario_reader import iter_messages  # type: ignore

        return iter_messages


def run_file(path: str | Path, root: str | Path, *, fresh: bool = False) -> list[dict[str, Any]]:
    """把一个 JSONL 演练文件回放到 root，返回每条消息的结构化结果事件。"""
    path = Path(path)
    root = Path(root)
    if fresh and root.exists():
        import shutil

        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)

    iter_messages = _import_iter_messages()
    events: list[dict[str, Any]] = []
    coordinator = Coordinator(root)

    for row in iter_messages(path, supported_types()):
        at = row["at"]
        kind = row["kind"]
        try:
            if kind == "restart":
                # 只重启协调服务：状态从磁盘重载并做崩溃对账，live/snapshot 文件不动。
                recovery = coordinator.recover(at=at)
                coordinator = Coordinator(root)
                result: dict[str, Any] = {"event": "coordinator_restarted", "recovery": recovery}
            else:
                try:
                    result = coordinator.handle_message(row)
                except ServiceError as error:
                    result = {"event": "error", "error": error.to_dict()}
        except Exception as error:  # 单条消息的意外失败不应中断整场演练
            result = {
                "event": "error",
                "error": {
                    "code": "internal_error",
                    "type": type(error).__name__,
                    "message": str(error),
                },
            }
        events.append({"at": at, "kind": kind, "result": result})

    return events


def run_paths(
    paths: list[str | Path], root: str | Path, *, fresh: bool = False
) -> list[dict[str, Any]]:
    """回放多个场景。不同场景的快照 id 可能冲突，默认每个文件使用 root/<文件名>。"""
    paths = [Path(p) for p in paths]
    root = Path(root)
    all_events: list[dict[str, Any]] = []
    for index, path in enumerate(paths):
        if len(paths) == 1:
            scenario_root = root
        else:
            scenario_root = root / path.stem
        events = run_file(path, scenario_root, fresh=fresh and index == 0)
        for event in events:
            event["file"] = path.name
            event["root"] = str(scenario_root)
        all_events.extend(events)
    return all_events
