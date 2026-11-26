"""storage_ctl 命令行：卷写入、建快照、取消申请、验证、恢复、检查与场景回放。"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from .coordinator import Coordinator
from .replay import run_file, run_paths
from .util import ConflictError, NotFoundError, ServiceError, ValidationError


def _default_root() -> str:
    return os.environ.get("GROUPVOL_ROOT", "./groupvol_lab")


def _print_json(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=False))


def _mapping_from_args(args: argparse.Namespace) -> dict[str, str]:
    mapping: dict[str, str] = {}
    raw_json = getattr(args, "mapping_json", None)
    if raw_json:
        try:
            mapping.update(json.loads(raw_json))
        except json.JSONDecodeError as error:
            raise ValidationError(f"--mapping-json 不是合法 JSON：{error}") from error
    for pair in getattr(args, "map", None) or []:
        if "=" not in pair:
            raise ValidationError(f"--map 需要写成 源卷=目标卷，收到 {pair!r}")
        src, dst = pair.split("=", 1)
        mapping[src] = dst
    return mapping


def _requires_list(raw: str | None) -> list[int] | None:
    if raw is None:
        return None
    if not raw.strip():
        return []
    result = []
    for part in raw.split(","):
        part = part.strip()
        try:
            result.append(int(part))
        except ValueError as error:
            raise ValidationError(f"--requires 需要逗号分隔的整数序号，无法解析 {part!r}") from error
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="storage_ctl.py",
        description="实验工作站成组卷一致快照与原子恢复服务（本地模拟驱动，仅标准库）",
    )
    # 允许 `storage_ctl.py --root X cmd ...` 与 `storage_ctl.py cmd --root X ...` 两种写法。
    parser.add_argument(
        "--root",
        default=_default_root(),
        help="状态/块文件根目录（默认 %(default)s 或 $GROUPVOL_ROOT）",
    )
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--root",
        default=argparse.SUPPRESS,
        help="状态/块文件根目录（覆盖全局 --root 与 $GROUPVOL_ROOT）",
    )
    common.add_argument("--at", type=int, default=None, help="仿真秒点；缺省沿用当前逻辑时钟")

    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("replay", help="按 JSONL 契约回放演练场景", parents=[common])
    p.add_argument("paths", nargs="+", type=Path)
    p.add_argument("--fresh", action="store_true", help="回放前清空对应状态根目录")
    p.add_argument("--strict", action="store_true", help="出现业务错误事件时以非零码退出")
    p.add_argument("--compact", action="store_true", help="每行输出一个紧凑 JSON 事件（默认缩进）")

    p = sub.add_parser("volume", help="登记卷", parents=[common])
    p.add_argument("--group", required=True)
    p.add_argument("--volume", required=True)
    p.add_argument("--blocks", required=True, type=int)

    p = sub.add_parser("write", help="写块（data 字符串，或 --clear 置 null）", parents=[common])
    p.add_argument("--group", required=True)
    p.add_argument("--volume", required=True)
    p.add_argument("--seq", required=True, type=int)
    p.add_argument("--block", required=True, type=int)
    p.add_argument("--batch", default=None)
    data_group = p.add_mutually_exclusive_group()
    data_group.add_argument("--data", default=None, help="块字符串内容")
    data_group.add_argument("--clear", action="store_true", help="把块清空为 null")

    p = sub.add_parser("flush", help="卷级落盘到 through 水位", parents=[common])
    p.add_argument("--group", required=True)
    p.add_argument("--volume", required=True)
    p.add_argument("--through", required=True, type=int)

    p = sub.add_parser("commit", help="宣布批次正式提交", parents=[common])
    p.add_argument("--group", required=True)
    p.add_argument("--batch", required=True)
    p.add_argument("--requires", default=None, help="逗号分隔的必需写入序号，如 1,2")

    p = sub.add_parser("freeze", aliases=["snapshot-create"], help="提出组快照冻结（创建快照）", parents=[common])
    p.add_argument("--group", required=True)
    p.add_argument("--id", required=True)
    p.add_argument("--deadline", required=True, type=int)
    p.add_argument("--parent", default=None)

    p = sub.add_parser("sealed", help="卷报告已停止接新写并回报水位", parents=[common])
    p.add_argument("--id", required=True)
    p.add_argument("--volume", required=True)
    p.add_argument("--through", required=True, type=int)

    p = sub.add_parser("cancel", help="取消未完成冻结或失败/进行中的恢复申请", parents=[common])
    p.add_argument("id", help="快照 id 或恢复申请 rid")
    p.add_argument("--kind", choices=["snapshot", "restore"], default=None)
    p.add_argument("--reason", default="operator_cancel")

    p = sub.add_parser("lost", help="模拟快照卷对象/块不可读", parents=[common])
    p.add_argument("--id", required=True)
    p.add_argument("--volume", required=True)
    p.add_argument("--block", default=None, type=int)

    sub.add_parser("recover", help="重启协调服务并做崩溃对账", parents=[common])

    p = sub.add_parser("inspect", help="按 group / id(快照) / target 检查", parents=[common])
    p.add_argument("--group", default=None)
    p.add_argument("--id", default=None)
    p.add_argument("--target", default=None)

    p = sub.add_parser("verify", help="验证快照父链、缺失块与可恢复性", parents=[common])
    p.add_argument("--id", required=True)

    p = sub.add_parser("restore", help="把快照整组恢复到隔离目标", parents=[common])
    p.add_argument("--id", required=True, help="快照 id")
    p.add_argument("--target", required=True)
    p.add_argument("--map", action="append", default=[], help="源卷=目标卷，可重复")
    p.add_argument("--mapping-json", default=None)
    p.add_argument("--rid", default=None, help="恢复申请 id；缺省由快照/目标/映射确定性生成（幂等）")
    p.add_argument("--force", action="store_true", help="允许覆盖该目标已有 winner，重挂新恢复点")
    p.add_argument("--resume", action="store_true", help="续跑一个中断的恢复申请（需 --rid 或相同参数）")

    p = sub.add_parser("resume", help="续跑中断的恢复申请", parents=[common])
    p.add_argument("--rid", required=True)

    p = sub.add_parser("rebuild", help="仅用本地快照块文件把工作区重建到任意新目录（不切换挂载）", parents=[common])
    p.add_argument("--id", required=True)
    p.add_argument("--into", required=True, type=Path)
    p.add_argument("--map", action="append", default=[])
    p.add_argument("--mapping-json", default=None)
    p.add_argument("--clean", action="store_true", help="目标目录非空时先清空")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        # replay 自己管理 Coordinator/重启，单独处理。
        if args.command == "replay":
            return _cmd_replay(args)
        return _cmd_dispatch(args)
    except NotFoundError as error:
        _print_json({"ok": False, "error": error.to_dict()})
        return 2
    except ValidationError as error:
        _print_json({"ok": False, "error": error.to_dict()})
        return 2
    except ConflictError as error:
        _print_json({"ok": False, "error": error.to_dict()})
        return 1
    except ServiceError as error:
        _print_json({"ok": False, "error": error.to_dict()})
        return 1


def _cmd_replay(args: argparse.Namespace) -> int:
    if len(args.paths) == 1:
        events = run_file(args.paths[0], args.root, fresh=args.fresh)
    else:
        events = run_paths(args.paths, args.root, fresh=args.fresh)
    if args.compact:
        for event in events:
            print(json.dumps(event, ensure_ascii=False, sort_keys=True))
    else:
        _print_json({"root": str(Path(args.root).resolve()), "events": events})
    had_error = any(e.get("result", {}).get("event") == "error" for e in events)
    if args.strict and had_error:
        return 1
    return 0


def _cmd_dispatch(args: argparse.Namespace) -> int:
    coordinator = Coordinator(args.root)
    at = getattr(args, "at", None)
    cmd = args.command

    if cmd == "volume":
        result = coordinator.handle_message(
            {"at": _at_or(coordinator, at), "kind": "volume",
             "record": {"group": args.group, "volume": args.volume, "blocks": args.blocks}}
        )
    elif cmd == "write":
        if not args.clear and args.data is None:
            raise ValidationError("write 需要 --data 字符串或 --clear 清空为 null")
        record = {
            "group": args.group, "volume": args.volume, "seq": args.seq,
            "block": args.block, "data": None if args.clear else args.data,
        }
        if args.batch:
            record["batch"] = args.batch
        result = coordinator.handle_message(
            {"at": _at_or(coordinator, at), "kind": "write", "record": record}
        )
    elif cmd == "flush":
        result = coordinator.handle_message(
            {"at": _at_or(coordinator, at), "kind": "flush",
             "record": {"group": args.group, "volume": args.volume, "through": args.through}}
        )
    elif cmd == "commit":
        record: dict[str, Any] = {"group": args.group, "batch": args.batch}
        requires = _requires_list(args.requires)
        if requires is not None:
            record["requires"] = requires
        result = coordinator.handle_message(
            {"at": _at_or(coordinator, at), "kind": "commit", "record": record}
        )
    elif cmd in {"freeze", "snapshot-create"}:
        record = {"group": args.group, "id": args.id, "deadline": args.deadline}
        if args.parent:
            record["parent"] = args.parent
        result = coordinator.handle_message(
            {"at": _at_or(coordinator, at), "kind": "freeze", "record": record}
        )
    elif cmd == "sealed":
        result = coordinator.handle_message(
            {"at": _at_or(coordinator, at), "kind": "sealed",
             "record": {"id": args.id, "volume": args.volume, "through": args.through}}
        )
    elif cmd == "cancel":
        record = {"id": args.id, "reason": args.reason}
        if args.kind:
            record["kind"] = args.kind
        result = coordinator.handle_message(
            {"at": _at_or(coordinator, at), "kind": "cancel", "record": record}
        )
    elif cmd == "lost":
        record = {"id": args.id, "volume": args.volume}
        if args.block is not None:
            record["block"] = args.block
        result = coordinator.handle_message(
            {"at": _at_or(coordinator, at), "kind": "lost", "record": record}
        )
    elif cmd == "recover":
        result = coordinator.recover(at=at)
        _print_json({"ok": True, "result": result})
        return 0
    elif cmd == "inspect":
        if args.at is not None:
            coordinator.tick(args.at)
        query = {}
        if args.id:
            query["id"] = args.id
        if args.group:
            query["group"] = args.group
        if args.target:
            query["target"] = args.target
        result = coordinator.inspect(query)
        _print_json({"ok": True, "report": result})
        return 0
    elif cmd == "verify":
        result = coordinator.verify_snapshot(args.id)
        _print_json({"ok": result["restorable"], "report": result})
        return 0 if result["restorable"] else 1
    elif cmd == "restore":
        mapping = _mapping_from_args(args)
        if args.resume:
            rid = args.rid or coordinator._default_rid(args.id, args.target, mapping)
            result = coordinator.resume_restore(rid, at=at)
        else:
            result = coordinator.restore_snapshot(
                args.id, args.target, mapping, rid=args.rid, at=at, force=args.force
            )
        _print_json({"ok": result.get("phase") == "switched", "result": result})
        return 0 if result.get("phase") == "switched" else 1
    elif cmd == "resume":
        result = coordinator.resume_restore(args.rid, at=at)
        _print_json({"ok": result.get("phase") == "switched", "result": result})
        return 0 if result.get("phase") == "switched" else 1
    elif cmd == "rebuild":
        mapping = _mapping_from_args(args)
        summary = coordinator.rebuild_into(args.id, args.into, mapping, clean=args.clean)
        _print_json({"ok": True, "result": summary})
        return 0
    else:  # pragma: no cover
        parser.error(f"未知命令：{cmd}")
        return 2

    _print_json({"ok": True, "result": result})
    return 0


def _at_or(coordinator: Coordinator, at: int | None) -> int:
    if at is not None:
        return at
    clock = int(coordinator.state.get("clock", -1))
    return max(clock, 0)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
