#!/usr/bin/env bash
# 可重复执行的值班演练：回放两个自带场景，并演示检查、验证、恢复与本地重建。
# 用法：bash drill.sh [演示根目录]   （默认 ./demo_lab，每次先清空）
set -u

cd "$(dirname "$0")"
PY="${PYTHON:-python3}"
LAB="${1:-./demo_lab}"
rm -rf "$LAB"
mkdir -p "$LAB"

line() { printf '\n\033[1;36m== %s ==\033[0m\n' "$1"; }
jq_py() { "$PY" -c "import json,sys; f=sys.argv[1]; d=json.load(open(f)); print(eval(sys.argv[2]))" "$1" "$2"; }

line "1) 回放：附件先于主记录提交（s1 应在 deadline 当秒超时，b1 不得判为已覆盖）"
"$PY" storage_ctl.py replay examples/attachment-ahead-of-commit.jsonl \
  --root "$LAB/ahead" --fresh --compact > "$LAB/ahead-events.jsonl"
echo "事件数：$(wc -l < "$LAB/ahead-events.jsonl")（已写入 $LAB/ahead-events.jsonl）"

line "2) 检查超时恢复点 s1（状态 / 覆盖提交 / 缺失块）"
"$PY" storage_ctl.py inspect --id s1 --root "$LAB/ahead" > "$LAB/ahead-s1.json"
"$PY" - <<'PY' "$LAB/ahead-s1.json"
import json,sys
r=json.load(open(sys.argv[1]))["report"]
print("status      :", r["status"], "| restorable:", r["restorable"])
print("covered     :", r["covered_batches"])
print("batch verdict:", [(b["batch"], b["verdict"]) for b in r["batches"]])
print("missing blocks:", r["missing_blocks"])
PY

line "3) 该场景的 restore 应被阻碍，目标仍为原卷在用"
"$PY" storage_ctl.py inspect --target replay --root "$LAB/ahead" \
  | "$PY" -c "import json,sys;r=json.load(sys.stdin)['report'];print('target state:',r['state'],'| phases:',[x['phase'] for x in r['restore_requests']])"

line "4) 回放：增量快照 + 卷丢失（s1 可用，s2 因 journal#0 缺失不可恢复）"
"$PY" storage_ctl.py replay examples/incremental-missing-volume.jsonl \
  --root "$LAB/inc" --fresh --compact > /dev/null
"$PY" storage_ctl.py verify --id s1 --root "$LAB/inc" > /dev/null \
  && echo "verify s1: OK（可恢复）"
if "$PY" storage_ctl.py verify --id s2 --root "$LAB/inc" > "$LAB/s2-verify.json"; then
  echo "verify s2: 意外通过"
else
  echo "verify s2: 被阻碍（退出码 1）"
fi
"$PY" - <<'PY' "$LAB/s2-verify.json"
import json,sys
r=json.load(open(sys.argv[1]))["report"]
print("missing blocks:", r["missing_blocks"])
print("problems:", sorted({p["code"] for p in r["problems"]}))
PY

line "5) 从健康的 s1 恢复到隔离工作区（作业员改用新目标卷名）"
"$PY" storage_ctl.py restore --id s1 --target good \
  --map blobs=data --map journal=manifest --root "$LAB/inc" \
  | "$PY" -c "import json,sys;r=json.load(sys.stdin)['result'];print('phase:',r['phase'],'| generation:',r['generation'])"
"$PY" storage_ctl.py inspect --target good --root "$LAB/inc" \
  | "$PY" -c "import json,sys;r=json.load(sys.stdin)['report'];m=r['mounted_version'];print('mounted:',m['snapshot_id'],'batches',m['covered_batches'],'mapping',m['mapping'])"

line "6) 仅用本地块文件把 s1 重建到一个全新目录"
rm -rf "$LAB/rebuilt"
"$PY" storage_ctl.py rebuild --id s1 --root "$LAB/inc" --into "$LAB/rebuilt" \
  --map blobs=data --map journal=manifest > /dev/null
echo "RECOVERY_POINT: $LAB/rebuilt/RECOVERY_POINT.json"
find "$LAB/rebuilt/volumes" -name 'blk-*.json' | sort | sed "s#$LAB/##"

line "7) 取消一次未完成冻结，随后卷可重新接写"
"$PY" storage_ctl.py --root "$LAB/manual" volume --group lab --volume v --blocks 1 --at 0 >/dev/null
"$PY" storage_ctl.py --root "$LAB/manual" freeze --group lab --id f1 --deadline 50 --at 1 >/dev/null
"$PY" storage_ctl.py cancel f1 --root "$LAB/manual" --at 2 \
  | "$PY" -c "import json,sys;print(json.load(sys.stdin)['result']['event'])"
"$PY" storage_ctl.py --root "$LAB/manual" write --group lab --volume v --seq 9 --block 0 --data ok --at 3 \
  | "$PY" -c "import json,sys;print('write after cancel:',json.load(sys.stdin)['result']['event'])"

line "完成。可继续用以下命令检查："
cat <<EOF
  $PY storage_ctl.py inspect --id s2 --root $LAB/inc
  $PY storage_ctl.py inspect --target dest --root $LAB/inc
  $PY -m unittest discover -v
EOF
