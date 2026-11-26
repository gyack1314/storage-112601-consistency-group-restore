# 实验工作站的成组卷快照与原子恢复

模拟卷只使用本地合成短块字符串，空块为 null。卷名在组内独立。

## 输入消息

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 at、消息名称 kind 与对象 record。at 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 record 中。

- **volume**　登记 group、volume、块数 blocks。
- **write**　上游以 batch 登记尚待业务提交确认的写入，指向指定 volume 的 block。data 为字符串或 null，null 清空。
- **flush**　驱动确认 volume 上截至 through 的写入已持久，through 是写入序号。
- **commit**　记录端宣布 batch 正式提交，requires 列该批必需写入序号。
- **freeze**　协调方提出 group 的快照 id、deadline 和可选 parent。 sealed 在 deadline 当秒到达为超时。
- **sealed**　卷报告已为该 id 停止接新写，并提供自身水位 through。
- **restore**　恢复 id 到 target，mapping 为卷名映射。
- **lost**　模拟只读快照的 node 卷块或前序卷对象不可读。
- **restart**　只重启协调服务，卷文件继续可用。
- **inspect**　按 group、id 或 target 查询。
- **cancel**　协调员取消指定快照 id 的未完成冻结。

## 示例

- attachment-ahead-of-commit.jsonl
- incremental-missing-volume.jsonl

## 读取演练资料

运行环境为 Python 3.11，只使用标准库。在根目录运行以下命令可核对文件格式及读取接口。

    python3 scenario_reader.py examples
    python3 -m unittest discover -v

资料读取器会按文件统计各类参与方消息的数量。演练记录只描述各参与方交给系统的输入。

---

# 成组卷一致快照 + 原子恢复服务

在上述消息契约之上，仓库提供了一个**只依赖标准库**的 Python 存储服务，代码在
`groupvol/`，统一入口是根目录的 `storage_ctl.py`。它把“先写附件、后提交记录”的
多卷一致性、冻结超时、增量引用、缺块阻碍、同目标单 winner 与崩溃后状态可辨认
全部落到可执行的状态机和本地块文件上。

## 一分钟演练

```bash
bash drill.sh                 # 端到端：回放两个示例 + 检查/验证/恢复/重建/取消
python3 -m unittest discover -v
```

所有状态默认落在 `--root`（或环境变量 `GROUPVOL_ROOT`，缺省 `./groupvol_lab`）。
每个命令都可用 `--root DIR` 指定；`--root` 放在子命令前后均可。

## 可重复执行的入口

| 值班动作 | 命令 |
| --- | --- |
| 回放整场 JSONL | `python3 storage_ctl.py replay examples/incremental-missing-volume.jsonl --root /tmp/lab --fresh --compact` |
| 登记卷 | `python3 storage_ctl.py volume --group exp --volume blobs --blocks 8 --at 0` |
| 卷写入（字符串/清空） | `… write --group exp --volume blobs --seq 1 --block 2 --data xyz --batch b1`；清空用 `--clear` |
| 卷级落盘 | `… flush --group exp --volume blobs --through 1` |
| 批次提交 | `… commit --group exp --batch b1 --requires 1,2` |
| 创建组快照（冻结） | `… freeze --group exp --id s1 --deadline 9 [--parent s0]`（别名 `snapshot-create`） |
| 卷 sealed 水位 | `… sealed --id s1 --volume blobs --through 1` |
| 取消失败/未完成申请 | `… cancel s1`（快照 id 或恢复申请 rid 均可，自动判别） |
| 模拟丢失 | `… lost --id s2 --volume journal [--block 0]` |
| 验证恢复点 | `… verify --id s2`（可恢复退出 0，有阻碍退出 1） |
| 恢复到隔离目标 | `… restore --id s1 --target dest --map blobs=data --map journal=manifest [--rid r1] [--force]` |
| 续跑中断的恢复 | `… resume --rid r1`（或用相同参数重跑 `restore`，幂等） |
| 检查 | `… inspect --id s1` / `--group exp` / `--target dest` |
| 仅用块文件重建新目录 | `… rebuild --id s1 --into /tmp/new-ws [--map blobs=data] [--clean]` |
| 重启协调服务并对账 | `… recover` |

退出码：`0` 成功/可恢复/已切换；`1` 冲突、验证失败、恢复被阻碍；`2` 找不到对象或
输入不合法。所有结果都是 JSON，便于脚本解析。

## 关键语义（与题目要求一一对应）

1. **共同恢复点与覆盖提交**：只有组内全部卷都在 deadline 之前（`at < deadline`；
   deadline 当秒的 sealed 判为超时）sealed，快照才 `complete` 并写出
   `manifest.json`。批次判为已覆盖，当且仅当存在一条此时点之前的 `commit`，且其
   `requires` 每个序号都被所属卷的 sealed 水位覆盖。检查报告把批次分为
   `committed_in_recovery_point`、`committed_but_incomplete_at_point`、
   `writes_present_but_not_committed`、`not_at_recovery_point`。
2. **附件先落盘、主记录未完成**：该批绝不会进入 manifest 的 `commits`；快照本身
   仍是崩溃一致点，可恢复出“有孤儿附件、无提交事实”的状态。前一个 `complete`
   快照始终保留为组的 `latest_complete_recovery_point`。
3. **超时释放**：deadline 一到，冻结转 `timed_out`，对每个请求卷写
   `release_freeze` 并移除驱动冻结令牌，卷可重新接写。已切出的单卷制品保留供检查，
   但 `restorable=false`——单卷快照不等于整组可用。不同 group 的冻结互不影响。
4. **取消**：`cancel <快照id>` 取消未完成冻结并释放卷；`cancel <rid>` 取消尚未
   切换的恢复申请并清理暂存。已完成快照、已切换申请、处于切换提交点的申请不可取消。
5. **增量与“空块不得回补”**：带 `--parent` 建 s2 时逐块解析父快照值，相同则写
   `{"t":"ref",...}`，不同（改写或清空）则写本快照自有的 `full` 块（`data` 可以为
   `null`）。恢复只对 `ref` 沿父链取值；任何 `full`（尤其 `data:null`）文件丢失都
   直接报 `block_file_missing`，**不会**回父快照找旧值。
6. **缺失阻碍明确化**：`verify`/`inspect` 给出 `missing_blocks`、
   `missing_parent_snapshots` 和带稳定 code 的 `problems`
   （`parent_snapshot_missing`、`parent_volume_missing`、`block_file_missing`、
   `volume_object_missing`、`block_checksum_mismatch`、`reference_cycle` 等）。
   有任何阻碍时 restore 在验证阶段就 `blocked`，不创建目标挂载。
7. **生产卷不被半套覆盖**：生产/实时卷在 `<root>/live/<group>/<volume>/`；恢复只写
   `<root>/workspaces/<target>/` 下的暂存与 generation 目录，绝不回写 `live/`。
8. **同一快照来源**：目标卷结构、`blocks_count`、逐块内容、`commits` 与
   `covered_batches` 全部来自同一个 `snapshots/<id>/manifest.json` 及其父链；恢复出的
   `RECOVERY_POINT.json` 固化这些事实。`--map 源=目标` 可在恢复前改目标卷名。
9. **同目标只有一个 winner**：跨进程 `fcntl` 全局操作锁 + 目标 winner 状态保证两个
   不同申请并发/顺序打同一目标时恰有一个 `switched`，另一个为 `conflict`。相同参数的
   重复申请得到同一确定性 rid，天然幂等；确需重挂可用 `--force`。
10. **崩溃可辨认、整组一起切换**：恢复阶段持久化为
    `verifying → preparing → prepared → switching → switched`。整组卷先完整物化到
    独立 `gen-NNNN/`（含 `PREPARED.json` 与逐块回读自检），最后用
    `mount.json.tmp → os.replace` 原子替换唯一的挂载指针。重新进入时
    `inspect --target T` 的 `state` 明确给出三态：
    - `original_in_use`：尚无 mount 指针（或恢复 blocked/conflict/cancelled）；
    - `preparing`：存在未完成申请（含 prepared/switching 子阶段），旧版本仍在用；
    - `switched`：mount 指针已发布，指针指向的 generation 包含整组卷。

    即使进程在“写 mount 指针之后、保存 switched 状态之前”被 `kill -9`，新进程也会以
    `mount.json` 为准对账为 `switched`，不会出现卷之间跨时间点混搭。可用
    `GROUPVOL_CRASH=restore_begin|restore_prepared|restore_switching|mount_renamed`
    注入真实 `os._exit` 演练崩溃（测试已覆盖）。

## 磁盘布局

```
<root>/
  coordinator-state.json          # 批次/提交/快照/恢复/winner 的原子状态
  locks/coordinator.lock          # 跨进程 fcntl 锁
  live/<group>/<volume>/          # 本地模拟驱动：wal/ 与 blocks/（生产原卷）
  snapshots/<id>/
    manifest.json                 # 组清单：卷尺寸、full/ref 块条目、覆盖提交
    volumes/<volume>/blk-*.json   # 该快照自有的 full 块（data 可为 null）
  workspaces/<target>/
    mount.json                    # 当前整组挂载版本（原子替换的唯一指针）
    generations/gen-0001/         # 某次成功切换的完整整组卷 + RECOVERY_POINT.json
    .staging/<rid>/               # 切换前的暂存整组卷
```

`lost` 会把对应卷目录/块移动到 `<root>/quarantine/`（等价不可读），随后验证与恢复
即报阻碍。

## 设计边界

- 仅标准库，Python 3.11；块为本地合成短字符串，空块为 JSON `null`。
- 时间由消息里的整数 `at` 驱动（同秒按行序），不依赖墙上时钟。
- 默认开启关键 JSON 的 fsync 落盘；跑测试时设 `GROUPVOL_FSYNC=0` 可显著加速，
  不影响原子改名与进程崩溃恢复语义。

