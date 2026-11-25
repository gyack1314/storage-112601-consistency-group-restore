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
