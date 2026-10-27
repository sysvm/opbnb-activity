# 系统交易与周/月活跃统计修正

本次仅修统计，HTML 可视化按用户要求暂缓。没有新增数据库服务、RPC 调用、依赖或配置项。

## 修正内容

1. 周/月周期起点使用 `YYYY-MM-DD`。旧实现将 DuckDB TIMESTAMP 转字符串得到带 `00:00:00` 的键，补齐日历周期时无法匹配，被替换为零。现统一格式，保留跨日去重、成功地址/含失败地址两个口径、零活动和部分周期；不能把 DAU 相加当 WAU/MAU。
2. 已知主网 TxDAG 记录新增 `system_tx_dag` 类别。以下条件同时满足才识别：
   - sender：`0x8eece8cb98dc9aaddb37a8177e4615469d771631`；
   - recipient：`0xda90000000000000000000000000000000000000`；
   - legacy type 0、顶层 value 为零、成功 receipt、明确观测 `effectiveGasPrice=0`；
   - 区块最后一笔；合法十六进制 calldata，长度至少包含 ABI 头，selector 为 `0x5517ed8c`。

目标地址与生成条件可在相邻 op-geth 源码 `miner/worker.go` 的 `DefaultTxDAGAddress` / `generateDAGTx` 和 `core/types/dag.go` 的 `setTxDAG(bytes)` 核对；sender 为本次已观察的主网地址，不假定未来轮换。此规则不完整解码 DAG payload，也不声称识别所有协议交易。其他发送方、失败、缺少费用证据、同地址普通调用仍按原规则处理。普通用户 deposit 不受影响。

## 兼容性

采集规范化和报告分区临时视图使用同一组条件；旧 Parquet 的 `is_system`/`classification` 不重写，报告时补识别。所有下游非系统活跃、项目/方法/接收方/发送方榜、留存、事件/Transfer 都消费修正后的分类。系统费用组包含 L1 attributes 与 TxDAG，两种技术类型单列；全部交易/区块数不减少。

`analysis.json.metadata.system_rules_version` 标记本次分析规则：`l1_attributes_and_known_txdag_v1`。`technical.md` 记录规则与旧字段差异。原始 Parquet、分区哈希、manifest、identity、checkpoint 不变，运行 ID 和 config 不用修改。此前 analysis.json/CSV 仍是旧口径；不能只改汇总数字来替代全量重算（排名、地址集合和留存需要重新计算）。

## 服务器更新与复算

同步新版源代码后，在保存了原始 Parquet 的项目目录执行：

```bash
.venv/bin/python -m pip install -e .
mkdir -p logs
nohup .venv/bin/opbnb-activity report --config config.toml --run-id mainnet-20260722-20261025 > logs/mainnet-20260722-20261025-report.log 2>&1 &
```

这是离线报告，不会重新扫描链上数据。保持已有 `analysis_memory_limit_mb`、`analysis_threads` 和临时目录设置；重算仍需要时间和临时磁盘空间。运行时不要把上次残留的 report 当作本次完成结果，先确认日志最终成功和 `analysis-status.json` 状态。

完成后同步新版 `report/`、`analysis.json`、`quality.json` 和 `manifest.json`。确认周/月有活动周期不再全零、metadata 为新规则版本、技术类型有 `system_tx_dag`。TxDAG 最终识别数以原始字段实际满足组合条件为准，不硬编码为旧报告的接收地址交易总数。

## 验证边界

回归测试覆盖周期键、精确集合去重、系统/普通用户 deposit、TxDAG 逐条件反例、Python/SQL 规则一致性、旧 Parquet 分区重分类、事件/资金/费用/榜单一致性，以及原始文件不变。另使用 25 万行 typed-Parquet 和 128MB DuckDB 预算的既有全管线测试核对周/月集合。测试预算不是进程 RSS 上限，合成测试不证明本机已重算完成主网 2.128 亿笔交易。
