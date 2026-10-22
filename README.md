# opBNB Activity：可审计的离线分析工具

这是独立 Python 工具，未修改 opBNB / op-geth 协议代码。已实现 A–C 阶段：配置与节点能力探测、可恢复的分片采集与质量检查、离线指标和报告。当前验证仅使用合成数据和 `httpx.MockTransport`，没有连接或采集真实节点，也没有主网活跃度结论。大规模三个月采集的耗时、磁盘与节点吞吐仍需在提供节点后实测。

## 安装与离线体验

项目声明 Python 3.11+，实际离线验证使用 **Python 3.12**，服务器推荐 3.12。`requirements-tested-py312.txt` 记录本次完整测试环境版本（包括仅支持 Python 3.12+ 的 NumPy 2.5.4）；不声称该快照可在 3.11 安装。3.11 通用依赖解析可选择其他兼容版本，但尚未验证。在本目录执行：

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[test]'
.venv/bin/opbnb-activity demo --dataset ./data --run-id synthetic-demo
.venv/bin/opbnb-activity validate --dataset ./data --run-id synthetic-demo
```

如需复现本次 Python 3.12 依赖，可先 `pip install -r requirements-tested-py312.txt`，再安装本项目。

`demo` 完全离线，使用随包 JSON fixture；不读取 `OPBNB_RPC_URL`。报告和图均标注 **SYNTHETIC / TEST DATA**。演示固定为 Asia/Shanghai **2026-10-01（含）至 2026-10-22（不含）**，使用 6 个连续编号、时间戳稀疏的合成区块。它包含重复地址、同项目多合约、失败交易、系统 L1 attributes、普通用户 deposit、超过 38 位整数、未知 L1 费用、已验证代币 Transfer、NFT 歧义事件、未成熟留存 cohort。没有交易的日期只是 fixture 的零值，不代表主网无交易。

输出在 `data/synthetic-demo/`：

- `manifest.json`：运行 ID、配置身份、精确 UTC 窗口、区块结束不含范围、数据来源和 schema 版本。
- `data/date=YYYY-MM-DD/blocks=START-END/`：每范围一组 `blocks`、`transactions`、`receipts`、`logs` Parquet，外加 `_partition.json` SHA256/行数。
- `checkpoint.sqlite`：SQLite WAL/FULL 模式的完成分片记录；分片目录原子落盘后再记 checkpoint。中间崩溃可校验并恢复已写分片。
- `quality.json`、`analysis.json`、`report/summary.md`、CSV 表、`daily.png`、`projects.png`。

离线命令均必须显式 `--run-id`。数据目录可移动；在新位置传 `--dataset`，manifest 不依赖开发者本地绝对目录。演示再次执行可恢复，不会重复累加。相同运行 ID 不允许混用窗口、系统识别规则、分片大小或数据来源。

## 服务器采集流程

复制 `config.example.toml` 为自己的配置。真实分析默认窗口为 Asia/Shanghai **2026-07-22（含）至 2026-10-22（不含）**，对应 UTC `[2026-07-21T16:00:00Z, 2026-10-21T16:00:00Z)`。支持替换日期/带时区时间。节点 URL 只通过环境变量提供：

```bash
read -r -s OPBNB_RPC_URL
export OPBNB_RPC_URL
mkdir -p logs
.venv/bin/opbnb-activity probe --config config.toml --run-id full-2026q3
nohup .venv/bin/opbnb-activity collect --config config.toml --run-id full-2026q3 >logs/collect.log 2>&1 &
tail -f logs/collect.log
.venv/bin/opbnb-activity validate --config config.toml --run-id full-2026q3
.venv/bin/opbnb-activity analyze --config config.toml --run-id full-2026q3
.venv/bin/opbnb-activity report --config config.toml --run-id full-2026q3
```

需要停止时对该后台进程发 SIGTERM，再用完全相同的命令恢复。尚未完成的分片会重新采集，最多损失当前分片的工作，不会把半分片视作完成。已完成分片先复核哈希。损坏分片保留为隐藏 `.replaced-*` 目录后重写；可在确认无恢复需求后手工清理。单个 run 同时仅运行一个采集进程。

推荐先另设 run ID、用 `--start` / `--end` 做一天样本，再评估全窗口。默认 **1 个逻辑 RPC 元素/秒、1 个并发 HTTP 请求、batch_size=1、每分片 500 区块**。批大小可显式设为 2–100；此时按元素扣预算、请求前等待 `元素数/qps`，是保守平均预算，但单次批请求仍会有批内突发。工具不会自动加压。三个月数据在 1 QPS 下可能耗时很长；没有任何“探测通过即可达到某吞吐”的承诺。

`probe` 首先检查 chain ID 204，再做时间戳下界二分和有界样本。探测覆盖批处理、block receipts、单交易 receipts、logs、可选能力意义上的 trace。二分使用 `[start,end)`，支持相同/不规则时间戳；不足历史或尚未到结束时间会拒绝采集，不静默截断。archive 不等于 tracing：仅 `-32601` 标为方法 unsupported；pruned/transient 独立记录。Header 覆盖不保证全窗口所有 receipt/state 都可用；采集遇到缺失会阻止该分片完成。trace 仅探测，不参加经济指标。

RPC 校验批响应 ID，拒绝遗漏/重复/意外 ID；仅重试 transient 的失败元素。HTTP 429/5xx 有界重试并遵循 Retry-After；等待超过 300 秒预算则停止，不提前重试。传输异常和节点错误不回显 URL、认证或原始错误文本。重定向关闭，不自动使用环境代理。未验证的/非标准 verified-total-fee 字段不会直接作为总费用。

## 标签、口径与财务精度

标签 CSV 必须有列：

```text
address,project,category,role,source_url,confidence,valid_from,valid_to
```

地址按小写匹配，区块有效期 `[valid_from,valid_to)`；空结束代表无上界。重叠标签必须先人工协调，否则拒绝。可用人工复核 CSV；来源 URL 与 confidence 保存在标签版本中，未命中的项目为 `Unknown`。报告保存标签、资产配置、数据版本 SHA256。合成运行的离线 `analyze/report` 自动采用该运行的 fixture 标签。

`validate` 只检查运行数据完整性，不读取外部标签文件；迁移数据目录后即使原标签文件不存在，仍可验证。`analyze/report` 继续读取并验证配置或 manifest 中指定的标签 CSV；迁移时应同时保留该文件，或通过 `--config` 指定新的标签路径，缺失或无效标签会报错。

项目主榜以**直接被调用地址**归属，单笔至多一个项目，同项目多个合约的地址先归组再去重；不从内部调用推测代理/路由归属。`contracts.csv` 实际是直接 recipient 排名，未知历史 code 的地址保留 unknown；空 input 不能证明 EOA。已知 `to=null` 单列合约创建并保留 receipt.created address 与 selector。技术/业务分类覆盖不足在报告中显式呈现。

系统识别需要同时满足 `type=0x7e`、`from=0xdeaddeaddeaddeaddeaddeaddeaddeaddead0001`、`to=0x4200000000000000000000000000000000000015`（可配置）。仅 deposit 类型或仅 recipient 不足以排除用户行为。地址依据协议源码：`op-node/rollup/derive/l1_block_info.go` 的 `L1InfoDepositerAddress` 与 `To=L1BlockAddress`；`op-service/predeploys/addresses.go` 的 `L1Block`。这些是协议身份规则，不代表所有系统交易均已穷尽。

- 主活跃指标：本地日内**成功、非系统交易**的发送地址去重；补充指标包含失败交易发送地址。地址不等于人。
- 全窗口、周（周一开始）、月各自去重；部分边缘周/月如实保留。
- 项目 Top1/Top5/Top10 贡献与 HHI 仅纳入已识别项目，以全部非系统交易为分母；Unknown 份额单列。Top1% 发送地址贡献按完整数据中的成功非系统地址数向上取整，以全部成功非系统交易为分母，不从截断榜单推算。
- D1/D7：窗口内首次观察到成功非系统交易的地址为 cohort；在准确 D1/D7 日再次成功则留存。只展示完整成熟的 cohort/目标日；窗口前首次活动未知，不声称生命周期新用户。
- 所有原始 token/wei 使用十进制字符串保存；Python `int` 精确求和，`Decimal` 在足够精度的局部 context 下缩放，不依赖 DuckDB 的 DECIMAL(38) 容量。
- 原生值只含成功非系统交易的顶层 `value`，包含可能的用户 deposit/自转账；不等于经济交易量。没有内部原生币转账指标。
- ERC20 需要配置**显式验证的 fungible 合约地址、decimals**；只统计成功非系统交易中的三-topic Transfer。四-topic NFT 和其他歧义日志单列排除，不混用金额。
- 费用含失败交易；完整费用只有 `gasUsed × effectiveGasPrice` 和 `l1Fee` 都存在才求和。未知不置零；单列已知 L2 小计与完整费用覆盖率。不虚构额外费用组件。
- 不支持的口径：价格、USD、TVL、DEX/桥经济交易量、机器人/真实人数、全面业务分类、trace 归因。

## 质量、规模与验证边界

质量检查验证：manifest 区块范围、分片校验和、连续编号/父哈希、窗口 UTC 边界、交易数量、receipt 一对一哈希/索引关联、日志关联、跨分片重复。质量失败时 `analyze/report` 拒绝产生最终结论；合法空窗口输出零值和空榜单。

采集内存受分片大小约束，500 区块一个范围而非每区块四文件；高负载块仍可能大，可降低 `blocks_per_partition`（新 run ID）。质量逐分片读取；聚合由 DuckDB 扫描 Parquet、512MB 内存预算并允许落盘；大 uint256 的金额按 4096 行流式读取后精确求和。`Store.read_table` 仅是小 fixture 测试便利接口，生产路径不使用它。没有三个月数据/生产负载基准。当前以单进程、串行 I/O 为主，没有多进程并发写入、自动重组回滚、外部对象存储或部署服务。已落盘数据可做内部一致性验证，但离线状态无法独立确认 provider canonical chain；历史重组/节点不一致应新建运行并重新采集。

```bash
.venv/bin/python -m pytest -q
.venv/bin/python -m compileall -q src
```

测试全部离线，覆盖 RPC MockTransport、精度、类型/标签、时间窗、分片恢复、损坏检测、质量拒绝、空结果及端到端报告。开发时先观察缺失功能测试 red，再逐段实现 green；执行证据见上级 `implementation-report.md`。
