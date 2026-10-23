# opBNB Activity：可审计的离线分析工具

这是独立 Python 工具，未修改 opBNB / op-geth 协议代码。已实现配置与节点能力探测、可恢复的分片采集与质量检查、离线行为/资金/费用指标、随包公开来源地址库、限量补充查询和中文汇报。当前版本验证使用合成数据和 `httpx.MockTransport`；本次升级未重新验证用户的真实一天原始数据，没有主网活跃度结论。大规模三个月采集的耗时、磁盘与节点吞吐仍需实测。

## 简单升级后重新报告（已有采集数据）

把新版源码同步到服务器原项目目录，在原虚拟环境安装更新；新增 `pycryptodome` 依赖，使用正常依赖安装，不要加 `--no-deps`。原来的 `config.toml`、数据目录、run ID 和已设置的 `OPBNB_RPC_URL` 保留；**无需重复 collect、rescan 或先手动 analyze，report 内部自动分析**。等待该 run 原采集完成后，在原项目目录执行：

```bash
.venv/bin/python -m pip install -e .
mkdir -p logs
nohup sh -c '.venv/bin/opbnb-activity enrich --config config.toml --run-id mainnet-20261021 --top 20 && .venv/bin/opbnb-activity report --config config.toml --run-id mainnet-20261021' >logs/enrich-report.log 2>&1 &
tail -f logs/enrich-report.log
```

`enrich` 只对排名靠前的接收地址及 Transfer emitter 各最多20个做有限查询：首/末出现区块 code、代币 symbol/decimals，复用本 run 缓存；失败保留状态，成功缓存默认不重查，需主动更新才用 `--refresh`。它不会改原始 Parquet 或 checkpoint，也不保证识别全部 Unknown。日志先显示补充进度，随后显示报告分析、表格、摘要和图表阶段；全量离线分析本身仍需时间。

补充查询可选：只需重新生成离线报告时，运行下面一条即可，无需节点连接；默认仍使用随包地址库和已存在的有效缓存。

```bash
mkdir -p logs
nohup .venv/bin/opbnb-activity report --config config.toml --run-id mainnet-20261021 >logs/report.log 2>&1 &
```

结果在 `config.toml` 的 `dataset/mainnet-20261021/report/`：先看中文 `summary.md`，完整定义/来源哈希见 `technical.md`，18张 CSV 已经聚合好可供 Excel 使用。长整数 wei/raw_amount 请用 Excel“数据→从文本/CSV”按**文本列**导入，避免 Excel 自动舍入；CSV 本身不改金融原值，自由文本的公式前缀已转义。输出文件地图及单日/三个月口径见 [自动分析使用说明](docs/2026-10-23-auto-analysis.md)。

进阶可选：仅当已有兼容 `getsourcecode`（`action`、`address` 参数）元数据服务 URL 时设置 `OPBNB_CONTRACT_API_URL`，命令会限量解析当前 ABI 候选。工具没有内置无需 key 的合约 API 承诺，也不要求申请/购买新 key；未设置时记录 `unavailable_no_api` 并保留 Unknown。不要将含凭据 URL 写进报告或版本控制。合约名/ABI、symbol/decimals 不单独证明项目或 ERC20 身份。

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
- `data/YYYY-MM-DD/START-END/`：每范围一组 `blocks`、`transactions`、`receipts`、`logs` Parquet，外加 `_partition.json` SHA256/行数。例如 `data/2026-10-19/197248677-197249177/`，区块号无补零。
- `checkpoint.sqlite`：SQLite WAL/FULL 模式的完成分片记录；分片目录原子落盘后再记 checkpoint。中间崩溃可校验并恢复已写分片。
- `quality.json`、`analysis.json`、中文 `report/summary.md`、`report/technical.md`、18张 CSV 表、`daily.png`、`projects.png`。

目录日期取分片首区块的 UTC 日期，END 为不含的结束区块。仅新写入的分片使用简洁路径；已有 `data/date=YYYY-MM-DD/blocks=START-END/` 目录保持原位置，继续支持读取、恢复和校验，同一 run 可混用两种布局；损坏的已有分片在原路径重写。无需迁移，也没有迁移命令。

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
.venv/bin/opbnb-activity report --config config.toml --run-id full-2026q3
```

需要停止时对该后台进程发 SIGTERM，再用完全相同的命令恢复。尚未完成的分片会重新采集，最多损失当前分片的工作，不会把半分片视作完成。已完成分片先复核哈希。损坏分片保留为隐藏 `.replaced-*` 目录后重写；可在确认无恢复需求后手工清理。单个 run 同时仅运行一个采集进程。

### 实时进度日志与升级已有任务

旧版采集器只在结束时输出 JSON 或错误，没有中间进度日志。因此看到 Parquet 分片持续增长、`nohup` 日志仍为空，是旧代码缺少日志，不能仅凭日志为空判断任务卡住。新版 CLI 使用标准库日志，UTC 时间戳、INFO 级别，输出到 **stderr**，每条立即 flush；上面的 `2>&1` 会将这些日志实时合并到文件，最终 stdout JSON 格式保持不变。

日志包括运行 ID、动作、UTC 窗口、QPS/批大小/分片大小/并发数；探测的 chain check、时间窗区块范围解析、能力样本与可选 trace 各阶段；采集开始/结束、每个提交/跳过/恢复分片的 `[start,end)` 范围，以及最终完整性校验开始/结束。RPC 重试记录方法、失败尝试序号、安全原因/HTTP 状态及退避秒数，不打印节点 URL、认证、响应正文或原始异常。QPS 限速等待仍会另行发生。错误保留安全 RPC 分类以及明确列入白名单的固定本地诊断（例如链 ID、身份不匹配、缺少环境变量）；其余仅打印异常类型，避免节点数据进入日志。

例如以下为离线合成 fixture 的进度字段示例（非主网吞吐数据）：

```text
2026-10-22T00:00:00 UTC INFO Collection partition committed: partition=[102,104) processed_blocks=4/6 committed_blocks=4/6 processed_percent=66.7 committed_percent=66.7 new_blocks=2 new_transactions=2 elapsed_seconds=4.0 rate_blocks_per_second=0.500 eta_seconds=4.0
```

`processed_blocks` 包含本次完成的区块和已复核的已有分片；`committed_blocks` 只含已经提交或恢复的完整分片。`new_blocks`、`new_transactions` 只累计本次新处理的数据，不重复累计跳过的数据。速率为 `new_blocks / 本次采集已用秒数`，不把历史完成区块算入速率；ETA 用尚未处理的区块数除以该实测速率，没有可用速率时显示 `unknown`，全部完成时为 `0.0`。ETA 是当前观测下的估计，包含未完成分片的处理进度，不包含后续完整性校验/分析耗时；尚未遍历复核的已有分片和未来节点负载变化都会影响估计。

未完成分片内约每 30 秒在**区块完成边界**报告一次，不逐交易刷屏。没有后台心跳线程；单次阻塞 RPC、重试等待、大区块处理或磁盘提交会推迟下一条日志，因此它不是“每 30 秒保证输出”的健康检查。

新增进度字段 `concurrency` 为当前配置，`scheduled_blocks` 是当前尚未被协调线程消费的已提交区块任务数，`buffered_blocks` 为其中已经完成的任务数，两者不包括已写入分片缓冲的数据。RPC 字段 `rpc_elements`、`http_attempts`、`retries`、`http_429`、`in_flight`、`peak_in_flight`、`rpc_elements_per_second` 从共享客户端创建时累计，包含前面的探测与重试；速率按客户端存活时间计算。它们与只统计本次新处理区块的采集速率具有不同口径。完整性校验单独记录开始/结束，采集 ETA 不包含它。

已经运行的旧进程不会自动加载本次代码修改。服务器升级时按以下顺序操作：

1. 将完整新版 `src/`（包含新增模块及 `fixtures/`）、`pyproject.toml`、`requirements-tested-py312.txt` 和使用文档同步到原安装项目目录；保留服务器的 `config.toml`、数据和 checkpoint。不要只更新旧版日志相关的四个模块，当前 CLI 还依赖新增的分析/补充模块。已有采集完成只需重新报告时，直接用本文开头“简单升级后重新报告”流程。
2. 用服务器上的进程信息核对旧任务的完整命令和 PID；**只对确认属于该 run 的旧 PID** 执行 `kill -TERM "$OLD_PID"`。不要使用宽泛的 `pkill`，不要删除数据。
3. 确认旧 PID 已退出，再在原项目/虚拟环境中执行 `.venv/bin/python -m pip install -e .`，确保本版本新增依赖也已安装。
4. 使用原来完全相同的 config、run ID、窗口和数据目录恢复，重用原来的 `OPBNB_RPC_URL` 环境变量；日志使用追加重定向保留旧记录：

```bash
nohup .venv/bin/opbnb-activity collect --config config.toml --run-id full-2026q3 >>logs/collect.log 2>&1 &
tail -f logs/collect.log
```

必须等旧任务退出后再启动恢复任务，不能同时对同一 run 启动第二个采集器。已提交分片经过校验后跳过；未完成分片按既有规则重采。升级不改变 manifest 身份、checkpoint、已有分片目录或指标数据格式，新分片使用上文的简洁路径。

推荐先另设 run ID、用 `--start` / `--end` 做一天样本，再评估全窗口。默认 **1 个逻辑 RPC 元素/秒、concurrency=1、batch_size=1、每分片 500 区块**。`concurrency` 可显式设为 1–128 的整数，控制共享连接池与同时在途的 HTTP 请求数；所有工作线程共享一个 QPS 和 HTTP 429 冷却预算，不会因并发数倍增 QPS。批大小可显式设为 2–100，主要用于不支持整块 receipts 时的逐交易回退；整块 receipts 快速路径不使用该批大小。批请求按元素在发送前扣预算，批内元素仍会同时到达；空闲时间不会积攒额外发送额度。工具不会自动加压。三个月数据在 1 QPS 下可能耗时很长；没有任何“探测通过即可达到某吞吐”的承诺。

需要提高采集速度时，见 [有界并发采集与调参](docs/2026-10-23-high-qps.md)。例如可明确试用 `qps=500`、`concurrency=32`、`batch_size=20`、`blocks_per_partition=500`，根据实测速率、429、超时和内存逐步调整。QPS 可以配置得更高，但这组参数不代表节点已确认容量。QPS/并发数属于运行参数，旧 manifest 缺少并发字段时默认为 1；停掉已确认的旧 PID 并等待退出后，可在保持原 run ID、窗口、分片大小与数据目录的情况下调整 QPS/并发数恢复，无需迁移数据。

离线复现实验：`PYTHONPATH=src python tools/benchmark_collection.py --blocks 120 --latency-ms 10 --concurrency 16 --qps 10000`。它只使用合成 fixture 和 `httpx.MockTransport` 模拟延迟，先验证串行/并发规范化内容及质量计数一致，再输出耗时、区块速率、实际 HTTP 次数与峰值在途数；不会访问生产节点，不承诺实际网络加速比例。

`probe` 首先检查 chain ID 204，再做时间戳下界二分和有界样本。探测覆盖批处理、block receipts、单交易 receipts、logs、可选能力意义上的 trace。二分使用 `[start,end)`，支持相同/不规则时间戳；不足历史或尚未到结束时间会拒绝采集，不静默截断。archive 不等于 tracing：仅 `-32601` 标为方法 unsupported；pruned/transient 独立记录。Header 覆盖不保证全窗口所有 receipt/state 都可用；采集遇到缺失会阻止该分片完成。trace 仅探测，不参加经济指标。

RPC 校验批响应 ID，拒绝遗漏/重复/意外 ID；仅重试 transient 的失败元素。HTTP 429/5xx 有界重试并遵循 Retry-After；等待超过 300 秒预算则停止，不提前重试。传输异常和节点错误不回显 URL、认证或原始错误文本。重定向关闭，不自动使用环境代理。未验证的/非标准 verified-total-fee 字段不会直接作为总费用。

## 标签、口径与财务精度

无需自行填写标签即可使用随包的 chain ID 204 公开地址/官方资产快照；自定义标签可选，CSV 必须有列：

```text
address,project,category,role,source_url,confidence,valid_from,valid_to
```

地址按小写匹配，区块有效期 `[valid_from,valid_to)`；空结束代表无上界。自定义标签优先覆盖对应区间，未覆盖区间才用随包地址库；自定义标签重叠必须先协调，否则拒绝。未命中保留 `Unknown`。随包内容包含协议地址、公开项目部署和官方桥接资产，保留 category；project_published 表示项目方公开关联，bnb_official 表示 BNB官方资料，均不证明逐笔业务或历史控制权。报告将标签、资产、数据、registry和cache版本/哈希写进技术附录。合成运行跳过真实地址库，自动采用该运行的 fixture 标签。

`validate` 只检查运行数据完整性，不读取外部标签文件；迁移数据目录后即使原标签文件不存在，仍可验证。`analyze/report` 继续读取并验证配置或 manifest 中指定的标签 CSV；迁移时应同时保留该文件，或通过 `--config` 指定新的标签路径，缺失或无效标签会报错。

项目/协议/资产主榜以**直接接收地址**归属，单笔至多一个关联，同组多个地址先归组再去重；不从内部调用推测代理/路由归属。`contracts.csv` 实际是接收地址按标签区间分组排名；唯一接收地址排名和来源看 `recipient_details.csv`，未知历史 code 保留 unknown，空 input 不能证明 EOA。`to=null` 单列创建交易，结果地址保留在 receipt.contract_address。技术分类自动区分系统、用户deposit、创建、空input、selector、短calldata和unknown；selector或事件签名仅提供候选及来源，全面业务意图仍不能自动确认。

系统识别需要同时满足 `type=0x7e`、`from=0xdeaddeaddeaddeaddeaddeaddeaddeaddead0001`、`to=0x4200000000000000000000000000000000000015`（可配置）。仅 deposit 类型或仅 recipient 不足以排除用户行为。地址依据协议源码：`op-node/rollup/derive/l1_block_info.go` 的 `L1InfoDepositerAddress` 与 `To=L1BlockAddress`；`op-service/predeploys/addresses.go` 的 `L1Block`。这些是协议身份规则，不代表所有系统交易均已穷尽。

- 主活跃指标：本地日内**成功、非系统交易**的发送地址去重；补充指标包含失败交易发送地址。地址不等于人。
- 全窗口、周（周一开始）、月各自去重；部分边缘周/月附 observed_days/expected_days/is_partial，零活动日计入观察天数，未成熟留存为 N/A。
- 项目 Top1/Top5/Top10 贡献与 HHI 仅纳入已识别项目，以全部非系统交易为分母；Unknown 份额单列。Top1% 发送地址贡献按完整数据中的成功非系统地址数向上取整，以全部成功非系统交易为分母，不从截断榜单推算。
- D1/D7：窗口内首次观察到成功非系统交易的地址为 cohort；在准确 D1/D7 日再次成功则留存。只展示完整成熟的 cohort/目标日；窗口前首次活动未知，不声称生命周期新用户。
- 所有原始 token/wei 使用十进制字符串保存；Python `int` 精确求和，`Decimal` 在足够精度的局部 context 下缩放，不依赖 DuckDB 的 DECIMAL(38) 容量。
- 原生值只含成功非系统交易的顶层 `value`，包含可能的用户 deposit/自转账；不等于经济交易量。没有内部原生币转账指标。
- ERC20 金额需要**显式验证的 fungible 地址/decimals 或随包可信官方资产匹配**；限量 eth_call 的 symbol/decimals 仅为 observed_unverified，不自动升级为已验证。只统计成功非系统三-topic Transfer；四-topic NFT 和歧义日志排除，各资产独立，不混用金额。
- 费用含失败交易；完整费用只有 `gasUsed × effectiveGasPrice` 和 `l1Fee` 都存在才求和。未知不置零；单列已知 L2 小计与完整费用覆盖率。不虚构额外费用组件。
- 不支持的口径：价格、USD、TVL、DEX/桥经济交易量、机器人/真实人数、全面业务分类、trace 归因。

## 质量、规模与验证边界

质量检查验证：manifest 区块范围、分片校验和、连续编号/父哈希、窗口 UTC 边界、交易数量、receipt 一对一哈希/索引关联、日志关联、跨分片重复。质量失败时 `analyze/report` 拒绝产生最终结论；合法空窗口输出零值和空榜单。

采集每次只处理一个分片；并发读取的运行中及已完成未消费任务合计不超过 `2 * concurrency` 区块，另加当前分片规范化缓冲，500 区块一个范围而非每区块四文件。这是区块数边界，不是严格的字节/RSS 上限；高负载块仍可能大，可降低并发数或降低 `blocks_per_partition`（后者需要新 run ID）。工作线程只取数据，协调线程按区块顺序规范化并写入 Parquet/SQLite；错误会取消排队任务，等待在途工作退出，当前未提交分片在恢复时重采。质量逐分片读取；聚合由 DuckDB 扫描 Parquet、512MB 内存预算并允许落盘；大 uint256 的金额按 4096 行流式读取后精确求和。`Store.read_table` 仅是小 fixture 测试便利接口，生产路径不使用它。没有三个月数据/生产负载基准。当前为单进程有界并发读取和单写入线程，没有多进程并发写入、自动重组回滚、外部对象存储或部署服务。已落盘数据可做内部一致性验证，但离线状态无法独立确认 provider canonical chain；历史重组/节点不一致应新建运行并重新采集。

```bash
.venv/bin/python -m pytest -q
.venv/bin/python -m compileall -q src
```

测试全部离线，覆盖 RPC MockTransport、精度、类型/标签、时间窗、分片恢复、损坏检测、质量拒绝、空结果及端到端报告。开发时先观察缺失功能测试 red，再逐段实现 green；执行证据见上级 `implementation-report.md`。
