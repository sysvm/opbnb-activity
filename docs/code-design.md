# opBNB Activity Analysis Code Design and Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. 本文件是待实施代码设计；文中的目录、接口和 CLI 尚未实现。

**Goal:** 建立一个可离线开发测试、可断点采集三个月 opBNB 数据、可重复生成统计结果的独立分析工具。

**Architecture:** CLI 调度 RPC 能力探测、区块采集、回执采集、事件解析、项目归属、统计与报告导出。数据按时间和区块范围分区保存为 Parquet，DuckDB 执行聚合，SQLite 保存采集状态；节点地址只作为接入配置，不阻塞本地解析、统计、导出及故障恢复逻辑的开发。

**Tech Stack:** Python 3.11+、asyncio/httpx、PyArrow、DuckDB、SQLite、pytest、Matplotlib。使用标准库 tomllib 读取 TOML 配置。依赖的具体版本在初始化项目时锁定并验证，不在本规划中声称已经安装或兼容测试。

## Global Constraints

- 新建独立的 `opbnb-activity/` 工具目录，不向 opBNB 或 op-geth 的协议代码加入业务分析逻辑。
- 开发位置为当前任务产物目录内的 `opbnb-activity/`；下文所有项目文件相对此目录。
- 实际运行位置为公司服务器，目录由运行配置指定，代码不硬编码本地开发机路径。通过 nohup 启动长时间采集，日志重定向到文件，不引入额外部署或服务管理组件。
- 首先交付离线可测试的软件，再接入真实 API 验证历史覆盖、RPC 扩展方法、性能和费用字段。
- 真实 URL 只用于集成验证和采集，不作为代码设计、单元测试或离线开发的前置条件。
- 窗口默认北京时间 2026-07-22 00:00 至 2026-10-22 00:00，左闭右开，完整覆盖 92 天；CLI 必须支持显式日期和时区。
- 原始时间保存 UTC，日统计时区写进运行元信息；与外部来源比较必须对齐时间边界。
- 金额以十进制字符串保存原始整数，避免 float 和 DuckDB 38 位 DECIMAL 无法容纳完整 uint256 的问题。估值与展示采用 Decimal，聚合需检验是否超出目标数值范围，禁止静默截断。
- 标签、运行配置和指标版本可追溯；结果可离线重算，不需重复请求节点。
- trace 为可选扩展，不做全量 trace，不把原生币顶层 value 宣称为完整原生币转移金额。
- 开发测试不访问真实节点、不提交 Git commit、不创建分支。

## 1. 代码与数据目录

```text
opbnb-activity/
  pyproject.toml
  config.example.toml
  README.md
  src/opbnb_activity/
    cli.py
    config.py
    models.py
    rpc.py
    probe.py
    block_range.py
    collect.py
    checkpoint.py
    storage.py
    decode.py
    labels.py
    metrics.py
    quality.py
    report.py
    sql/
      daily.sql
      contracts.sql
      projects.sql
      senders.sql
      retention.sql
  tests/
    fixtures/
      blocks.json
      receipts.json
      rpc_responses.json
      project_labels.csv
    test_rpc.py
    test_block_range.py
    test_collection_recovery.py
    test_decode.py
    test_metrics.py
    test_quality.py
    test_report.py
  labels/
    projects.csv
    assets.csv
  data/
    runs/
    blocks/
    transactions/
    receipts/
    logs/
    state.sqlite
  output/
    tables/
    charts/
    report.md
```

`data/` 和 `output/` 不纳入源码版本管理。RPC 凭据和真实连接配置不写入示例配置或运行日志。

## 2. 模块职责与接口

| 文件 | 核心接口 | 责任与边界 |
| --- | --- | --- |
| `config.py` | `load_config(path: Path) -> AnalysisConfig` | 读取窗口、时区、链 ID、批量和速率上限；RPC URL 从 `OPBNB_RPC_URL` 读取；仅联网命令要求 URL |
| `models.py` | `AnalysisConfig`, `RpcCapabilities`, `BlockRange`, `CollectionTask`, `QualityResult` | 定义输入输出结构，地址和哈希标准化，不混用 UTC 与本地日期 |
| `rpc.py` | `RpcClient.call(method: str, params: list[object]) -> object`; `RpcClient.batch(calls: list[RpcCall]) -> list[RpcResult]` | ID 映射、并发限制、限流、超时、退避、响应校验；JSON-RPC batch 不假定按顺序返回 |
| `probe.py` | `probe_node(client: RpcClient) -> RpcCapabilities` | 验证 chain ID、历史范围、batch、receipt、logs、trace 能力，并记录探测证据 |
| `block_range.py` | `resolve_range(client: RpcClient, start: datetime, end: datetime) -> BlockRange` | 按区块时间二分查找第一个大于等于起止时间的区块，输出左闭右开区块范围 |
| `collect.py` | `collect_range(client: RpcClient, block_range: BlockRange, store: DatasetStore, checkpoints: CheckpointStore) -> CollectionSummary` | 分块读取完整交易及回执，支持 resume，receipt 缺失不能标记完成 |
| `checkpoint.py` | `CheckpointStore.claim_task() -> CollectionTask | None`; `complete_task(task: CollectionTask, output: PartitionManifest) -> None` | SQLite 事务管理 pending/running/done/failed，恢复崩溃遗留任务，记录数据路径和校验值 |
| `storage.py` | `DatasetStore.write_partition(dataset: str, rows: list[dict], partition_key: str) -> PartitionManifest` | Parquet 临时文件写入、校验及原子重命名；同一任务重跑替换其确定分区，避免重复 append |
| `decode.py` | `decode_receipt(transaction: dict, receipt: dict, labels: LabelRegistry) -> DecodedTransaction` | 解析状态、事件、费用、协议系统行为、业务标签；不能确认的保持 unknown |
| `labels.py` | `load_labels(path: Path) -> LabelRegistry` | 合约→项目→业务类别、证据、可信度和有效区块范围；代理与实现地址分列 |
| `metrics.py` | `build_metrics(dataset_dir: Path, labels: LabelRegistry, config: AnalysisConfig, output_dir: Path) -> MetricsSummary` | DuckDB 聚合交易和地址，Decimal 处理超大金额；重新贴标签后可离线重算 |
| `quality.py` | `validate_dataset(dataset_dir: Path, block_range: BlockRange) -> QualityResult` | 连续性、交易和回执对应、去重、统计恒等式、窗口覆盖及缺失检测 |
| `report.py` | `render_report(tables_dir: Path, output_dir: Path) -> ReportManifest` | 生成 CSV、PNG 图表和 Markdown 报告；缺失值显示不可用，不替换为 0 |

`RpcCall` 包含 `id`、`method`、`params`；`RpcResult` 包含 `id`、`result` 或 `error`。批量部分成功时仅重试失败项；方法明确不支持时更换路径，不无限重试。任何接口不得在异常中暴露完整含敏感参数 URL。

## 3. 数据表设计

| 表 | 关键字段与逻辑键 |
| --- | --- |
| `blocks` | `number`, `hash`, `parent_hash`, `timestamp_utc`, `transaction_count`；键为链 ID 与区块号 |
| `transactions` | `hash`, `block_number`, `block_hash`, `transaction_index`, `from_address`, `to_address`, `type`, `value_raw`, `input`, `selector`；键为链 ID 与交易哈希 |
| `receipts` | `transaction_hash`, `block_hash`, `status`, `gas_used`, `effective_gas_price_raw`, `l1_fee_raw`, `fee_total_raw`, `contract_address`；键为链 ID 与交易哈希 |
| `logs` | `transaction_hash`, `block_hash`, `log_index`, `address`, `topics`, `data`；键为区块哈希、交易哈希与日志序号 |
| `project_labels` | `address`, `project_id`, `project_name`, `category`, `role`, `valid_from_block`, `valid_to_block`, `source_url`, `confidence` |
| `asset_labels` | `address`, `symbol`, `decimals`, `price_source`, `valid_from_block`, `valid_to_block` |
| `collection_tasks` | `task_id`, `start_block`, `end_block_exclusive`, `stage`, `status`, `attempts`, `output_path`, `checksum`, `error_code` |
| `run_manifest` | `run_id`, `chain_id`, `start_utc`, `end_utc`, `report_timezone`, `config_hash`, `labels_hash`, `dataset_version`, `quality_status` |

表按日期及区块分片写入，避免一天生成一个超大文件。保留 receipt 内全部事件的结构化记录，允许之后补标签与解码。完整原始 JSON 镜像默认不开启，避免重复占用存储；关键抽查样例单独保存。

费用字段缺失时保存 null 和覆盖状态，不当成零。L1、L2 费用的字段含义通过节点回执及代表性交易核对，不同时累加 total 与组成项。

## 4. 采集与性能策略

1. `probe` 验证基础 RPC、历史区块、扩展接口；先确认链 ID，再扩展请求。
2. `resolve_range` 固定窗口的区块范围，记录边界证据。
3. `collect` 按任务分区请求完整区块；优先 `eth_getBlockReceipts`，不支持则 batch 获取逐笔 receipt，再不支持则逐笔请求。
4. 将交易、回执和事件写为分区文件，确认文件和元信息一致后标记任务完成；启动恢复时核对已完成任务的文件与校验值。
5. 在临时文件写完、文件改名后、checkpoint 更新前等边界发生中断时，恢复逻辑分别清理临时文件或重新核对最终分区，禁止漏采或重复计数。
6. 起步单并发、每秒最多一次请求；在显式配置的 QPS、并发与 batch 上限内测 1、2、4 并发，遇到 429、超时或延迟恶化即退避。没有显式上限时维持保守配置，不自动无限加压。
7. batch 以元素数计入客户端速率预算；服务端计费方式另行记录，不能宣称一次 HTTP 等于一次节点请求费用。
8. 单日试采后报告实际请求数、吞吐量、字节量和存储量，再给三个月估算，不预先承诺采集时间。

## 5. 统计定义在代码中的落实

- 交易技术主分类互斥：系统、创建、已确认的普通原生币转账、合约交互、未知；业务标签独立，不能将多标签占比强制相加为 100%。
- 系统交易根据协议角色和类型组合识别，不能将所有 deposit 类型都当成系统交易排除。
- 日活主指标为成功非系统交易发送地址去重；同时输出全部非系统发送地址数。周/月活使用全周期集合去重。
- 主要项目榜基于直接调用归属，一笔交易最多归一个项目或 unknown；路由下游覆盖另表统计。
- 合约归并到项目后重新去重地址，不累加合约独立地址数；项目间地址覆盖允许重叠并披露。
- 高频发送者输出交易数、占比、主要目标、活跃天数和小时分布，中继和合约账户不直接解释为真实人数。
- 转账额、DEX 成交额、衍生品名义额、桥流量、TVL 和手续费分别展示，不能求和为所谓全链交易金额。
- L2 提现发起和 L1 最终完成分开；未接入 L1 数据源时不声称验证跨链最终到账。
- 金额聚合包含精度、价格来源及时间，未知代币不强行美元化；失败交易 value 不计入成功转移额，失败交易已支付手续费仍纳入相应费用口径。
- D1/D7 留存只计算已成熟 cohort，窗口内首次出现不等于全链新用户；报告说明 cohort 定义与观察范围。
- 边界月份不与完整月份直接做月环比，主要变化采用连续 7 天和连续 30 天对照。

## 6. 分阶段开发与验收

### 阶段 A：离线采集基础

**Files:** `config.py`、`models.py`、`rpc.py`、`block_range.py`、`checkpoint.py`、`storage.py`、`collect.py` 及对应测试。

- [ ] 用固定的两块数据和本地测试 HTTP 服务验证乱序 batch、部分失败、429、超时、方法不支持、receipt 缺失。
- [ ] 每项行为先写失败测试，再完成实现，验证失败来自该行为尚未实现，而不是测试数据或依赖错误。
- [ ] 验证起止时间恰好等于区块时间、空时间范围、北京时间跨日和非均匀区块间隔的边界定位。
- [ ] 模拟分区落盘前后和 checkpoint 提交前的中断，恢复后交易集合必须等于完整采集的交易集合。
- [ ] 验证日志和错误消息不包含测试 URL 中植入的凭据标记。

**验收:** 不依赖真实 URL；两次采集和中断重跑结果相同；缺失 receipt 阻止任务完成。

### 阶段 B：解析和统计

**Files:** `decode.py`、`labels.py`、`metrics.py`、`quality.py`、`sql/` 及对应测试。

- [ ] fixture 覆盖系统交易、用户跨链 deposit、成功转账、失败交易、创建、授权、代币 Transfer、未知 selector、代理项目和跨项目地址。
- [ ] 用超过 38 位的原始整数和具有不同精度的代币验证金额不丢精度，原始值可以往返恢复。
- [ ] fixture 中一个地址跨两天参与同项目两个合约，断言项目去重地址仍为 1、月活仍为 1；另设跨项目参与验证覆盖数不误当全网去重数。
- [ ] fixture 中一个成功转账和一个失败转账，断言转移额只含前者，费用按已支付口径含两者。
- [ ] fixture 包含未成熟 D7 cohort、无 L1 费用字段和未知价格，断言分别从留存分母排除、输出费用缺失和原币金额，而不是补零。
- [ ] 删除一个区块、复制一笔交易、替换一个 receipt 区块哈希，分别验证完整性检查能定位错误。

**验收:** fixture 的预期总数、金额和去重集合由手工计算校对；不只检验代码正常运行。

### 阶段 C：报告与离线闭环

**Files:** `cli.py`、`report.py`、`README.md`、`config.example.toml`、`tests/test_report.py`。

- [ ] 提供 `probe`、`collect`、`validate`、`analyze`、`report` 五个命令，离线后三者不要求 RPC URL。
- [ ] 从 fixture 生成逐日表、合约榜、项目榜、发送方榜、留存/集中度表和 Markdown 报告。
- [ ] 图表包括交易/地址趋势、业务构成、项目贡献、发送者集中度及资金活动；资金和归属缺失时明确标注覆盖范围。
- [ ] 报告元信息包含窗口、时区、指标版本、标签版本和数据质量状态；不完整数据报告不能标记最终完成。
- [ ] 一次测试运行覆盖离线采集、落盘、解析、聚合和报告，检查 fixture 预期结论与输出一致。

**验收:** 无 URL 的环境可生成一份注明为测试数据的完整示例报告；README 清楚区分示例结果和真实主网结果。

### 阶段 D：真实节点接入与一天试采

**依赖:** 阶段 A～C 完成，用户提供 endpoint。

- [ ] 配置 `OPBNB_RPC_URL`，运行 `probe`，验证 chain ID、历史覆盖与扩展方法。
- [ ] 从 1 QPS 单并发开始，按明确上限验证安全吞吐量。
- [ ] 采集一个完整自然日，运行 `validate` 和 `analyze`，给出真实数据样例、缺口及三个月资源估算。
- [ ] 比对少量代表性交易和同口径浏览器统计，修正经证据确认的适配问题。

**验收:** 数据无未解释的完整性缺口；确认费用和系统交易字段语义；只有这一步才需要真实 URL。

### 阶段 E：三个月分析和汇报

- [ ] 采集完整窗口并逐分区验证，输出进度和可恢复状态。
- [ ] 核验头部 30～50 合约归属，按覆盖率递补，记录证据和未知比例。
- [ ] 生成三个月所有核心指标、主要波动的项目分解和资金活动覆盖说明。
- [ ] 完成一页领导摘要、6～8 页报告提纲、图表和可复算表格。

**验收:** 92 天全部覆盖或显著披露缺口；结论有证据；三个月统计不能被一天试采结果替代。

## 7. 预定命令形态

以下命令是拟实现 CLI，不表示当前可执行。

```bash
uv run pytest
uv run opbnb-activity probe --config config.toml
uv run opbnb-activity collect --config config.toml --start 2026-10-21 --end 2026-10-22 --timezone Asia/Shanghai --resume
uv run opbnb-activity validate --config config.toml
uv run opbnb-activity analyze --config config.toml
uv run opbnb-activity report --config config.toml
uv run opbnb-activity collect --config config.toml --start 2026-07-22 --end 2026-10-22 --timezone Asia/Shanghai --resume
```

`collect` 每次运行保存独立窗口 manifest；`validate`、`analyze` 和 `report` 使用配置中明确指定的 run ID，不能混用一天试采和三个月运行的数据边界。

服务器上在项目目录内准备 Python、uv、依赖及运行配置后，通过以下拟实现命令后台采集。RPC URL 从运行环境或受限的本地配置读取，不直接出现在命令行参数中。

```bash
nohup uv run opbnb-activity collect --config config.toml --start 2026-07-22 --end 2026-10-22 --timezone Asia/Shanghai --resume > collect.log 2>&1 &
```

查看进度使用 `tail -f collect.log`。进度日志定期记录已完成区块、失败任务数、处理速率及最近成功分区，不逐笔打印交易。进程退出后使用同一配置及 `--resume` 继续；采集完成后执行 `validate`、`analyze`、`report`。

## 8. 与上一份分析计划的关系

上一份文件定义分析目标、指标、证据和汇报验收；本文件补齐软件模块、数据模型、性能与恢复设计、离线测试和开发顺序。无需等 URL 才能开展阶段 A～C；真实节点的不确定性集中在阶段 D 验证，不包装成已经确认的接口能力。
