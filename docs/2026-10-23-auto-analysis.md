# 自动分析与中文汇报使用说明

已有数据直接升级安装再运行 `enrich`、`report`，完整 nohup 命令见 [README](../README.md)。`report` 自带分析，不需要手动执行 SQL、数据库操作或额外 analyze。补充查询可选，缺少网络/元数据 API 时仍可生成离线报告。

## 一天和三个月采用同样的命令

命令都使用原来的 `--config config.toml --run-id RUN_ID`，窗口由该 run 的 manifest 决定。不要把已采集一天的 run 改成三个月；三个月统计必须有完整三个月数据，新采集按原流程使用独立 run ID。旧版原始 Parquet、配置身份、checkpoint 不变，不需要重采或迁移。

`enrich --top 20` 最多选20个接收地址与20个 Transfer emitter，对历史实际出现区块查询 code/symbol/decimals；缓存成功默认复用，失败可重试，`--refresh` 主动刷新。可选 `OPBNB_CONTRACT_API_URL` 仅提供当前合约 ABI 候选，不要求新 key。公开地址库默认随包加载，自定义标签按有效区块区间优先；缺失/错误的显式 labels 不会静默忽略。Unknown 仍可能很多。

## 先看这些输出

| 文件（位于 dataset/RUN_ID/report） | 如何解释 |
|---|---|
| summary.md | 中文领导摘要：系统/非系统、失败率、成功活跃地址、归属覆盖、集中度、逐资产流转、手续费、成熟留存和缺口 |
| technical.md | 定义、原始字段含义、版本哈希、registry/cache 来源与时效边界、各表分母、财务原始值和 Excel 导入说明 |
| daily.png | 一天为系统/非系统成功/非系统失败三柱构成；多天为带点的非系统交易与成功发送方趋势 |
| projects.png | 有已知归属时显示公开关联贡献，并注明 Unknown 被排除及覆盖率；全未知时改为接收地址排名 |
| daily.csv | 全本地日交易、地址、精确资金及费用，零活动日仍有一行 |
| contracts.csv / recipient_details.csv | 前者是兼容旧名的接收地址×标签分组排名；后者是唯一接收地址 Top100、全地址、类别、证据、置信度和首末 code 状态 |
| projects.csv / senders.csv | 公开项目/协议/资产归组；非系统发送方 Top100；地址数不等于人数，跨项目地址不相加 |
| weekly.csv / monthly.csv | 各周期独立去重；observed_days 包含零活动日，expected_days 为完整周期，is_partial 提示部分周期 |
| retention.csv | 窗口内首次成功活动 cohort 的成熟 D1/D7；没有成熟行是 N/A，不是流失率100% |
| transaction_types.csv / methods.csv | 互斥技术行为；接收地址/selector Top100 与候选签名及来源；方法份额分母为全部非系统交易 |
| events.csv | 成功非系统交易中 emitter/topic0 Top100 日志、组内交易去重、签名候选；份额分母为合格日志条数，不能和交易份额混用 |
| hourly.csv / sender_breakdown.csv | 有活动小时的 UTC/本地分布；Top20 发送方的目标/selector/小时分布，各维度最多20项 |
| fee_groups.csv | 系统、用户deposit、常规非系统分别列 L1/L2/完整费用覆盖及精确小计；包含失败交易费用 |
| recipient_concentration.csv / sender_concentration.csv | Top1/5/10/20，以全部非系统交易（含失败）为分母；创建交易不在接收地址排名但仍留在分母 |
| token_metadata.csv / unresolved.csv | Top100 Transfer emitter 的可信程度、精度、符号与金额；Top100 接收地址内按影响排序的未知归属、原因和查询状态 |

所有18张 CSV 空表仍有表头，使用 UTF-8 BOM 方便 Excel。自由文本以 `= + - @` 或危险控制符开头时会加单引号以避免当作公式；金额、计数和原始 wei 不加单引号、不转浮点。Excel导入长整数时必须把 wei/raw_amount/scaled_amount、地址列设为文本，否则 Excel 自己可能只保留15位有效数字。

## 结论的可信范围

“项目/协议/资产归属覆盖率（公开资料匹配）”是直接接收地址关联覆盖，不是 dApp 用户覆盖。类别包含项目、协议预部署与官方桥接资产。project_published=项目方公开关联；bnb_official=BNB官方资料；都不证明逐笔业务、历史控制权或代理实现所有权。source_url 等字段只抽样地址首个出现区块的标签，跨区间变化见 technical.md/CSV。

历史 code 仅说明首/末抽样区块，当前 ABI 和合约名不补推历史所有权。symbol/decimals 成功返回仍是 observed_unverified；只有显式或可信官方资产 verified_fungible 才计入 ERC20金额。三topic Transfer 仅是流转证据，四topic NFT与歧义日志排除，不把事件名当作业务证明。

主活跃是成功非系统发送地址；含失败地址另作补充。单日 DAU 与多日全窗口去重分别命名；多日不能用 DAU 相加作为 WAU/MAU。部分周/月不按完整月环比，不成熟 cohort 不记0，不从一天外推三个月。系统识别需 type+depositor+recipient 同时匹配，用户deposit仍是非系统。

BNB 只含成功非系统顶层 value，无内部转账；已验证代币逐资产独立，不合计成总 token 数或经济成交额。完整手续费必须同时观测 L2 和 L1，缺失不是零，分组覆盖分母为各组全部交易，包含失败支出。未提供价格/USD/TVL/DEX经济量、桥最终完成量、机器人或真实人数判断。质量校验是本地数据一致性，不能独立认证节点链真实性；合成demo不构成主网结论。
