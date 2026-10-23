# 自动分析与汇报增强实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. No commits or branches are authorized.

**Goal:** 复用原始 Parquet，自动解释系统/非系统活动、交易行为、活跃地址及资金口径；通过有限补充查询和有来源的地址库识别项目，输出中文领导摘要。

**Architecture:** 离线聚合不依赖网络；独立 `enrich` 命令进行限量历史状态/合约元数据查询并缓存；`report` 自动消费可信地址库及本 run 补充结果。不能识别的地址保留 Unknown，生成按影响排序的待确认清单，不把名称猜测视为项目证据。

**Tech Stack:** Python 3.11+, DuckDB, PyArrow, httpx, Matplotlib; existing local SQLite checkpoints unchanged.

## Global Constraints
- 不修改原始 Parquet、采集 schema、Config.identity 或已有 checkpoint；无需重采集。
- 仅在本 staging 目录修改，最终由主 agent 哈希检查后复制到用户指定项目；保留用户 config/data/未跟踪文件；不创建分支或 commit。
- 金额使用整数/Decimal，缺失手续费不当作 0；用户 deposit 与系统交易分开；不将空 calldata 推断为 EOA。
- 日活为成功非系统发送地址，不等于人数；native/Transfer 金额不是 DEX/经济交易额，不编造 USD。
- 历史状态只能证明指定区块的观测，不能从一次查询推断整段历史；合约名/selector 候选不能当作项目归属。
- 网络失败可降级为未知并记录状态；不泄露 endpoint/响应中的凭证；不新增付费服务要求。
- 使用 apply_patch 编辑，先失败测试再实现；执行完整测试和独立审查。

## Task 1: 离线行为与费用指标
- [x] 修改 metrics.py，新增 focused activity_metrics.py 和 test_activity_metrics.py。
- [x] 测试并实现 daily/summary 中非系统成功/失败/失败率/份额，按 system/user_deposit/regular 三组给出 fee counts、覆盖率、精确 wei/BNB 金额，不假定缺失字段为不适用。
- [x] 按 classification/call input 互斥技术行为聚合，保留 unknown；selector 仅候选签名，不断言业务类型。
- [x] 提供小时交易/日活、recipient Top1/5/10/20 与高频发送者 Top1/5/10/20 集中度；分母明确，排序确定，recipient 不因 label 变更重复排名。
- [x] Top20 sender 的 recipient/selector/hour breakdown，按 sender 每项限20，且有占比、全量总数；高频不直接称机器人。
- [x] 为 weekly/monthly 追加 observed_days、expected_days、is_partial（包括非零不足整日边界）；无识别项目 concentration 为 None 而非 0。
- [x] 在分析 JSON 提供 transaction_types, methods, hourly, fee_groups, recipient_concentration, sender_concentration, sender_breakdown（列表行）；兼容现有字段和空窗口。
- [x] RED/GREEN、全套测试，写报告，生成相对基线 diff 供审查。

## Task 2: 有来源地址库与限量补充识别
- [x] 新增 enrichment.py、registry 数据、测试，CLI enrich --top 20 [--refresh]；默认仅有限 Top recipients 和 Transfer emitting token addresses（各最多 top）。
- [x] 官方部署资料和 BNB 官方 tokenlist 的快照映射精确地址；记录 chain=204、来源、获取时间、有效范围/当前证据限制。用户 labels 优先且按区块切分，冲突不静默覆盖。
- [x] 使用 OPBNB_RPC_URL 历史 eth_getCode 对 Top recipient 在首个/末个实际出现区块观测；状态/hash/cache 按链/地址/区块，结果不泛化全历史。不支持 archive/元数据照样可生成部分报告。
- [x] 自动尝试 NodeReal 合约 ABI/源码只在支持时（先验证官方接口），不支持一次后跳过；ABI 方法/事件可用于有证据的技术签名，不通过合约名猜项目。若 API 不能验证，不依赖此接口且如实注明。
- [x] token decimals/symbol 可限量 eth_call 查询并保留为 observed_unverified；只有官方匹配/显式 verified assets 才算可信 fungible；ABI/名称/返回 decimals 单独不证明 ERC20。
- [x] 原始响应作为不可信数据，验证结构/大小，日志不输出秘密；成功缓存复用，失败可重试，原子写入，版本/identity 检查。
- [x] report/analyze 自动合并可用 registry/cache；生成 recipient_details/token_metadata/unresolved 优先清单，Unknown 不强行识别；不隐式发网络请求。
- [x] 测试缓存复用、失败降级、synthetic 隔离、链不符、官方匹配、label 冲突/范围、超时/无 key。

## Task 3: 中文汇报与使用说明
- [x] report 输出中文摘要：活动与用户口径、系统占比、BNB 金额、各组 fee 覆盖、Top recipients/senders、项目识别覆盖及待核实事项。
- [x] 输出新增 CSV 及 technical appendix，Unknown 项目集中度 N/A，周期不完整明确提示，留存未成熟不显示零。
- [x] 单日用条形构成图，多日趋势 marker，项目均未知时画 recipient 排名且不称合约；保留旧 PNG 文件名兼容引用。
- [x] 补充 README/nohup 服务端升级与 enrich/report 命令，无需重复 collect。真实原始数据本地缺失，不声称重新验证用户一日全量。
- [x] TDD + 全套测试 + demo + 图像检查 + 独立最终审查，安全部署并打包源码。
