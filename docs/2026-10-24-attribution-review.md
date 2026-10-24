# 七天头部未知地址归因核查及分析功能缺口

核查日期：2026-10-24。数据窗口：北京时间 2026-10-15 至 2026-10-22（不含结束）。

本文件为研究记录和待确认设计，不是已发布标签库，也没有修改历史分析结果。

## 1. 本轮完成情况

对 analysis.json 中 Unknown 交易量前十接收地址逐一检索，核对本地方法、事件及首尾代码采样，检查现有分析实现。十个地址共 3,593,580 笔，占非系统交易 43.22%。

找到两条项目方公开部署关联：BeatSwap 的 IPLicensingIndex、Cortis AI 的 CortisEngagement，共 192,820 笔。若按现有“当前公开部署关联应用到观察窗口”的口径采纳这两个标签，项目覆盖率可由 37.77% 变为约 40.09%（+2.32 个百分点）。这不是代码已经生成的新统计，亦不代表完成了历史所有权/代码对应证明。

其余地址不强行补项目名。可以增强业务类型证据，但“业务可识别”与“项目可归属”必须分别统计。

## 2. 前十未知接收地址逐项结论

分母：8,315,198 笔非系统交易。地址均为 opBNB chain ID 204 的观察记录。

|地址|交易数|当前可支持的结论|项目归属状态|
|---|---:|---|---|
|`0xafbba7ed09c1988fd103e44ffca496afceb4b587`|869,158|本地全为 `0xe95a644f`；公开历史交易解码为 checkIn(uint256)，事件特征相符|Unknown；出现 SERAPH 文案只能作为线索|
|`0x640ef170d28645b50a04abe1913872587cf8dcbb`|628,756|本地全为 `0x183ff085`；公开历史交易为 checkIn()，事件特征相符|Unknown|
|`0x8fc0178e07310bf081897c7d9625b8180e7c5cef`|419,337|公开合约页为 SignInSystem，ABI 含 signIn(string)，与本地 selector 相符|Unknown；历史签入文本提到 AOA，不足以确认运营项目|
|`0xa40e1d8ea266387088ab3904d982d146e2727f34`|419,113|本地全为 `0xe95a644f`；opBNB 历史交易解码 checkIn(uint256)，事件特征相符|Unknown|
|`0x5c91681073f23fd1a3ef0896e7db45b4e686cd2b`|386,227|其中 386,226 笔为 `0xa2e62045`；公开历史交易为 update()；具体业务未证实|Unknown；不得套用其他链同地址的项目标签|
|`0xda2ebd19435e74a165c3710e51d98eff4cac098b`|378,151|本地全为 `0xf54b14bd`；公开历史交易 dailyCheckIn()、CheckIn 事件与本地特征相符|Unknown|
|`0x843e4ff9242c3e81b676434b2876fb090d4a5783`|200,422|本地为 `0x183ff085`，checkIn() 仍仅是标准签名候选，未取得地址专属 ABI 证据|Unknown|
|`0xdeb3fc49eb63765cdabcd0917ae4d90b75847001`|106,825|BeatSwap 项目仓库明确列出 opBNB IPLicensingIndex；本地 MintOccurred topic 与历史解码相符|可新增 project_published 关联；历史归属边界需注明|
|`0xcab7d263860b95ee56e7ac788bc3694ab315c4b1`|99,596|全为空 calldata；首尾采样均 no_code_observed|Unknown；不应把它直接称为热门合约，也未证明完整历史 EOA 身份|
|`0xf77c3f4c0b835b93d8d47d52f3a44f7fe8d2269b`|85,995|Cortis AI 官方仓库列出 opBNB CortisEngagement 地址|可新增 project_published 关联；不等于已证明实际 AI 工作量|

前述五个签到/签入证据较强的地址（afbba、640ef、8fc、a40、da2）共 2,714,515 笔，约占非系统交易 32.65%。这是可进一步验证的“签到类调用/尝试”规模，不是五个项目已确认，也不是逐笔验证后的真实签到用户数；其中包含失败交易。

## 3. 项目与业务证据

### BeatSwap / Licensing-to-Earn

[项目仓库 README](https://github.com/beatswap-labs/licensing-to-earn) 列出 chain 204、IPLicensingIndex 地址与 IPL 标识，描述 IP 许可索引及奖励记账用途。这里只采纳部署关联，不为其“真实许可活动”背书。

本地该地址的两种主要 selector：`0xe67e402c` 为 93,728 笔，`0xb91b88db` 为 13,096 笔，Top100 方法表合计比该地址总量少一笔，不能把截断方法表当作完整清单。本地另有 93,138 条 MintOccurred topic，分布于 93,138 笔交易；以及相同数量的 Transfer 事件，但在没有原始逐交易关联时不能仅凭相同计数证明两组完全重合。

[opBNB 历史交易示例](https://opbnb.bscscan.com/tx/0xa7d791114cbc63a3c5b1cba03048e53f700d4452fd073b7c27a463cf5d59a9fd) 将该 topic 解码为 MintOccurred(address,uint256,uint256,uint8,string)。示例不在本周窗口，仅加强事件语义线索，不代表本周每笔交易的许可金额。

### Cortis AI

[官方合约仓库](https://github.com/CortisAI/cortis-contracts) 列出 chain 204 的 CortisEngagement 与 AgentPassport，前者地址精确匹配本地 f77c 地址。公开用途为代理身份及参与记录，包括签到、映射、工作流、部署交互。

本地四个方法计数分别为：`0x183ff085` 25,873，`0x6d220902` 20,064，`0x54bb42e0` 20,031，`0x9752f163` 20,027，合计 85,995。未成功取得源码/部署 JSON 的独立页面内容，本轮不将后三个 selector 硬映射为某个具体功能。项目描述不证明每笔交易是 AI 推理或付费需求。

### 签到线索的地址级证据

- afbba：[历史交易](https://opbnb.bscscan.com/tx/0x60084db0f2ec12e277ec7bbaad7db4006a0cc4020d7370d1a88a93b18b2d6249)，checkIn(uint256)，事件中出现 Seraph 相关文案。没有发现本轮可核实的项目方地址公告，保留 Unknown。
- 640ef：[历史交易](https://opbnb.bscscan.com/tx/0xfa41f01c5c800d0bd1b1546ac32269ff11daec2602d16c5f2604d6d44d641296)，checkIn()，CheckIn(address) 事件。
- 8fc：[合约公开页](https://opbnb.bscscan.com/address/0x8fc0178e07310bf081897c7d9625b8180e7c5cef) 提供 SignInSystem ABI；[历史交易](https://opbnb.bscscan.com/tx/0x55dc3fe1a544d1b6c8fb4e79254cdb9b600fbd69dabcd78636cb7b88aa131a55) 出现 AOA 签入文案，文案不等于运营主体证明。
- a40：[opBNB 历史交易](https://opbnb.bscscan.com/tx/0x92c6c68642e761f300da3e9c0bf3e80ed23e35309c618309ac1b0407c3a8dcec)，checkIn(uint256)，CheckIn(uint256) 事件。BSC 上同地址资料没有被用于本次归属。
- da2：[历史交易](https://opbnb.bscscan.com/tx/0x8f3fea419fa10b28851ec7ec7cc1e9066fdc1d5b58d492d0a3d1083cee6c678d)，dailyCheckIn()，CheckIn(address,uint256) 事件。
- 5c916：[历史交易](https://opbnb.bscscan.com/tx/0xb23c57bd3795e60d9e54e7d852fd17900b6683b2f4d3efbce04a1ae3db3409b9) 只能确认解码为 update()，不能推断训练、签到或数据存储等业务。

以上网页及源码/ABI 可能为当前或历史索引结果，不自动证明整个观察窗口代码恒定。首尾代码 hash 相同也不证明中间无变化或代理实现未变化。第三方镜像仅用于发现线索，最终项目依据使用项目方原始仓库。

## 4. 现有代码检查结果

代码路径均位于 `/Users/zhaoyu/corp/bnbchain/opbnb/opbnb-activity`：

- `src/opbnb_activity/activity_metrics.py` 已有全网日/小时趋势、Top100 接收地址×selector、Top20 sender 的 recipient/selector/hour Top20 分布，但没有日×项目、日×头部接收地址、日×方法和日环比贡献分解。
- `src/opbnb_activity/metrics.py` 的 activity 视图已经包含本地日、项目有效区块关联、sender、recipient、input、value、交易结果，服务器原始文件足够计算这些离线指标；无需用户连接外部数据库。
- `src/opbnb_activity/models.py` 已保留交易 hash、时间戳、value、input、sender/recipient/status，可用于资金、时间间隔及定向样本。未保存 revert reason 或 trace 结果，离线报告不能直接给出具体回滚原因。
- `src/opbnb_activity/probe.py` 仅测试 debug_traceBlockByNumber 能力，探测成功不表示已经采集或分析 trace。
- `src/opbnb_activity/registry.py` 已支持 bundled registry 与显式 labels 的区块区间优先级，可最小化增加有证据的项目关联；不要把含项目名的事件文本直接作为可靠项目标签。
- 本地七天目录是报表导出，没有本次完整原始 Parquet。新增日维度统计需在公司服务器运行，不可能在现有本地 TopN 导出上无损反推。

## 5. 建议方案与取舍（待确认后实施）

### 方案 A：只补项目标签

改动最小，现有 report 可重新归组，但仍解释不了日活涨跌，也不能回答高频地址是否为分发活动。不建议单独作为最终方案。

### 方案 B：离线归因增强 + 独立、可选的失败抽样（推荐）

第一阶段复用原始文件与现有 report 流程：

1. 将上述两条有项目方公开证据的部署关联加入地址库，记录来源、获取日期、证据级别和当前关联应用于历史窗口的限制。业务证据与项目归属分开，未取得历史证据的标签不得称为历史独立确认。
2. 新增每日项目总表：非系统交易/成功/失败、成功活跃地址、明确的 Unknown 行；日总量与 daily.csv 对齐。项目地址数不能跨项目求和成全网 DAU。
3. 新增每日全窗口 Top20 接收地址面板，其余收拢为 Other，并保留创建交易组；零交易日补零，避免每日换榜导致趋势失真。添加非系统交易数环比贡献分解。日活去重非可加，不将各项目 DAU 变化硬加成全网 DAU 变化。
4. 对全窗口 Top20 sender 做完整离线聚合：每日笔数、空 calldata/零 value/非零 value、接收地址去重、失败率、交易间隔与分布、头部发送方两两接收对象重叠。重叠和高频只标记观察信号，不输出未经证明的“机器人/同一控制人”。
5. 新增失败 recipient×selector 排名及有界样本 tx hash、区块、时间、status，不冒充 revert 原因。交易 hash 及 selector 的 TopN 截断规则要明确。
6. 在 summary.md 用这些表解释交易笔数峰谷，并修正 recipient_kind、费用覆盖、留存窗口提示。正常 report 无需 RPC，避免新增意外网络调用或再次全链采集。

第二阶段再设计显式可选的 RPC trace 样本入口：用户明确执行时才查询，对第一阶段选出的失败样本设数量/超时/并发上限与缓存，方法不支持或失败则展示缺失，不阻断离线报告。trace 重放、返回错误和历史 eth_call 不能混为真实交易 revert 证据。

验证：合成 fixture 覆盖零活动日、Unknown、失败交易、创建交易、标签区间切换、跨项目同 sender、TopN/Other、精确金额和两两重叠；各日计数与现有 daily 对齐。使用现有测试框架，不改采集/分区/checkpoint 身份，不引入常驻服务或外部数据库。

### 方案 C：先全量 trace / 资金图谱

解释能力更强，但大幅增加 RPC、存储和实现成本；不适合作为本轮首选。先用方案 B 确定重点对象，再按需定向查询更稳妥。

## 6. 当前状态

本轮已完成头部十地址的公开证据核查及代码缺口检查；两条新项目关联尚未写入正式标签库，分析增强功能也尚未实施。下一步确认方案 B 的第一阶段后，形成实现计划并补代码；不用重扫七天，届时在服务器重跑 report，导出新增结果即可继续分析。
