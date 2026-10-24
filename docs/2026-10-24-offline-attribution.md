# 七天数据离线归因增强

## 在公司服务器上执行

将本次修改同步到服务器原项目目录；保留 config.toml、原始数据和 enrichment.json。无需重新 collect 或 enrich，无需数据库服务或新 API。先等待同一 run 的采集/报告进程结束，不要同时写同一报告目录。

```bash
.venv/bin/python -m pip install -e .
mkdir -p logs
nohup .venv/bin/opbnb-activity report --config config.toml --run-id mainnet-20261015-7d > logs/mainnet-20261015-7d-report.log 2>&1 &
tail -f logs/mainnet-20261015-7d-report.log
```

窗口以该 run 的 manifest 为准，不必改 config 的 start/end。数据集位置仍由 config 的 dataset 选择。若 labels 配置指向服务器文件，保留它；显式标签按有效区块优先，可能覆盖内置新项目关联。命令会重新生成 analysis.json、quality.json 和 report，不改原始 Parquet、manifest 身份或 checkpoint。

离线计算仍需读完整原始文件并校验，不是瞬间完成。三个月复用同样 report 命令，但换成已完整采集的三个月 run ID；扫描耗时与报告计算耗时是两回事。本地只有旧报表时，不能运行这些完整新统计，也不能从 TopN 导出无损推回每日贡献。

## 新增的八张表

|文件|范围和解释|
|---|---|
|daily_projects.csv|全部公开关联项目（含协议/资产）×本地自然日；显式 Unknown，零活动补零；交易成功/失败及成功发送地址去重|
|daily_recipients.csv|按全窗口交易数选固定 Top20 非空接收地址，另外 Other、creation 两组；每天每组均有一行，交易合计等于当天非系统交易|
|daily_changes.csv|project 和 recipient 两套独立变化；绝对差额可正可负、各自可对齐全网日交易变化；首日 prior/delta/rate 空，前日零时 growth_rate 空；contribution_rank 按绝对差额|
|sender_profiles.csv|全窗口交易数 Top20 非系统发送地址；所有所选地址交易的完整聚合，不从旧 sender_breakdown Top20 子项求和；精确 value、空 calldata、失败率、去重接收对象、间隔|
|sender_daily.csv|同一组 Top20 sender×全本地日，零活动补零；successful_active_days 在单日只能是0或1；unique_recipients 排除创建交易|
|sender_overlap.csv|所选 sender 两两最多190组；不同接收地址集合交集、并集、Jaccard，含失败尝试，排除创建交易；并集为空时 Jaccard 空|
|failure_methods.csv|按失败数排序的 recipient×selector Top20；transactions 包含成功和失败，share_of_failures 分母为全网非系统失败数，榜单截断不可当作全网失败合计|
|failure_samples.csv|上述每组最多三笔最早实际失败交易，总计最多60；保留 hash/区块/索引/时间/发送方/value；revert_evidence 固定 not_collected|

所有 TopN 同分使用地址/selector 排序，失败样本按区块号、交易索引、hash 排序。报表输出采用既有 CSV 文本公式转义，wei 是十进制字符串；Excel 导入时选择文本，避免长整数被自动截断。

`daily_projects` 各日 transactions 可加成当天非系统总量；active_senders 不能跨项目相加。全窗口合约改标签时，按已有 valid_from/valid_to 区块区间归组，不以当前标签无条件覆盖显式历史区间。Other 和 creation 是接收分组，不是项目，不把 Other 的不同地址误认为同一个地址。

sender attempted_value_wei 是所有尝试的 value 之和（含失败），successful_value_wei 只含成功的顶层转移；两者都不是净资金流入或经济交易额。原始整数用 Python 精确累加，不能转 double/有限精度 DECIMAL。间隔来自链上秒级 timestamp（含失败），同块为0不代表重复交易或亚秒发送信息。目标重叠、高频、近似笔数都只是行为线索，不足以认定刷量/同一控制者。

## 中文摘要的变化

- 自动选取非系统交易笔数绝对变化最大的相邻观察日，分别列前三项目与接收地址组的差额及剩余净变化；全日期明细仍在 CSV。交易贡献仅是会计分解，不证明业务因果。部分日不能视为完整日增长，DAU 也不能用项目变化相加解释。
- 输出头部发送方行为及失败组，提供可核查的实际失败交易样本。样本是确定性最早记录，不是随机样本，不能据此估计所有失败原因。
- 显示剔除窗口首日 cohort 后的加权 D1，提示左边界选择偏差；D7 至少需八个完整自然日才有首个成熟 cohort。
- 提醒全局手续费覆盖不等于普通用户费用覆盖，以 regular 分组为准。
- recipient_details 新增 code_observation_scope：endpoints_code_present / endpoints_no_code_observed / endpoints_mixed_or_incomplete / unavailable。historical_kind 继续 unknown；首尾采样不证明中间代码恒定、EOA 身份或代理实现未变，不能把 unknown 当作查询失败。

## 新增项目关联与未完成范围

内置 registry 增加 BeatSwap IPLicensingIndex 与 Cortis AI CortisEngagement，包含项目方来源、2026-10-24 获取日期、固定 README 提交链接及字节 SHA256；不把 IPL 自动升级为已确认 ERC20 金额资产。证据只支持当前公开部署关联，不独立证明历史所有权、逐笔 IP 收入或实际 AI 工作。

其他未知项目不硬匹配；业务候选与项目身份不同，签到 selector 不自动生成项目归属。完整证据见 2026-10-24-attribution-review.md。

本轮不实现 trace 查询入口。标准 report 不发 RPC，也不会生成 revert reason。失败样本可为后续显式、有界、可缓存的 trace 查询提供输入；支持 trace 的 probe 结果不代表本次已采集 trace。不得用目标区块历史 eth_call 简单替代真实失败交易重放。

## 重新生成后发回什么

发回新 report 目录，以及 analysis.json、manifest.json、quality.json、enrichment.json（如果存在）即可继续归因。暂不必下载七天全部原始文件；若需详细资金来源图谱或回滚原因，再提供定向样本/查询结果。
