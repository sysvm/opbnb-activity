import html
import logging
import os
from datetime import datetime, time
from decimal import Decimal
from urllib.parse import quote, urlsplit
from zoneinfo import ZoneInfo

from .decode import decimal_units
from .attribution_report import attribution_section
from .metrics import analyze
from .report_tables import TABLE_FIELDS, export_tables

logger = logging.getLogger(__name__)
BEHAVIORS = {
    "system_l1_attributes": "协议 L1 attributes 系统交易",
    "user_deposit": "用户 deposit（非系统；不等于完整跨链量）",
    "contract_creation": "创建交易（to 为空）",
    "empty_calldata": "空 input（不证明 EOA，也不证明普通转账）",
    "selector_call": "含四字节 selector（不证明业务意图）",
    "short_calldata": "短 calldata（不足四字节）",
    "unknown": "未分类或无效 calldata",
}


def cell(value):
    if value is None or value == "":
        return "N/A"
    return (html.escape(str(value), quote=True).replace("\r", " ").replace("\n", " ")
            .replace("|", "&#124;").replace("`", "&#96;").replace("[", "&#91;")
            .replace("]", "&#93;").replace("\\", "&#92;").replace("*", "&#42;"))


def percent(value):
    return "N/A" if value is None else f"{value:.2%}"


def number(value):
    return "N/A" if value is None else f"{int(value):,}"


def amount(value):
    if value is None:
        return "N/A"
    formatted = format(Decimal(str(value)), ",f")
    return formatted.rstrip("0").rstrip(".") if "." in formatted else formatted


def short_address(value):
    return value[:8] + "…" + value[-6:] if value and value.startswith("0x") and len(value) == 42 else cell(value)


def evidence_link(value):
    if not isinstance(value, str) or any(character.isspace() or ord(character) < 32 for character in value):
        return "来源未提供可用链接"
    try:
        parsed = urlsplit(value)
        if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username or parsed.password:
            return "来源未提供可用链接"
    except ValueError:
        return "来源未提供可用链接"
    return "[来源](" + quote(value, safe=":/?=&%#~+-._") + ")"


def table(headers, rows):
    return "\n".join(["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers),
                      *("| " + " | ".join(str(value) for value in row) + " |" for row in rows)])


def retention_text(rows, lag):
    mature = [row for row in rows if row["lag_days"] == lag]
    total = sum(row["cohort_size"] for row in mature)
    retained = sum(row["retained"] for row in mature)
    return (f"D{lag}：{percent(retained / total)}（{number(retained)}/{number(total)} 个地址，"
            f"{number(len(mature))} 个成熟 cohort）") if total else f"D{lag}：N/A（无成熟 cohort）"


def recipient_rows(result):
    unresolved = {row["address"]: row for row in result["unresolved"]}
    rows = []
    for detail in result["recipient_details"][:5]:
        unknown = unresolved.get(detail["address"])
        state = ("Unknown；未匹配公开归属；" + cell(unknown["status"])) if unknown else cell(detail["confidence"])
        rows.append([short_address(detail["address"]), number(detail["transactions"]), cell(detail["project"]),
                     cell(detail["category"]), state, evidence_link(detail["source_url"])])
    return rows


def executive_summary(result, store):
    metadata, summary = result["metadata"], result["summary"]
    zone = ZoneInfo(metadata["timezone"])
    local_start = datetime.fromisoformat(metadata["start_utc"]).astimezone(zone)
    local_end = datetime.fromisoformat(metadata["end_utc"]).astimezone(zone)
    start, end = local_start.isoformat(), local_end.isoformat()
    complete_day = (local_start.time() == time.min and local_end.time() == time.min
                    and (local_end.date() - local_start.date()).days == 1)
    incomplete_day = local_start.time() != time.min or local_end.time() != time.min
    source = "SYNTHETIC / TEST DATA — 合成测试数据，非主网统计" if metadata["synthetic"] else "RPC 采集数据；来源节点未独立认证"
    active_label = "单日成功非系统发送地址（DAU）" if complete_day else "全窗口成功非系统发送地址去重"
    day_note = "窗口含不完整日，地址数仅表示观察窗口去重，不视为完整 DAU。" if incomplete_day else ""
    activity = table(["指标", "结果"], [
        ["全部交易", number(summary["transactions"])],
        ["系统交易", number(summary["system_transactions"]) + "（占全部 " + percent(summary["system_share"]) + "）"],
        ["非系统交易（含失败）", number(summary["non_system_transactions"])],
        ["非系统成功交易", number(summary["successful_non_system_transactions"])],
        ["非系统失败交易", number(summary["failed_non_system_transactions"])],
        ["非系统失败率", percent(summary["non_system_failure_rate"])],
        [active_label, number(summary["unique_successful_senders"])],
        ["补充：全窗口全部非系统发送地址去重（含失败）", number(summary["unique_all_non_system_senders"])],
        ["成功非系统顶层 BNB 流转额", amount(summary["native_value_bnb"]) + " BNB"],
        ["已观测完整费用小计（含失败）", amount(decimal_units(summary["known_total_fee_wei"], 18)) + " BNB；覆盖 " + percent(summary["full_fee_coverage"])],
    ])
    coverage = percent(summary["known_project_coverage"])
    known = any(row["project"] != "Unknown" for row in result["projects"])
    project_note = ("已知归属图排除 Unknown，覆盖率 " + coverage + "；不能据此代表全部生态。" if known else
                    "归属覆盖低/Unknown：无已知归属；项目集中度 N/A，projects.png 改为接收地址排名。")
    concentration = " / ".join(percent(summary["project_top" + str(top) + "_share"]) for top in (1, 5, 10))
    recipient_shares = " / ".join(percent(row["share"]) for row in result["recipient_concentration"])
    sender_shares = " / ".join(percent(row["share"]) for row in result["sender_concentration"])
    recipients = table(["接收地址", "交易数", "公开关联", "类别", "置信度/待核实", "证据"], recipient_rows(result))
    senders = table(["发送地址", "非系统交易", "成功", "失败", "成功活跃天数"], [
        [short_address(row["sender"]), *(number(row[field]) for field in (
            "transactions", "successful_transactions", "failed_transactions", "active_days"))] for row in result["senders"][:3]])
    fees = table(["费用组", "交易数", "完整费用已知/分母", "完整覆盖", "L1覆盖", "L2覆盖", "完整小计 BNB"], [
        [{"system": "系统", "user_deposit": "用户 deposit", "regular": "常规非系统"}[row["fee_group"]],
         number(row["transactions"]), number(row["known_total_fee_transactions"]) + "/" + number(row["transactions"]),
         percent(row["total_fee_coverage"]), percent(row["l1_fee_coverage"]), percent(row["l2_fee_coverage"]),
         amount(row["known_total_fee_bnb"])] for row in result["fee_groups"]])
    token_details = {row["address"]: row for row in result["token_metadata"]}
    tokens = table(["已验证资产（逐资产，不混加）", "Transfer 流转额"], [
        [cell(token_details.get(address, {}).get("symbol")) + "（" + short_address(address) + "）", amount(value)]
        for address, value in sorted(summary["verified_token_amounts"].items())])
    token_note = "" if summary["verified_token_amounts"] else "无可确认的 ERC20 流转额（N/A，不代表没有代币活动）。"
    behaviors = table(["技术行为（互斥）", "交易数", "解释"], [
        [name, number(next((row["transactions"] for row in result["transaction_types"] if row["transaction_type"] == name), 0)), explanation]
        for name, explanation in BEHAVIORS.items()])
    partials = [f"{label} {row['period_start']}：{row['observed_days']}/{row['expected_days']} 天"
                for label, name in (("周", "weekly"), ("月", "monthly")) for row in result[name] if row["is_partial"]]
    partial_note = ("未完整周/月（observed_days/expected_days，含零活动日）：" + "；".join(partials) +
                    "。不可按完整 WAU/MAU 外推或直接做整月环比。") if partials else "周/月按各自完整周期去重；不将 DAU 相加作为 WAU/MAU。"
    empty_note = "\n**当前有效窗口无交易；金额小计为 0，无法定义的比例或留存为 N/A。**\n" if not summary["transactions"] else ""
    return f"""# opBNB 活动分析摘要

**{source}**

统计窗口：**[{cell(start)}, {cell(end)})**（开始含、结束不含）；**{cell(metadata['timezone'])}** 分日。
来源：本 run 已校验 Parquet；chain ID 204；运行 {cell(metadata['run_id'])}；声明区块范围 [{store.start_block:,}, {store.end_block:,}) 校验通过，尚未独立核对节点链真实性。
{empty_note}
{activity}

失败率分母为全部非系统交易（含失败）；主活跃口径为成功非系统发送地址，地址不等于人数。多日窗口去重数不能称为 DAU。{day_note}

## 贡献与归属

项目/协议/资产归属覆盖率（公开资料匹配）：**{coverage}**；Unknown 占非系统交易 **{percent(summary['unknown_project_share'])}**。
已知归属 Top1/5/10：{concentration}（分母：全部非系统交易（含失败））。{project_note}
接收地址 Top1/5/10/20：{recipient_shares}；发送地址 Top1/5/10/20：{sender_shares}（同一分母：全部非系统交易（含失败））。
Top 1% 成功非系统发送地址（向上取整 {number(summary['top1_percent_sender_count'])} 个）贡献成功非系统交易的 **{percent(summary['top1_percent_sender_successful_tx_share'])}**。

{recipients}

归属包含项目、协议地址、官方桥接资产，保留类别，不等于 dApp 使用率。project_published=项目方公开关联，bnb_official=BNB官方资料；均不代表逐笔业务/历史控制权证明。来源字段抽样首个出现区块的标签；跨标签区间见 [recipient_details.csv](recipient_details.csv)。Unknown 原因/状态见 [unresolved.csv](unresolved.csv)。

{senders}

发送方目标、selector、小时分布见 [sender_breakdown.csv](sender_breakdown.csv)（Top20 发送方，各维度最多20项），高频不等于机器人或恶意行为。项目间地址重叠，不可把项目去重地址直接相加。

{attribution_section(result, table, number, percent, cell, short_address)}

## 资金与成本

{tokens}

{token_note}
顶层 BNB 与逐资产 Transfer 均为流转额，不是经济成交量；不覆盖内部 BNB 转账。排除歧义/NFT样式 Transfer 日志 {number(summary['ambiguous_transfer_logs'])} 条、未验证 Transfer 日志 {number(summary['unverified_transfer_logs'])} 条；symbol/decimals 观测不能证明 ERC20。全部已验证资产原始金额见 [technical.md](technical.md)，Top100 元数据见 [token_metadata.csv](token_metadata.csv)。

{fees}

费用分母为各组全部交易（含失败）；缺失费用不按零补齐。完整费用必须同时观测 L2 和 L1；窗口内已知 L2 小计 {amount(decimal_units(summary['partial_l2_fee_wei'], 18))} BNB，覆盖 {percent(summary['l2_fee_coverage'])}，与完整小计有重叠，不能相加或称作利润。原始 wei 见 [fee_groups.csv](fee_groups.csv)。

## 行为与持续性

{behaviors}

selector 签名候选及来源见 [methods.csv](methods.csv)；emitter/topic0 事件签名候选见 [events.csv](events.csv)。方法占比分母为非系统交易；事件占比分母为成功非系统交易的日志条数，同笔交易可有多条日志，二者不能混用或直接认定业务。

{partial_note}
{retention_text(result['retention'], 1)}；{retention_text(result['retention'], 7)}。只纳入完整成熟 cohort/目标日，首次出现仅指窗口内首次成功活动，不称全链新用户。

![交易与活跃趋势；单日显示交易构成](daily.png)
![公开关联贡献；未知时显示接收地址排名](projects.png)

边界：未给出价格、USD、TVL、DEX/桥经济交易量、真实人数、内部调用或全面业务分类；历史 code 仅证明抽样区块观测。完整定义、哈希和表索引见 [technical.md](technical.md)；所有统计已由 CLI 聚合，无需 SQL。
"""


def technical_appendix(result, store):
    metadata, summary = result["metadata"], result["summary"]
    keys = ("run_id", "synthetic", "timezone", "start_utc", "end_utc", "identity", "dataset_version", "labels_version",
            "assets_version", "registry_status", "registry_hash", "registry_temporal_scope", "enrichment_status",
            "enrichment_schema", "enrichment_hash")
    versions = table(["字段", "值"], [[key, cell(metadata.get(key))] for key in keys])
    raw_money = table(["指标", "精确原始整数 wei"], [
        [key, summary[key]] for key in ("native_value_wei", "known_total_fee_wei", "partial_l2_fee_wei")])
    assets = table(["资产地址", "原始整数金额", "缩放金额"], [
        [cell(address), raw, summary["verified_token_amounts"][address]]
        for address, raw in sorted(summary["verified_token_raw_amounts"].items())])
    descriptions = {
        "daily": "全本地日指标，零活动日保留；successful_transactions/failed_transactions 包含系统，主活跃和 non_system 字段排除系统",
        "contracts": "直接接收地址 × 标签分组 Top100；名称兼容旧版，不保证地址是合约；标签区间变化可拆行",
        "projects": "全部项目/协议/资产关联，含 Unknown；组内地址去重，组间可重叠",
        "senders": "非系统发送方 Top100，active_days 为成功活动天数",
        "weekly": "周一开始，每周内独立去重，含零活动周期与部分周期标志",
        "monthly": "各自然月内独立去重，含零活动周期与部分周期标志",
        "retention": "窗口内首次成功出现 cohort 的精确 D1/D7；仅完整成熟目标日",
        "transaction_types": "互斥技术分类，transactions 之和等于全部交易；share 分母为全部交易",
        "methods": "直接 recipient/selector Top100，含失败；share 分母为全部非系统交易",
        "hourly": "有活动 UTC 小时及本地时间/日期；非活跃小时不补行；active_senders 是该小时成功非系统去重地址",
        "fee_groups": "system/user_deposit/regular 三组；每种费用分别保留已知数、缺失数、覆盖率、精确小计",
        "recipient_concentration": "Top1/5/10/20 唯一直接接收地址（to为空不排名），以全部非系统交易为分母",
        "sender_concentration": "Top1/5/10/20 非系统发送方，以全部非系统交易为分母",
        "sender_breakdown": "Top20 非系统发送方的 recipient/selector/hour，各维度最多20项，share 分母为该发送方全部非系统交易",
        "recipient_details": "按唯一接收地址 Top100；公开关联及首末历史观测，不保证全部历史状态；来源仅抽样首个出现区块的标签",
        "token_metadata": "Transfer emitter Top100；verified_fungible 才能计入金额，observed_unverified 仅元数据观测；log_count 可包含 NFT歧义日志",
        "unresolved": "Top100 接收地址中未匹配项目的影响排序及查询状态，不是全部未知地址清单",
        "events": "成功非系统交易日志的 emitter/topic0 Top100；share 分母为所有合格日志 eligible_log_count，transactions 为组内交易去重",
        "daily_projects": "全部项目×本地日，显式 Unknown，零活动补行；成功发送地址组内去重，项目间不可相加",
        "daily_recipients": "全窗口固定 Top20 接收地址×日，另列 Other 和创建交易；各日交易数可与 daily 对齐",
        "daily_changes": "project/recipient 两套独立的日交易变化贡献；首日差额 N/A、前日零时增长率 N/A；排名按绝对变化",
        "sender_profiles": "全窗口 Top20 非系统 sender 完整统计；精确尝试/成功 value，时间戳间隔统计，非机器人结论",
        "sender_daily": "所选 Top20 sender×全本地日，零活动补行；接收地址排除 to 为空，包含失败尝试",
        "sender_overlap": "所选 sender 无重复配对，最多190组；不同接收对象的交并比，排除创建交易，空并集 Jaccard N/A",
        "failure_methods": "recipient×selector 失败数 Top20；组内失败率分母为该组全部非系统交易，全网份额分母为所有非系统失败",
        "failure_samples": "每个所选失败组最多3笔最早实际失败交易，最多60条；not_collected 表示没有 revert/trace 证据",
    }
    tables = table(["文件", "用途与分母"], [[f"[{name}.csv]({name}.csv)", descriptions[name]] for name in TABLE_FIELDS])
    return f"""# 技术附录与复算口径

本报告在原始 Parquet 上离线复算；数据根目录是报告的上一级（相对路径 `..`）。不需要 SQL 或数据库人工操作。
声明区块范围 [{store.start_block}, {store.end_block})，UTC 时间范围 [{cell(metadata['start_utc'])}, {cell(metadata['end_utc'])})；结束不含，按 {cell(metadata['timezone'])} 分日。
quality 校验已通过：范围、分区哈希、区块连续性/父哈希、交易数、receipt对应、日志关联和重复检查；不能独立证明节点返回的 canonical chain。

## 版本与来源

{versions}

`identity` 为窗口、链、系统规则、分片等配置身份摘要，路径/运行限速/外部标签资产可更新项不参与同样的身份规则；保持原始 manifest 与 checkpoint 兼容。离线报告不获取 endpoint，也不隐式发网络请求。
registry 为随包 chain ID 204 的公开部署/官方资产快照，记录 schema、来源时间及适用范围；显式 labels 按区块区间优先，冲突拒绝，不靠合约名猜归属。project_published=项目方公开关联，bnb_official=BNB官方资料，均不代表逐笔业务/历史控制权证明；类别保留项目、协议、资产的区别。
cache 为本 run 的 enrichment.json，按 schema/链/identity/区块号及哈希校验；missing/invalid 不使用，partial 可用部分观测。代码状态仅为首/末出现区块的 code_present/no_code_observed/unknown；不能证明中间区块、EOA身份或历史代理实现所有权。code_hash 是 SHA256 字节码摘要。当前 ABI 名称/签名候选不证明历史实现、项目或业务。
observed_unverified 的 symbol/decimals 只是历史 eth_call 观测；只有显式 verified 或可信 registry 资产才有 verified_fungible 资格。当前合约元数据接口可选；无 key、无接口、不支持或失败均降级未知，不影响离线基础统计。

## 交易、地址与周期

系统判定同时要求 deposit type 126（0x7e）、配置的 L1 attributes depositor 与 recipient；普通用户 deposit 单列且属非系统，不能按 type 排除全部用户存入。系统规则可能并未穷尽链上所有协议行为。
全部交易包含系统；非系统成功/失败互斥，失败率分母为全部非系统交易。主 DAU 为本地日成功非系统发送方去重；补充 all_non_system_senders 包含失败。全窗口、周、月分别做集合去重，不能对 DAU 或项目地址简单求和；智能账户、中继、批处理和自转账都不等于独立自然人。
接收地址集中度按实际 recipient 去重（不会因标签变化重复排名），创建交易仍留在分母。已知归属 Top1/5/10 和 HHI 排除 Unknown，分母仍为全部非系统交易；无已知项目为 N/A。Top1% 成功发送方按完整集合向上取整，分母为成功非系统交易，不从截断榜单外推。
技术分类优先顺序：系统、用户deposit、创建、空input、selector、短calldata、unknown；互斥。empty_calldata 不证明 EOA/普通转账，selector_call 不证明业务意图。方法/事件签名可来自当前已验证 ABI 候选或标准签名候选，signature_evidence 区分来源；selector/topic碰撞及历史实现变化仍可能存在。
events 的 log_count 是日志条数，transactions 是本组交易去重；eligible_log_count 为全部成功非系统交易日志分母。每笔可有多条事件，事件名字并不证明代币或业务语义。
observed_days 包括窗口覆盖的零活动日；expected_days 为整周/整月天数。is_partial 还捕捉非整日边界；观察一天不等于观察满24小时。部分周期不能直接比较整月环比，也不做 WAU/MAU 外推。
D1/D7 以窗口内首次成功非系统日作为 cohort，精确目标本地日再次成功才留存；cohort日/目标日须完整且成熟。汇总用 retained总和/cohort_size总和加权，无成熟行是 N/A，不是0；窗口前历史未知，不能称全链新用户。

## 资金与费用

{raw_money}

{assets}

整数/Decimal 精确求和和缩放；报告 BNB 保留最多18位小数（去无意义尾零），CSV 保留完整原始 wei。native_value 仅成功非系统顶层 value，可能含 deposit/自转账，不含内部原生币转账；与 token 不混加。
已验证 token 只纳入成功非系统三topic Transfer；四topic NFT样式或歧义事件排除，未验证合约排除。金额是事件流转，不是独立经济成交额；当前没有单独的 mint/burn 统计，也不保证收集到全部代币/内部资金路径。各资产独立，精度与来源在 token_metadata.csv。
L2 = gasUsed × effectiveGasPrice，L1 = l1Fee；两者同时可用才有 total_fee。未知不当零，未验证的其他total字段不信任。每组已知/缺失分母为组内全部交易（含失败），没有交易时覆盖 N/A；有已知值时零可为真实零。完整费用小计、L1/L2小计覆盖重叠，不可相加；费用不是利润。
缺口：没有价格/USD、TVL、DEX现货或衍生品成交量、多跳终端成交额、官方/第三方桥完成量、BSC最终确认、trace内部调用归属、全面业务标签或机器人/人数判断。只有L2存入/提现线索不能称桥最终完成量。

## 原始字段与文件

- blocks：number/hash/parent_hash 为链连接；timestamp 为 UTC epoch 秒，timestamp_utc 为 ISO UTC；transaction_count 为块内交易数。
- transactions：hash/block_number/block_hash/transaction_index 关联块与顺序；sender/from、recipient/to（空表示创建）；value_wei 为十进制原始值，input 为 calldata，selector 为前4字节候选；type 为交易类型，is_system/classification 为既定识别规则，recipient_kind 为采集时可确认程度（unknown 不补猜）。
- receipts：transaction_hash 与区块/索引关联；status 1成功/0失败；gas_used、effective_gas_price_wei、l2_fee_wei、l1_fee_wei、total_fee_wei 保留未知；contract_address 为创建结果地址。
- logs：transaction_hash/block_number/block_hash/log_index 标识事件；address 为发出地址，topics 为索引主题（首个为topic0），data 为原始payload；已拒绝 removed/错配日志。

{tables}

榜单按全量计数排序后限行，不能把截断CSV行求和视作全网。contracts.csv 兼容旧名，不是已确认合约/项目清单；历史标签可拆行，唯一接收地址主表用 recipient_details.csv。
CSV 为带 UTF-8 BOM 的标准表头；空表仍有稳定字段。对可能被 Excel 当作公式的自由文本添加前导单引号，数值/wei/raw_amount 从不添加或截断。Excel会自动舍入长整数：请用“数据→从文本/CSV”导入，将地址、wei、raw_amount、scaled_amount 列设为文本，保留精度；CSV本身保留原值。
Markdown 对标签、符号、元数据单元格转义，只允许无认证信息的显式 http(s) 来源链接。原始 Parquet、采集器、checkpoint 和配置身份未改变。
合成数据仅供测试：稀疏时间戳/零活动日不代表主网表现。报告只证明当前已校验窗口数据，不声称完成三个月真实主网采集或实时吞吐验证。
"""


def chart_label(value, fallback):
    return value if value and value.isascii() and value.isprintable() and len(value) <= 48 and "$" not in value else fallback


def charts(destination, result, store):
    os.environ.setdefault("MPLCONFIGDIR", str(store.root / ".matplotlib"))
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plot
    prefix = "SYNTHETIC / TEST DATA | " if result["metadata"]["synthetic"] else "RPC dataset | "
    daily = result["daily"]
    figure, axes = plot.subplots(figsize=(11, 4.5), layout="constrained")
    if len(daily) == 1:
        row = daily[0]
        axes.bar(["System", "Successful non-system", "Failed non-system"],
                 [row["system_transactions"], row["successful_non_system_transactions"], row["failed_non_system_transactions"]],
                 color=["#8092a3", "#147d92", "#dc9454"])
        axes.set_title(prefix + "Transaction composition | " + row["day"])
        axes.set_xlabel("Local date (" + store.config.timezone + ")")
    else:
        axes.plot([row["day"] for row in daily], [row["non_system_transactions"] for row in daily], marker="o",
                  label="Non-system transactions (failures included)", color="#147d92")
        axes.plot([row["day"] for row in daily], [row["active_senders"] for row in daily], marker="s",
                  label="Successful non-system sending addresses", color="#dc9454")
        axes.set_title(prefix + "Daily activity")
        axes.set_xlabel("Local date (" + store.config.timezone + ")")
        axes.legend()
        axes.tick_params(axis="x", rotation=45)
        if len(daily) > 12:
            axes.set_xticks(range(0, len(daily), max(1, len(daily) // 10)))
    axes.set_ylabel("Count")
    axes.set_ylim(bottom=0)
    figure.savefig(destination / "daily.png", dpi=140)
    plot.close(figure)
    figure, axes = plot.subplots(figsize=(11, 4.5), layout="constrained")
    projects = [row for row in result["projects"] if row["project"] != "Unknown"][:10]
    if projects:
        labels = [chart_label(row["project"], f"Association #{rank + 1}") for rank, row in enumerate(projects)]
        axes.barh(list(reversed(labels)), [row["transactions"] for row in reversed(projects)], color="#147d92")
        axes.set_title(prefix + "Published associations (Unknown excluded)\nCoverage: " + percent(result["summary"]["known_project_coverage"]))
    elif result["recipient_details"]:
        recipients = result["recipient_details"][:10]
        axes.barh([row["address"][:8] + "..." + row["address"][-6:] for row in reversed(recipients)],
                  [row["transactions"] for row in reversed(recipients)], color="#8092a3")
        axes.set_title(prefix + "Top recipient addresses | Project attribution unknown")
    else:
        axes.text(0.5, 0.5, "No recipient transactions in valid window", ha="center", transform=axes.transAxes)
        axes.set_title(prefix + "No identified project activity")
    axes.set_xlabel("Non-system transactions (failures included)")
    figure.savefig(destination / "projects.png", dpi=140)
    plot.close(figure)


def generate(store, labels, assets):
    logger.info("Report analysis starting: run=%s", store.config.run_id)
    result = analyze(store, labels, assets)
    destination = store.root / "report"
    destination.mkdir(exist_ok=True)
    export_tables(destination, result)
    logger.info("Report tables exported: tables=%d", len(TABLE_FIELDS))
    (destination / "summary.md").write_text(executive_summary(result, store), encoding="utf-8")
    (destination / "technical.md").write_text(technical_appendix(result, store), encoding="utf-8")
    logger.info("Report summary and technical appendix complete")
    charts(destination, result, store)
    logger.info("Report charts complete: daily.png projects.png")
    return destination
