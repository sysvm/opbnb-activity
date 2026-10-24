def attribution_section(result, table, number, percent, cell, short_address):
    daily = result["daily"]
    transitions = [(prior, current) for prior, current in zip(daily, daily[1:])]
    lines = ["## 每日交易变化归因", "",
        "按本地自然日分组；项目和接收地址是两套独立分解，不能相加。下列为交易笔数贡献，不证明涨跌的业务原因。项目 DAU 不可相加，不能用项目去重地址变化之和解释全网 DAU。窗口边缘若为部分日，不可当完整日环比。"]
    if transitions:
        prior, current = max(transitions, key=lambda pair: abs(
            pair[1]["non_system_transactions"] - pair[0]["non_system_transactions"]))
        delta = current["non_system_transactions"] - prior["non_system_transactions"]
        lines.extend(["", f"窗口内非系统交易绝对变化最大的日期：{cell(current['day'])}，较 {cell(prior['day'])} 变化 {delta:+,} 笔。"])
        for dimension, title in (("project", "项目"), ("recipient", "接收地址组")):
            contributions = sorted((row for row in result.get("daily_changes", [])
                if row["day"] == current["day"] and row["dimension"] == dimension),
                key=lambda row: row["contribution_rank"])
            selected = contributions[:3]
            display = []
            for row in selected:
                label = (cell(row["project"]) if dimension == "project" else
                    short_address(row["recipient"]) if row["recipient_group"] == "top20" else
                    {"other": "其余接收地址（Other）", "creation": "创建交易"}[row["recipient_group"]])
                display.append([label, number(row["previous_transactions"]), number(row["transactions"]),
                    f"{row['delta_transactions']:+,}"])
            remaining = sum(row["delta_transactions"] for row in contributions[3:])
            display.append(["其余分组净变化", "—", "—", f"{remaining:+,}"])
            lines.extend(["", table([title, "前日笔数", "当日笔数", "笔数变化"], display)])
    else:
        lines.extend(["", "不足两个观察日，日环比贡献为 N/A。"])
    lines.extend(["", "全部日期及零活动日见 [daily_projects.csv](daily_projects.csv)、[daily_recipients.csv](daily_recipients.csv)、[daily_changes.csv](daily_changes.csv)。接收地址使用全窗口固定 Top20，另列 Other 和创建交易；没有每日换榜。首日变化为 N/A，前日为零时增长率为 N/A。",
        "", "### 高频发送方与失败诊断", "",
        table(["发送地址", "笔数", "空 calldata 占比", "不同接收地址", "中位间隔（秒）"], [
            [short_address(row["sender"]), number(row["transactions"]),
                percent(row["empty_calldata_transactions"] / row["transactions"]), number(row["unique_recipients"]),
                cell(row["median_gap_seconds"])] for row in result.get("sender_profiles", [])[:5]]),
        "", "[sender_profiles.csv](sender_profiles.csv) 与 [sender_daily.csv](sender_daily.csv) 保留全窗口 Top20 发送方的完整聚合、逐日零活动行及精确顶层 value；attempted_value_wei 含失败尝试，successful_value_wei 才为成功转移口径。间隔按链上时间戳计算，同块交易间隔可为零，不能推断亚秒发送频率。",
        "[sender_overlap.csv](sender_overlap.csv) 为所选发送方两两不同接收地址集合交集/并集及 Jaccard（排除 to 为空，包含失败尝试）。高频、相同计数或对象重叠均不证明机器人、恶意刷量或同一控制人。",
        "", table(["失败接收地址", "selector", "失败笔数", "组内失败率", "占全网非系统失败"], [
            [short_address(row["recipient"]) if row["recipient"] else "创建交易", cell(row["selector"]),
                number(row["failed_transactions"]), percent(row["failure_rate"]), percent(row["share_of_failures"])]
            for row in result.get("failure_methods", [])[:5]]),
        "", "[failure_methods.csv](failure_methods.csv) 按失败笔数选全窗口 recipient×selector Top20；组内分母含该方法成功和失败交易。"
        "[failure_samples.csv](failure_samples.csv) 每组最多三笔、按区块/索引/hash 最早排序，最多60笔；是定位样本而非随机代表性样本。未采集 revert 原因（not_collected），不能只凭失败率认定重复领取、余额不足或项目故障。报告不会隐式调用 trace。"])
    first_day = daily[0]["day"] if daily else None
    mature = [row for row in result["retention"] if row["lag_days"] == 1 and row["cohort_day"] != first_day]
    size = sum(row["cohort_size"] for row in mature)
    retained = sum(row["retained"] for row in mature)
    lines.extend(["", "### 口径补充", "",
        "D1（剔除窗口首日 cohort）：" + percent(retained / size if size else None) +
            f"（{number(retained)}/{number(size)} 个地址）。首日 cohort 混入窗口前已活跃的地址，"
            "后续 cohort 只是窗口首次观察地址，不等于生命周期新用户；两者不能直接比较为真实留存恶化。D7 至少需要八个完整自然日才有首个成熟 cohort。",
        "完整费用全局缺失占比不是普通用户费用缺失率，应分别看 system/user_deposit/regular 的覆盖；未知费用不补零。"])
    scopes = {}
    for detail in result["recipient_details"]:
        key = detail.get("code_observation_scope", "unavailable")
        scopes[key] = scopes.get(key, 0) + 1
    lines.extend(["", "接收地址首尾代码采样状态（仅 Top100）：" + "；".join(
        cell(key) + "=" + number(value) for key, value in sorted(scopes.items())) +
        "。historical_kind=unknown 保持保守口径，不能将其理解为代码查询全部失败。"
        "code_observation_scope 只描述两个端点，不证明整个窗口、EOA 身份或代理实现；ABI/项目元数据缺失不等于交易漏采。"])
    return "\n".join(lines)
