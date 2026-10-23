# 2026-10-23 自动分析增强验证记录

## 已实现

- 离线系统/非系统活动、技术行为、小时分布、接收/发送地址集中度，以及 Top20 发送方的目标/selector/小时拆解。
- 系统、用户 deposit、常规非系统分别计算 L1/L2/完整费用覆盖和精确金额；未知不补零。
- 随包公开资料地址库包含26个 chain204 地址、5种官方桥接资产元数据；来源、SHA256、日期、置信度和适用范围随数据保留。
- `enrich --top 20` 限量历史区块哈希查询、断点缓存、失败降级；可选当前 ABI 元数据，方法/事件只作候选解释，不靠名称猜项目。
- 中文摘要、技术附录、18张聚合 CSV；单日构成图、多日趋势、Unknown 接收地址回退图。

## 验证结果

- Python 3.12，完整离线测试 **158 passed**；`compileall` 通过。
- 按任务执行失败测试到通过测试；独立审查覆盖指标、识别/缓存、安全边界、报告及整体集成，最终无未解决问题。
- 已修复审查发现的无效 selector、缓存证据校验、合成网络隔离及元数据 URL query 参数丢失问题。
- 非 editable 安装后，从项目目录之外且移除 RPC/元数据环境变量运行 CLI demo/validate，通过6区块、11交易、11receipts、2日志的合成一致性检查。
- 已确认安装包包含26地址/5资产的 registry、SQL、合成 fixture，生成18张 CSV、中文摘要与技术附录；MockTransport 验证查询参数保留且不进入缓存/日志。
- 已查看多日趋势/已知关联、单日构成/全Unknown回退四张 PNG，可读且无裁切。
- `config.py`、`models.py`、`storage.py`、`collect.py`、`checkpoint.py` 与升级前字节一致；未修改采集 schema、身份计算和 checkpoint。

复验命令：

```bash
.venv/bin/python -m pytest -q
.venv/bin/python -m compileall -q src tests
```

## 真实数据边界

本机只有用户导出的旧报告文件，没有该 run 的 manifest、原始 Parquet 和完整分析结果。本次没有连接用户 archive API，也没有重新计算其真实一天/三个月数据。所有新增测试及图像预览均为合成数据，不是主网结论。

服务器同步完整新版源码和依赖后，保留原配置/数据，按 README 开头运行 `enrich` → `report`；不必重复 collect。没有可选元数据 API 仍能离线报告，未知项目继续保留 Unknown。公开关联不等于历史控制权或逐笔业务证明；资产流转不等于经济成交额。
