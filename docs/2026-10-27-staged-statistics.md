# 统计阶段8GB内存不足的修复

## 问题与方案

此前日志显示已通过quality校验，但在 daily/contracts/projects/senders/retention 的全窗口SQL阶段耗尽7.6GiB（8192MB）预算。统计原来把2亿笔交易/回执关联直接嵌入每个查询，并把多个条件COUNT DISTINCT/留存等操作组合在一起。仅提高内存或允许落盘不足以保证这种执行方式成功。

现在report/analyze在配置的临时目录下创建自己拥有的磁盘数据库，不是新建数据库服务，也不需要用户连接SQL：

1. 原始数据完整性校验照常执行，失败仍拒绝报告。
2. 每个物理分区内关联交易/回执/标签，存为activity；成功非系统日志单独落盘为eligible_logs，避免后续重复全窗口原始关联。
3. 发送地址×日期、项目×发送地址、项目×接收地址等精确键分步GROUP BY落盘，再统计数量。跨日同一地址只在所需周期去重一次，绝不将每天/每项目DAU简单相加成全窗口人数。
4. 合约排名先用所有交易的精确计数选Top100，再算所选组的成功发送地址；事件/Transfer按emitter/topic/交易键预分组，避免COUNT DISTINCT的大型聚合状态。留存使用精确sender-day与首次成功cohort。
5. 头部发送方间隔中位数通过落盘排序和精确中间位置计算；高频发送方目标重叠仍为精确集合交并比。金额继续以Python任意精度整数累加，维持原wei/逐资产口径。

保持26张CSV及既有字段/分母、零活动日、创建交易、标签有效区间、成熟留存和费用缺失规则；新增metadata.analysis_engine=disk_staged_v1便于确认版本。

## 公司服务器执行

先同步本次源码，再执行：

```bash
.venv/bin/python -m pip install -e .
free -h
df -h .
```

继续使用config.toml顶层设置，不需要继续把预算提高到12GB或16GB：

```toml
analysis_memory_limit_mb = 8192
analysis_threads = 2
```

如果当前数据盘空间不够，可在config顶层指定已有充足空间的磁盘：

```toml
analysis_temp_directory = "/空间充足分区/opbnb-analysis-tmp"
```

目录由工具创建；相对路径按配置文件目录解析。磁盘数据库包含规范化交易、合格日志及去重键表，可能需要大量额外空间，不能只按8GB内存预算预留8GB磁盘，也不能把磁盘总容量1TB当成剩余容量。临时目录可以放到另一块可写磁盘；请同时检查该盘df。磁盘容量和IO仍是三个月运行的重要限制，没有固定空间倍率保证。

确认没有同run的其他报告进程，再用实际run-id执行：

```bash
mkdir -p logs
nohup .venv/bin/opbnb-activity report --config config.toml --run-id mainnet-20260722-20261025 > logs/mainnet-20260722-20261025-report.log 2>&1 &
tail -f logs/mainnet-20260722-20261025-report.log
```

不用再collect，不改manifest/checkpoint，也不跳过校验。每次报告都重新计算；这不是中间结果断点续算。正常成功或Python异常退出时关闭连接并清理本次拥有的analysis-*目录（数据库和溢写）；SIGKILL、断电或磁盘/权限异常可能留下目录。只在确认进程已退出后按需清理旧临时目录，不能删除原始data分区，不清空用户指定临时目录的其他内容。

## 如何定位后续错误

日志包含 `Analysis stage start: report_sender_day`、`core_daily`、`report_retention_counts` 等明确阶段。该run根目录新增analysis-status.json，记录running/complete/failed、stage及错误类别；不记录SQL中的数据、凭据或原始异常文本。complete仅表示分析完成，报告导出/画图完成仍以CLI日志和正常退出为准。

若仍失败，发送analysis-status.json和最后50行日志，而不是盲目再跑一遍。错误发生时可能保留上次成功生成的analysis.json/report，不能把它们当作本轮新结果。分析任务不会隐式连接RPC；独立enrich命令的旧版目标扫描路径不属于本次报告优化范围。

## 验证范围

测试使用真实typed Parquet多分区输入，覆盖完整quality→磁盘关联→所有统计→26张CSV/摘要/图表。低内存案例为25万笔交易、18.75万个不同发送地址、5个物理分区，DuckDB128MB/单线程；核对精确交易数、成功地址、跨日去重、wei费用、代币事件金额、日志数及原始文件/manifest不变。另有旧SQL结果等价、标签切换、空窗口、失败/空目标、精确中位数与异常清理测试。

128MB只是DuckDB预算，不是整个测试进程RSS上限。该测试不能证明服务器2.13亿笔数据必然完成，也不能推算真实耗时或磁盘使用。后续仍须以服务器实际阶段日志为准；不以近似去重、跳过留存或减少数据来掩盖内存问题。

本轮全套388项测试通过（47.19秒）；独立代码复核未发现P1/P2问题。报告工作目录及其溢写目录都在本次拥有的临时目录内，成功和普通异常路径都有清理回归测试。新增分阶段执行元数据不修改原始manifest或checkpoint身份。
