# 三个月报告内存不足修复

## 原因与修复范围

旧版 DuckDB 内存固定512MB，关系校验对全窗口分区建立大表hash关联和分组；约2.13亿笔交易时出现 OutOfMemoryException。计数与范围一致只能说明已读到这些行，不等于所有关联验证完成。保留原始数据，不删数据、不补写quality结果、不绕过报告门禁。

新版支持运行时资源配置；旧manifest没有这些字段仍可读取，资源参数不参与identity，不改变窗口、系统规则、分区或checkpoint。报告/分析/校验/补充命令显式提供 --config 时，新的资源设置会实际覆盖旧run的资源预算；未提供配置时沿用manifest中的设置或新版默认。

关系检查逐物理分区执行，避免2亿行同时构建hash关联。原有六类检查全部保留；blocks/transactions/receipts的跨分区hash重复仍精确检查，采用全量单列排序、每批最多4096行读取并比较相邻完整hash。它不是抽样、截断hash或只检查分区内部；全量排序可能溢写磁盘，代价不是零。

## 服务器执行

1. 同步新版源码，确认同一run没有其他采集/校验/报告进程，安装更新：

```bash
.venv/bin/python -m pip install -e .
free -h
df -h .
```

2. 在 config.toml 顶层（任何 [assets] 表之前）添加：

```toml
analysis_memory_limit_mb = 8192
analysis_threads = 2
```

可选 `analysis_temp_directory = "/空间充足分区/opbnb-analysis-tmp"`。相对路径按config文件目录解析；不填则使用该run的 `.duckdb-tmp`。程序每个连接使用独立session子目录，不删用户指定目录或其他session内容。连接关闭后DuckDB释放其溢写文件，可能留下空session目录；未确认所有分析进程退出前不要手动清理临时目录。

8192MB是DuckDB预算，Python/PyArrow及其他进程也需要内存。它不是整机16GB的硬上限，也不保证任何数据分布都不OOM。先保持2线程，不随采集concurrency增加分析线程。磁盘总容量1TB不等于剩余1TB，请检查实际可用空间；不要同时启动多个各预算8GB的分析任务。

3. 使用本次实际run-id重新生成报告（不是最早建议的10/22结束的run）：

```bash
mkdir -p logs
nohup .venv/bin/opbnb-activity report --config config.toml --run-id mainnet-20260722-20261025 > logs/mainnet-20260722-20261025-report.log 2>&1 &
tail -f logs/mainnet-20260722-20261025-report.log
```

不需要再执行collect/enrich，不需要改QPS或分区大小。正常report完全离线，但会重新完整校验；大规模排序/关联依然可能耗时。

## 日志和失败处理

日志显示实际 `Analysis resources: memory_limit_mb=8192 threads=2`、类型扫描进度、分区关系检查及各全局hash排序阶段。若仍失败，quality.json的failure_stage给出安全的阶段标识；errors继续保留错误类别，不输出可能含凭据的原始异常文本。ok=false的报告仍被拒绝。

如果再次OOM，请提供新的quality.json、末尾阶段日志、free -h与临时目录所在磁盘的df -h结果。先定位哪个阶段和资源达到限制，不通过盲目增大预算/线程、删除分区、清空quality错误来处理。

## 验证边界

测试覆盖旧run身份兼容、资源覆盖真正进入DuckDB、错误配置、连接失败关闭、按分区关系检查、跨分区重复及批边界重复、异常安全和日志。未在本地重新执行服务器2亿笔真实数据；最终以服务器复跑结果为准，不承诺增加内存就线性提速或一定成功。

本地 Python3.12 / DuckDB2.0.0 / PyArrow26 全套375项测试通过（38.04秒），离线合成CLI生成26张CSV、摘要、附录和图表。额外以100万条完整SHA256哈希Parquet、64MB内存预算、单线程运行全局排序，实际观察到临时磁盘文件且完整读取100万行。该实验只验证落盘排序路径，不是2亿行校验/分析性能保证；最低DuckDB1.1版本未额外验证。
