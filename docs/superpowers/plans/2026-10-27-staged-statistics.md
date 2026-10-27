# Disk-staged exact statistics repair

User-approved design: partition-local transaction/receipt association, disk intermediates, independent exact dedup/retention stages, meaningful phase logs, whole-pipeline low-memory Parquet tests. Never sum day/project DAU into window users, never recollect or weaken quality. Keep existing26tables/financial precision/identity/labels/cache provenance. No commits unless separately requested.

## Interfaces and task owners

1. Root: report-owned temporary directory under configured spill parent; persistent DuckDB database, partition-local activity and successful non-system eligible_logs staging; existing Store.connection(database=None) gets optional owned DB path. `analysis_stages.materialize(connection, name, query, parameters=None)` executes logged `CREATE TABLE name AS query` into disk catalog, drains result, returns None; static safe identifiers, logs start/end/error. `analysis_stages.run_query(connection,name,query,parameters=None)` returns cursor with same logs (callers consume). Root ensures exception cleanup closesconnection beforeowned directory deletion, raw files unchanged, report error stage known.
2. Core worker: new core_metrics.py + core SQL files only + new tests/test_core_stages.py. `prepare_core(connection)` creates persistent `report_sender_day` (non-system one row per sender/day, successful flag plus counts) and `report_senders` (one row per non-system sender with successful_transactions etc). `core_tables(connection)` returns dict keys daily,contracts,projects,senders,retention; `period_tables(connection)` returns weekly/monthly lists (raw coverage laterexistingperiods); `unique_sender_counts(connection)` returns unique_successful_senders,unique_all_non_system_senders; `top_sender_transactions(connection,limit)` exact successful countsum. Inputs activity staged persistenttable (reduced independenttestviews allowed). Use materialized GROUP BY dedup tables, no unbounded multiple COUNT(DISTINCT) aggregates. Top100 contracts chosen by full exact counts BEFORE expensive successful-sender distinct; preserve null-creation & label interval semantics. TopN tie order unchanged.
3. Augmentation worker: activity_metrics.py, attribution.py + newtests/test_augmentation_stages.py only. Use same helper materialize/run_query. Keep existing metrics-only tests which create reduced schemas. Split hourly/dayproject/recipient sender dedup and sender profiles into separate persistent stages, eliminate all large COUNT(DISTINCT) states. Preserve COUNT_FIELDS/public APIs/outputsemantics, zero days/topN deterministic and exactvalues. No source logs/metadata APIs changes. Avoid changing SELECT lists expectedby tests unlessreviewed.
4. Root enrichment: targets report path uses eligible_logs ifpresent, no global logs×transactions×receipts join. Events exact count via address/topic0/tx pre-group; metadata correct. Standaloneenrich oldpath retained unlessit can useboundedjoins cheaply (not requiredforreport).

## TDD and validation

- Write missing-interface/phase/equivalence tests, witness expectedfails beforeimplementation. Originalfixtureexpectedcounts/money preserved.
- Physical partition-local staging mustassert each rawjoin sees onefile pertable; reuse activity/eligible_logs storedtables inlaterqueries notrawjoins. Source bytes/checkpoint/manifest identity unchanged on successandexception; scratchownedcleanup only. Include invalid stage SQL andOOMtoverifyfailurelogstage.
- Whole report lowmemory syntheticParquet (>=100k tx, highcardinality, multiplephysicalparts, 128MBor256MB) through quality+staging+all26exports, expected exact results. Test fullschema andquery equivalence across dates/users/projects/labelsintervals/empty/creation/failed/metadata/retention. 100m testcase notclaimed. No sampling orapproximate distinct.
- Independent finalreview performance/correctness. Run fullsuite plus offlineCLI smoke, fixfailuresin-scope. Transfer onlychangedfilesafteroriginalcleancheck, no data/schema/configidentity rewrite.

## Delivery

Document configured8GB/2threads, extra scratchdisk for persistentintermediateDB, no newservice/externaldatabase, no automaticqualityskip, no repeatedmanualrescan. Log phase names so anyremainingOOM maps a concretequery. State actualtestedsize andlimits; do notpromise200mperformance fromsmallfixture.

## Execution record

Completed staging, all core/augmentation/enrichment exact dedup rewrites, phase errors/status, docs and tests. TDD observedmissinginterfaces, missingreportstagingcall and spillownership failures beforeimplementation/fix. Core/augmentation implementers tested against oldSQL definitions and reducedschema APIs. Independent finalreview found noP1/P2; its emptyspilldirectory observation was fixed by placing persistentconnection spill withinownedworkspace (red/green assertion).

Full suite388passed in47.19seconds with real DuckDB2/PyArrow26. Includes25万typedParquettx,187500differentnon-systemsenders,5physicalpartitions,128MBDuckDB/1thread, wholequality→all26CSV/summary/charts, exactwei/token/logs/userdedup and sourcebyteschecks. Rootsourcecopy willbeSHAverified; no commit. Server212mrawdata unavailablelocally, so no server-scale success/time/diskguarantee.
