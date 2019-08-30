

# 混合查询性能测试报告

混合性能测试报告是基于业务数据的性能测试报告。用于展现多个数据源之间混合查询的性能。

## 测试环境

**操作系统**：	CentOS release 6.2 (Final)
​	
**虚拟机版本**： Java HotSpot(TM) 64-Bit Server VM (build 25.60-b23, mixed mode)
​	
**测试机器**：	client01v.qss.zzzc.qihoo.net、clientadmin.dfs.shbt.qihoo.net

**XSQL配置**：	

- Driver Memory：5G
- Executor Instances：100
- Executor Memory：1536M或5G

## 测试目标

本次测试涵盖性能测试与功能测试，主要包含两个目的：

> - 展示XSQL在针对单数据源、多数据源的SQL解析及执行能力

> - 找出XSQL在复杂SQL处理以及大规模数据集计算时存在的问题

## 测试数据集大小

**数据均为业务部门的线上数据。**

| 数据源        | 50 G/63178212 docs                                          | 100 G/123714760 docs                                       | 200 G/260567692 docs                                        | 500G/371503959 docs                | 1 T /466168986 docs          |
| ------------- | ----------------------------------------------------------- | ---------------------------------------------------------- | ----------------------------------------------------------- | ---------------------------------- | ---------------------------- |
| Elasticsearch | logsget-user-qdas-newuser-channel-all.msdocker_frontmidres1 | logsget-user-qdas-newuser-channel-all.huajiao_frontmidres1 | logsget-user-qdas-newuser-channel-all.chromium_frontmidres1 | logsget-user-pc-profile-v1.profile | logsget-user-profile.profile |

| 数据源      | 5 G/9105516 rows   | 10 G/116119018 rows | 20 G/26252422 rows                | 50 G/317473749 rows               | 100 G/450071755 rows |
| ----------- | ------------------ | ------------------- | --------------------------------- | --------------------------------- | -------------------- |
| Hive (shbt) | pre_newusergamepay | gbc_20171128_hot_1  | shoujizhushou_push_logshare_event | pre_qdas_huajiao_unlogin_behavior | pre_usergame_monthly |

| 数据源      | 50 M/1062854 rows              | 100 M/3230371 rows         | 500 M/9459722 rows                  | 1 G/10100177 rows                                            | 2 G/29055658 rows                |
| ----------- | ------------------------------ | -------------------------- | ----------------------------------- | ------------------------------------------------------------ | -------------------------------- |
| MySQL (DB7) | rpt_mso_english_result_q_daily | rpt_shouyou_week_retention | rpt_360game_plugin_new_report_daily | rpt_cloudlive_drop_hourly1（后来更换为rpt_mso_chns_360aphone_query_pvuv_daily） | rpt_cloudlive_errornum_all_daily |

## Elasticsearch与Hive的混合性能测试报告

### 测试语句集

| 编号 | SQL                                                          |
| ---- | ------------------------------------------------------------ |
| 1    | SELECT a.serverip, b.active_channel FROM (SELECT serverip FROM default.logsget.shoujizhushou_push_logshare_event WHERE serverip = '59.44.240.36') a JOIN (SELECT active_channel FROM myes.logsget_user_qdas_newuser_channel_all.msdocker_frontmidres1 WHERE channel = '100054' LIMIT 10) b ON a.serverip = b.active_channel |
| 2    | (SELECT serverip FROM default.logsget.shoujizhushou_push_logshare_event) UNION (SELECT active_channel FROM myes.logsget_user_qdas_newuser_channel_all.msdocker_frontmidres1 WHERE channel = '100054' LIMIT 10) |
| 3    | SELECT serverip, model FROM default.logsget.shoujizhushou_push_logshare_event WHERE serverip IN (SELECT channel FROM myes.logsget_user_qdas_newuser_channel_all.msdocker_frontmidres1 LIMIT 10) |
| 4    | SELECT first_table.active_channel FROM (SELECT active_channel FROM myes.logsget_user_qdas_newuser_channel_all.msdocker_frontmidres1 WHERE channel = '100054' LIMIT 10) first_table WHERE first_table.active_channel IS NOT NULL |

### 测试结果（单位：秒）

| 执行引擎                                   | Join    | Union   | SubQuery（WHERE） | SubQuery（FROM） |
| ------------------------------------------ | ------- | ------- | ----------------- | ---------------- |
| XSQL[ElasticSearch]   & XSQL[Hive]         | 201.862 | 190     | 360               | 21.183           |
| XSQL Pushdown [ElasticSearch] & XSQL[Hive] | 141.476 | 153.667 | 125.207           | 0.249            |

<img src="..\pictures\calcite\ES-HIVE.jpg" />

### **结论**

- XSQL Pushdown相比于XSQL，执行ElasticSearch查询，性能有较大幅度的提升。

## MySQL与Hive的混合性能测试报告

### 测试语句集

| 编号 | SQL                                                          |
| ---- | ------------------------------------------------------------ |
| 1    | select a.serverip, b.bid from (SELECT serverip FROM default.logsget.shoujizhushou_push_logshare_event where serverip = '59.44.240.36') as a join (SELECT bid FROM  test_mysql.db_share.rpt_cloudlive_errornum_all_daily where bid = 'huajiao' limit 10) as b on(a.serverip = b.bid) |
| 2    | (SELECT serverip FROM default.logsget.shoujizhushou_push_logshare_event) UNION (SELECT bid FROM  test_mysql.db_share.rpt_cloudlive_errornum_all_daily where bid = 'huajiao' limit 10) |
| 3    | SELECT serverip, model FROM default.logsget.shoujizhushou_push_logshare_event WHERE serverip in (SELECT cid FROM  test_mysql.db_share.rpt_cloudlive_errornum_all_daily LIMIT 10) |
| 4    | SELECT serverip, (SELECT COUNT(bid) FROM test_mysql.db_share.rpt_cloudlive_errornum_all_daily where cid = 'live_huajiao_v2') FROM  default.logsget.shoujizhushou_push_logshare_event WHERE serverip = '59.44.240.36' |

### 测试结果（单位：秒）

| 执行引擎                          | Join     | Union | SubQuery(WHERE [MySQL]) | SubQuery(SELECT [MySQL]) |
| --------------------------------- | -------- | ----- | ----------------------- | ------------------------ |
| XSQL[MySQL] & XSQL[Hive]          | 196.0173 | 241.5 | 165.3875                | 230.1265                 |
| XSQL Pushdown[MySQL] & XSQL[Hive] | 169.1963 | 183   | 163.9458                | 220.5993                 |

<img src="..\pictures\calcite\MySQLandHive.JPG" />

### 结论

由上图分析可知，执行MySQL与Hive的混合查询时,XSQL PushDown相比于XSQL,性能有一定幅度的提升，执行join操作和union操作的提升效率明显。

## MySQL与ElasticSearch的混合性能测试报告

### 测试语句集

| 编号 | SQL                                                          |
| ---- | ------------------------------------------------------------ |
| 1    | select a.active_channel, b.bid from (SELECT active_channel FROM myes.logsget_user_qdas_newuser_channel_all.msdocker_frontmidres1 where channel='100054' limit 10) as a join (SELECT bid FROM test_mysql.db_share.rpt_cloudlive_errornum_all_daily where bid = 'huajiao' limit 10) as b on(a.active_channel = b.bid) |
| 2    | (SELECT active_channel FROM myes.logsget_user_qdas_newuser_channel_all.msdocker_frontmidres1 where channel='100054' limit 10) UNION (SELECT bid FROM test_mysql.db_share.rpt_cloudlive_errornum_all_daily where bid = 'huajiao' limit 10) |
| 3    | SELECT channel FROM myes.logsget_user_qdas_newuser_channel_all.msdocker_frontmidres1 WHERE channel in (SELECT cid FROM test_mysql.db_share.rpt_cloudlive_errornum_all_daily LIMIT 10) |
| 4    | SELECT cid FROM test_mysql.db_share.rpt_cloudlive_errornum_all_daily where cid in (SELECT channel FROM myes.logsget_user_qdas_newuser_channel_all.msdocker_frontmidres1 limit 10) |
| 5    | select active_channel,(select count(bid) from test_mysql.db_share.rpt_cloudlive_errornum_all_daily) from myes.logsget_user_qdas_newuser_channel_all.msdocker_frontmidres1 where channel='100054' limit 10 |
| 6    | select cid,(select count( active_channel) from myes.logsget_user_qdas_newuser_channel_all.msdocker_frontmidres1) from test_mysql.db_share.rpt_cloudlive_errornum_all_daily where cid  = 'live_polo' |

## 测试结果（单位：秒）

| 执行引擎                                              | Jion   | Union   | SubQuery（WHERE[MySQL]） | SubQuery（WHERE[ES]） | SubQuery（SELECT[MySQL]） | SubQuery（SELECT[ES]） |
| ----------------------------------------------------- | ------ | ------- | ------------------------ | --------------------- | ------------------------- | ---------------------- |
| XSQL   Pushdown[MySQL] & XSQL Pushdown[ElasticSearch] | 0.56   | 1.1315  | 262.07675                | 11.40975              | 15.31425                  | 2.5765                 |
| XSQL[MySQL] &   XSQL Pushdown[ElasticSearch]          | 7.941  | 6.53375 | 288.0025                 | 11.40975              | 20.77525                  | 2.5765                 |
| XSQL Pushdown[MySQL] & XSQL[ElasticSearch]            | 20.4   | 9.977   | 262.07675                | error                 | 15.31425                  | 287                    |
| XSQL[MySQL] &   XSQL[ElasticSearch]                   | 20.745 | 15.75   | 288.0025                 | error                 | 20.77525                  | 287                    |

<img src="..\pictures\calcite\MySQLandES.JPG" />

### 结论

上图分析可知，执行ElasticSearch与MySQL的混合查询时，两个数据源下推的执行效率要高于非下推的执行效率；尤其对于ES的执行，非下推时执行很慢，并且有时子查询执行会出现超时错误。