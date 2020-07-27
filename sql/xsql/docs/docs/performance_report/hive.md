# Hive性能测试报告

Hive的性能测试报告分为基于TPCDS的性能测试报告和基于业务数据的性能测试报告。

## 测试环境

**操作系统**：	CentOS release 6.2 (Final)
​	
**虚拟机版本**： Java HotSpot(TM) 64-Bit Server VM (build 25.60-b23, mixed mode)
​	

**XSQL配置**：	

- Driver Memory：5G
- Executor Instances：100
- Executor Memory：1536M或5G

## 基于TPCDS的性能测试报告

### 测试目标

本次测试涵盖性能测试与功能测试，主要包含两个目的：

> - 展示XSQL在针对单数据源、多数据源的SQL解析及执行能力

> - 找出XSQL在复杂SQL处理以及大规模数据集计算时存在的问题

### 测试数据集大小：1G/9710124

### 测试语句集

| 编号 | SQL语句                                                      |
| ---- | ------------------------------------------------------------ |
| 1    | select * from taxi where   trip_distance=7.75 and ratecodeid=1 limit 10; |
| 2    | select count(*) from taxi;                                   |
| 3    | select count(*) from taxi   where mta_tax >100;              |
| 4    | select   sum(trip_distance),store_and_fwd_flag from taxi group by store_and_fwd_flag; |
| 5    | select max(mta_tax) as max_tax   from taxi order by max_tax desc limit 10; |
| 6    | select avg(trip_distance) as   avg_dis,max(trip_distance) as max_dis,vendorid,store_and_fwd_flag from taxi   group by vendorid,store_and_fwd_flag order by max_dis desc limit 100; |
| 7    | select sum(t1.trip_distance)   from taxi t1 join taxi2 t2  on   t1.pulocationid = t2.pulocationid where t2.pulocationid>300; |
| 8    | select   count(*),avg(t1.trip_distance) as avg_dis,t1.dolocationid ,t1.pulocationid   from taxi t1 join taxi2 t2  on   t1.vendorid = t2.vendorid  where   t2.vendorid='1' and t2.pulocationid>=360 group by   t1.pulocationid,t1.dolocationid order by avg_dis desc; |
| 9    | select max(trip_distance) as   max_dis,pulocationid from taxi where pulocationid>200 and   pulocationid<300 group by pulocationid order by max_dis desc; |
| 10   | create table taxi_copy from   select * from taxi where trip_distance=7.75 order by trip_distance desc; |

### 测试结果（单位：秒）

| 执行引擎 | 1      | 2      | 3       | 4      | 5      | 6       | 7       | 8       | 9       | 10      |
| -------- | ------ | ------ | ------- | ------ | ------ | ------- | ------- | ------- | ------- | ------- |
| Hive     | 66.408 | 190.93 | 122.291 | 134.45 | 104.48 | 231.631 | 244.139 | 448.953 | 295.705 | 139.184 |
| XSQL     | 5.515  | 42.463 | 34.983  | 53.079 | 29.424 | 63.561  | 118.163 | 95.816  | 69.001  | 193.906 |

<img src="..\pictures\tpcds\Hive-XSQL.jpg" />

### **结论**

- XSQL相比于Hive，执行性能得到明显的提升。



## 基于业务数据的性能测试报告

### 测试目标

本次测试涵盖性能测试与功能测试，主要包含两个目的：

> - 展示XSQL在针对单数据源、多数据源的SQL解析及执行能力

> - 找出XSQL在复杂SQL处理以及大规模数据集计算时存在的问题

### 测试数据集大小

| 引擎\表名   | 5 G/9105516 rows   | 10 G/116119018 rows | 20 G/26252422 rows                | 50 G/317473749 rows               | 100 G/450071755 rows |
| ----------- | ------------------ | ------------------- | --------------------------------- | --------------------------------- | -------------------- |
| Hive (shbt) | pre_newusergamepay | gbc_20171128_hot_1  | shoujizhushou_push_logshare_event | pre_qdas_huajiao_unlogin_behavior | pre_usergame_monthly |

### 测试语句集

| 编号 | SQL                                                          |
| ---- | ------------------------------------------------------------ |
| 1    | SELECT count(channel2) FROM pre_newusergamepay WHERE channel2 IS NOT NULL |
| 2    | SELECT count(pos) FROM gbc_20171128_hot_1 WHERE pos IS NOT NULL |
| 3    | SELECT count(m1) FROM shoujizhushou_push_logshare_event WHERE m1 IS NOT NULL |
| 4    | SELECT count(watch_tag) FROM pre_qdas_huajiao_unlogin_behavior WHERE watch_tag IS NOT NULL |
| 5    | SELECT count(plat_channel3) FROM pre_usergame_monthly WHERE plat_channel3 IS NOT NULL |
| 6    | SELECT game_id, count(channel2), sum(p_day_id),avg(p_day_id) FROM pre_newusergamepay WHERE channel2 IS NOT NULL GROUP BY game_id |
| 7    | SELECT m2, count(pos) FROM gbc_20171128_hot_1 WHERE pos IS NOT NULL GROUP BY m2 |
| 8    | SELECT model, count(m1) FROM shoujizhushou_push_logshare_event WHERE m1 IS NOT NULL GROUP BY model |
| 9    | SELECT servercountry, count(watch_tag) FROM pre_qdas_huajiao_unlogin_behavior WHERE watch_tag IS NOT NULL GROUP BY servercountry |
| 10   | SELECT plat_id, count(plat_channel3),sum(paycnt),avg(paycnt) FROM pre_usergame_monthly WHERE plat_channel3 IS NOT NULL GROUP BY plat_id |
| 11   | SELECT count(channel2) FROM pre_newusergamepay WHERE channel2 in (SELECT channel2 FROM pre_newusergamepay as table2 WHERE channel2 IS NOT NULL) |
| 12   | SELECT count(pos) FROM gbc_20171128_hot_1 WHERE pos in (SELECT pos FROM gbc_20171128_hot_1 as table2 WHERE pos IS NOT NULL) |
| 13   | SELECT count(m1) FROM shoujizhushou_push_logshare_event WHERE m1 in (SELECT m1 FROM shoujizhushou_push_logshare_event as table2 WHERE m1 IS NOT NULL) |
| 14   | SELECT count(watch_tag) FROM pre_qdas_huajiao_unlogin_behavior WHERE watch_tag in (SELECT distinct watch_tag FROM pre_qdas_huajiao_unlogin_behavior as table2 WHERE watch_tag IS NOT NULL) |
| 15   | SELECT count(plat_channel3) FROM pre_usergame_monthly WHERE plat_channel3 in (SELECT plat_channel3 FROM pre_usergame_monthly as table2 WHERE plat_channel3 IS NOT NULL) |
| 16   | SELECT count(a.channel2), count(b.game_id) FROM pre_newusergamepay a JOIN pre_newusergamepay b ON(a.user_id = b.user_id) |
| 17   | SELECT count(a.pos), count(b.uid) FROM gbc_20171128_hot_1 a JOIN gbc_20171128_hot_1 b ON(a.uid= b.uid) WHERE a.uid != 'UNKNOWN' and b.uid != 'UNKNOWN' |
| 18   | SELECT count(a.m1), count(b.servercountry) FROM shoujizhushou_push_logshare_event a JOIN shoujizhushou_push_logshare_event b ON(a.serverip= b.serverip) |
| 19   | SELECT count(a.watch_tag), count(b.model) FROM pre_qdas_huajiao_unlogin_behavior a JOIN pre_qdas_huajiao_unlogin_behavior b ON(a.liveid= b.liveid) |
| 20   | SELECT count(a.plat_channel3), count(b.game_id) FROM pre_usergame_monthly a JOIN pre_usergame_monthly b ON(a.user_id= b.user_id) |

### 测试结果（单位：秒）

- 选择操作

|                   | 5 G/9105516 rows | 10 G/116119018 rows | 20 G/26252422 rows | 50 G/317473749 rows | 100 G/450071755 rows |
| ----------------- | ---------------- | ------------------- | ------------------ | ------------------- | -------------------- |
| Hive              | 288.921 s        | 299.864 s           | 178.928 s          | 176.595 s           | 247.195 s            |
| XSQL [Hive] 1536M | 80.288 s         | 79.956 s            | 62.488 s           | 80.713 s            | 124.946 s            |
| XSQL [Hive] 5G    | 87.433 s         | 76.751 s            | 67.481 s           | 118.724 s           | 124.656 s            |

<img src="..\pictures\tpcds\Hive选择操作柱状图.jpg" />

- 聚合操作

|                   | 5 G/9105516 rows | 10 G/116119018 rows | 20 G/26252422 rows | 50 G/317473749 rows | 100 G/450071755 rows |
| ----------------- | ---------------- | ------------------- | ------------------ | ------------------- | -------------------- |
| Hive              | 265.160 s        | 332.685 s           | 197.688 s          | 271.324 s           | 363.762 s            |
| XSQL [Hive] 1536M | 55.571 s         | 139.857 s           | 85.429 s           | 79.161 s            | 171.741 s            |
| XSQL [Hive] 5G    | 69.143 s         | 90.857 s            | 62.013 s           | 88.219 s            | 148.372 s            |

<img src="..\pictures\tpcds\Hive聚合操作柱状图.jpg" />

- 子查询操作

  Hive本身不支持where条件后的子查询操作，故本报告不做此处的测试。

|                   | 5 G/9105516 rows | 10 G/116119018 rows | 20 G/26252422 rows | 50 G/317473749 rows | 100 G/450071755 rows |
| ----------------- | ---------------- | ------------------- | ------------------ | ------------------- | -------------------- |
| Hive              | 484.135 s        | 513.226 s           | 267.819 s          | 668.408 s           | 842.294 s            |
| XSQL [Hive] 1536M | 150.691 s        | X                   | 81.608 s           | 264.522 s           | 363.889 s            |
| XSQL [Hive] 5G    | 126.336 s        | 186.671 s           | 63.181 s           | 248.902 s           | 261.815 s            |

<img src="..\pictures\tpcds\Hive子查询操作柱状图.jpg" />

- 连接操作

|                   | 5 G/9105516 rows | 10 G/116119018 rows | 20 G/26252422 rows | 50 G/317473749 rows | 100 G/450071755 rows |
| ----------------- | ---------------- | ------------------- | ------------------ | ------------------- | -------------------- |
| Hive              | 428.481 s        | 2058.069 s          | 521.880 s          | 6674.270 s          | 552.231 s            |
| XSQL [Hive] 1536M | 168.356 s        | 348.017 s           | 162.350 s          | 778.396 s           | 335.320 s            |
| XSQL [Hive] 5G    | 142.399 s        | 188.24 s            | 81.000 s           | 558.605 s           | 211.967 s            |

<img src="..\pictures\tpcds\Hive连接操作柱状图.jpg" />

### **结论**

- XSQL相比于Hive，执行性能得到明显的提升。
- 子查询、连接操作，XSQL配给Executor的内存多少，对执行时间也有影响。
