# Elasticsearch性能测试报告

Elasticsearch的性能测试报告分为基于TPCDS的性能测试报告和基于业务数据的性能测试报告。

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

## 基于TPCDS的性能测试报告

### 测试目标

本次测试涵盖性能测试与功能测试，主要包含两个目的：

> - 展示XSQL在针对单数据源、多数据源的SQL解析及执行能力

> - 找出XSQL在复杂SQL处理以及大规模数据集计算时存在的问题

### 测试数据集大小：1G/9710124

### 测试语句集

| 编号 | SQL语句                                                      |
| ---- | ------------------------------------------------------------ |
| 1    | select * from myes.xitong_xsql_test.taxis                    |
| 2    | select * from myes.xitong_xsql_test.taxis where do_location_id="249" |
| 3    | select * from myes.xitong_xsql_test.taxis where fare_amount < 3 |
| 4    | select * from myes.xitong_xsql_test.taxis where dropoff_datetime is null |
| 5    | select * from myes.xitong_xsql_test.taxis where dropoff_datetime is not null |
| 6    | select * from myes.xitong_xsql_test.taxis where rate_code_id in ("1","2") |
| 7    | select  vendor_id,total_amount,trip_distance from myes.xitong_xsql_test.taxis where payment_type = "1" and tolls_amount = 0 |
| 8    | select * from myes.xitong_xsql_test.taxis order by total_amount desc |
| 9    | select * from myes.xitong_xsql_test.taxis order by vendor_id |
| 10   | select * from myes.xitong_xsql_test.taxis limit 20           |
| 11   | select count(payment_type),sum(total_amount) from myes.xitong_xsql_test.taxis |
| 12   | select avg(population) from myes.xitong_xsql_test.geonames_all |
| 13   | select payment_type from myes.xitong_xsql_test.taxis group by payment_type |
| 14   | select count(payment_type),payment_type from myes.xitong_xsql_test.taxis group by payment_type |
| 15   | select count(payment_type),payment_type,total_amount from myes.xitong_xsql_test.taxis group by payment_type,total_amount |
| 16   | select sum(total_amount) from myes.xitong_xsql_test.taxis group by vendor_id |
| 17   | select sum(population) from myes.xitong_xsql_test.geonames_all group by country_code,timezone |
| 18   | select sum(total_amount),vendor_id,payment_type from myes.xitong_xsql_test.taxis group by vendor_id,payment_type |
| 19   | select max(population) from myes.xitong_xsql_test.geonames_all group by country_code |
| 20   | select min(population),country_code from myes.xitong_xsql_test.geonames_all group by country_code |
| 21   | select avg(population) from myes.xitong_xsql_test.geonames_all group by country_code |
| 22   | select count(population),country_code from myes.xitong_xsql_test.geonames_all group by country_code order by country_code limit 100 |
| 23   | select avg(population),max(population),timezone,country_code from myes.xitong_xsql_test.geonames_all group by country_code,timezone order by max(population) limit 100 |
| 24   | select count(population),timezone,country_code from myes.xitong_xsql_test.geonames_all group by country_code,timezone  order by count(population) limit 100 |
| 25   | select distinct dem,location from myes.xitong_xsql_test.geonames_all |
| 26   | select count(distinct passenger_count) from myes.xitong_xsql_test.taxis |

### 测试结果（单位：秒）

| 执行引擎        | 1     | 2     | 3     | 4     | 5     | 6     | 7     | 8     | 9     | 10    | 11    | 12    | 13    | 14    | 15    | 16    | 17    | 18    | 19    | 20    | 21    | 22    | 23    | 24    | 25    | 26    |
| --------------- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- |
| XSQL [Pushdown] | 0.211 | 0.163 | 0.173 | 0.223 | 0.224 | 0.249 | 0.262 | 0.271 | 0.195 | 0.227 | 0.288 | 0.158 | 0.135 | 0.134 | 0.198 | 0.138 | 0.226 | 0.161 | 0.351 | 0.137 | 0.132 | 0.205 | 0.736 | 0.559 | 0.487 | 0.137 |
| ES              | 0.147 | 0.088 | 0.102 | 0.177 | 0.148 | 0.162 | 0.165 | 0.16  | 0.161 | 0.178 | 0.098 | 0.113 | 0.08  | 0.08  | 0.1   | 0.098 | 0.173 | 0.114 | 0.116 | 0.097 | 0.114 | 0.145 | 0.286 | 0.243 | 0.262 | 0.114 |

<img src="..\pictures\tpcds\ES-XSQL.jpg" />

### **结论**

- XSQL [Pushdown]相比于直接调用Elasticsearch的API，执行性能仅有轻微的损耗。



## 基于业务数据的性能测试报告

### 测试目标

本次测试涵盖性能测试与功能测试，主要包含两个目的：

> - 展示XSQL在针对单数据源、多数据源的SQL解析及执行能力

> - 找出XSQL在复杂SQL处理以及大规模数据集计算时存在的问题

### 测试数据集大小

| 数据源        | 50 G/63178212 docs                                          | 100 G/123714760 docs                                       | 200 G/260567692 docs                                        | 500G/371503959 docs                | 1 T /466168986 docs          |
| ------------- | ----------------------------------------------------------- | ---------------------------------------------------------- | ----------------------------------------------------------- | ---------------------------------- | ---------------------------- |
| Elasticsearch | logsget-user-qdas-newuser-channel-all.msdocker_frontmidres1 | logsget-user-qdas-newuser-channel-all.huajiao_frontmidres1 | logsget-user-qdas-newuser-channel-all.chromium_frontmidres1 | logsget-user-pc-profile-v1.profile | logsget-user-profile.profile |

### 测试语句集

- 选择操作

| 编号 | SQL                                                          |
| ---- | ------------------------------------------------------------ |
| 1    | SELECT m2, active_channel FROM msdocker_frontmidres1 WHERE m2 IS NOT NULL LIMIT 10 |
| 2    | SELECT m2, active_channel FROM huajiao_frontmidres1 WHERE m2 IS NOT NULL LIMIT 10 |
| 3    | SELECT m2, active_channel FROM chromium_frontmidres1 WHERE m2 IS NOT NULL LIMIT 10 |
| 4    | SELECT city, ip FROM logsget_user_pc_profile_v1.profile WHERE city IS NOT NULL LIMIT 10 |
| 5    | SELECT city, ip FROM logsget_user_profile.profile WHERE city IS NOT NULL LIMIT 10 |

- 聚合操作

| 编号 | SQL                                                          |
| ---- | ------------------------------------------------------------ |
| 1    | SELECT appkey, COUNT(m2) FROM msdocker_frontmidres1 WHERE m2 IS NOT NULL GROUP BY appkey LIMIT 10 |
| 2    | SELECT appkey, COUNT(m2) FROM huajiao_frontmidres1 WHERE m2 IS NOT NULL GROUP BY appkey LIMIT 10 |
| 3    | SELECT appkey, COUNT(m2) FROM chromium_frontmidres1 WHERE m2 IS NOT NULL GROUP BY appkey LIMIT 10 |
| 4    | SELECT province, COUNT(ip), SUM(pro_daohang), AVG(pro_daohang) FROM logsget_user_pc_profile_v1.profile WHERE ip IS NOT NULL GROUP BY province LIMIT 10 |
| 5    | SELECT pro_zhushou, COUNT(m2), SUM(pro_info_flow), AVG(pro_info_flow) FROM logsget_user_profile.profile WHERE m2 IS NOT NULL GROUP BY pro_zhushou LIMIT 10 |

- 子查询操作



- 连接操作



### 测试结果（单位：秒）

- 选择操作

|                               | 50G/63178212 docs | 100G/123714760 docs | 200G/260567692 docs | 500G/371503959 docs | 1T/466168986 docs |
| ----------------------------- | ----------------- | ------------------- | ------------------- | ------------------- | ----------------- |
| Elasticsearch                 | 0.399 s           | 0.558 s             | 0.949 s             | 1.080 s             | 0.381 s           |
| XSQL Pushdown [Elasticsearch] | 0.426 s           | 0.575 s             | 1.025 s             | 1.107 s             | 0.407 s           |
| XSQL [Elasticsearch]          | 324.813           | 568.389             | 1045.91             | 1121.31             | 271.082           |

<img src="..\pictures\tpcds\ES选择操作柱状图-all.jpg" />

<img src="..\pictures\tpcds\ES选择操作柱状图.jpg" />

- 聚合操作

|                               | 50G/63178212 docs | 100G/123714760 docs | 200G/260567692 docs | 500G/371503959 docs | 1T/466168986 docs |
| ----------------------------- | ----------------- | ------------------- | ------------------- | ------------------- | ----------------- |
| Elasticsearch                 | 0.774 s           | 1.436 s             | 2.665 s             | 0.096 s             | 0.094 s           |
| XSQL Pushdown [Elasticsearch] | 0.790 s           | 1.478 s             | 2.671 s             | 0.135 s             | 0.130 s           |
| XSQL [Elasticsearch]          | 338.514           | 896.007             | 2507.09             | 3915.32             | 3886.388          |

<img src="..\pictures\tpcds\ES聚合操作柱状图-all.jpg" />

<img src="..\pictures\tpcds\ES聚合操作柱状图.jpg" />

- 子查询操作

  ES本身不支持where条件后的子查询操作，故本报告不做此处的测试。

- 连接操作

  ES本身支持的连接操作非常有限且昂贵，故本报告不做此处的测试。

### **结论**

- XSQL [Pushdown]相比于直接调用Elasticsearch的API，执行性能仅有约30毫秒的损耗。
- XSQL借助于Spark执行时，执行效率很低。