# MongoDB性能测试报告

MongoDB的性能测试报告分为基于TPCDS的性能测试报告和基于业务数据的性能测试报告。

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

### 测试数据集

- 数据量一：1G/9710124
- 数据量二：14G/113496874

### 测试语句集

| 编号 | SQL语句                                                      |
| ---- | ------------------------------------------------------------ |
| 1    | select * from mymongo.xitong_mongo.taxis                     |
| 2    | select VendorID, trip_distance from mymongo.xitong_mongo.taxis |
| 3    | select * from mymongo.xitong_mongo.taxis order by trip_distance |
| 4    | select * from mymongo.xitong_mongo.taxis where DOLocationID = 229 |
| 5    | select * from mymongo.xitong_mongo.taxis where RatecodeID is null |
| 6    | select * from mymongo.xitong_mongo.taxis where payment_type in (1,2) |
| 7    | select * from mymongo.xitong_mongo.taxis where tpep_pickup_datetime like '2017-01-01 00:00:0_' |
| 8    | select sum(total_amount) from mymongo.xitong_mongo.taxis     |
| 9    | select sum(total_amount),  payment_type from mymongo.xitong_mongo.taxis group by payment_type |
| 10   | select count(*) from mymongo.xitong_mongo.taxis              |
| 11   | select count(*),  payment_type from mymongo.xitong_mongo.taxis group by payment_type |
| 12   | select distinct payment_type from mymongo.xitong_mongo.taxis |
| 13   | select count(distinct payment_type) from mymongo.xitong_mongo.taxis |

### 测试结果一（单位：秒，数据量：1G/9710124）

| 执行引擎      | 1     | 2     | 3      | 4      | 5      | 6      | 7      | 8      | 9      | 10     | 11     | 12     | 13     |
| ------------- | ----- | ----- | ------ | ------ | ------ | ------ | ------ | ------ | ------ | ------ | ------ | ------ | ------ |
| Mongo   API   | 0.849 | 0.459 | 15.184 | 0.466  | 5.555  | 0.468  | 0.471  | 12.652 | 13.865 | 4.239  | 10.48  | 10.151 | 10.222 |
| XSQL Pushdown | 0.458 | 0.172 | 14.859 | 0.135  | 5.456  | 0.1    | 0.494  | 12.291 | 13.557 | 4.444  | 10.044 | 9.845  | 9.863  |
| XSQL          | 6.29  | 3.169 | 64.728 | 28.466 | 10.778 | 21.097 | 22.592 | 12.197 | 18.77  | 54.345 | 13.145 | 13.615 | 15.662 |

<img src="..\pictures\tpcds\MongoDB-XSQL.jpg" />

### **结论**

- XSQL [Pushdown]相比于直接调用MongoDB的API，性能上几乎没有损耗。
- 在小数据量情况下，XSQL使用Spark的效率相对来说不高。

### 测试结果二（单位：秒，数据量：14G/113496874）

| 执行引擎        | 1     | 2     | 3       | 4       | 5       | 6       | 7       | 8       | 9       | 10      | 11      | 12      | 13      |
| --------------- | ----- | ----- | ------- | ------- | ------- | ------- | ------- | ------- | ------- | ------- | ------- | ------- | ------- |
| Mongo   API     | 0.788 | 0.513 | 236.142 | 0.508   | 117.646 | 0.497   | 0.521   | 207.773 | 224.417 | 97.639  | 182.1   | 179.221 | 179.229 |
| XSQL   Pushdown | 0.617 | 0.129 | 233.148 | 0.103   | 119.321 | 0.067   | 0.096   | 204.984 | 223.216 | 98.152  | 181.033 | 177.489 | 178.387 |
| XSQL            | 6.063 | 8.793 | 435.851 | 407.283 | 230.014 | 362.561 | 347.276 | 50.207  | 71.676  | 430.705 | 52.205  | 54.794  | 48.358  |

<img src="..\pictures\tpcds\MongoDB2-XSQL.jpg" />

### **结论**

- XSQL [Pushdown]相比于直接调用MongoDB的API，性能上几乎没有损耗。
- 在大数据量情况下，对于投影操作来说，XSQL使用Spark的效率相对来说不高，对于分组、聚合等运算Spark反而有优势。

## 基于业务数据的性能测试报告（暂无）
