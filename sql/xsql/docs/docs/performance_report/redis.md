# Redis性能测试报告

Redis的性能测试报告主要是基于TPCDS的性能测试报告。

## 测试环境

**操作系统**：	CentOS release 6.2 (Final)
​	
**虚拟机版本**： Java HotSpot(TM) 64-Bit Server VM (build 25.60-b23, mixed mode)
​	

**XSQL配置**：	

- Driver Memory：5G

## 基于TPCDS的性能测试报告

### **测试目标**

本次测试旨在对比XSQL下推与jedis api的性能表现。

### 测试数据集

- 数据量一：2W条
- 数据量二：10W条

### 测试结果汇总

| 序号 | jedis api | xsql  | sql语句                                   |
| ---- | -------- | ----- | ----------------------------------------- |
|1|0.003|0.479|select * where key = 'geonames2w:10000029';|
|2|0.005|0.142|select * from geonames2w where suffix in ('10000028','10000030');|
|3|55.818|59.991|select * from geonames2w;|
|4|0.003|0.465|select * where key = 'geonames10w:10000029';|
|5|0.006|0.072|select * from geonames10w where suffix in ('10000028','10000030');|
|6|297.196|294.35|select * from geonames10w;|
|7|0.004|0.047|select * where key = 'geonames10w:10000029';|
|8|0.006|0.051|select * from geonames100w where suffix in ('10000028','10000030');|
|9|2832.887|2960.651|select * from geonames100w;|

采用pipeline方法后：

| 序号 | jedis get key only | xsql    | sql语句                     |
| ---- | ------------------ | ------- | --------------------------- |
| 3    | 5.17               | 8.219   | select * from geonames2w;   |
| 6    | 25.895             | 30.513  | select * from geonames10w; |
| 9    | 261.946            | 333.238 | select * from geonames100w; |

### 结论

- 对于点查询，使用jedis api查询用时为0.003s，使用xsql查询用时约为0.05s。
- 对于scan类型的查询，使用jedis api和xsql的查询性能均不理想，用时估算方法为：每1万/延迟3s。
- 更新：采用pipeline方法后，用时估算方法为第1万/延迟0.3s。
