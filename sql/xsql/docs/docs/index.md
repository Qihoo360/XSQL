![XSQL-logo](images\XSQL-300200.png)

# Contents
=================

- [项目概况-Overview](getting_started/Getting_Started.md)
- [自定义配置-Configuration](tutorial/configuration.md)
- [特殊语法-Special Syntax](tutorial/syntax.md)
- [支持的API](tutorial/api.md)
- [数据源详情-Data Sources](datasources/common.md)
- [性能报告-Performance Report](performance_report/common.md)
- [常见问题-Troubleshooting](troubleshooting/common.md)
- [更新日志-ReleaseNote](#version-compatibility)

# External Resources

[360分布式查询引擎XSQL：技术揭秘与使用介绍](getting_started/360分布式查询引擎XSQL技术揭秘与使用介绍.pdf)

[XSQL推广PPT](getting_started/xsql推广ppt.pdf)

# Release Notes

| 版本号           | 发布日期       | Bug                  | Improvement | New Feature |
| ---------------- | -------------------- | ----------- | ----------- | ---------------- |
| 2.3.1.xsql-0.1.0 | 2019.01.31 |  |  |  |
| 2.3.1.xsql-0.2.0 | 2019.03.04 | 1.Desc Table展示Elasticsearch时，列类型显示不正确<br />2.解决NoSuchDatabaseException异常数据库名和数据源名称颠倒的问题<br />3.Alter table时，无法将Spark的列类型转换为Elasticsearch的<br />4.将XSQL内部的数据库名转化为物理上的实际数据库名称<br />5.修复SQL页面丢失的问题<br />6.RenameXSQLTable must update the collection property in CatalogTable. | 1.支持对limit 0时的探测<br />2.show databases将数据库名放在第一列，show tables将表名放在第一列<br />3.优化ElasticSearchManager，抽象getTableOption<br />4.避免编译Spark core模块<br />5.完善XSQLSqlParser的注释<br />6.实现默认listDatabases(ds, pattern)<br />7.Add default options for ES and mongo. | 1.增加Cache Level机制<br />2.支持 SQL页面显示查询语句<br />3.HBase限制非索引字段查询和增删表, force=true强制开启<br />4.Add rename table syntax for MongoDB. |
| 2.3.1.xsql-0.2.1 | 2019.03.06 | 1.解决spark.mongodb.input.uri没有传递给CatalogTable的问题<br />2.解决部分jersey包冲突 | 更新XSQL文档，增加对Cache Level、Whitelist、spark.xsql.properties.file等的描述 |  |
| 2.3.1.xsql-0.2.2 | 2019.03.14 | 1.解决：解析Elasticsearch的object类型时出错的问题<br />2.解决：当Elasticsearch的discover文件不存在时出错的问题<br />3.解决：Elasticsearch relation没有包含es.read.field.as.array.include属性的问题<br />4.解决：Elasticsearch的float和array[float\]在解析时发生混淆的问题 | 1.增加运维诊断文档<br />2.减少耦合，不再修改InsertIntoHiveTable | 1.增加Add datasource语法<br />2.增加Remove\|Refresh datasource语法<br />3.增加探索Elasticsearch的字段schema的功能 |
| 2.3.1.xsql-0.2.3 | 2019.03.22 | 1.解决：Elasticsearch下推解析数据时，字段为null时的异常<br />2.解决：Druid在SQL包含中文时解析出错的问题<br />3.解决：执行select * group by时出错的问题 | 1.Druid查询时没有指定__time时,友好提示用户<br />2.完善Getting_Started文档的用例<br />3.前置SQL语法校验到Local模式切换Yarn模式之前 | 1.Druid支持二级缓存 |
| 2.3.1.xsql-0.3.0 | 2019.04.04 | 1.解决：避免下推Mysql不支持的函数<br />2.解决：Mysql分区信息丢失 | 1.Hbase,Druid 模块化<br />2.优化 DataSourceManager插件化代码<br />3.清理冗余的依赖 | 1.支持给SQL中可以下推的子查询加别名<br />2.Elasticsearch支持Scroll接口，加速查询效率<br />3.Elasticsearch支持es.read.field.empty.as.null配置<br />4.增加Hive 权限验证模块spark-authorizer |
| 2.3.1.xsql-0.3.1 | 2019.04.09 | 1.解决：Local模式切换Yarn模式时，任务链接部分标签页不显示的问题<br />2.解决：Local模式执行create table using parquet等后，切换Yarn模式导致底层创建的HadoopFsRelation还是Local模式下已经关闭的SparkSession<br />3.解决：移除spark-hive脚本后，难以无缝迁移<br />4.解决：Hbase,Druid 模块化后，ServiceLoader找不到对应DataSourceRegister实现的Bug |             | 1. 保留支持bin/spark-hive<br />2. Hbase、Druid需额外配置，从而控制jars大小 |
| 2.3.1.xsql-0.3.2 |  | 1. bin/spark-xsql支持${}字符串变量<br />2. datasource关键字与列名冲突<br />3. processSingleTable assert失败<br />4. !开头的命令cast报错<br />5. OutOfDriectMemoryError | | 1. 支持Hive的remove datasource if exists<br />2. 支持Hive的insert overwrite directory using xxx<br />3. 增加Yarn优先级控制参数spark-sql --priority |
| 2.3.1.xsql-0.4.0 |  | 1. load data partition<br/>2. StackOverflow caused by window functions<br/> 3. select using script| 1. set 不再触发job <br/>2. local模式不再产生日志文件  | 1.支持Hive的distribute by<br/> 2.接入Kafka数据源 |
| 2.4.3.xsql-0.5.0 |  | 1.修复任务结束时报错java.io.IOException: Filesystem closed<br />2.解决HDFS0.2版本与Spark流式SQL所使用HDFS高版本的兼容性问题<br/>3.修复MongoManager、ElasticSearchManager在Local模式下，使用SparkSession.getActiveSession的bug<br/>4.解决XSQL升级Spark2.4.3后，流式SQL无法支持下推的问题 | 升级至Spark2.4.3核心   | 支持流式SQL计算Kafka数据 |


# Version Compatibility

| Compute/Storage/Language                                     | Version | Grade                       | Documentation                                               | Notes                                                        |
| ------------------------------------------------------------ | ------- | --------------------------- | ----------------------------------------------------------- | ------------------------------------------------------------ |
| <img src="images/scala.png" width="90" height="40" />        | 2.11.8  | PRODUCTION                  |                                                             | Data API is built on scala 2.11.8  |
| <img src="images/spark.png" width="90" height="40" />        | 2.4.3 |                             |                                                             | This is the recommended version      |
| <img src="images/hadoop.png" width="120" height="40" />      | 2.7.3   |                             |                                                             | This is the recommended version      |
| <img src="images/hive.png" width="80" height="60" />         | 1.2     | PRODUCTION                  | [Hive Doc](datasources/hive.md)                        |                                    |
| <img src="images/mysql.png" width="100" height="67" />       | 5.6.19    | PRODUCTION                  | [Mysql Doc](datasources/mysql.md)                      | POWER BY mysql-jdbc                   |
| <img src="images/druid.png" width="120" height="40" />       | 0.10.0    | PRODUCTION                  | [Druid Doc](datasources/druid.md)                      |                                   |
| <img src="images/elasticsearch.png" width="120" height="60" /> | 5.2  | PRODUCTION                  | [ElasticSearch Doc](datasources/elasticsearch.md)      |        |
| <img src="images/mongo.png" width="120" height="35" />       | 2.3.0    | PRODUCTION                  | [MongoDB Doc](datasources/mongo.md)                      |                                  |
| <img src="images/hbase.png" width="100" height="35" />       | 2.0     | PRODUCTION WITH LIMITATIONS | [HBase Doc](datasources/hbase.md)                      | POWER BY SHC Connector  |
| <img src="images/redis.png" width="100" height="33" />       | 4.0.10    | EXPERIMENTAL                  | [Redis Doc](datasources/redis.md)                      | POWER BY jedis                                  |
