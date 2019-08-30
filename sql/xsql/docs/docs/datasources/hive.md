​	Hive是Hadoop生态圈中的一种数据仓库工具，提供了将结构化数据映射为数据库及表的能力，并支持SQL查询，最终将SQL转换为MapReduce进行并行计算。Hive是XSQL支持的最早的数据源。本节将对Hive接入XSQL的安装、配置、运行作简单的介绍。

## Installation

​	Hive接入XSQL，首先需要在Linux环境中配置好Hive依赖的HDFS、Hive Metadata Server。这些内容超出了XSQL的范围，需要了解的用户请分别查阅[HDFS官方文档][1]和[Hive官方文档][2]。

[ 1 ]: http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html "HDFS帮助"
[  2 ]: http://hive.apache.org/  "Hive帮助"

## Configuration

​	Hive接入XSQL的配置继承了[Configurations](../configurations/common.md)中介绍的通用配置（**注意**：Hive接入XSQL不支持通用配置spark.xsql.datasource.$dataSource.pushdown）。Hive接入XSQL还有一些特有的配置，下表将对他们进行介绍。

| Property Name                                   | Default | Meaning                       |
| ----------------------------------------------- | ------- | ----------------------------- |
| spark.xsql.datasource.$dataSource.metastore.url | None    | Hive Metadata Server的访问URI |

此配置非常关键，XSQL将通过访问Hive Metadata Server获得Hive的元数据信息。

这里给出一个Hive接入XSQL的配置示例：

```properties
spark.xsql.datasources        default

spark.xsql.datasource.default.type   hive
spark.xsql.datasource.default.metastore.url   thrift://127.0.0.1:9083
spark.xsql.datasource.default.user   test
spark.xsql.datasource.default.password   test
spark.xsql.datasource.default.version   1.2.1
spark.xsql.datasource.default.whitelist  hive-whitelist.conf
```

## Execution

​	Hive接入XSQL支持[Common Commands](../execution/common.md)中介绍的通用原则。Hive接入XSQL将支持绝大多数的Hive QL语法。本节将以Hive特有的SQL为例，来介绍如何使用XSQL执行Hive QL。

### Create Hive Table

创建Hive表，与关系型数据库（例如MySQL、Oracle等）创建表的方式有很多不同之处。例如：

```sql
create table test (
  key String,
  value String,
  other String
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
```

上面的SQL中需要定义行数据的分隔符以及存储格式等。

