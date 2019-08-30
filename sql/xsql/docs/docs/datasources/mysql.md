MySQL是一个关系型数据库管理系统，关系数据库将数据保存在不同的表中，而不是将所有数据放在一个大仓库内，这样就增加了速度并提高了灵活性。MySQL是XSQL支持的数据源之一。本节将对MySQL接入XSQL的安装、配置、运行作简单的介绍。

## Installation

​	XSQL通过JDBC访问MySQL数据库，所以不需要任何安装。对于想要了解MySQL的用户请查阅[MySQL官网][1]。

[ 1 ]: https://dev.mysql.com/doc/refman/8.0/en/ "MySQL帮助文档"
## MySQL Configuration

MySQL参数配置继承自XSQL,通用的配置信息请查看[Configurations](../configurations/common.md)，以下表中为MySQL独有的配置：

| Property Name                                            | Default | Meaning                                                      |
| -------------------------------------------------------- | ------- | ------------------------------------------------------------ |
| spark.xsql.datasource.$dataSource.partitionConf          | None    | 在非下推情况下用于指定MySQL数据源的Database中Table的分区信息 |
| spark.xsql.datasource.$dataSource.showSchemaDatabase     | false   | 用于控制是否显示MySQL自带的信息数据库information_schema.默认为不显示 |
| spark.xsql.datasource.$dataSource.pushdown.considerRows  | true    | 在设置数据源下推的情况下，用于控制是否根据表的行数（表的行数从information_schema库中获取，为近似值）来决定下推执行。 |
| spark.xsql.datasource.$dataSource.pushdown.considerIndex | true    | 在设置数据源下推的情况下，用于控制是否根据表的索引信息来决定下推执行。目前仅用于多表连接的情况。 |

这里给出一个MySQL接入XSQL的配置示例：

```properties
spark.xsql.datasources                   mysql
spark.xsql.default.datasource            mysql
spark.xsql.default.database              mysqltest

spark.xsql.datasource.mysql.type         mysql
spark.xsql.datasource.mysql.url          jdbc:mysql://10.142.97.177:2336
spark.xsql.datasource.mysql.user         mysqltest
spark.xsql.datasource.mysql.password     37ec19604101cd8a
spark.xsql.datasource.mysql.version      5.6.19
spark.xsql.datasource.mysql.pushdown       true
spark.xsql.datasource.mysql.isShowSchemaDatabase  false
spark.xsql.datasource.mysql.partitionConf  mysql-tablePartition.conf
spark.xsql.datasource.mysql.whitelist      mysql-whitelist.json
```

**partitionConf**

表格的分区信息是通过json格式的配置文件进行配置的，这里给出一个简单的mysql-tablePartition.conf的配置内容。建议在编辑该conf文件时使用 [JSON编辑器](../../jsoneditor/index.html?c={  "mysqltest":[ { "table":"geonames","partitionColumn":"id", "lowerBound":"38225","upperBound":"11903131", "numPartitions":"10" }]}) 避免低级错误。

```json
{
  "mysqltest":[
    { "table":"geonames",
      "partitionColumn":"id",
      "lowerBound":"38225",
      "upperBound":"11903131",
      "numPartitions":"10"
    }
  ]
}
```

以上例子中，mysqltest为***数据库名***，用户可在此位置自行指定自己的数据库名。以下表格为分区信息的各字段的具体说明：

| Name            | Meaning                                                      |
| --------------- | ------------------------------------------------------------ |
| table           | 表名                                                         |
| partitionColumn | 用于分区的列名，一般最好是表的索引字段，且要求为字段类型为数字，并最好是连续的，这样分区才不会数据倾斜 |
| lowerBound      | 用于分区的列的下限                                           |
| upperBound      | 用于分区的列的上限                                           |
| numPartitions   | 分区数，数值应该小于等于upperBound-lowerBound                |

**Note**

目前仅支持partitionColumn的数据类型为long型，lowerBound和upperBound的值可自行查询数据库。如有分区列为其他类型的需要，请联系开发人员。
## Execution

​	MySQL接入XSQL支持[Common Commands](../execution/common.md)中介绍的通用原则。在XSQL中创建、删除、修改MySQL表的方式与直接在MySQL中的操作方式相同，但要注意的一点是创建表时需要在末尾加上using mysql来申明创建的是MySQL数据源的table。以下分别为XSQL中MySQL数据源实现的DDL的示例：

**Create MySQL table**

```sql
CREATE TABLE [IF NOT EXISTS] [db_name.]table_name(
    `col0` int not null auto_increment PRIMARY KEY,
    `col1` varchar(255),
    ...
    ) USING mysql
```

**Drop  table if exists**

```sql
DROP TABLE IF EXISTS mysqltest.test1
```

**Alter table  add/drop/change  columns**

```sql
# 添加表字段
ALTER TABLE mysqltest.test1 ADD(`col2` datetime，`col3` bigint DEFAULT 0)
# 修改表字段
ALTER TABLE mysqltest.test1 CHANGE `col3` `col4` int
# 删除表字段
ALTER TABLE mysqltest.test1 DROP column `col2`,DROP `col3
```

**Rename table**

```sql
# 方式一
ALTER TABLE mysqltest.test1 RENAME TO test2
# 方式二
RENAME TABLE Mysqltest.test1 TO test2
```
