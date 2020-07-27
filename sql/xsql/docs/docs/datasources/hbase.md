HBase是一个分布式的、面向列的开源数据库，旨在为非结构化数据提供海量存储空间，同时提供索引查询。
## Installation
​ 由于HBase提供了Java API，所以不需要任何安装。对于想要了解HBase Java API的用户请查阅 [官方文档](https://hbase.apache.org/2.0/apidocs/index.html)。

## Configuration
​    HBase接入XSQL的配置继承了[Configurations](../configurations/common.md)中针对特定数据源的type、version、pushdown、schemas配置。HBase接入XSQL还有一些特有的配置，下表将对他们进行介绍。

| Property Name                          | Default | Meaning                                 |
| -------------------------------------- | ------- | --------------------------------------- |
| spark.xsql.datasource.$dataSource.host | None    | 配置hbase.zookeeper.quorum              |
| spark.xsql.datasource.$dataSource.port | 2181    | 配置hbase.zookeeper.property.clientPort |

这里给出一个HBase接入XSQL的配置示例：

```python
spark.xsql.datasources          hbase_ds_name
spark.xsql.default.datasource   hbase_ds_name
spark.xsql.datasource.hbase_ds_name.type  hbase
spark.xsql.datasource.hbase_ds_name.host hostname1,hostname2,hostname3
spark.xsql.datasource.hbase_ds_name.port  2181
# 配置元数据存储文件名称，需要放置在SPARK_CONF_DIR中
spark.xsql.datasource.hbase_ds_name.schemas  hbase.schemas
# version为预留配置，目前只支持连接HBase 2.0+
spark.xsql.datasource.hbase_ds_name.version  2.0
spark.xsql.datasource.hbase_ds_name.pushdown  true
```

## Advanced Configuration

xsql为了访问HBase，使用了 [shc](https://github.com/hortonworks-spark/shc) 作为底层连接工具，延用了shc的元数据定义方式。

| HBase概念            | 对应的关系数据库概念 |
| -------------------- | -------------------- |
| 命名空间             | 数据库名             |
| 表名                 | 表名                 |
| 列族名:列名          | 列名                 |
| ROW                  | 主键                 |
| 列在反序列化后的类型 | 字段类型             |

**Note**

xsql目前不支持timestamp相关的操作，如有需要，请联系开发人员。

### 两种元数据注册方式：

HBase自身只维护列族级别的元数据，用户在使用xsql查询HBase之前，必须显式给出数据表的完整元数据，具体可以通过以下两种方式：

### 1. 直接编辑schemas文件

​    xsql延用了shc的元数据定义方法，详情请参考 [链接](https://github.com/hortonworks-spark/shc#application-usage) 。hbase.schemas文件是多个表元数据json对象构成的数组，建议在编辑schemas文件时使用 [JSON编辑器](../../jsoneditor/index.html?c=[{"table": {"namespace": "default", "name": "HBaseSourceExampleTable"}, "rowkey": "key", "columns": {"col0": {"cf": "rowkey", "col": "key", "type": "string"}, "col1": {"cf": "cf1", "col": "col1", "type": "boolean"} } }, {"another table":{} } ]) 避免低级错误。

**Examples**

```json
[
    {
        "table": {
            "namespace": "default",
            "name": "HBaseSourceExampleTable"
        },
        "rowkey": "key",
        "columns": {
            "col0": {
                "cf": "rowkey",
                "col": "key",
                "type": "string"
            },
            "col1": {
                "cf": "cf1",
                "col": "col1",
                "type": "boolean"
            }
        }
    },
    {
        "another table":{
            
        }
    }
]
```

**Note**

上例中的 `"rowkey": "key"`，其中 `"key"`可以是任意字符串，与`columns`中的`col0`的 `col` 保持引用一致即可。

**Note**

关于shc元数据的高级用法，如复合主键，Avro数据类型，由于测试不稳定，不建议进行尝试。如有需要，请联系开发人员。

### 2. create [if not exists] table

**Examples**

```sql
CREATE IF NOT EXISTS TABLE HBaseSourceExampleTable (
        `col0` string PRIMARY KEY,
        `col1` boolean
        ) using hbase
        options (col1='cf1:col1')
        tblproperties (write_schemas = true)
```

**Note**

`create if not exists table` 在表未注册但已存在时，可以实现元数据注册

`create table` 在表不存在时，会创建表且注册它，但不推荐使用。原因是：

1.  xsql目前还不支持批量写入hbase，用户始终需要在其他场景下产生待查询数据。
2. xsql无法提供HBase表的全参数创建，使用默认参数创建出的表很可能存在性能问题。

**Note**

`write_schemas = true` 表示create table注册后的表会自动以json格式追加在schemas文件末尾。另外，使用drop [if exists] table可以同时删除该表在hbase.schemas中的元数据和HBase 数据库中的真实数据，数据无价，请谨慎使用。

## Execution

### 可下推的select语句

#### select *

------

返回所有字段

**Examples**

```bash
> select * from geonames limit 3;
geoname_id	dem	population	latitude	longitude	name	alternatenames	country_code	feature_code	feature_class	admin1_code	timezone
10003397	385	0	-17	145	Rifle Creek	NULL	AU	STM	H	NULL	Australia/Brisbane
10004419	300	0	-32	116	Canning Mills	NULL	AU	PPLX	P	NULL	Australia/Perth
10006947	28	737	-32	116	Carabooda	NULL	AU	PPLX	P	NULL	Australia/Perth
```

| 实现方式 | 等效查询 |
| -------------- | ----------------------------- |
| xsql | `select * from geonames limit 3;` |
| hbase shell    | `scan 'geonames',{LIMIT=>3}` |
| hbase java api | `Scan scan = new Scan();` <br />`scan.setLimit(3);` |

#### select col...

------

返回选择字段

**Examples**

```bash
> select dem,population from geonames limit 3;
dem	population
385	0
300	0
28	737
```

| 实现方式       | 等效查询                                                     |
| -------------- | ------------------------------------------------------------ |
| xsql           | `select dem,population from geonames limit 3;`               |
| hbase shell    | `scan 'geonames',{COLUMNS=>['ct:dem','ct:population'],LIMIT=>10}` |
| hbase java api | `Scan scan = new Scan();`<br /> `scan.setLimit(3);`<br /> `scan.addColumn(Bytes.toByte("ct"),Bytes.toByte("dem"));` <br />`scan.addColumn(Bytes.toByte("ct"),Bytes.toByte("population"));` |

#### select + where

##### 点过滤 `=` `==` `<>` `in` `not in`

------

返回rowkey或列等于/不等于 某些特定值 的记录

**Examples**

```bash
> select geoname_id,dem,population from geonames where geoname_id = '10003397';
geoname_id	dem	population
10003397	385	0
> select geoname_id,dem,population from geonames where geoname_id == '10003397';
geoname_id	dem	population
10003397	385	0
> select geoname_id,dem,population from geonames where geoname_id <> '10003397' limit 2;
geoname_id	dem	population
10004419	300	0
10006947	28	737
> select geoname_id,dem,population from geonames where geoname_id in ('10003397','10004419');
geoname_id	dem	population
10003397	385	0
10004419	300	0
> select geoname_id,dem,population from geonames where geoname_id not in ('10003397','10004419') limit 2;
geoname_id	dem	population
10006947	28	737
10035407	37	6354
> select geoname_id,name,latitude from geonames where latitude = '40' limit 2;
geoname_id	name	latitude
10055326	Porto Eda	40
10063141	Royal Tulip Grand	40
> select geoname_id,name,latitude from geonames where latitude <> '40' limit 2;
geoname_id	name	latitude
10003397	Rifle Creek	-17
10004419	Canning Mills	-32
```

| 实现方式       | 等效查询                                                     |
| -------------- | ------------------------------------------------------------ |
| xsql           | `select geoname_id,dem,population from geonames where geoname_id = '10003397';` |
| hbase shell    | `get 'geonames','10003397',{COLUMNS=>['ct:dem','ct:population']}` |
| hbase java api | `Get get = new Get(Bytes.toBytes("10003397"));`<br />`get.addColumn(Bytes.toByte("ct"),Bytes.toByte("name"));`<br />`get.addColumn(Bytes.toByte("ct"),Bytes.toByte("name"));` |

| 实现方式       | 等效查询                                                     |
| -------------- | ------------------------------------------------------------ |
| xsql           | `select geoname_id,name,latitude from geonames where latitude = '40' limit 2;` |
| hbase shell    | `scan 'geonames',{LIMIT=>2,FILTER=>"SingleColumnValueFilter('ct','latitude',=,'binary:40')"}` |
| hbase java api | `Scan scan = new Scan();`<br />`scan.setLimit(2);`<br />`scan.addColumn(Bytes.toByte("ct"),Bytes.toByte("name"));` <br />`scan.addColumn(Bytes.toByte("ct"),Bytes.toByte("latitude"));`<br />`scan.setFilter(new SingleColumnValueFilter(Bytes.toBytes("ct"),Bytes.toBytes("latitude"),     CompareOperator,EQUAL,new BinaryComparator(Bytes.toBytes("40")));` |

##### 范围过滤 `<` `<=` `>=` `>` `!>` `!<` `between and`

------

返回rowkey或列 介于 特定范围 的记录

**Note**

`between and` 相当于`>= and <=`

**xsql在对有符号数类型字段进行比较时，会自动根据符号位是否为1，切割比较范围**

**Examples**

```bash
> select geoname_id from geonames where geoname_id between '10003397' and '10009999';
geoname_id
10003397
10004419
10006947
> select dem from geonames where dem <= 1000 and dem > 990 limit 3;
dem
993
993
1000
> select dem from geonames where dem < 1000 and dem > 990 limit 3;
dem
993
993
993

```

| 实现方式       | 等效查询                                                     |
| -------------- | ------------------------------------------------------------ |
| xsql           | `select geoname_id from geonames where geoname_id between '10003397' and '10009999';` |
| hbase shell    | `scan 'geonames',{STARTROW=>'10003397',STOPROW=>'10009999'}` |
| hbase java api | `Scan scan = new Scan();`<br />`scan.withStartRow('10003397',true).withStopRow('10009999',true);` |

##### 模糊过滤 `like` `not like`

------

返回rowkey或列匹配/不匹配 某些模式 的记录

**Note**

目前仅支持两个通配符：

| 任意单个字符 | 任意多个字符 |
| ------------ | ------------ |
| `_`          | `%`          |

仅支持四种模式：

| startsWith  | endsWith    | startsAndEndsWith   | contains     |
| ----------- | ----------- | ------------------- | ------------ |
| `([^_%]+)%` | `%([^_%]+)` | `([^_%]+)%([^_%]+)` | `%([^_%]+)%` |

**Examples**

```bash
> select geoname_id from geonames where geoname_id like '111101%' limit 2;
geoname_id
11110100
11110101
> select name from geonames where name like 'Ca%' limit 2;
name
Canning Mills
Carabooda
```

| 实现方式       | 等效查询                                                     |
| -------------- | ------------------------------------------------------------ |
| xsql           | `select geoname_id from geonames where geoname_id like '111101%' limit 2;` |
| hbase shell    | `scan 'geonames',{ROWPREFIXFILTER=>'111101',LIMIT=>2}`       |
| hbase java api | `Scan scan = new Scan();`<br />`scan.setLimit(2);`<br />`scan.setFilter(new PrefixFilter(Bytes.toBytes("111101"));` |

| 实现方式       | 等效查询                                                     |
| -------------- | ------------------------------------------------------------ |
| xsql           | `select name from geonames where name like 'Ca%' limit 2;`   |
| hbase shell    | `scan 'geonames',{LIMIT=>2,FILTER=>"SingleColumnValueFilter('ct','name',=,'binaryprefix:Ca')"}` |
| hbase java api | `Scan scan = new Scan();`<br /> `scan.setLimit(2);`<br /> `scan.setFilter(new SingleColumnValueFilter(Bytes.toBytes("ct"),Bytes.toBytes("name"),     CompareOperator,EQUAL,new BinaryPrefixComparator(Bytes.toBytes("Ca")));` |

##### 空值过滤 `is null` `is not null`

------

返回特定列 为空/不为空 的记录

**Note**

xsql将列不存在、byte[0]均视为`is null`，较之HBase的NullComparator，额外将byte[0]判定为空值，原因是byte[0]同样无法进行后续计算。

**Examples**

```bash
> select name,alternatenames from geonames where alternatenames is null limit 2;
name	alternatenames
Rifle Creek	NULL
Canning Mills	NULL
> select name,alternatenames from geonames where alternatenames is not null limit 2;
name	alternatenames
Suhūl az̧ Z̧afrah	Suhul az Zafrah,Suhūl az̧ Z̧afrah
Mählsack	Gendarmen
```

| 实现方式       | 等效查询                                                     |
| -------------- | ------------------------------------------------------------ |
| xsql           | `select name,alternatenames from geonames where alternatenames is null;` |
| hbase shell    | `scan 'geonames',{FILTER=>"SingleColumnValueFilter('ct','alternatenames',=,'binary:')"}` |
| hbase java api | `Scan scan = new Scan();` <br />`scan.setFilter(new SingleColumnValueFilter(Bytes.toBytes("ct"),Bytes.toBytes("alternatenames"),     CompareOperator,EQUAL,new BinaryComparator(Bytes.toBytes("")));` |

| 实现方式       | 等效查询                                                     |
| -------------- | ------------------------------------------------------------ |
| xsql           | `select name,alternatenames from geonames where alternatenames is not null;` |
| hbase java api | `Scan scan = new Scan();`<br />`Filter filter = new SingleColumnValueFilter(Bytes.toBytes("ct"),Bytes.toBytes("alternatenames"),     CompareOperator,NOT_EQUAL,new BinaryComparator(Bytes.toBytes("")));` `filter.setFilterIfMissing(true);`<br /> `scan.setFilter(filter);`<br /> |

