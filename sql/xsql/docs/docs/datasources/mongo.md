MongoDB是一个文档数据库，具有可伸缩性和您需要的查询和索引的灵活性。MongoDB是XSQL支持的数据源之一。本节将对MongoDB接入XSQL的安装、配置、运行作简单的介绍。

## Installation

​	由于MongoDB提供了Java API，所以不需要任何安装。对于想要了解MongoDB的用户请查阅[MongoDB官网][1]。

[ 1 ]: https://www.mongodb.com/ "MongoDB官网"
## Configuration

​	MongoDB接入XSQL的配置继承了[Configurations](../configurations/common.md)中介绍的通用配置。MongoDB接入XSQL还有一些特有的配置，下表将对他们进行介绍。

| Property Name                                            | Default | Meaning                                |
| -------------------------------------------------------- | ------- | -------------------------------------- |
| spark.xsql.datasource.$dataSource.authenticationDatabase | admin   | MongoDB进行权限验证的MongoDB数据库名称 |

MongoDB没有严格、明确的schema定义，所以需要

这里给出一个MongoDB接入XSQL的配置示例：

```properties
spark.xsql.datasources        default

spark.xsql.datasource.default.type   mongo
spark.xsql.datasource.default.url   mongodb://test:test@127.0.0.1:7787/?replicaSet=7787
spark.xsql.datasource.default.authenticationDatabase   admin
spark.xsql.datasource.default.user   test
spark.xsql.datasource.default.password   test
spark.xsql.datasource.default.version   3.4.13
spark.xsql.datasource.default.schemas  mongo.schemas
```

## Execution

​	MongoDB接入XSQL支持[Common Commands](../execution/common.md)中介绍的通用原则。MongoDB接入XSQL后，XSQL会把SQL转换为MongoDB的Java API。XSQL支持大多数的MongoDB功能。本节将XSQL的SQL为例，来介绍如何使用XSQL查询MongoDB。

**Create MongoDB Collection**

创建MongoDB的collection，与关系型数据库（例如MySQL、Oracle等）创建表的方式有很多不同之处。例如：

```sql
create table test (
  key keyword,
  value text,
  other String
) using com.mongodb.spark.sql
```

上面using的含义是创建MongoDB中的collection。不过，这种用法也许会让人觉得繁琐，所以也可以用下面的SQL：

```mysql
create table test (
  name string,
  age int,
  comment String
) using mongo
```

## Discover Table Schema

由于MongoDB是一个文档数据库，因此各个字段可能会存储各种类型的数据。如果存储的内容包含了复合结构（例如：对象、数组），用户想要把复合结构反映到表的元信息中，那么可以关闭二级缓存（此时不需要提供额外的schema定义文件），这将促使XSQL帮助用户去解析、推断表的元数据信息。