Elasticsearch是一个高度可扩展的开源全文搜索和分析引擎。它允许您快速，近实时地存储，搜索和分析大量数据。Elasticsearch是XSQL支持的第二个数据源。本节将对Elasticsearch接入XSQL的安装、配置、运行作简单的介绍。

## Installation

​	由于Elasticsearch提供了REST API，所以不需要任何安装。对于想要了解Elasticsearch的用户请查阅[Elasticsearch官方文档][1]。

[ 1 ]: https://www.elastic.co/guide/en/elasticsearch/reference/5.2/getting-started.html "Elasticsearch帮助文档"
## Configuration

​	Elasticsearch接入XSQL的配置继承了[Configurations](../configurations/common.md)中介绍的通用配置。Elasticsearch接入XSQL还有一些特有的配置，下表将对他们进行介绍。

| Property Name                                                | Default | Meaning                                                      |
| ------------------------------------------------------------ | ------- | ------------------------------------------------------------ |
| spark.xsql.datasource.$dataSource.es.mapping.date.rich       | true    | 是否启用date自动转换，其实际影响的是Elasticsearch的es.mapping.date.rich配置。在处理日期时，Elasticsearch始终使用ISO 8601格式作为日期/时间。如果需要自定义日期格式，请将其添加到默认选项，而不是替换它。 有关详细信息，请参阅[Elasticsearch参考文档中的/mapping-date-format.html 部分](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html)。 请注意，在读取数据时，如果日期不是ISO8601格式，则默认情况下XSQL可能无法理解，因为它不会复制Elasticsearch中精心设置的日期解析。 在这些情况下，可以通过es.mapping.date.rich属性简单地禁用日期转换并将原始信息作为long或String传递。 |
| spark.xsql.datasource.$dataSource.es.scroll.keepalive        | 5m      | 查询之间的scroll结果的最大周期。目前仅在关闭下推时有效。请参阅https://www.elastic.co/guide/en/elasticsearch/hadoop/5.2/configuration.html中对es.scroll.keepalive的详细介绍。 |
| spark.xsql.datasource.$dataSource.es.scroll.size             | 50      | 单个请求返回的条目（结果）数。目前仅在关闭下推时有效。请参阅https://www.elastic.co/guide/en/elasticsearch/hadoop/5.2/configuration.html中对es.scroll.size的详细介绍。 |
| spark.xsql.datasource.$dataSource.es.scroll.limit            | 20      | 单个scroll所返回的条目（结果）总数。目前仅在关闭下推时有效。负数代表返回所有匹配的文档。请参阅https://www.elastic.co/guide/en/elasticsearch/hadoop/5.2/configuration.html中对es.scroll.limit的详细介绍。 |
| spark.xsql.datasource.$dataSource.es.field.read.empty.as.null | yes     | 是否将空field作为null。目前仅在关闭下推时有效。请参阅https://www.elastic.co/guide/en/elasticsearch/hadoop/5.2/configuration.html中对es.field.read.empty.as.null的详细介绍。 |
| spark.xsql.datasource.$dataSource.discover                   | None    | 是否启用并配置数据源的类型推断。实际会影响Elasticsearch的es.read.field.as.array.include配置。Elasticsearch中的字段（也可称为属性）默认都是可以存储Array的。XSQL没有办法准确获得一个字段是它的定义类型，还是数组类型。此配置用于告诉XSQL，哪些字段需要进行类型推断。有关详细信息，请参阅https://www.elastic.co/guide/en/elasticsearch/hadoop/5.2/configuration.html中对es.read.field.as.array.include的详细介绍。 |

这里给出一个Elasticsearch接入XSQL的配置示例：

```properties
spark.xsql.datasources        default

spark.xsql.datasource.default.type   elasticsearch
spark.xsql.datasource.default.url   http://127.0.0.1:9025
spark.xsql.datasource.default.user   test
spark.xsql.datasource.default.password   test
spark.xsql.datasource.default.version   5.2
spark.xsql.datasource.default.whitelist  es-whitelist.conf
```

## Concept Mapping

​	Elasticsearch相比于传统关系型数据库，在底层设计上有诸多不同。比如：Elasticsearch中是没有table的。那么用户会在XSQL中看到table时，造成一些困惑。下表对Elasticsearch与关系型数据库在XSQL中的映射关系逐一说明。

| Elasticsearch概念  | 关系数据库概念 |
| ------------------ | -------------- |
| 索引（即Index）    | 数据库实例     |
| 类型（即Type）     | 表             |
| 文档（即Document） | 行             |

## Schema Discover

​	Elasticsearch中的字段（也可称为属性）默认都是可以存储Array的，这使得XSQL没有办法准确获得一个字段是它的定义类型，还是数组类型。用户可以通过提供Json格式的配置文件，并通过spark.xsql.datasource.$dataSource.discover属性来指定路径。这里给出Discover文件的格式定义示例：

```json
{
  "es_index_example_one": {
    "es_type_example_one": "field_A.property_A_0,field_B.property_B_0",
    "es_type_example_two": "field_C.property_C_0,field_D",
  },
  "es_index_example_two": {
    "es_type_example_thr": "field_E.property_E_0,field_F.property_F_0"
  }
}
```

可以看出每个Type的各个需要探测的字段之间用英文逗号分隔。

## Limit with Group By

XSQL在[下推模式](../../getting_started/Getting_Started/#pushdown)中实现group by时，借助了Elasticsearch的[Aggregations](https://www.elastic.co/guide/en/elasticsearch/reference/5.2/search-aggregations.html)。[Aggregations](https://www.elastic.co/guide/en/elasticsearch/reference/5.2/search-aggregations.html)是可以嵌套的，通过给size添加size属性只能控制局部的结果限制。这种方式无法实现全局的结果限制，因此XSQL将忽略group by语句后跟随的limit限制，查询的结果数目可能跟你的预期有些不同。如果你真的想要group by后的limit依然起作用，可以关闭下推模式。

## Execution

​	Elasticsearch接入XSQL支持[Common Commands](../execution/common.md)中介绍的通用原则。Elasticsearch接入XSQL后，XSQL会把SQL转换为Elasticsearch的REST API。XSQL支持大多数的Elasticsearch功能。本节将XSQL的SQL为例，来介绍如何使用XSQL查询Elasticsearch。

**Create Elasticsearch Type**

创建Elasticsearch的type，与关系型数据库（例如MySQL、Oracle等）创建表的方式有很多不同之处。例如：

```sql
create table test (
  key keyword,
  value text,
  other String
) using org.elasticsearch.spark.sql
```

上面using的含义是创建Elasticsearch中的type。不过，这种用法也许会让人觉得繁琐，所以也可以用下面的SQL：

```mysql
create table test (
  key keyword,
  value text,
  other String
) using es
```

