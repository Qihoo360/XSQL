Kafka是一个分布式数据流平台，具有三种关键的能力：

- 发布和订阅消息记录
- 存储消息记录的容错、可靠
- 实时处理消息记录

Kafka是XSQL支持的数据源之一。本节将对Kafka接入XSQL的安装、配置、运行作简单的介绍。

## Installation

​	由于Kafka提供了Java API，所以不需要任何安装。对于想要了解Kafka的用户请查阅[Kafka官网][1]。

[1]: http://kafka.apache.org "Kafka官网"
[2]: http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks	"Spark Structed Streaming对Sink的介绍"
[3]: http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes	"Spark Structed Streaming对Output Modes的介绍"

[4]: http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking	"Spark Structed Streaming对Watermark的介绍"



## Configuration

​	Kafka接入XSQL的配置继承了[Configurations](../configurations/common.md)中介绍的通用配置（**注意**：Kafka接入XSQL不支持通用配置spark.xsql.datasource.$dataSource.pushdown和spark.xsql.datasource.$dataSource.url）。Kafka接入XSQL还有一些特有的配置，下表将对他们进行介绍。

| Property Name                                       | Default | Meaning        |
| --------------------------------------------------- | ------- | -------------- |
| spark.xsql.datasource.$dataSource.bootstrap.servers | None    | Kafka的brokers |

这里给出一个Kafka接入XSQL的配置示例：

```properties
spark.xsql.datasources        default

spark.xsql.datasource.default.type   kafka
spark.xsql.datasource.default.bootstrap.servers   127.0.0.1:9092
spark.xsql.datasource.default.user   test
spark.xsql.datasource.default.password   test
spark.xsql.datasource.default.version   2.2
```

注意：以上配置相当于把Kafka当做了数据库使用。我们查询数据库的数据时，往往只会返回查询那一刻所存储的数据，所以Kafka的行为也是类似。Kafka作为流式数据平台，里面的数据是持续变化、连续生成的，如果想要获取连续的数据，那么请看<a href="#Streaming">Streaming</a>。

### Streaming Configuration

上文将Kafka作为数据库看待，如果需要启用流的能力，需要打开Kafka数据源的流开关。以上文的“Kafka接入XSQL的配置示例”为例，我们需要加入：

```properties
spark.xsql.datasource.default.stream   true
```

流式处理中，有诸多额外的配置参数可以控制，参照下表：

| Property Name                                                | Default    | Meaning                                                      |
| ------------------------------------------------------------ | ---------- | ------------------------------------------------------------ |
| spark.xsql.datasource.$dataSource.stream                     | True       | XSQL流式处理Kafka的总开关                                    |
| spark.xsql.datasource.$dataSource.checkpointLocation         | None       | XSQL流式处理Kafka的检查点路径。此路径必须是分布式文件系统的路径，例如：S3、HDFS等。注意：在开启流式处理后，此参数必须配置。 |
| spark.xsql.datasource.$dataSource.sink                       | None       | 流式处理后的结果的输出目标。sink有多种可供选择，请参考<a href="#Streaming Sink">Streaming Sink</a>，默认为parquet。更多相关信息，请阅读[Spark Structed Streaming对Sink的介绍][2]。 |
| spark.xsql.datasource.$dataSource.sink.path                  | None       | 当Sink选择的是文件格式输出时（如：parquet），必须指定此配置。此路径必须是分布式文件系统的路径，例如：S3、HDFS等。 |
| spark.xsql.datasource.$dataSource.sink.kafka.bootstrap.servers | None       | 当Sink选择输出到Kafka时，必须指定此配置。                    |
| spark.xsql.datasource.$dataSource.sink.kafka.topic           | None       | 当Sink选择输出到Kafka时，必须指定此配置。                    |
| spark.xsql.datasource.$dataSource.output.mode                | Append     | 流式处理的输出模式。有多种选择，请参考<a href="#Streaming Output Modes">Streaming Output Modes</a>，默认为Append。更多相关信息，请阅读[Spark Structed Streaming对Output Modes的介绍][3]。 |
| spark.xsql.datasource.$dataSource.trigger.type               | microbatch | 流式处理所用触发器的类型。有多种选择，请参考<a href="#Streaming Triggers">Streaming Triggers</a>，默认为processing。注意：Continuous只支持Projections、Selections相关SQL语法、除了聚合函数、`current_timestamp()` 、 `current_date()`以外的所有函数。 |
| spark.xsql.datasource.$dataSource.trigger.duration           | 1 second   | 在触发器类型不同时，有不同的含义。触发器是processing时，则表示微批之间的间隔；触发器是processing时，则表示异步写检查点的时间间隔。 |
| spark.xsql.datasource.$dataSource.watermark                  | None       | 用于跟踪数据中的当前事件时间，并尝试相应地清理旧状态。有了Watermark，XSQL就能够处理延迟数据。更多内容，请阅读[Spark Structed Streaming对Watermark的介绍][4] |

#### Streaming Sink

| Sink    | Meaning            |
| ------- | ------------------ |
| console | 输出到命令行控制台 |
| parquet | 输出为Parquet格式  |
| orc     | 输出为Orc格式      |
| text    | 输出为Text格式     |
| json    | 输出为Json格式     |
| csv     | 输出为Csv格式      |
| kafka   | 输出到Kafka        |

#### Streaming Output Modes

| Output Modes | Meaning  |
| ------------ | -------- |
| **Append**   | 追加模式 |
| **Complete** | 完全模式 |
| **Update**   | 更新模式 |

#### Streaming Triggers

| **Trigger Type** | Meaning                                      | Notice                                                       |
| ---------------- | -------------------------------------------- | ------------------------------------------------------------ |
| **microbatch**   | 采用微批方式处理，微批之间保持固定的时间间隔 | 适用于连续不断地消费、加工数据流。如果前一个微批执行的时间小于间隔大小，XSQL也将等待该间隔结束后，才开始下一个微批；如果前一个微批执行的时间大于间隔大小，下一个微批将在前一个微批执行结束后开始，而不会考虑间隔大小；如果数据流中没有数据可用，则不会触发下一个微批。 |
| **once**         | 采用微批方式处理，但只执行一次               | 非常适用于用户按照自己的周期（如：天、周）处理自上一个时间段以来可用的所有内容。 |
| **continuous**   | 采用连续流方式处理                           | 只支持Projections、Selections相关SQL语法、除了聚合函数、`current_timestamp()` 、 `current_date()`以外的所有函数。 |

## Concept Mapping

Kafka毕竟不是数据库，跟别说与传统关系型数据库相比较，因此在底层设计上有巨大的差异。比如：Kafka中是没有table的。那么用户会在XSQL中看到table时，造成一些困惑。下表对Kafka与关系型数据库在XSQL中的映射关系逐一说明。

| Kafka概念                        | 关系数据库概念              |
| -------------------------------- | --------------------------- |
| Kafka集群（也可以理解为Brokers） | 数据库实例，名称固定为kafka |
| 主题（即Topic）                  | 表                          |
| 事件（即Event）                  | 行                          |

## Kafka Schema

XSQL直接借用了Spark对于Kafka的schema定义：

```scala
  def kafkaSchema: StructType = StructType(Seq(
    StructField("key", BinaryType),
    StructField("value", BinaryType),
    StructField("topic", StringType),
    StructField("partition", IntegerType),
    StructField("offset", LongType),
    StructField("timestamp", TimestampType),
    StructField("timestampType", IntegerType)
  ))
```

无论你要消费的Kafka消息的key和value究竟是怎样的格式，它们都被当做字节数组来处理。因此，key或者value，往往需要转换为字符串来处理。key或者value可以存储键值对、Json串、Xml，甚至任何你自定义的格式。下面对Schema中的各个字段对照Kafka进行一个简短的说明：

| Field Name    | Field Type | Kafka mapping                                                |
| ------------- | ---------- | ------------------------------------------------------------ |
| key           | Binary     | Message key                                                  |
| value         | Binary     | Message value                                                |
| topic         | String     | Kafka topic                                                  |
| partition     | Integer    | Topic partition                                              |
| offset        | Long       | Partition offset                                             |
| timestamp     | Timestamp  | Message timestamp                                            |
| timestampType | Integer    | Message timestamp type，分别是：-1:NoTimestampType, 0: CreateTime（消息创建时间）, 1:LogAppendTime（日志追加时间） |

## Execution

​	Kafka接入XSQL支持[Common Commands](../execution/common.md)中介绍的通用原则。Kafka接入XSQL后，XSQL会把SQL转换为Kafka的Java API。在<a href="#Kafka Schema">Kafka Schema</a>一节提到Kafka的Schema定义是固定的，因此XSQL目前没有考虑实现Kafka对应的会修改元信息的DDL，例如：create table, create database, alter table等。本节使用XSQL，执行Desc语句，来查看它的元信息是否与<a href="#Kafka Schema">Kafka Schema</a>一节的描述相一致。

### Desc Kafka Topic

查看Kafka的Topic，与关系型数据库（例如MySQL、Oracle等）查看表的方式几乎相同。例如：

```sql
spark-xsql> desc mykafka.kafka.xsql_test;
key     binary  NULL
value   binary  NULL
topic   string  NULL
partition       int     NULL
offset  bigint  NULL
timestamp       timestamp       NULL
timestampType   int     NULL
Time taken: 2.681 s
```

如果想要查看更多的元信息，例如Topic的Partition信息，可以执行：

```sql
spark-xsql> desc formatted mykafka.kafka.xsql_test;
key     binary  NULL
value   binary  NULL
topic   string  NULL
partition       int     NULL
offset  bigint  NULL
timestamp       timestamp       NULL
timestampType   int     NULL

# Detailed Table Information
Database        kafka
Table   xsql_test
Created Time    Thu Aug 08 11:27:33 CST 2019
Last Access     Thu Jan 01 07:59:59 CST 1970
Created By      Spark 
Type    TOPIC
Provider        kafka
Table Properties        [originDBName=kafka]
Storage Properties      [kafka_partitions=Buffer(Partition(topic = xsql_test, partition = 0, leader = 0, replicas = [0,], isr = [0,], Partition(topic = xsql_test, partition = 1, leader = 0, replicas = [0,], isr = [0,]), subscribe=xsql_test, output.mode=append, sink=console, bootstrap.servers=127.0.0.1:9092, version=2.2, kafka.bootstrap.servers=127.0.0.1:9092, stream=true, checkpointLocation=hdfs://127.0.0.1:9000/home/spark/ss/checkpoint, cache.level=2, type=kafka, watermark=10 minutes]
Time taken: 0.08 s
```

### How to treat Kafka value?

在<a href="#Kafka Schema">Kafka Schema</a>一节对Kafka的Topic的固定结构进行了说明。其中，Kafka消息的value是被作为二进制数组看待的。Kafka的value中可以存储千变万化的数据形式，如果要对value中的内容进行更详细的处理该怎么办？本节以经典的word count例子，来引导大家如何操作。

我们有一个动物相关的Topic，名为animals. 有个Kafka生产者一直向animals写入以空格分隔的动物名称，现在我们使用XSQL来实现对动物名称的计数。下面是实现此功能的SQL：

```sql
select count(t1.value2) as num, t1.value2 as name from (select explode(split(CAST(value AS STRING), ' ')) value2 from mykafka.kafka.animals) t1 group by t1.value2;
```

我们对上面的SQL进行一个详细的分析：

首先，由于value是字节数组，我们将它通过CAST函数转换为字符串；

然后，使用split函数将字符串以空格进行拆分，并使用explode函数把每个单词从水平方向转换到垂直方向；

最后，结合count函数，使用group by从句，输出计算和对应的动物名称。