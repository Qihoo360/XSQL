Redis是一个可基于内存亦可持久化的日志型、Key-Value数据库，提供高性能的数据缓存。

## Installation

由于Redis提供了Java API，所以不需要任何安装。对于想要了解Jedis的用户请查阅 [Jedis官方文档](http://www.javadoc.io/doc/redis.clients/jedis/2.9.0) 。

## Configuration

 Redis接入XSQL的配置继承了[Configurations](http://10.161.184.19:51019/configurations/common/)中针对特定数据源的type、url、version、pushdown、schemas配置。这里给出一个Redis接入XSQL的配置示例：

```python
spark.xsql.datasources            redis_ds_name
spark.xsql.default.datasource     redis_ds_name
sprak.xsql.default.database       0
# 声明redis_ds_name是一个Redis数据源
spark.xsql.datasource.redis_ds_name.type     redis
# 配置jedis格式的url redis://:$password@$host:$port/[dbnum]
spark.xsql.datasource.redis_ds_name.url      redis://:xxx@xx.xx.xx.xx:xx
# 配置元数据存储文件名称，需要放置在SPARK_CONF_DIR中
spark.xsql.datasource.redis_ds_name.schemas  redis.schemas
# version为预留配置
spark.xsql.datasource.redis_ds_name.version  4.0.10
spark.xsql.datasource.redis_ds_name.pushdown true
```

## Advanced Configuration

xsql为了访问Redis，用冒号分割每个键，得到键的前缀和后缀，将一个键值对视为表的一条记录，将键的前缀视为表名，表的列结构固定为`key`和`value`，key的字段类型固定为`string`，value的字段类型取决于值类型。

| Redis概念                               | 对应的关系数据库概念                      |
| :-------------------------------------- | ----------------------------------------- |
| 数据库（默认编号0-15）                  | 数据库名                                  |
| 键（keyprefix:keysuffix）               | 字段key                                   |
| 值                                      | 字段value                                 |
| keyprefix                               | 表名                                      |
| keysuffix                               | 过滤字段suffix                            |
| 值类型（hash、list、set、zset、string） | 字段value的字段类型（string、map、array） |

**Note**

- list值类型和set值类型表示为spark的ArrayType(StringType)
- zset值类型表示为MapType(StringType,DoubleType)
- string值类型表示为StringType
- hash值类型直接表示为记录，hash的每个键作为一个字段，对应的值作为字段值，这种情况下列结构不再固定为`key`和`value`。总结如下：

| 值类型   | xsql表示类型                   |
| -------- | ------------------------------ |
| string   | StringType                     |
| list set | ArrayType(StringType)          |
| zset     | MapType(StringType,DoubleType) |
| hash     | 一系列类型可以不同的字段       |

**Note**

当相同前缀的键，值类型不统一时，不能使用xsql进行查询

当list、set、zset、hash出现复合对象时，即数组的数组时，不能使用xsql进行查询。

### 元数据配置方法：

Redis自身没有任何元数据，用户在使用xsql查询Redis之前，需要显式给出数据表的元数据，具体包括：

### 1. 直接编辑schemas文件

   redis.schemas存储的元数据是以数据库为索引的json对象，建议在编辑schemas文件时使用 [JSON编辑器](../../jsoneditor/index.html?q=1) 避免低级错误。

**Examples**

```json
{
    "0":[
        {
            "table":"RedisSourceExampleTable",
            "fields":[
                {
                    "name":"type"，
                    "type":"string"
                }
            ]
        },{
            "table":"hashTable",
            "fields":[
                {
                    "name":"key",
                    "type":"string"
                },{
                    "name":"name",
                    "type":"string"
                },{
                    "name":"age",
                    "type":"int"
                }
            ]
        },...
    ],
    "1":[
        ...
    ]
}
```

**Note**

```json
"fields":[
    {
        "name":"type"，
        "type":"string"
    }
]
上面内容等价于下面内容，属于简写
"fields":[
    {
        "name":"key"，
        "type":"string"
    },{
        "name":"value",
        "type":"string"
    }
]
```

### 2. 自动推断未注册表的元数据（不推荐）

xsql可以通过全库扫描，找到一个以表名为前缀的键，获取该键的值类型，从而推测出一个未注册表的元数据。但自动推断方式缺点比较明显：

- 全库扫描的速度缓慢
- 使用任意一个前缀满足要求的键去推测元数据类型，理论上是不可靠的

因此xsql对redis开启自动推断机制，只是为了查询redis中的临时表，以及应对某些修改并重新加载schemas文件代价较大的情况。

## Execution

### 可下推的select语句

#### 点查询 `key =` `key in` `suffix =` `suffix in`

------

获取特定键 对应的值

**Examples**

```bash
> use redis.`0`;
> select * from `360:redis` where key = '360:redis:monitor';
key	value
360:redis:monitor	1539853184
> select * where key = '360:redis:monitor';
key	value
360:redis:monitor	1539853364
> select * where key in ('360:redis:monitor','360:redis:save')
key	value
360:redis:monitor	1539854444
360:redis:save	idobi
> select * from `360:redis` where suffix = 'monitor';
key	value
360:redis:monitor	1539854624
> select * from `360:redis` where suffix in ('monitor','save');
key	value
360:redis:monitor	1539854684
360:redis:save	idobi
```

#### 限制value范围

#### `range between and`（list、zset）

------

获取坐标介于特定区间的值

**Examples**

```bash
> select * from `redis-list`;
key	value
redis-list:1	["1","123","231","231"]
redis-list:2	["row3","row2","list1"]
> select * from `redis-list` where range between 0 and -1;
key	value
redis-list:1	["1","123","231","231"]
redis-list:2	["row3","row2","list1"]
> select * from `redis-list` where range between 1 and 2;
key	value
redis-list:1	["123","231"]
redis-list:2	["row2","list1"]
```

#### `score between and`（zset）

------

获取score介于特定区间的值

**Examples**

```bash
> select * where key = 'redis-zset:2' and score between 20 and 1000;
key	value
redis-zset:2	{"12":700.0}
```

### 使用Spark函数处理value

- 对于值类型为list和set的表，建议使用[sort_array](../functions.md#sort_array)、[array_contains](../functions.md#array_contains)等函数
- 对于值类型为zset的表，建议使用[map_values](../functions.md#map_values)等函数
- 对于值类型为string的表，建议使用[regexp_extract](../functions.md#regexp_extract)等函数
- 对于值类型为json string的表，建议使用[json_tuple](../functions.md#json_tuple)、[get_json_object](../functions.md#get_json_object)、[from_json](../functions.md#from_json)、[to_json](../functions.md#to_json)等函数

Spark提供的所有函数见 [链接](../functions.md)。