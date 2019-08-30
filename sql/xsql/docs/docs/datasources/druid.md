Druid 是一个开源的，分布式的，列存储的，适用于实时数据分析的存储系统，能够快速聚合、灵活过滤、毫秒级查询、和低延迟数据导入。
## Installation
​ 由于Druid提供了Rest API，所以不需要任何安装。对于想要了解Druid Rest API的用户请查阅 [官方文档](http://druid.io/docs/latest/querying/querying.html)。
## Configuration

Druid接入XSQL的配置继承了[Configurations](../configurations/common.md)中针对特定数据源的type、version、schemas配置。Druid接入XSQL还有一些特有的配置，下表将对他们进行介绍。
    
| Property Name                          | Default | Meaning                                 |
| -------------------------------------- | ------- | --------------------------------------- |
| spark.xsql.datasource.$dataSource.coordinator.uri | None    | 配置coordinator uri              |

这里给出一个Druid接入XSQL的配置示例：

```
xsql.conf文件:
spark.xsql.datasource.mydruid.type   DRUID
spark.xsql.datasource.mydruid.uri    http://r883.dfs.shbt.qihoo.net:8082
spark.xsql.datasource.mydruid.coordinator.uri    r883.dfs.shbt.qihoo.net:8081
spark.xsql.datasource.mydruid.user   xxxx
spark.xsql.datasource.mydruid.password   xxxx
spark.xsql.datasource.mydruid.version   0.10.1
spark.xsql.datasource.mydruid.whitelist druid-whitelist.conf

druid-whitelist.conf文件：
数据库名称必须为：druid
lineitem10,xsql为Druid的DataSource
{
  "druid": {
    "includes": [
        "lineitem10",
        "xsql"
    ],
    "excludes": []
   }
}
```

## Execution
### 1、Select Queries


```
select * from xx where __time >= '' and __time <= '' and xxx or xxx 
```

**Examples**

```bash
> select * 
> from mydruid.druid.lineitem10 
> where __time >='1991-01-01' and __time <'1999-01-01' 
> and l_commitdate='1997-02-08'
> limit 10;
结果：
1996-11-11T00:00:00.000+08:00   1       ic deposits. quickly    1997-02-08      0.07999999821186066     59096.3984375   3       O       36001798        1222868    33      1996-11-23      N       COLLECT COD     TRUCK   72893   0.07000000029802322     NULL
1996-11-11T00:00:00.000+08:00   1       ns. requests cajole blith       1997-02-08      0.05000000074505806     28436.0703125   2       O       43300226  57715    17      1996-11-12      N       COLLECT COD     MAIL    57716   0.05999999865889549     NULL
1996-11-11T00:00:00.000+08:00   1       lar sentiments. quickly pen     1997-02-08      0.07999999821186066     5510.1298828125 4       O       45456578  979757   3       1996-11-18      N       NONE    SHIP    29776   0.03999999910593033     NULL
1996-11-11T00:00:00.000+08:00   1        serve carefully along th       1997-02-08      0.019999999552965164    45402.4609375   2       O       47689120  455032   46      1996-11-16      N       COLLECT COD     MAIL    80037   0.07999999821186066     NULL
1996-11-11T00:00:00.000+08:00   1       ar requests wa  1997-02-08      0.03999999910593033     40127.4296875   1       O       47689120        934876  211996-12-05       N       NONE    AIR     34877   0.019999999552965164    NULL
1996-11-11T00:00:00.000+08:00   1        never final accounts wake carefully qu 1997-02-08      0.0     3999.659912109375       4       O       58656996  971262   3       1996-11-12      N       NONE    RAIL    46290   0.05000000074505806     NULL
1996-11-12T00:00:00.000+08:00   1       al requests wake. blithely unusual dep  1997-02-08      0.07999999821186066     68074.078125    4       O       31170243   1046893 37      1996-11-23      N       DELIVER IN PERSON       MAIL    46894   0.029999999329447746    NULL
1996-11-12T00:00:00.000+08:00   1       uctions affix carefully! furio  1997-02-08      0.029999999329447746    21567.69921875  2       O       35882630  1417623  14      1996-12-01      N       COLLECT COD     AIR     17624   0.029999999329447746    NULL
1996-11-12T00:00:00.000+08:00   1       carefully alo   1997-02-08      0.03999999910593033     9488.580078125  4       O       44366240        1192489 6 1996-11-20       N       TAKE BACK RETURN        FOB     42512   0.05000000074505806     NULL
1996-11-12T00:00:00.000+08:00   1       tes. fluffily ironic deposits   1997-02-08      0.05999999865889549     54193.6015625   1       O       46005638  1758635  32      1996-12-02      N       COLLECT COD     RAIL    33687   0.05999999865889549     NULL

```

| 实现方式 | 等效查询 |
| -------------- | ----------------------------- |
| xsql | `select * from mydruid.druid.lineitem10  limit 3;` |
| druid json    | {"queryType":"select","dataSource":"lineitem10","granularity":"all"...} |

```
1、__time:必写项 转换成Druid intervals
2、granularity:"all", "none", "second","minute", "fifteen_minute", "thirty_minute","hour", "day", "week", "month", "quarter", "year" 默认"all"
3、where 后面的条件除了__time,granularity 其他条件转化Druid的filter </font>
```

### 2、GroupBy Queries

```
select l_shipinstruct,sum(l_quantity),sum(l_discount) 
from mydruid.druid.lineitem10 
where __time >='1991-01-01' and __time <'1999-01-03' and l_commitdate='1997-02-08' 
group by l_shipinstruct limit 10;
```
**Examples**

```bash
> select l_shipinstruct,sum(l_quantity) as l_quantity,sum(l_discount) 
> from mydruid.druid.lineitem10 
> where __time >='1991-01-01' and __time <'1999-01-03' and l_commitdate='1997-02-08' 
> group by l_shipinstruct 
> limit 10;
结果:
COLLECT COD     	158972  311.4700002372265
DELIVER IN PERSON   158778  305.7599999830127
NONE    		    160785  313.9799999296665
TAKE BACK RETURN    160337  314.15999987721443
```
```
--也支持granularity时间粒度的查询
select l_shipinstruct,sum(l_quantity) as l_quantity,sum(l_discount),count(__time) 
from mydruid.druid.lineitem10 
where __time >='1991-01-01' and __time <'1999-01-01' and granularity = 'year' 
group by l_shipinstruct order by l_quantity asc limit 10;
结果：
DELIVER IN PERSON       48373277        94782.64012855478       1895995
TAKE BACK RETURN        48412356        94971.76013177447       1899524
NONE    48446820        94990.60012911633       1899514
COLLECT COD     48472785        95076.87012936734       1900477
TAKE BACK RETURN        58044094        113911.56015220843      2277249
DELIVER IN PERSON       58048477        113781.76015352085      2276162
带有granularity时间粒度时，如果没有group by __time 不显示时间，也可以如下操作
该操作其实是druid TopN query
```
```
带时间的group by
select __time,l_shipinstruct,sum(l_quantity) as l_quantity,sum(l_discount),count(__time) 
from mydruid.druid.lineitem10 
where __time >='1991-01-01' and __time <'1999-01-01' and granularity = 'year' 
group by l_shipinstruct,__time order by l_quantity asc limit 10;
结果：
1992-01-01T00:00:00.000+08:00   DELIVER IN PERSON       48373277        94782.63984715939       1895995
1992-01-01T00:00:00.000+08:00   TAKE BACK RETURN        48412356        94971.75982236862       1899524
1992-01-01T00:00:00.000+08:00   NONE    48446820        94990.59980535507       1899514
1992-01-01T00:00:00.000+08:00   COLLECT COD     48472785        95076.869743824 1900477
1993-01-01T00:00:00.000+08:00   TAKE BACK RETURN        58044094        113911.56018066406      2277249
1993-01-01T00:00:00.000+08:00   DELIVER IN PERSON       58048477        113781.75991821289      2276162
1993-01-01T00:00:00.000+08:00   COLLECT COD     58057693        113913.849609375        2276807
1993-01-01T00:00:00.000+08:00   NONE    58137066        113969.76986694336      2279423
1994-01-01T00:00:00.000+08:00   NONE    57931103        113608.79022216797      2272749
1994-01-01T00:00:00.000+08:00   TAKE BACK RETURN        58047015        113828.31967163086      2275702
该SQL与上面的SQL不用，该操作是Druid的group by query
```

| 实现方式       | 等效查询                                                     |
| -------------- | ------------------------------------------------------------ |
| xsql           | `select l_shipinstruct,sum(l_quantity),sum(l_discount)...;`               |
| druid json    | `{"queryType":"groupBy","dataSource":"lineitem10"...}` |



```
1、支持的聚合函数：sum,max,min,count,count(distinct)
2、count:对应Druid  { "type" : "count", "name" : <output_name> }
3、count(distinct a,b):支持多个字段，会根据a,b数据类型采用不同的算法
	hyperUnique:{ "type":"hyperUnique","name":,"fieldName":<metric_name>}
	cardinality:{"type": "cardinality","name": "xxx","fields": xx,"byRow" : true}
	cardinality:{"type": "cardinality","name": "xxx","fields": xx,"byRow" : true}
	多个字段时转换成 {"type": "cardinality","name": "distinct_people","fields": [ "first_name", "last_name" ],"byRow" : true}，cardinality只支持"byRow" : true
4、order by 目前只支持数字类型的asc,desc 字符类型的支持asc (druid中的lexicographic)</font>
```

### 3、Timeseries queries

```
select sum(l_extendedprice), sum(l_discount) 
from mydruid.druid.lineitem10 
where __time >='1991-01-01' and __time <'1999-01-03';
```

------

**Examples**

```bash
> select sum(l_extendedprice), sum(l_discount) 
> from mydruid.druid.lineitem10 
> where __time >='1991-01-01' and __time <'1999-01-03';
结果：
2.29381315672004E12     2999373.2440630347
```
```
当有聚合函数但是没有group by时，转换成druid timeseries查询
```

```
select __time,sum(l_quantity) as l_quantity,sum(l_discount),count(__time)
from mydruid.druid.lineitem10
where __time >='1991-01-01' and __time <'1997-01-04' and  granularity = 'year' 
group by __time limit 10;
```
| 实现方式       | 等效查询                                                     |
| -------------- | ------------------------------------------------------------ |
| xsql           | `select sum(l_extendedprice), sum(l_discount) where xxx` |
| druid json    | `{"queryType":"timeseries","dataSource":"lineitem10"...}` |
```bash
> select __time,sum(l_quantity) as l_quantity,sum(l_discount),count(__time) 
> from mydruid.druid.lineitem10 
> where __time >='1991-01-01' and __time <'1997-01-04' and  granularity = 'year' 
> group by __time limit 10;
结果：
1992-01-01T00:00:00.000+08:00   193705238       379821.8705188129       7595510
1993-01-01T00:00:00.000+08:00   232287330       455576.94061456993      9109641
1994-01-01T00:00:00.000+08:00   232113470       454986.6906189304       9099165
1995-01-01T00:00:00.000+08:00   232236548       455190.7406086065       9106637
1996-01-01T00:00:00.000+08:00   232654414       456373.2906226516       9123688
1997-01-01T00:00:00.000+08:00   1904679         3737.5100051425397      74869
```

| 实现方式       | 等效查询                                                     |
| -------------- | ------------------------------------------------------------ |
| xsql           | `select __time,sum(l_quantity) granularity = 'year'  group by __time` |
| druid json    | `{"queryType":"timeseries","dataSource":"lineitem10"...}` |


```
当group by 只有一个字段且为__time时,也会转换成druid timeseries查询
granularity指的是时间粒度也就是对某个时间段的统计
```


### 4、TopN queries

```
select l_shipinstruct,sum(l_quantity) as l_quantity,sum(l_discount),count(__time)
from mydruid.druid.lineitem10 
where __time >='1991-01-01' and __time <'1999-01-01'
group by l_shipinstruct 
order by l_quantity asc 
limit 10;
```
| 实现方式       | 等效查询                                                     |
| -------------- | ------------------------------------------------------------ |
| xsql           | `select sum(l_extendedprice), sum(l_discount) group by oder by ` |
| druid json    | `{"queryType":"topN","dataSource":"lineitem10"...}` |
```bash
> select l_shipinstruct,sum(l_quantity) as l_quantity,sum(l_discount),count(__time)
> from mydruid.druid.lineitem10 
> where __time >='1991-01-01' and __time <'1999-01-01' 
> group by l_shipinstruct 
> order by l_quantity asc 
> limit 10;

结果：
TAKE BACK RETURN        382377564       749815.1810177844       14995638
COLLECT COD     382410465       749926.601011185        14995241
DELIVER IN PERSON       382437327       749621.3410196826       14994611
NONE    382512680       750010.1210143827       15000562
```