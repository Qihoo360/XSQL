XSQL支持SQL2003标准，并且能够运行所有99 TPC-DS中的查询。更为突出的是，XSQL已经支持：

- 对ANSI-SQL和Hive QL进行支持的本地SQL解析器。
- 本地DDL命令的实现。
- 子查询的支持，包括：
  - 不相关的标量子查询。
  - 相关的标量子查询。
  - 谓词In中的子查询（在WHERE/HAVING从句中）。
  - 谓词Not In中的子查询（在WHERE/HAVING从句中）。
  - 谓词Exists中的子查询（在WHERE/HAVING从句中）。
  - 谓词Not Exists中的子查询（在WHERE/HAVING从句中）。
- 视图的支持。

除了以上特点外，XSQL由于引入了DataSource的概念，因此XSQL增加了一些SQL2003中不存在的语法，并扩展了一些SQL2003的语法。下面将基于[Getting Started](../getting_started/Getting_Started.md)中的MySQL实例，对这些额外的语法进行介绍。

## DataSource相关语法

​	为了便于用户查看XSQL中目前已经配置可访问的DataSource，我们增加了以下命令：

```mysql
show datasources;
```

**Single DataSource**

在XSQL中执行此DDL语句，将输出每个数据源在xsql.conf中配置的名称：

```mysql
spark-xsql> show datasources;
18/10/29 18:51:23 INFO SparkXSQLShell: current SQL: show datasources
18/10/29 18:51:23 WARN SparkXSQLShell: hive.cli.print.header not configured, so doesn't print colum's name.
default
Time taken: 0.039 s
spark-xsql>
```

**Multi DataSource**

我们暂时先使用以下命令退出XSQL：

```mysql
exit;
```

我们在xsql.conf配置中增加一个Elasticsearch数据源，命名为myes。现在再次进入XSQL命令行，并再次查看有哪些数据源：

```mysql
spark-xsql> show datasources;
18/10/29 19:26:37 INFO SparkXSQLShell: current SQL: show datasources
18/10/29 19:26:37 WARN SparkXSQLShell: hive.cli.print.header not configured, so doesn't print colum's name.
myes
default
Time taken: 0.02 s
spark-xsql>
```

**<a name="cd">Current DataSource</a>**

​	在[Configurations](../configurations/common.md)中曾经介绍过spark.xsql.default.datasource属性，该属性的默认值是default，用于指定默认的当前数据源。假如我们现在的当前数据源就是default，那么执行的所有SQL，都默认在当前数据源中，除非在SQL中明确指定了数据源。

​	此处以展示Database清单为例，如果只输入了：

```mysql
show databases;
```

那么，展示的是数据源myes中的Database清单，还是数据源default中的Database清单？XSQL在用户未明确指定DataSource的情况下，将在当前数据源中。因此，将展示出以下信息：

```mysql
spark-xsql> show databases;
18/10/29 15:23:44 INFO SparkXSQLShell: spark.enable.hiverc:true
18/10/29 15:23:44 INFO SparkXSQLShell: current SQL: show databases
18/10/29 15:23:48 WARN SparkXSQLShell: hive.cli.print.header not configured, so doesn't print colum's name.
default	MYSQL	mysqltest
Time taken: 0.028 s
spark-xsql>
```

即default数据源中的Database清单。

​	我添加的数据源myes中有xitong_xsql_test、yarn_use_ms_2018和xsql_test三张表，那么如何展示它们呢？可以增加DataSource进行限定：

```mysql
show databases [In|From] myes;
```

使用以上命令，将得到类似下面的输出：

```mysql
spark-xsql> show databases in myes;
18/10/30 10:27:28 INFO SparkXSQLShell: current SQL: show databases in myes
18/10/30 10:27:28 WARN SparkXSQLShell: hive.cli.print.header not configured, so doesn't print colum's name.
myes	ELASTICSEARCH	xitong_xsql_test
myes	ELASTICSEARCH	yarn_use_ms_2018
myes	ELASTICSEARCH	xsql_test
Time taken: 0.084 s
spark-xsql>
```

或者：

```mysql
spark-xsql> show databases from myes;
18/10/30 10:30:27 INFO SparkXSQLShell: current SQL: show databases from myes
18/10/30 10:30:27 WARN SparkXSQLShell: hive.cli.print.header not configured, so doesn't print colum's name.
myes	ELASTICSEARCH	xitong_xsql_test
myes	ELASTICSEARCH	yarn_use_ms_2018
myes	ELASTICSEARCH	xsql_test
Time taken: 0.035 s
spark-xsql>
```

其他SQL都有类似的当前数据源语义。

**Current Database**

​	同MySQL或Hive相类似，XSQL也提供了当前的Database。在[Configurations](../configurations/common.md)中曾经介绍过spark.xsql.default.database属性，该属性的默认值是default，用于指定默认的当前数据库。假如我们现在的当前数据源依然是default，当前数据库是mysqltest，那么执行的所有SQL，都默认在当前数据源的当前数据库中，除非在SQL中明确指定了数据源和数据库。

​	此处以展示Table清单为例，如果只输入了：

```mysql
show tables;
```

根据<a href="#cd">Current DataSource</a>小节的介绍，当前数据源为default，因此查看的必然是default数据源中某个Database中的Table清单。由于，当前数据库是mysqltest，因此将展示出mysqltest中的Table清单：

```mysql
spark-xsql> show tables;
18/10/30 11:25:11 INFO SparkXSQLShell: current SQL: show tables
18/10/30 11:25:11 WARN SparkXSQLShell: hive.cli.print.header not configured, so doesn't print colum's name.
default	mysqltest	activities	false
default	mysqltest	course	false
default	mysqltest	geonames	false
default	mysqltest	geonames_small	false
default	mysqltest	person	false
default	mysqltest	taxis	false
default	mysqltest	taxis_type	false
default	mysqltest	test123	false
Time taken: 0.013 s
spark-xsql>
```

如果用户不想通过use来切换当前数据源和数据库，但是又想查看数据源myes中数据库xsql_test的Table清单，那么该如何操作？可以增加DataSource和Database进行限定：

```mysql
show tables [In|From] myes.xsql_test;
```

使用以上命令，将得到类似下面的输出：

```mysql
spark-xsql> show tables in myes.xsql_test;
18/10/30 11:29:56 INFO SparkXSQLShell: current SQL: show tables in myes.xsql_test
18/10/30 11:29:56 WARN SparkXSQLShell: hive.cli.print.header not configured, so doesn't print colum's name.
myes	xsql_test	city	false
myes	xsql_test	class_room	false
myes	xsql_test	doc	false
myes	xsql_test	es_test	false
myes	xsql_test	user	false
Time taken: 0.03 s
spark-xsql>
```

或者：

```mysql
spark-xsql> show tables from myes.xsql_test;
18/10/30 11:29:56 INFO SparkXSQLShell: current SQL: show tables in myes.xsql_test
18/10/30 11:29:56 WARN SparkXSQLShell: hive.cli.print.header not configured, so doesn't print colum's name.
myes	xsql_test	city	false
myes	xsql_test	class_room	false
myes	xsql_test	doc	false
myes	xsql_test	es_test	false
myes	xsql_test	user	false
Time taken: 0.03 s
spark-xsql>
```

## Current DataSource与Current Database对SQL的影响

这里，我们再次强调当前数据源是`default`，当前数据库是`mysqltest`，然后对各种SQL与Current DataSource及Current Database之间的关系，通过一个表格来说明：

| 不指定DataSource的SQL                                        | 等效SQL                                                      |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| use mysqltest                                                | use default.mysqltest                                        |
| use myes.xsql_test                                           | use myes.xsql_test                                           |
| show databases                                               | show databases [in\|from] default                            |
| show databases [in\|from] myes                               | show databases [in\|from] myes                               |
| show tables                                                  | show tables [in\|from] default.mysqltest                     |
| show tables [in\|from] mysqltest                             | show tables [in\|from] default.mysqltest                     |
| show tables [in\|from] myes.xsql_test                        | show tables [in\|from] myes.xsql_test                        |
| create table test (key String,value String) ...              | create table default.mysqltest.test (key String,value String) ... |
| create table mysqltest.test (key String,value String) ...    | create table default.mysqltest.test (key String,value String) ... |
| create table myes.xsql_test.test (key String,value String) ... | create table myes.xsql_test.test (key String,value String) ... |
| select * from course                                         | select * from default.mysqltest.course                       |
| select * from mysqltest.course                               | select * from default.mysqltest.course                       |
| select * from myes.xsql_test.city                            | select * from myes.xsql_test.city                            |

XSQL支持的各种数据源，所能支持的SQL语法不同，具体请阅读各个数据源对SQL的介绍。

## Create table语法：创建空的数据源表

​	XSQL尽量将各种异构数据源的表创建语法统一起来。但是各种数据源天生支持的create table语法就是不同的，比如Hive的类似于下面这样：

```mysql
create table test (
  key String,
  value String,
  other String
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
```

但是，MySQL的则是：

```mysql
CREATE TABLE person (
	id int NOT NULL AUTO_INCREMENT,
	name varchar(8),
	birthday datetime,
	CONSTRAINT pk__person PRIMARY KEY (id)
)
```

两者支持的数据类型首先是不一样的，此外语法上也有差异（比如Hive可以指定行格式，而MySQL可以支持自增主键）。

还有些数据源不支持SQL或者支持的SQL非常有限，因此从底层就决定了XSQL提供的create table语法因数据源的不同而不同。XSQL种创建Hive之外的其他数据源的表时，需要指定数据源。

以MySQL为例，使用XSQL创建表的SQL是：

```mysql
CREATE TABLE person (
	id int NOT NULL AUTO_INCREMENT,
	name varchar(8),
	birthday datetime,
	CONSTRAINT pk__person PRIMARY KEY (id)
) using mysql
```

可以看到创建MySQL表时，在using后跟了"mysql"这个数据源标识。那么其他类型的数据源需要跟什么标识呢？请看下表：

**<a name="using">各数据源对应using标识表</a>**

| 序号 | 数据源类型            | 数据源标识 | 备注                        |
| ---- | --------------------- | ---------- | --------------------------- |
| 1    | Hive                  | -          | 操作hive数据源不需要using   |
| 2    | MySQL                 | mysql      |                             |
| 3    | Elasticsearch         | es         |                             |
| 4    | MongoDB               | mongo      |                             |
| 5    | HBASE                 | hbase      |                             |
| 6    | Redis                 | redis      | Redis的create table暂未实现 |
| 7    | Druid                 | druid      | Druid的create table暂未实现 |
| 8    | Kafka                 | kafka      | Kafka的create table暂未实现 |
| 9    | Oracle（Pending）     | -          |                             |
| 10   | Clickhouse（Pending） | -          |                             |

## CTAS语法：使用查询结果创建数据源表

XSQL目前支持将select查询结果直接创建为hive表或mysql表，对应命令如下：

```mysql
//查询结果创建为hive表
create table dst_ds.dst_db.dst_tb as select * from src_ds.src_db.dst_tb;
//查询结果创建为mysql表
create table dst_ds.dst_db.dst_tb using mysql as select * from src_ds.src_db.dst_tb;
```

## Insert into语法：将查询结果添加到已存在的数据源表

XSQL目前支持将select查询结果直接添加到hive表或mysql表，对应命令如下：

```mysql
//查询结果追加到hive表
insert into (table)? dst_table select * from src_table;
//查询结果追加到mysql表
insert into (table)? dst_table using mysql select * from src_table;
//使用查询结果替换hive表
insert overwrite table dst_table select * from src_table;
//使用查询结果替换mysql表
insert overwrite table dst_table using mysql select * from src_table;
```

## Insert overwrite directory语法：输出到hdfs目录或本地磁盘

XSQL支持将select查询结果直接输出到hdfs目录或本地磁盘目录，对应命令如下：

```mysql
//将查询结果输出到hdfs目录
insert overwrite directory (path='hdfspath') (row format xxx)?;
//将查询结果输出到本地目录
insert overwrite local directory (path='localpath') (row format xxx)?;
```

## Add/Remove/Refresh datasource语法：操纵临时数据源

为了应对一些XSQL作为常驻查询服务（thrift jdbc服务、rest服务）的应用场景下，用户所要查询的数据源发生变化，但修改xsql.conf后重启SparkSession会打断任务流程的问题。XSQL特别提供了不需要重启SparkSession的数据源添加/删除/刷新接口：

```mysql
//添加临时数据源
add datasource ds_name (
	type='',
	url='',
	user='',
	password='',
	version=''
)
//移除临时数据源
remove datasurce (if exists)? ds_name
//刷新数据源元数据
refresh datasource ds_name
```

其中，add datasource括号内的配置项与[xsql.conf中数据源配置方式](../../configurations/common/#_1)相同，为了代码简洁，这里不再需要spark.xsql.datasource.ds_name的配置项前缀。

## Create temproary view语法：操纵临时数据表

添加临时数据表的需求背景与添加临时数据源的如出一辙，且添加临时数据表的解析代价较小，是SparkSQL原生支持的一种语法，命令具体格式如下：

```mysql
create (or replace)? temporary view view_name using mysql
    options(
        url="jdbc:mysql://hostname:port/db"，
        user="your_username",
        password="your_password",
        dbtable="tb"
    )
```

其中，using的可选项见 <a href="#using">各数据源对应using标识表</a>，不同数据源的options可能不尽相同。临时数据表在使用完毕后，可以使用drop table语法删除，临时数据表的删除是反注册，而不会实际删除原表，且XSQL优先将表名解析为临时数据表，如果临时数据表和原表表名相同，drop table也不会出现解析失准。但为了避免不必要的麻烦，还是建议临时数据表不要使用原表表名。

**Examples:**

将hdfs文件注册为临时数据表

```mysql
create or replace temporary view view_name
	using json/csv/orc/parquet/libsvm/text
	options(path='hdfspath');
```

