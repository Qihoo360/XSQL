# 配置清单

| Property Name                               | Default | Meaning                                                      |
| ------------------------------------------- | ------- | ------------------------------------------------------------ |
| spark.xsql.properties.file | xsql.conf | 用于指定包含所有XSQL配置的属性文件。单独的XSQL属性文件便于对XSQL配置信息进行运维、管理。如果不指定此属性，用户需要将XSQL参数配置到spark-default.conf或者通过--conf传递。 |
| spark.xsql.datasources                      | None    | 用于指定用户使用的数据源。数据源可以有多个，之间使用英文逗号分隔，例如：default,customer,order。 |
| spark.xsql.default.datasource               | default | 用于指定XSQL默认的数据源。                                   |
| spark.xsql.default.database                 | default | 用于指定XSQL在默认数据源中默认使用的数据库。                 |
| spark.xsql.datasource.$dataSource.type      | None    | 用于指定数据源的类型，例如：mysql。                          |
| spark.xsql.datasource.$dataSource.url       | None    | 用于指定数据源的url地址，不同数据源的Url格式不同，请分别查看各数据源Configuration配置。 |
| spark.xsql.datasource.$dataSource.user      | None    | 用于指定数据源账号的用户名。                                 |
| spark.xsql.datasource.$dataSource.password  | None    | 用于指定数据源账号的密码。                                   |
| spark.xsql.datasource.$dataSource.version   | None    | 用于指定数据源的版本，例如MySQL的版本有5.6.19。              |
| spark.xsql.datasource.$dataSource.whitelist | None    | 用于指定数据源的Database及Table白名单。由于一些数据源中有大量的Database及Table，所以会导致启动XSQL时花费大量的时间。另一方面，每个用户只对其中的少数Database及Table感兴趣，因此可以提供白名单加速XSQL的启动。 |
| spark.xsql.datasource.$dataSource.pushdown  | true    | 用于控制指定数据源的查询是否优先采用下推方式。此配置将建议XSQL对此数据源的查询使用下推方式，但是并不能保证。很多情况下，XSQL并不会下推，例如：数据源查询还包含有其他数据源的子查询，或者引用了外部查询的别名。 |
| spark.xsql.datasource.$dataSource.schemas   | None    | 用于定义数据源中表的Schema信息。只适用于无严格Schema的数据源，例如：Redis、HBASE、MongoDB |
| spark.xsql.datasource.$dataSource.schemas.discover | false | 对于无严格Schema的数据源，使用spark.xsql.datasource.$dataSource.schemas指定Schema配置文件，对于用户不太友好，而且一些复杂数据类型的定义（例如：ElasticSearch的nested类型）也十分繁琐。XSQL提供了对schema信息进行探索的能力，用户可以打开此开关，启用schema探索。注意：目前，此配置只对ElasticSearch和MongoDB有效。 |
| spark.xsql.datasource.$dataSource.cache.level | 1     | 用于指定数据源的元数据缓存级别，1表示Level One，2表示Level Two。 |
| spark.xsql.datasource.$dataSource.cluster   | None    | 用于定义数据源优先采用的Yarn集群。如果用户首次提交非下推的任务，那么此任务将会被提交到对应的Yarn集群。如果未指定此配置，对于Hive将选择Hive元数据服务所在的集群，其他数据源则仍然选择$XSQL_HOME/hadoopconf/yarn-site.xml文件所配置的Yarn集群。 |
| spark.xsql.yarn.$clusterName                | None    | 用于指定用户使用的Yarn集群的名称及相关配置文件。             |

## 配置数据源库表白名单

​	白名单是通过json格式的配置文件进行配置的，为便于用户理解，我们指定default数据源的白名单配置文件为hive-whitelist.conf：

```properties
spark.xsql.datasource.default.whitelist        hive-whitelist.conf
```

这里给出一个简单的hive-whitelist.conf的配置内容：

```json
{
  "default": {
    "includes": [
      "tableA",
      "tableB",
      "tableC",
      "tableD",
      "tableE",
      "tableF"
    ],
    "excludes": ["tableD"]
  }
}
```

default是数据库名称。数据库级别只有白名单，而没有黑名单，也就是说XSQL中只有在白名单中明确配置的数据库才是可见的。每个数据库都可以配置includes和excludes两个属性，分别表示白名单和黑名单。在白名单中包括了table[A-F]共6张表，同时黑名单中包括了tableD一张表。在XSQL中黑名单拥有最高的优先级，将会从白名单中清楚表，因此在XSQL中只有table[A-D]、tableE、tableF共5张表可见。表默认是属于白名单的，所以当用户需要数据库中的所有表都可见时，可以不配置includes。

## 配置NoSQL数据源的Schema文件

​	有些类型的数据源没有明确、严格的Schema定义，XSQL无法通过访问数据源得到各个表的Schema信息。为了能够接入XSQL，需要用户手动配置数据源的Schema信息。配置采用了json格式，就像下面的例子一样：

```json
{
	"database_A": [{
			"table": "table_A_1",
			"fields": [{
					"name": "_id",
					"type": "object_Id",
					"hidden": false
				}, {
					"name": "item",
					"type": "string",
					"hidden": false
				},
				{
					"name": "qty",
					"type": "int",
					"hidden": false
				},
				{
					"name": "tags",
					"type": "array<string>",
					"hidden": false
				},
				{
					"name": "dim_cm",
					"type": "array<int>",
					"hidden": false
				}
			]
		},
		{
			"table": "table_A_2",
			"fields": [{
					"name": "id",
					"type": "long",
					"hidden": false
				},
				{
					"name": "name",
					"type": "string",
					"hidden": false
				},
				{
					"name": "is_man",
					"type": "boolean",
					"hidden": false
				},
				{
					"name": "age",
					"type": "int",
					"hidden": false
				},
				{
					"name": "salary",
					"type": "double",
					"hidden": false
				},
				{
					"name": "format_salary",
					"type": "decimal(12,2)",
					"hidden": false
				},
				{
					"name": "graduation_time",
					"type": "date",
					"hidden": false
				},
				{
					"name": "birthday",
					"type": "string",
					"hidden": false
				},
				{
					"name": "score",
					"type": "decimal(5,1)",
					"hidden": false
				}
			]
		}
	],
	"database_B": [{
		"table": "table_B_1",
		"fields": [{
				"name": "_id",
				"type": "object_Id",
				"hidden": false
			}, {
				"name": "VendorID",
				"type": "int",
				"hidden": false
			},
			{
				"name": "pickup_datetime",
				"type": "string",
				"hidden": false
			},
			{
				"name": "total_amount",
				"type": "double",
				"hidden": false
			}
		]
	}]
}
```

此处以MongoDB的Schema为例。其他数据源主要是字段允许的类型不同。

## 选择运行集群

有时候，用户的数据源位于不同的集群环境中。为了使得Yarn分配的资源与数据之间更加接近，可以为数据源显示指定Yarn集群。spark.xsql.yarn.$clusterName属性中的clusterName是用户给Yarn集群起的别名，对应的值必须是一个有效的属性配置文件。

例如，我们定义了一个名为yarn-cluster0的Yarn集群，并指定了对应的配置文件为

```properties
spark.xsql.yarn.cluster0  yarn-cluster0.conf
```

yarn-cluster0.conf文件中的配置信息可能为：

```properties
spark.yarn.stagingDir    hdfs://namenode.dfs.cluster0.yahoo.com:9000/home/spark/cache
spark.hadoop.yarn.resourcemanager.cluster-id  cluster0-yarn
spark.hadoop.yarn.resourcemanager.zk-state-store.address  m2.dfs.cluster0.qihoo.net:2181,m3.dfs.cluster0.yahoo.com:2181,m4.dfs.cluster0.yahoo.com:2181,m5.dfs.cluster0.yahoo.com:2181,m6.dfs.cluster0.yahoo.com:2181
spark.hadoop.yarn.resourcemanager.zk-address  m2.dfs.cluster0.yahoo.com:2181,m3.dfs.cluster0.yahoo.com:2181,m4.dfs.cluster0.yahoo.com:2181,m5.dfs.cluster0.yahoo.com:2181,m6.dfs.cluster0.yahoo.com:2181
spark.hadoop.yarn.resourcemanager.zk-state-store.parent-path  /cluster0/yarn/rmstore
spark.hadoop.yarn.resourcemanager.hostname.rm1  m7.dfs.cluster0.yahoo.com
spark.hadoop.yarn.resourcemanager.hostname.rm2  m8.dfs.cluster0.yahoo.com
spark.hadoop.yarn.resourcemanager.scheduler.address.rm1  m7.dfs.cluster0.yahoo.com:8830
spark.hadoop.yarn.resourcemanager.resource-tracker.address.rm1  m7.dfs.cluster0.yahoo.com:8831
spark.hadoop.yarn.resourcemanager.address.rm1  m7.dfs.cluster0.yahoo.com:8832
spark.hadoop.yarn.resourcemanager.admin.address.rm1  m7.dfs.cluster0.yahoo.com:8833
spark.hadoop.yarn.resourcemanager.webapp.address.rm1  m7.dfs.cluster0.yahoo.com:8888
spark.hadoop.yarn.resourcemanager.ha.admin.address.rm1  m7.dfs.cluster0.yahoo.com:23142
spark.hadoop.yarn.resourcemanager.scheduler.address.rm2  m8.dfs.cluster0.yahoo.com:8830
spark.hadoop.yarn.resourcemanager.resource-tracker.address.rm2  m8.dfs.cluster0.yahoo.com:8831
spark.hadoop.yarn.resourcemanager.address.rm2  m8.dfs.cluster0.yahoo.com:8832
spark.hadoop.yarn.resourcemanager.admin.address.rm2  m8.dfs.cluster0.yahoo.com:8833
spark.hadoop.yarn.resourcemanager.webapp.address.rm2  m8.dfs.cluster0.yahoo.com:8888
spark.hadoop.yarn.resourcemanager.ha.admin.address.rm2  m8.dfs.cluster0.yahoo.com:23142
spark.hadoop.yarn.log.server.url  http://m8.dfs.cluster0.yahoo.com:19888/jobhistory/logs
```

## 下推执行时的条数限制

当对数据源的查询实际采用了下推方式时，XSQL将在用户未指定limit时，默认会加上limit 10的限制。这样做得目的有两点：

首先，Pushdown实际运行在Driver进程中，Driver的内存往往是有限的。如果不加以限制，将数GB甚至更多的数据读取到Driver，将会引发XSQL的内存问题，譬如：GC频繁、OOM。

其次，对于某些数据源，如果没有显式的limit限制，数据源本身也有限制（例如Elasticsearch会加上limit 10的限制）。

如果用户需要更多的返回数据，请显式增加limit语句。目前XSQL能够接受的limit的最大值是10000。

## 使用私有的xsql.conf

为了防止用户的数据源连接帐号密码泄露，XSQL提供了使用私有xsql.conf的配置方法：

```properties
spark.xsql.properties.file customer_name.conf
spark.xsql.conf.dir /home/username/xxx/xx
```

与之配合的在目录/home/username/xxx/xx下创建customer_name.conf，写入适当的数据源连接信息。此类配置不建议修改在spark-defaults.conf配置文件中，完整的提交命令如下：

```shell
/usr/bin/hadoop/software/spark-xsql/bin/spark-xsql --conf spark.xsql.properties.file=customer_name.conf --conf spark.xsql.conf.dir=/home/username/xxx/xx
```

## 使用字符串变量拼接SQL查询

对于一些定时任务，查询语句中需要传入随时间变化的查询变量，XSQL同样提供了这样的功能，首先看下面的示例提交命令：

```
/usr/bin/hadoop/software/spark-xsql/bin/spark-xsql --conf spark.key=value
```

那么，在之后的查询语句可以以下表达式：

```sql
select '${spark.key}' as date from example_table;
```

注意，${}符号必须被单引号或双引号所包裹，不能够独立存在，否则会语法解析报错。由于SparkConf要求所有的配置项必须以`spark.`开头，因此\${}内的查询变量也必须以`spark.`开头。

## 开启HBase或Druid数据源

为了控制XSQL依赖jar包的数量，XSQL默认不开启依赖较多的HBase模块和Druid模块，当需要使用这些模块时，可以添加以下配置：

```properties
spark.driver.extraClassPath  /usr/bin/hadoop/software/spark-xsql/hbase/*:/usr/bin/hadoop/software/spark-xsql/druid/*
spark.xsql.extraDatasourceManagers org.apache.spark.sql.xsql.manager.HBaseManager,org.apache.spark.sql.xsql.manager.DruidManager
```

## 开启hive权限控制

对于XSQL带权限控制的部署版本，要想开启权限控制，还需要添加额外的配置：

```
spark.sql.extensions org.apache.ranger.authorization.spark.authorizer.RangerSparkSQLExtension
```

hive-site.mxl也需要添加如下配置：

```xml
<property>
    <name>hive.security.authorization.enabled</name>
        <value>false</value>
        </property>
<property>
    <name>hive.security.authorization.manager</name
     <value>org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory</value>
   </property>
<property>
    <name>hive.security.authenticator.manager</name>
        <value>org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator</value>
        </property>
<property>
    <name>hive.conf.restricted.list</name>
 <value>hive.security.authorization.enabled,hive.security.authorization.manager,hive.security.authenticator.manager</value>
        </property>
```

## 开启自适应执行

对于XSQL带自适应执行的部署版本，要想开启自适应执行，还需要添加额外的配置：

```
spark.sql.adaptive.enabled true
spark.sql.adaptive.shuffle.targetPostShuffleInputSize 134217728
spark.sql.adaptive.join.enabled true
spark.sql.adaptive.skewedJoin.enabled true
spark.sql.adaptive.skewedPartitionFactor 5
spark.sql.adaptive.skewedPartitionSizeThreshold 134217728
spark.sql.adaptive.skewedPartitionRowCountThreshold 20000000
spark.shuffle.statistics.verbose true

spark.shuffle.service.enabled  false
spark.dynamicAllocation.enabled   false
spark.executor.instances     150
```

