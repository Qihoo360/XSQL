XSQL是一款低门槛、更稳定的分布式查询引擎。它允许你快速、近实时地查询大量数据。对于一些数据源（例如：Elasticsearch、MongoDB、Druid等），他可以大幅地降低学习曲线，并节省人力成本。除Hive外，每种数据源都支持除子查询外的下推执行优化。用户有时希望将位于不同数据源上的数据关联起来进行查询，但是由于各种数据源是异构的且一些数据源不支持SQL或者支持的SQL语法非常有限，因此传统互联网公司的做法是，将不同的数据同步到统一的存储介质中，再进行OLAP的查询。数据同步的过程中可能面临数据迁移、主从同步、网络带宽等诸多困难和挑战，而且需要浪费大量的人力、物力及时间，无法满足大数据产品当前阶段对于近实时甚至准实时的场景。通过XSQL你将可以避免数据迁移和时间浪费，更加专注于业务本身。XSQL可以通过下推、并行计算、迭代计算等底层支撑技术，对各种数据源的查询加速。

以下列出XSQL适用的用例：

- 小张是新来数据部门的社招生，有五年的数据开发经验。在之前的公司经常进行数据统计类的工作，通过SQL语句对Hive和MySQL中的数据进行检索和加工，使得本人对于SQL语法非常熟悉。由于当前产品有从海量数据中按照关键字进行搜索的需求，因此部门决定使用Elasticsearch。小张在Elasticsearch面前，完全是一个小白。小张在初步查阅了Elasticsearch的介绍后，感觉无所适从。
- A部门的业务数据大多都维护在一些MySQL表中，其中有一张customer表用来记录用户的基本信息（例如：用户ID、昵称、年龄、性别、住址等）。同时，一些博客文档的数据却存储在MongoDB的blogs集合中blogs集合的_id字段采用了用户ID。部门产品新增的需求是，在用户的“个人中心”展示用户的博客列表，列表只显示博客标题、发布时间、博客摘要等信息。对于工程师来说，如果能直接将customer表和blogs集合进行关联查询就好了。事情看似非常ease，这也是XSQL的使用场景。
- 老王是数据部门的老员工，各种大数据工具都使用的游刃有余。老王经常使用Spark的API来编写从各种异构数据源读写数据的作业，由于这种工作重复度很高，老王感觉对这门技术的反复使用非常枯燥，而且时间成本也较高，因此他希望能有一种方式可以改善现在的工作。
- B部门是一个对数据库技术非常发烧的部门，目前的各个机器上都安装了Hive、MySQL、Redis、MongoDB等一系列客户端。大家日常开发的任务也都部署在这些机器上，导致这些机器的CPU、内存、磁盘等资源常常出现报警。使用XSQL可以避免Hive之外的各种数据库客户端的安装。
- C部门对Druid有广泛使用，通过预计算加快了查询效率。Web端需要展示Druid结果及其配置信息，但是配置信息存储在MySQL中。Web端对查询展示有极高的响应需求，因此C部门利用XSQL将Druid查询结果导入到MySQL中，并通过MySQL表之间的关系进行展示。

后面，此文档将带领大家逐步了解和学习XSQL。首先，会介绍XSQL中的基本概念和架构设计。然后告诉大家如何配置各种不同的数据源，并进行深入的定制。最后，我们将展示如何使用XSQL对各种数据源进行查询。

## <a name="Basic">Basic Concepts</a>

本文档介绍XSQL中的一些核心概念。理解这些概念将有助于您更加轻松的学习和使用XSQL。

**Near Realtime（近实时）**

XSQL是一个近实时的查询、计算引擎。当用户执行一条SQL时，根据数据量、SQL复杂度、是否下推、资源大小、集群环境等因素，一些SQL将在100毫秒级别完成。执行时间最长的则可能花费几十分钟。一般而言，绝大多数SQL都将在分钟级别之内执行完成。

**Cluster（集群）**

XSQL的分布式计算依托于Yarn，运行于Yarn的各个NodeManager所管理的节点上。

**Driver（驱动）**

XSQL复用了Spark的Driver，并对其功能进行了扩展。XSQL在Yarn下有两种运行模式，分别是client和cluster。在client模式下，Driver与用户应用程序都运行在客户端的同一个JVM上。在cluster模式下，用户应用程序运行在客户端上，而Driver与ApplicationMaster都运行在Yarn集群的某个节点的同一个JVM上。

**ApplicationMaster（简称AM）**

XSQL复用了Spark的ApplicationMaster。Yarn负责分配给用户应用程序的第一个Container中将运行ApplicationMaster。ApplicationMaster将后续与Yarn的ResourceManager交互，以申请资源、释放资源。ApplicationMaster可以说是用户应用程序的监护人与管理者，负责用户应用程序各个任务尝试（对于XSQL来说就是Executor）在Yarn集群的各个节点上运行。

**Executor（执行器）**

XSQL复用了Spark的Executor。Executor运行在由Yarn分配给Driver的某个集群节点的Container内部。

**DataSource（数据源）**

XSQL不同于关系型数据库，也不同于其他No SQL数据库。每个数据库实例本身可以算作一个数据源的话，数据库实例只有Database（一些数据源中可能没有Database的概念，例如Elasticsearch中的Index）和Table（一些数据源中可能没有Table的概念，例如Elasticsearch中的Type）两个常见的层级结构。由于XSQL需要支持多数据源，因此增加了DataSource这一概念。XSQL的管理层级是DataSource——>Database——>Table的三层结构。

为了便于对DataSource进行管理，XSQL增加了一些对于DataSource操作的SQL语法。当需要访问XSQL中的表时，全限定表名将从DataSource开始。例如，全限定名称是hive_test.xsql_schema.test的表，涉及名为hive_test的数据源，名为xsql_schema的Database以及表test。

**Database（数据库）**

XSQL中的Database是对表进行组织管理的方式。DataSource和Database共同组织管理了表的集合。当访问Hive和关系型数据库（例如MySQL）时，XSQL的Database与目标数据库相对应。其他类型的数据源需要根据底层实现的不同，选择合适的对象作为Database，例如，XSQL选择Elasticsearch的Index作为Database。

**Table（表）**

XSQL中的Table并不完全等同于传统意义上的表，实际是对一些行数据的组织管理，这些行实际又组织了一系列定义了名称和类型的字段。对于关系型数据库（例如MySQL），XSQL的Table与目标数据库中的表向对应。其他类型的数据源需要根据底层实现的不同，选择合适的对象作为Table，例如：Elasticsearch中的Type、MongoDB中的Collection。

**Table Schema（表的元信息）**

XSQL接入的数据源并不是都有明确的Table Schema的，例如：MongoDB的Collection是没有元信息的。所以为了使用这类数据源，需要用户自定义Schema配置文件。对于关系型数据库（例如MySQL）或者Elasticsearch等具有明确Schema信息的数据源，则不需要用户给定Schema配置文件。XSQL目前虽然实现了对MongoDB、Hbase等数据源的Schema的探测，但是这种方式存在一定的效率或准确性问题，暂时作为一种实验性的功能提供。

**<a name="pushdown">Pushdown（下推）</a>**

XSQL中运行SQL，有两种模式：Pushdown（下推）与No Pushdown（非下推）。Pushdown可以直接利用数据源的API，对目标数据进行检索或分析。No Pushdown则将具体执行交给Spark执行引擎。Pushdown通常用于数据源API比Spark执行引擎更加高效的场景下，例如Elasticsearch的API直接查询往往会比Spark执行引擎快。Pushdown结合数据源特点对于数据行数目前有不超过10000行的数据量限制。无论是Pushdown还是No Pushdown都可以用于多数据源混合查询的场景。

**Cache Level（缓存级别）**

XSQL对于数据元信息的管理，采用了去中心化的方式。这有助于XSQL更加轻量和灵活。在去中心化的设计思想下，一些元数据信息需要被缓存起来。XSQL目前提供了两种缓存级别：Level One（默认）和Level Two。Level One只会缓存DataSource和Database的元信息，这种方式适应于临时性的查询请求或者元数据变更频繁的场景；Level Two除了缓存Level One中所缓存的内容外，还会缓存Table、Column等更加详细的元信息，这种方式适应于元数据不变化或极少变化，并且长时间运行的任务。特别需要注意的是——如果用户配置的数据源过多，Level Two可能导致任务初始化时间较长，此时可以通过白名单机制减少要缓存的元数据。

**White List（白名单）与Black List（黑名单）**

一些用户之间可能会共享同一个DataSource或者同一个Database，但是只关心少数几个Database或者Table。此时，用户可以通过White List包含自己需要的Database或者Table，也可以使用Black List排除自己不需要的Database或者Table。这还可以缩短任务的初始化时间。

## <a name="Installation">Installation</a>

XSQL至少需要Java 8的环境，当前推荐使用Oracle JDK的1.8.0_152版本。Oracle推荐的安装文档可以在[Oracle网站][1]上找到。我想说的是，请在安装XSQL之前检查Java的版本，命令如下：

```sh
java -version
echo $JAVA_HOME
```

Java设置好之后，就可以安装XSQL了。XSQL目前提供了两种方式安装：一种是内置Spark的安装包，另一种是以Spark插件的方式。

### <a name="Package">XSQL内置Spark的安装包</a>

这种XSQL安装包内置了Spark，并且包名一般以`XSQL版本号`+bin+`Spark版本号`来命名（例如：xsql-0.6.0-bin-spark-2.4.3.tgz）。可以通过下面的命令提取压缩包中的内容：

```shell
tar -zxvf xsql-0.6.0-bin-spark-2.4.3.tgz
```

这条命令将在当前目录创建大量的文件、文件夹。解压缩的根目录名称为xsql-0.6.0-bin-spark-2.4.3。一个良好的习惯是建立此文件夹的软链：

```shell
ln -s xsql-0.6.0-bin-spark-2.4.3 xsql
```

现在，用户需要在xsql/conf目录配置自己的数据源信息，在<a href="#Configuration">Configuration</a>有相关的介绍。

在确定配置好数据源后，就可以进入xsql的bin目录并启动xsql了：

```shell
cd xsql/bin
./spark-xsql
```

如果顺利的话，你将看到以下信息：

```
Java HotSpot(TM) 64-Bit Server VM warning: Using the ParNew young collector with the Serial old collector is deprecated and will likely be removed in a future release
Warning: Master yarn-client is deprecated since 2.0. Please use master "yarn" with specified deploy mode instead.
18/10/25 12:28:03 WARN SparkConf: The configuration key 'spark.scheduler.executorTaskBlacklistTime' has been deprecated as of Spark 2.1.0 and may be removed in the future. Please use the new blacklisting options, spark.blacklist.*
18/10/25 12:28:03 WARN SparkConf: The configuration key 'spark.akka.frameSize' has been deprecated as of Spark 1.6 and may be removed in the future. Please use the new key 'spark.rpc.message.maxSize' instead.
18/10/25 12:28:04 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
```

并最终停留在spark-xsql的提示符上：

```shell
spark-xsql>
```

### XSQL作为Spark插件

XSQL插件是一个压缩包，一般以`XSQL版本号`+plugin+`Spark版本号`来命名（例如：xsql-0.6.0-plugin-spark-2.4.3.tgz）。因此你需要确定自己的机器上已经有了Spark客户端，并且版本与XSQL所对应的Spark版本一致，否则可能导致运行出错。

你需要首先将XSQL插件解压缩到Spark目录下，例如：

```shell
tar zxvf xsql-0.6.0-plugin-spark-2.4.3.tgz -C $SPARK_HOME
```

这样实际会在Spark目录下创建一个保护了XSQL所需要的jar文件的目录xsql-jars，还会在$SPARK_HOME/bin目录创建一个spark-xsql的脚本。

然后进入Spark的conf目录：

```shell
cd $SPARK_HOME/conf
```

需要创建XSQL所需的配置文件，例如：

```shell
vi xsql.conf
```

向xsql.conf写入你所需的数据源配置（请参照<a href="#Configuration">Configuration</a>介绍的例子）。

最后，我们就可以启动spark-xsql命令了，请参照<a href="#Package">XSQL内置Spark的安装包</a>。

注意：XSQL将默认加载`$SPARK_HOME/conf/xsql.conf`。当然，xsql.conf不一定要放在conf目录下，可以是用户想要的任何位置，你将可以在[XSQL配置][1]找到对应的配置。

### 启动XSQL Cli的注意事项

#### Yarn Cluster模式

用户将XSQL安装好后，使用spark-submit、spark-shell、spark-sql、spark-xsql等命令时，如果指定了Yarn Cluster模式，例如：

```shell
bin/spark-submit --class path.to.your.Class --master yarn --deploy-mode cluster yourApp.jar
```

那么切记要增加将xsql.conf文件添加到上传配置中，例如：

```shell
bin/spark-submit --files xsql.conf --class path.to.your.Class --master yarn --deploy-mode cluster yourApp.jar
```

否则会导致运行在集群上的Driver找不到xsql.conf文件。

[1]:https://docs.oracle.com/javase/8/docs/technotes/guides/install/install_overview.html "Oracle网站"

## <a name="Configuration">Configuration</a>

首先进入conf目录：

```shell
cd xsql/conf
```

用户需要将xsql/conf目录下的xsql.conf.template重命名为xsql.conf：

```shell
mv xsql.conf.template xsql.conf
```

打开xsql.conf：

```shell
vi xsql.conf
```

可以看到以下内容：

```properties
# Example:
# spark.xsql.datasources                     default
# spark.xsql.default.database                mysqltest
# spark.xsql.datasource.default.type         mysql
# spark.xsql.datasource.default.url          jdbc:mysql://127.0.0.1:2336
# spark.xsql.datasource.default.user         user
# spark.xsql.datasource.default.password     password
# spark.xsql.datasource.default.version      5.6.19
```

此数据源是提供给用户学习使用，所以如果用户想要拿此配置练手，请将"#"去掉，并且将各个配置调整为你准备好的数据源。例如：

```properties
spark.xsql.datasources                     default
spark.xsql.default.database                real_database
spark.xsql.datasource.default.type         mysql
spark.xsql.datasource.default.url          jdbc:mysql://127.0.0.1:2336
spark.xsql.datasource.default.user         real_username
spark.xsql.datasource.default.password     real_password
spark.xsql.datasource.default.version      5.6.19
```

这个配置中指定了一个名为default的数据源，XSQL默认会选择别名是default的数据源作为默认数据源。本例中，default数据源的类型是MySQL，此外还提供了default数据源的其他连接配置信息及版本号。默认情况下，XSQL除了选择default数据源作为当前数据源外，还会选择默认数据源中的default数据库实例作为默认数据库，这有些类似于进入Hive命令行后的默认数据库。如果用户提供的数据源中没有名为default的数据库实例，那么需要通过`spark.xsql.default.database`指定默认数据库。本例中，通过`spark.xsql.default.database`指定默认数据库为实际存在的real_database。更多的配置介绍请阅读[Configurations](../tutorial/configuration.md)

## Running

如果你严格按照<a href="#Installation">Installation</a>一节的方式启动了XSQL，现在可以来看看我们能做些什么？我们犹如刚刚经过了激烈的颤抖后脱离了地球引力，面前是浩渺、幽暗的太空。我十分理解新用户对于一个陌生工具的恐惧，但是XSQL团队始终与你同坐在同一艘飞船上。暂时不妨将XSQL理解成我们所熟知世界的MySQL或Hive，它们能做什么？地球上的用户每天都在使用这两个工具。即便如此，由于健忘是人类与生俱来的能力，因此绝大多数用户依然习惯于输入以下命令：

```mysql
show databases;
```

这不仅可以有效治愈人们的健忘，对于我们而言也能减轻恐惧。所以请尝试这条命令：

```mysql
spark-xsql> show databases;
18/10/29 15:23:44 INFO SparkXSQLShell: spark.enable.hiverc:true
18/10/29 15:23:44 INFO SparkXSQLShell: current SQL: show databases
18/10/29 15:23:48 WARN SparkXSQLShell: hive.cli.print.header not configured, so doesn't print colum's name.
default	MYSQL	mysqltest
Time taken: 0.028 s
spark-xsql>
```

怎么样？是不是很熟悉？但是也是有区别的。第一列显示的是Database所属DataSource的名称；第二列是DataSource的类型（这里是MySQL）；第三列是Database的名称。这样我们能看到在default数据源中有一个名为mysqltest的Database。

有了上面的成功经验，我相信你会大胆一些，尝试使用Use：

```mysql
use mysqltest;
```

执行信息如下：

```mysql
spark-xsql> use mysqltest;
18/10/29 15:28:33 INFO SparkXSQLShell: current SQL: use mysqltest
18/10/29 15:28:33 WARN SparkXSQLShell: hive.cli.print.header not configured, so doesn't print colum's name.
Time taken: 0.028 s
spark-xsql>
```

与MySQL一样，你已经选择mysqltest作为当前的Database。你应该想要看看mysqltest里面有哪些Table，以满足你的好奇，习惯上你会输入以下命令：

```mysql
show tables;
```

在XSQL中执行时，你将看到：

```mysql
spark-xsql> show tables;
18/10/29 15:30:27 INFO SparkXSQLShell: current SQL: show tables
18/10/29 15:30:28 WARN SparkXSQLShell: hive.cli.print.header not configured, so doesn't print colum's name.
default	mysqltest	activities	false
default	mysqltest	course	false
default	mysqltest	geonames	false
default	mysqltest	geonames_small	false
default	mysqltest	person	false
default	mysqltest	taxis	false
default	mysqltest	taxis_type	false
default	mysqltest	test123	false
Time taken: 0.066 s
spark-xsql>
```

这跟你在MySQL中执行的结果是不是也非常相似，不过仍然有一小点不同之处——第一列显示的是Table所属DataSource的名称；第二列是Table所属Database的名称；第三列是Table的名称；第四列表示Table是否是临时表。

在本节内容的最后，我们选择course表作为查询例子，请在XSQL中输入：

```mysql
select * from course;
```

你将看到以下输出内容：

```mysql
spark-xsql> select * from course;
18/10/29 15:38:15 INFO SparkXSQLShell: current SQL: select * from course
18/10/29 15:38:15 WARN SparkXSQLShell: hive.cli.print.header not configured, so doesn't print colum's name.
1	math
2	english
3	chinese
Time taken: 0.148 s
spark-xsql>
```

这张测试用途的表中共有三条数据输出。

本节以最简单的方式，向用户介绍了XSQL中运行DDL和查询SQL的例子。有关更多XSQL自身语法的介绍请阅读[Special Syntax](../tutorial/syntax.md)中的内容。

