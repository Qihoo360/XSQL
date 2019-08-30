# Spark故障诊断

目前系统部运维的Spark主要有Spark2.3和Spark1.6两个版本。用户在使用的过程中难免会发生各种各样的问题，为了提高故障诊断的效率，也为了对经验进行沉淀，这里将对各类问题如何处理进行介绍。

本文将Spark故障分为通用故障、Spark2.3特有故障、Spark1.6特有故障。

## 一、通用故障

### 1、集群环境类

#### 1-1、提交的spark任务，长时间处于ACCEPTED状态。

​	这种问题非常常见，此时需要从客户端日志中找到tracking URL，例如：

```verilog
18/12/14 17:42:29 INFO Client: 
	 client token: N/A
	 diagnostics: N/A
	 ApplicationMaster host: N/A
	 ApplicationMaster RPC port: -1
	 queue: root.test
	 start time: 1544780544655
	 final status: UNDEFINED
	 tracking URL: http://test.qihoo.net:8888/proxy/application_1543893582405_838478/
	 user: test
18/12/14 17:42:32 INFO Client: Application report for application_1543893582405_838478 (state: ACCEPTED)
```

其中的tracking URL为http://test.qihoo.net:8888/proxy/application_1543893582405_838478/，从浏览器打开页面将看到类似信息：

```properties
User: test
Queue:  root.test
clientHost: 
Name: runFeatureAnalysis
Application Type: SPARK
Application Tags: 
Application Priority: NORMAL (Higher Integer value indicates higher priority)
YarnApplicationState: ACCEPTED: waiting for AM container to be allocated, launched and register with RM.
FinalStatus Reported by AM: Application has not completed yet.
Started:  Fri Dec 14 15:50:20 +0800 2018
Elapsed:  2hrs, 3mins, 55sec
Tracking URL: ApplicationMaster
Diagnostics:  
```

可以看到状态也是ACCEPTED。并且队列是root.test。

打开http://test.qihoo.net:8888/cluster/scheduler?openQueues=root.test，找到root.test队列的资源，将看到如下信息：

```properties
Used Resources: <memory:799232, vCores:224, gCores:0>
Reserved Resources: <memory:0, vCores:0, gCores:0>
Num Active Applications:  2
Num Pending Applications: 12
Min Resources:  <memory:40000, vCores:20, gCores:0>
Max Resources:  <memory:800000, vCores:400, gCores:0>
Accessible Node Labels: CentOS6,CentOS7,DEFAULT_LABEL
Steady Fair Share:  <memory:800000, vCores:0, gCores:0>
Instantaneous Fair Share: <memory:800000, vCores:0, gCores:0>
```

主要关注Max Resources和Used Resources，说明用户队列的资源已经消耗完了。

#### 1-2、没有安装Java8

用户提交的作业在未运行task之前，AM已经退出，导致作业失败。

```
Container exited with a non-zero exit code 127
Failing this attempt. Failing the application.
	 ApplicationMaster host: N/A
	 ApplicationMaster RPC port: -1
	 queue: root.default
	 start time: 1546850986115
	 final status: FAILED
	 tracking URL: http://xxxxxxxx:8888/cluster/app/application_1493705730010_45634
	 user: hdp-360sec
Moved to trash: /home/spark/cache/.sparkStaging/application_1493705730010_45634
19/01/07 16:48:07 INFO Client: Deleted staging directory hdfs://xxxxxxx:9000/home/spark/cache/.sparkStaging/application_1493705730010_45634
19/01/07 16:48:07 ERROR SparkContext: Error initializing SparkContext.
org.apache.spark.SparkException: Yarn application has already ended! It might have been killed or unable to launch application master.
	at org.apache.spark.scheduler.cluster.YarnClientSchedulerBackend.waitForApplication(YarnClientSchedulerBackend.scala:89)
	at org.apache.spark.scheduler.cluster.YarnClientSchedulerBackend.start(YarnClientSchedulerBackend.scala:63)
	at org.apache.spark.scheduler.TaskSchedulerImpl.start(TaskSchedulerImpl.scala:164)
	at org.apache.spark.SparkContext.<init>(SparkContext.scala:502)
	...
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:497)
	at org.apache.spark.deploy.JavaMainApplication.start(SparkApplication.scala:52)
	at org.apache.spark.deploy.SparkSubmit$.org$apache$spark$deploy$SparkSubmit$$runMain(SparkSubmit.scala:879)
	at org.apache.spark.deploy.SparkSubmit$.doRunMain$1(SparkSubmit.scala:197)
	at org.apache.spark.deploy.SparkSubmit$.submit(SparkSubmit.scala:227)
	at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:136)
	at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)
Exception in thread "main" java.lang.ExceptionInInitializerError
	...
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:497)
	at org.apache.spark.deploy.JavaMainApplication.start(SparkApplication.scala:52)
	at org.apache.spark.deploy.SparkSubmit$.org$apache$spark$deploy$SparkSubmit$$runMain(SparkSubmit.scala:879)
	at org.apache.spark.deploy.SparkSubmit$.doRunMain$1(SparkSubmit.scala:197)
	at org.apache.spark.deploy.SparkSubmit$.submit(SparkSubmit.scala:227)
	at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:136)
	at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)
Caused by: org.apache.spark.SparkException: Yarn application has already ended! It might have been killed or unable to launch application master.
	at org.apache.spark.scheduler.cluster.YarnClientSchedulerBackend.waitForApplication(YarnClientSchedulerBackend.scala:89)
	at org.apache.spark.scheduler.cluster.YarnClientSchedulerBackend.start(YarnClientSchedulerBackend.scala:63)
	at org.apache.spark.scheduler.TaskSchedulerImpl.start(TaskSchedulerImpl.scala:164)
	at org.apache.spark.SparkContext.<init>(SparkContext.scala:502)
	... 11 more
```

查看tracking URL，发现如下信息：

```language
/bin/bash: /home/xxx/xxxxx/java8/bin/java: &ucirc;&#65533;&#65533;&#65533;&#504;&#65533;&#65533;&#316;&#65533;&#65533;&#65533;&#319;&frac14;
```

和明显是集群的一些机器漏装java8了

### 2、spark应用类
#### 2-1、连不上某个executor，导致任务失败

```
Caused by: java.net.ConnectException: Connection refused: /10.160.113.58:39941 
	at sun.nio.ch.SocketChannelImpl.checkConnect(Native Method) 
	at sun.nio.ch.SocketChannelImpl.finishConnect(SocketChannelImpl.java:735) 
	at io.netty.channel.socket.nio.NioSocketChannel.doFinishConnect(NioSocketChannel.java:223)
```
该节点上的nodemanager上某个container的日志显示

```
INFO org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl: Memory usage of ProcessTree 29181 for container-ia
container e124 1 888381: 10.7 GB of 11 GB physical memory used; 11.7 GB of 23.1 GB virtual memory used
INFO org.apac e.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl: Container container e124 1543893582485 1154818 81 
transitioned f om RUNNING to KILLING
INFO org.apache.hadoop.yarn.server.nodemanager.contaxnermanager.launcher.ContaznerLaunch. Cleaning up container container e124 1543893582485 1154
18 
INFO org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl: Memory usage of ProcessTree 29181 for container-ia container e124 1543893582485 1154818 81 888381: -1B of 11 GB physical memory used; -1B of 23.1 GB virtual memory used
INFO org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl: Container container e124 1543893582485 1154818 81 
transitioned from KILLING to EXITED WITH FAIIURE
WARN org.apache.hadoop.yarn.server.nodemanager.NMAuditLogger: USER=hdp-portrait OPERATION=Container Finished — Failed TARGET=ContainerImpl P SULT=FAILURE DESCRIPTION=Container failed with state: EXITED WITH FAILURE APPID=application 1543893582485 1154818 CONTAINERID=container e124 1543893582485 115481 1 888381
INFO org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl: Container container e124 1543893582485 1154818 81 888381 transitioned from EXITED WITH FAILURE to DONE
```
或者任务日志显示

```
ERROR executor.CoarseGrainedExecutorBackend: RECEIVED SIGNAL 15: SIGTERM
```
原因：

```
问题的最终原因是某个节点上的某个executor挂掉了，导致无法获取上面的数据，导致任务失败。
```
解决方法：

```
归根结底还是内存的问题，有两个方法可以解决这个错误，加大excutor-memory的值 或者 减少executor-cores的数量 或者增加分区等手段就可以解决。
```


#### 2-2、spark kryo size 设置太小或者超过最大值（<2G）

```
Job aborted due to stage failure: Task 2 in stage 3.0 failed 4 times, most recent failure: 
Lost task 2.3 in stage 3.0 (TID 28, hpc152.sys.lycc.qihoo.net, executor 11): 
org.apache.spark.SparkException: Kryo serialization failed: Buffer overflow. Available: 2, required: 8
```
合理设置spark.kryoserializer.buffer.max,spark.kryoserializer.buffer
spark.kryoserializer.buffer.max应该小于2G
如果设置小于2G还是报错，可能是输入数据太大或者逻辑复杂生成的数据多，建议减少每个task的处理数据量

#### 2-3、executor 内存溢出

```
executor java.lang.OutOfMemoryError:Java heap space
```

解决方法：

```
加大excutor-memory的值或者减少executor-cores的数量或者增加分区等手段就可以解决。
```

#### 2-4、spark作业超时

```
Connection to /10.203.34.203:36650 has been quiet for 300000 ms while there are outstanding requests. Assuming connection is dead; please adjust spark.network.timeout if this is wrong.
Connection to /10.203.34.203:36650 has been quiet for 300000 ms while there are outstanding requests. Assuming connection is dead; please adjust spark.network.timeout if this is wrong.
Connection to /10.203.34.203:36650 has been quiet for 300000 ms while there are outstanding requests. Assuming connection is dead; please adjust spark.network.timeout if this is wrong.
Connection to /10.203.34.203:36650 has been quiet for 300000 ms while there are outstanding requests. Assuming connection is dead; please adjust spark.network.timeout if this is wrong.
Connection to /10.203.34.203:36650 has been quiet for 300000 ms while there are outstanding requests. Assuming connection is dead; please adjust spark.network.timeout if this is wrong
```

原因

```
该executor gc非常频繁，导致超时
```
解决方法：

```
1、加大excutor-memory的值或者减少executor-cores的数量或者增加分区等手段就可以解决。
2、合理设置spark.network.timeout
```

#### 2-5、WARN cannot remove /xxxxx/data_mode=d/xxx:No such file or directory

```
18/12/17 17:17:17 WARN FileSystem:java.io.FileNotFoundException: 
cannot remove /xxxxx/data_mode=d/hist_dur=1/part-00000-cad0a9bc-0c8b-4298-b636-207f107732c1-c000: 
No such file or directory. at org.apache.hadoop.fs.FsShell.delete(FsShell.java:1423) 
at org.apache.hadoop.hdfs.DistributedFileSystem.deleteUsingTrash(DistributedFileSystem.java:956)		
```

对于这种警告信息，不影响作业，用户可以忽略。

#### 2-6、WARN fs.TrashPolicyDefault: Can’t create trash directory: hdfs://xxx/xxx/...

```
19/01/07 11:51:31 WARN fs.TrashPolicyDefault: Can’t create trash directory: hdfsold://xxxxx:9000/user/xxxxx/.Trash/Current/home/xxxxx/project/bool/userdata/optimization/targeting/xxxxxxxxxx/xxxxxxx
Problem with Trash.java.io.FileNotFoundException: Parent path is not a directory: /user/xxxxx/.Trash/Current/home/xxxxx/project/bool/userdata/optimization/targeting/xxxxxxx/xxxxxxx
at org.apache.hadoop.hdfs.server.namenode.FSDirectory.mkdirs(FSDirectory.java:1637)
at org.apache.hadoop.hdfs.server.namenode.FSNamesystem.mkdirsInternal(FSNamesystem.java:4177)
at org.apache.hadoop.hdfs.server.namenode.FSNamesystem.mkdirs(FSNamesystem.java:4107)
at org.apache.hadoop.hdfs.server.namenode.NameNode.mkdirs(NameNode.java:1184)
at sun.reflect.GeneratedMethodAccessor122.invoke(Unknown Source)
at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
at java.lang.reflect.Method.invoke(Method.java:606)
at org.apache.hadoop.ipc.RPCServer.call(RPC.java:743) at org.apache.hadoop.ipc.ServerServer.call(RPC.java:743)atorg.apache.hadoop.ipc.ServerHandler1.run(Server.java:1189) at org.apache.hadoop.ipc.Server1.run(Server.java:1189)atorg.apache.hadoop.ipc.ServerHandler1.run(Server.java:1185) at java.security.AccessController.doPrivileged(Native Method) at javax.security.auth.Subject.doAs(Subject.java:415) at org.apache.hadoop.ipc.Server1.run(Server.java:1185)atjava.security.AccessController.doPrivileged(NativeMethod)atjavax.security.auth.Subject.doAs(Subject.java:415)atorg.apache.hadoop.ipc.ServerHandler.run(Server.java:1183)
. Consider using -skipTrash option
```

对于这种警告信息，不影响作业，用户可以忽略。

#### 2-7、Total size of serialized results of ... is bigger than spark.driver.maxResultSize (1024.0 MB)

用户执行的报错信息：

```verilog
ERROR TaskSetManager: Total size of serialized results of 329 tasks (1025.4 MB) is bigger than spark.driver.maxResultSize (1024.0 MB)
```

​	这种问题一般是用户在spark-sql或spark-hive中直接查询返回的数据量过大造成。也可能是用户应用中使用了拉取数据到driver端的API（例如：collect、show）。

​	解决方法：用户应该考虑拉取数据到driver端是否合理？如果不合理，增加过滤条件或者采用insert overwrite directory命令解决；如果合理，则适当增加spark.driver.maxResultSize的大小。

#### 2-8、 Current usage: 3.5 GB of 3.5 GB physical memory used

用户作业出现如下错误：

```
19/01/10 16:01:01 ERROR YarnScheduler: Lost executor 70 on 10.162.90.26: Executor for container container_e46_1545125871120_683318_01_000071 exited because of a YARN event (e.g., pre-emption) and not because of an error in the running j
ob.
19/01/10 16:02:32 ERROR YarnScheduler: Lost executor 265 on 10.160.98.120: Container marked as failed: container_e46_1545125871120_683318_01_000252 on host: 10.160.98.120. Exit status: 137. Diagnostics: Container killed on request. Exit
code is 137. More Information
Container exited with a non-zero exit code 137
Killed by external signal

19/01/10 16:02:35 ERROR YarnScheduler: Lost executor 159 on 10.160.107.169: Container marked as failed: container_e46_1545125871120_683318_01_000164 on host: 10.160.107.169. **\*==Exit status: 15. Diagnostics: Container [pid=31942,containerID=container_e46_1545125871120_683318_01_000164] is running beyond physical memory limits. Current usage: 3.5 GB of 3.5 GB physical memory used; 5.3 GB of 7.3 GB virtual memory used. Killing container.**==*
```

遇到这种问题，应该首先查看数据是否有倾斜，如果没有倾斜，看看能否适当增加分区数（分区设置过多是有代价的），或者从源头过滤或者减少数据量。最后不行，再调高executor的内存。

### 3、PySpark相关

#### 3-1、python使用不当

```
ERROR ApplicationMaster: User class threw exception: java.io.IOException: Cannot run 
program "./python27/bin/python": error=2, No such file or directory
java.io.IOException:Cannot run program "./python27/bin/python": error=2, 
No such file or directory	
```

解决方法：用户使用archives方式提交python2.7.tgz，该tgz有多级目录指定的Python的路径不对。

#### 3-2、python任务长时间不结束

查看用户的executor线程，发现：

```language
java.net.SocketInputStream.socketRead0(Native Method)
java.net.SocketInputStream.read(SocketInputStream.java:152)
java.net.SocketInputStream.read(SocketInputStream.java:122)
java.io.BufferedInputStream.fill(BufferedInputStream.java:235)
java.io.BufferedInputStream.read1(BufferedInputStream.java:275)
java.io.BufferedInputStream.read(BufferedInputStream.java:334)
java.io.DataInputStream.readFully(DataInputStream.java:195)
java.io.DataInputStream.readFully(DataInputStream.java:169)
org.apache.spark.api.python.PythonRunner$$anon$1.read(PythonRDD.scala:142)
org.apache.spark.api.python.PythonRunner$$anon$1.next(PythonRDD.scala:129)
org.apache.spark.api.python.PythonRunner$$anon$1.next(PythonRDD.scala:125)
org.apache.spark.InterruptibleIterator.next(InterruptibleIterator.scala:43)
scala.collection.Iterator$$anon$11.next(Iterator.scala:328)
org.apache.spark.shuffle.sort.BypassMergeSortShuffleWriter.write(BypassMergeSortShuffleWriter.java:149)
org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:73)
org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:41)
org.apache.spark.scheduler.Task.run(Task.scala:89)
org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:213)
java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)
java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)
java.lang.Thread.run(Thread.java:724)
```

pyspark实际是创建了python进程与java进程通过socket通信实现的，pyspark执行在架构上天生就有效率、性能差的问题。建议改为scala实现。有spark同行告诉我，可以通过apache arrow来优化，不过目前暂时没有调研这块。

### 4、用户程序自身问题

这类问题是用户代码本身的错误，比如空指针异常，数组访问越界什么的。

#### 4-1、测试代码

用户反馈：使用yarn-client模式运行成功，但是切换成cluster模式执行失败。经过排查，发现用户的代码中的测试代码没有改成正式的。用户的部分代码如下：

```scala
def main(args: Array[String]): Unit={
  val spark = SparkSession.builder().master("local")
  .appName("test****").getOrCreate()

  // 省略其余代码逻辑
  spark.stop()
}
```

#### 4-2、SparkSession、SparkContext等核心类，不要写在main函数外

用户的作业提交执行不成功，经过排查发现代码使用方式不对。

```
val sparkConf = new SparkConf().setAppName("xxxxxx")
.set("spark.driver.maxResultSize","3g")
.setMaster("yarn-client")
val sc = new SparkContext(sparkConf)

def main (args: Array[String]): Unit = {
val checklist = sc.textFile("hdfs://xxxxxx:9000/home/xxxxx/xxxxx/*/*.txt")
.filter(x=>x.split(",").length==9).map(x=>x.toString.replace(" ",""))
```



##  二、Spark2.3特有故障

请阅读[Spark2.3特有故障](spark2.3-troubleshooting.md)。

## 三、Spark1.6特有故障

请阅读[Spark1.6特有故障](spark1.6-troubleshooting.md)。



