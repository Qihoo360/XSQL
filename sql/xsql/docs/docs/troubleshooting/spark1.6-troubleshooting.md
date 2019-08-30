## Spark1.6特有故障

### 一、集群环境类

#### 1、ExecutorLostFailure

##### 1-1、Diagnostics: Container released on a *lost* node

​	用户提交的任务在运行过程中，部分executor出现以下异常：

```verilog
ExecutorLostFailure (executor 268 exited caused by one of the running tasks) Reason: Container marked as failed: container_e46_1545125871120_21448_01_000282 on host: 10.160.140.153. Exit status: -100. Diagnostics: Container released on a *lost* node
```

实际是NodeManager重启所致。

##### 1-2、Killed by external signal

​	用户提交的任务，executor设置了较大的内存后，部分executor出现以下异常：

```verilog
ExecutorLostFailure (executor 335 exited caused by one of the running tasks) Reason: Container marked as failed: container_e124_1543893582405_1626578_01_000394 on host: 10.203.21.109. Exit status: 143. Diagnostics: Container killed on request. Exit code is 143. More Information 
Container exited with a non-zero exit code 143
Killed by external signal
```

初步判断executor是因为内存超限，被NodeManager杀掉。但是具体原因需要进一步诊断。

经过诊断，发现如下信息：

#### Summary Metrics for 409 Completed Tasks

| Metric                       | Min         | 25th percentile | Median      | 75th percentile | Max               |
| ---------------------------- | ----------- | --------------- | ----------- | --------------- | ----------------- |
| Duration                     | 62 ms       | 0.6 s           | 0.7 s       | 1 s             | 33 s              |
| Scheduler Delay              | 26 ms       | 0.1 s           | 0.1 s       | 0.1 s           | 0.9 s             |
| Task Deserialization Time    | 26 ms       | 2 s             | 2 s         | 2 s             | 6 s               |
| GC Time                      | 0 ms        | 53 ms           | 66 ms       | 87 ms           | 4 s               |
| Result Serialization Time    | 0 ms        | 1 ms            | 2 ms        | 2 ms            | 30 ms             |
| Getting Result Time          | 0 ms        | 0 ms            | 0 ms        | 0 ms            | 0 ms              |
| Peak Execution Memory        | 0.0 B       | 64.0 KB         | 64.0 KB     | 64.1 MB         | 1024.0 MB         |
| Shuffle Read Blocked Time    | 0 ms        | 0 ms            | 0 ms        | 0 ms            | 16 s              |
| Shuffle Read Size / Records  | 126.0 B / 0 | 126.0 B / 0     | 126.0 B / 0 | 1679.0 B / 11   | 74.2 MB / 3936473 |
| Shuffle Remote Reads         | 105.0 B     | 126.0 B         | 126.0 B     | 1378.0 B        | 74.2 MB           |
| Shuffle Write Size / Records | 0.0 B / 0   | 0.0 B / 0       | 0.0 B / 0   | 564.0 B / 11    | 5.1 KB / 150      |

可以看出有明显的数据倾斜，用户也许觉得74.2 MB的数据倾斜也不应该出现内存问题而被kill。实际上被kill掉的executor的shuffle统计数据无法准确传递到driver UI。

#### 2、HDFS环境相关

##### 2-1、DSQuotaExceededException: The DiskSpace quota of /home/logsget is exceeded

​	用户提交的任务在运行过程中，部分executor出现以下异常：

```verilog
org.apache.hadoop.hdfs.protocol.DSQuotaExceededException: org.apache.hadoop.hdfs.protocol.DSQuotaExceededException: The DiskSpace quota of /home/logsget is exceeded: quota=4178144185548800 diskspace consumed=3891200.7g
	at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
	at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:57)
	at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
	at java.lang.reflect.Constructor.newInstance(Constructor.java:526)
	at org.apache.hadoop.ipc.RemoteException.instantiateException(RemoteException.java:96)
	at org.apache.hadoop.ipc.RemoteException.unwrapRemoteException(RemoteException.java:58)
	at org.apache.hadoop.hdfs.DFSClient$DFSOutputStream.locateFollowingBlock(DFSClient.java:6111)
	at org.apache.hadoop.hdfs.DFSClient$DFSOutputStream.nextBlockOutputStream(DFSClient.java:5817)
	at org.apache.hadoop.hdfs.DFSClient$DFSOutputStream.access$3400(DFSClient.java:4660)
	at org.apache.hadoop.hdfs.DFSClient$DFSOutputStream$DataStreamer.run(DFSClient.java:5023)
Caused by: org.apache.hadoop.ipc.RemoteException: org.apache.hadoop.hdfs.protocol.DSQuotaExceededException: The DiskSpace quota of /home/logsget is exceeded: quota=4178144185548800 diskspace consumed=3891200.7g
	at org.apache.hadoop.hdfs.server.namenode.INodeDirectoryWithQuota.verifyQuota(INodeDirectoryWithQuota.java:159)
	at org.apache.hadoop.hdfs.server.namenode.FSDirectory.verifyQuota(FSDirectory.java:1748)
	at org.apache.hadoop.hdfs.server.namenode.FSDirectory.updateCount(FSDirectory.java:1506)
	at org.apache.hadoop.hdfs.server.namenode.FSDirectory.addBlock(FSDirectory.java:499)
	at org.apache.hadoop.hdfs.server.namenode.FSNamesystem.allocateBlock(FSNamesystem.java:3404)
	at org.apache.hadoop.hdfs.server.namenode.FSNamesystem.getAdditionalBlock(FSNamesystem.java:2768)
	at org.apache.hadoop.hdfs.server.namenode.NameNode.addBlockWithBlockType(NameNode.java:963)
	at org.apache.hadoop.hdfs.server.namenode.NameNode.addBlockAndFetchMetaInfoAndBlockType(NameNode.java:919)
	at sun.reflect.GeneratedMethodAccessor28.invoke(Unknown Source)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:606)
	at org.apache.hadoop.ipc.RPC$Server.call(RPC.java:743)
	at org.apache.hadoop.ipc.Server$Handler$1.run(Server.java:1189)
	at org.apache.hadoop.ipc.Server$Handler$1.run(Server.java:1185)
	at java.security.AccessController.doPrivileged(Native Method)
	at javax.security.auth.Subject.doAs(Subject.java:415)
	at org.apache.hadoop.ipc.Server$Handler.run(Server.java:1183)

	at org.apache.hadoop.ipc.Client.call(Client.java:863)
	at org.apache.hadoop.ipc.RPC$Invoker.invoke(RPC.java:227)
	at com.sun.proxy.$Proxy18.addBlockAndFetchMetaInfoAndBlockType(Unknown Source)
	at sun.reflect.GeneratedMethodAccessor77.invoke(Unknown Source)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:606)
	at org.apache.hadoop.io.retry.RetryInvocationHandler.invokeMethod(RetryInvocationHandler.java:82)
	at org.apache.hadoop.io.retry.RetryInvocationHandler.invoke(RetryInvocationHandler.java:59)
	at com.sun.proxy.$Proxy18.addBlockAndFetchMetaInfoAndBlockType(Unknown Source)
	at org.apache.hadoop.hdfs.DFSClient$DFSOutputStream.locateFollowingBlock(DFSClient.java:6051)
	... 3 more
```

实际是分配给用户的hdfs的quato满了，需要登录云图申请加资源，具体找何任天审批。

###  二、spark-submit使用

#### 1、java.lang.ExceptionInInitializerError

##### 1-1、MountRootFileSystem can not mkdir

​	用户提交Scala应用程序，成功提交application，获得application_1544090606001_349694。在获得executor资源后开始运行，在运行过程中发现以下错误：

```verilog
java.lang.ExceptionInInitializerError
	at net.qihoo.scanns.Test$$anonfun$transform$1.apply(Test.scala:85)
	at net.qihoo.scanns.Test$$anonfun$transform$1.apply(Test.scala:85)
	at org.apache.spark.rdd.PairRDDFunctions$$anonfun$mapValues$1$$anonfun$apply$41$$anonfun$apply$42.apply(PairRDDFunctions.scala:755)
	at org.apache.spark.rdd.PairRDDFunctions$$anonfun$mapValues$1$$anonfun$apply$41$$anonfun$apply$42.apply(PairRDDFunctions.scala:755)
	at scala.collection.Iterator$$anon$11.next(Iterator.scala:328)
	at scala.collection.Iterator$$anon$13.hasNext(Iterator.scala:371)
	at org.apache.spark.shuffle.sort.UnsafeShuffleWriter.write(UnsafeShuffleWriter.java:163)
	at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:73)
	at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:41)
	at org.apache.spark.scheduler.Task.run(Task.scala:89)
	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:213)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)
	at java.lang.Thread.run(Thread.java:724)
Caused by: java.io.IOException: MountRootFileSystem can not mkdir /user/hdp-guanggao/.sparkStaging/application_1544090606001_349710
	at org.apache.hadoop.fs.viewfs.MountTree$MountRootFileSystem.mkdirs(MountTree.java:372)
	at org.apache.hadoop.fs.viewfs.ViewFs.mkdirs(ViewFs.java:365)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:57)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:606)
	at net.qihoo.spinner.HYReflection.invoke(HYReflection.java:130)
	at net.qihoo.spinner.SpinnerDistributedFileSystem.mkdirs(SpinnerDistributedFileSystem.java:466)
	at org.apache.hadoop.fs.FileSystem.mkdirs(FileSystem.java:1900)
	at org.apache.hadoop.fs.FileSystem.mkdirs(FileSystem.java:617)
	at org.apache.spark.deploy.yarn.Client.prepareLocalResources(Client.scala:366)
	at org.apache.spark.deploy.yarn.Client.createContainerLaunchContext(Client.scala:732)
	at org.apache.spark.deploy.yarn.Client.submitApplication(Client.scala:142)
	at org.apache.spark.scheduler.cluster.YarnClientSchedulerBackend.start(YarnClientSchedulerBackend.scala:57)
	at org.apache.spark.scheduler.TaskSchedulerImpl.start(TaskSchedulerImpl.scala:144)
	at org.apache.spark.SparkContext.<init>(SparkContext.scala:542)
	at net.qihoo.scanns.Test$.<init>(Test.scala:18)
	at net.qihoo.scanns.Test$.<clinit>(Test.scala)
	... 14 more
```

在日志中，我们看到：

```verilog
Caused by: java.io.IOException: MountRootFileSystem can not mkdir /user/hdp-guanggao/.sparkStaging/application_1544090606001_349710
```

怎么会获得application_1544090606001_349694的情况下，还会有application_1544090606001_349710。仔细查看其他executor的输出，发现还有很多其他的application id生成。看来是每个Executor里面都把Test执行了一遍。据此怀疑，用户的spark代码使用上有问题。

​	查看用户的代码，如下：

```scala
object Test {
  private val sparkConfig = new SparkConf().setMaster("yarn-client").setAppName("scanns")
  private val sparkContext = new SparkContext(sparkConfig)
  ...

  def main(args: Array[String]): Unit = {
      val queryRaw = sparkContext.textFile(keyInput)
      ...
  }
```

SparkContext如果在main函数外创建，将会在各个executor上执行。改为main函数中创建后，解决。

##### 1-2、java.io.FileNotFoundException

​	用户执行发生错误：

```verilog
java.lang.ExceptionInInitializerError
Caused by: java.io.FileNotFoundException: File does not exist: /home/spark/spark_eventLog
```

用户的私有集群不存在此目录，且用户账号没有权限，建议联系hdfs同学看。

##### 1-3、没有安装java8

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

##### 1-4、SparkSession、SparkContext等核心类，不要写在main函数外

用户作业出现如下错误：

```
java.lang.ExceptionInInitializerError
at A$$anonfun$transform1.apply(A.scala:62) at A.apply(A.scala:62)atAanonfunanonfuntransform1.apply(A.scala:62) at org.apache.spark.rdd.PairRDDFunctions1.apply(A.scala:62)atorg.apache.spark.rdd.PairRDDFunctionsanonfunanonfunmapValues11anonfunanonfunapply4141anonfunanonfunapply42.apply(PairRDDFunctions.scala:755) at org.apache.spark.rdd.PairRDDFunctions42.apply(PairRDDFunctions.scala:755)atorg.apache.spark.rdd.PairRDDFunctionsanonfunanonfunmapValues11anonfunanonfunapply4141anonfunanonfunapply42.apply(PairRDDFunctions.scala:755) at scala.collection.Iterator42.apply(PairRDDFunctions.scala:755)atscala.collection.Iterator$anon11.next(Iterator.scala:328) at scala.collection.Iterator11.next(Iterator.scala:328)atscala.collection.Iterator$anon13.hasNext(Iterator.scala:371) at org.apache.spark.shuffle.sort.UnsafeShuffleWriter.write(UnsafeShuffleWriter.java:163) at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:73) at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:41) at org.apache.spark.scheduler.Task.run(Task.scala:89) at org.apache.spark.executor.Executor13.hasNext(Iterator.scala:371)atorg.apache.spark.shuffle.sort.UnsafeShuffleWriter.write(UnsafeShuffleWriter.java:163)atorg.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:73)atorg.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:41)atorg.apache.spark.scheduler.Task.run(Task.scala:89)atorg.apache.spark.executor.ExecutorTaskRunner.run(Executor.scala:213)
at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)
at java.util.concurrent.ThreadPoolExecutorWorker.run(ThreadPoolExecutor.java:615) at java.lang.Thread.run(Thread.java:724) Caused by: java.io.IOException: MountRootFileSystem can not mkdir /user/xxxxx/.sparkStaging/application_1544090606001_349710 at org.apache.hadoop.fs.viewfs.MountTreeWorker.run(ThreadPoolExecutor.java:615)atjava.lang.Thread.run(Thread.java:724)Causedby:java.io.IOException:MountRootFileSystemcannotmkdir/user/xxxxxxx/.sparkStaging/application1544090606001349710atorg.apache.hadoop.fs.viewfs.MountTreeMountRootFileSystem.mkdirs(MountTree.java:372)
at org.apache.hadoop.fs.viewfs.ViewFs.mkdirs(ViewFs.java:365)
at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:57)
at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
at java.lang.reflect.Method.invoke(Method.java:606)
at net.qihoo.spinner.HYReflection.invoke(HYReflection.java:130)
at net.qihoo.spinner.SpinnerDistributedFileSystem.mkdirs(SpinnerDistributedFileSystem.java:466)
at org.apache.hadoop.fs.FileSystem.mkdirs(FileSystem.java:1900)
at org.apache.hadoop.fs.FileSystem.mkdirs(FileSystem.java:617)
at org.apache.spark.deploy.yarn.Client.prepareLocalResources(Client.scala:366)
at org.apache.spark.deploy.yarn.Client.createContainerLaunchContext(Client.scala:732)
at org.apache.spark.deploy.yarn.Client.submitApplication(Client.scala:142)
at org.apache.spark.scheduler.cluster.YarnClientSchedulerBackend.start(YarnClientSchedulerBackend.scala:57)
at org.apache.spark.scheduler.TaskSchedulerImpl.start(TaskSchedulerImpl.scala:144)
at org.apache.spark.SparkContext.(SparkContext.scala:542)
at A.<init>(sgktoHbase.scala:62) at A.<init>(sgktoHbase.scala:62)atA.(A.scala)
… 14 more
```

经过排查发现代码使用方式不对。

```scala
val sparkConf = new SparkConf().setAppName("xxxxxx")
.set("spark.driver.maxResultSize","3g")
.setMaster("yarn-client")
val sc = new SparkContext(sparkConf)

def main (args: Array[String]): Unit = {
val checklist = sc.textFile("hdfs://xxxxxx:9000/home/xxxxx/xxxxx/*/*.txt")
.filter(x=>x.split(",").length==9).map(x=>x.toString.replace(" ",""))
```

#### 2、java.lang.NullPointerException

​	用户代码出现此异常，需要根据线程栈信息，自己修改。

#### 3、org.apache.spark.sql.AnalysisException: Table not found

​	用户使用spark-submit提交作业，出现此错误，目前有三种可能：

1. **表不存在**

   这种情况最为常见。一般是用户写错的表名或者没有这个表。

2. **Hive元数据服务不正常**

   之前碰到过Hive元数据服务内存不足、频繁GC导致服务不响应的问题。

3. **没有切换到相应的DB**

   这种情况的出现完全是用户使用习惯导致。使用过hive、spark-hive的用户有这样一个经验：进入命令行后已经在自己当前账号的数据库中了，因此不需要切换DB。spark本身不具备这个功能，是系统部在spark-hive上定制增加的一项功能。所以当你使用其他spark命令，如spark-shell、spark-sql、spark-submit时要格外注意这个情况。