## Spark2.3特有故障

### 一、集群环境类

#### 1、...

#### 2、...

###  二、spark2.3特有问题总结

#### 1、org.apache.spark.sql.AnalysisException: Table or view not found

有些用户在使用hive、spark-hive或者spark-shell的时候能够正确操作Hive表，但是改为使用Spark API，并用spark-submit提交时，会出现此问题。

这里给一个例子：

```scala
 def main(args: Array[String]): Unit = {
     val sparkConf = new SparkConf()
     val sc: SparkContext = new SparkContext(sparkConf)
     val sparkSession: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()

     //省略其余代码
```

提交任务后，出现：

```properties
org.apache.spark.sql.AnalysisException: Table or view not found: `dbA`.`tableA`; line 1 pos 14;
```

形式的错误。

这种使用方法是有问题的，Spark2.x.x提供了一个新的API——SparkSession，就是想封装并替换老版本中的SparkContext、SqlContext、HiveContext等API。应该使用如下方式：

```scala
 def main(args: Array[String]): Unit = {
      val sparkSession: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()

 //省略其余代码
```

