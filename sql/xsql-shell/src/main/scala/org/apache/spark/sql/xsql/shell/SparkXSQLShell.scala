/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.xsql.shell

import java.io.File

import scala.collection.mutable
import scala.io.Source

import jline.console._
import jline.console.history._
import sun.misc.BASE64Decoder

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{Command, Union}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.xsql.{ResolveScanSingleTable, XSQLSessionCatalog}

case class Config(file: String = "", batch: String = "")

object SparkXSQLShell extends Logging {
  //scalastyle:off
  def main(args: Array[String]) {
    val originMaster = System.getProperty(SparkLauncher.SPARK_MASTER)

    var spark = SparkSession
      .builder()
      .config(SparkLauncher.SPARK_MASTER, "local")
      .config("spark.eventLog.enabled", "false")
      .enableXSQLSupport()
      .getOrCreate()
    var sqlContext = spark.sqlContext
    var conf = spark.sparkContext.getConf
    var sc = spark.sparkContext
    val appNameDefault = "SparkSQLShell"
    var resolve = new ResolveScanSingleTable(spark)
    var sqlContent: Option[String] = None

    var restarted = false
    var wholeLine = ""
    var isContinue = false
    val appNameMaxLength = 30
    val HIVERCFILE = ".hiverc"
    val regex = """set\s+spark\.app\.name\s*=\s*(.+)""".r
    val parser = new scopt.OptionParser[Config]("scopt") {
      head("scopt", "3.x")
      opt[String]('f', "file") action { (x, c) =>
        c.copy(file = x)
      } text ("file is a hql file")
      opt[String]('e', "batch") action { (x, c) =>
        c.copy(batch = x)
      } text ("a batch hql")
    }

    // parser.parse returns Option[C]
    parser.parse(args, Config()) map { config =>
      if (config.file != "") {
        val fileContent = Source.fromFile(config.file).mkString
        initialize(dropComment(fileContent), conf)
        run(fileContent)
      } else if (config.batch != "") {
        val batchContent =
          if (conf.get("spark.master") == "yarn-cluster" ||
              (conf.get("spark.master") == "yarn" && conf.get("spark.submit.deployMode") == "cluster")) {
            logDebug(s"batchContent base64 is ${config.batch}")
            new String(new BASE64Decoder().decodeBuffer(config.batch));
          } else {
            config.batch
          }
        initialize(dropComment(batchContent), conf)
        run(batchContent)
      } else {
        initialize(appNameDefault, conf)
        loop()
      }
    } getOrElse {
      // arguments are bad, error message will have been displayed
    }

    def restart(restartConf: Map[String, String]) = {
      spark = spark.restartSparkContext(
        restartConf ++
          Map(
            SparkLauncher.SPARK_MASTER -> originMaster,
            "spark.sql.string" -> sqlContent.getOrElse(appNameDefault),
            CATALOG_IMPLEMENTATION.key -> "xsql",
            "spark.eventLog.enabled" -> "true"))
      resolve = new ResolveScanSingleTable(spark)
      sqlContext = spark.sqlContext
      conf = spark.sparkContext.getConf
      sc = spark.sparkContext
      processInitFile(conf)
    }

    def initialize(sqlContent: String, conf: SparkConf) = {
      updateAppName(sqlContent)
      setSqlString(sqlContent)
      processInitFile(conf)
    }

    def dropComment(sqlContent: String): String = {
      sqlContent.split("\n").map(_.trim).filter(!_.startsWith("--")).mkString(" ")
    }

    def updateAppName(appNameRaw: String) = {
      val appName = appNameRaw.trim
      if (appName.length > 0) {
        if (!conf.getBoolean("spark.app.name.predefine", false)) {
          val appNameAbbreviate = if (appName.length <= appNameMaxLength) {
            appName
          } else {
            appName.take(appNameMaxLength / 2) + "..." + appName.takeRight(appNameMaxLength / 2)
          }
          conf.setAppName(java.net.URLEncoder.encode(appNameAbbreviate))
        }
      }
    }

    def setSqlString(sqlString: String) = {
      sqlContent = Option(java.net.URLEncoder.encode(sqlString))
      conf.set("spark.sql.string", sqlContent.get)
    }

    def setDescription(sql: String) = {
      sc.setCallSite(if (sql.length < 100) sql else sql.substring(0, 100) + "...")
      sc.setJobDescription(sql)
    }

    def processInitFile(conf: SparkConf): Unit = {
      val hivercFile = System.getProperty("user.home") + File.separator + HIVERCFILE;
      val enableHiverc = conf.getBoolean("spark.enable.hiverc", true)
      logInfo("spark.enable.hiverc:" + enableHiverc)
      if (new File(hivercFile).exists() && enableHiverc) {
        val fileContent = Source.fromFile(hivercFile).mkString
        run(fileContent, true)
      }
    }

    /**
     * 执行SQL语句
     */
    def run(commands: String, silent: Boolean = false) = {
      var startTime = System.currentTimeMillis()
      for (command <- commands.split(";")) {
        var sql = command.trim().replaceAll("[\\t\\n\\r]", " ")
        if (sql != "") {
          if (sql.equals("exit") || sql.equals("quit")) {
            exit()
          }
          logInfo("current SQL: " + sql)
          setDescription(sql)
          var sql2 = sql.toLowerCase
          if (sql2.startsWith("set") && !regex.findFirstMatchIn(sql2).isEmpty) {
            updateAppName(regex.findFirstMatchIn(sql2).get.group(1).trim)
          } else if (sql2.startsWith("delete")) {
            val fromIndex = sql2.indexOf("from")
            val whereIndex = sql2.indexOf("where")
            val tableNames = sql.substring(fromIndex + 4, whereIndex).trim.split("\\s")
            val (tableName, hasAlias, tableNameAlias) = if (tableNames.length == 2) {
              (tableNames(0), true, tableNames(1))
            } else {
              (tableNames(0), false, "")
            }
            val filter = if (hasAlias) {
              sql.substring(whereIndex + 5).replace(tableNameAlias + ".", "")
            } else {
              sql.substring(whereIndex + 5)
            }
            sql = "insert overwrite table %s select * from %s where not (%s)".format(
              tableName,
              tableName,
              filter)
            logDebug("New SQL:" + sql)
          } else if (sql2.startsWith("update")) {
            val setIndex = sql2.indexOf("set")
            val whereIndex = sql2.indexOf("where")
            val tableNames = sql.substring(6, setIndex).trim.split("\\s")
            val (tableName, hasAlias, tableNameAlias) = if (tableNames.length == 2) {
              (tableNames(0), true, tableNames(1))
            } else {
              (tableNames(0), false, "")
            }
            val (updateClause, filter) = if (whereIndex == -1) {
              (sql.substring(setIndex + 3).replace(tableNameAlias + ".", ""), "")
            } else {
              (
                sql.substring(setIndex + 3, whereIndex).replace(tableNameAlias + ".", ""),
                sql2.substring(whereIndex + 5).replace(tableNameAlias + ".", ""))
            }
            val updateMap = new mutable.HashMap[String, String]
            for (x <- updateClause.split(",")) {
              val colUpdate = x.trim().split("=")
              updateMap(colUpdate(0).trim) = colUpdate(1).trim
            }
            var project = ""
            for (col <- spark.sql("desc " + tableName).collect) {
              val colName = col.get(0).toString
              project += updateMap.getOrElse(colName, colName) + ","
            }
            project = project.substring(0, project.length - 1)
            sql = if (whereIndex == -1) {
              "insert overwrite table %s select %s from %s".format(tableName, project, tableName)
            } else {
              "insert overwrite table %s select %s from %s where %s union all select * from %s where not(%s)"
                .format(tableName, project, tableName, filter, tableName, filter)
            }
            logDebug("New SQL:" + sql)
          }
          if (!silent && !sql.toLowerCase.startsWith("add") && !sql.toLowerCase.startsWith("set")) {
            //modify for spark2.3 , to first start local , then restart scheduler
            if (!restarted && sql.toLowerCase.contains("select ") &&
                spark.sparkContext.isLocal && originMaster != "local") {
              val parsed = spark.sessionState.sqlParser.parsePlan(sql)
              if (resolve.isRunOnYarn(parsed)) {
                restart(resolve.selectYarnCluster(parsed))
                setDescription(sql)
                restarted = true
              }
            }

            val df = spark.sql(sql)
            val execution = spark.sessionState.executePlan(df.logicalPlan)

            try {
              val printable = spark.sqlContext.getConf("hive.cli.print.header")
              if (printable != null && printable.toBoolean) {
                println(df.schema.map(_.name).mkString("\t"))
              }
            } catch {
              case e: NoSuchElementException =>
                logWarning("hive.cli.print.header not configured, so doesn't print colum's name.")
            }
            if (conf.getBoolean("spark.hive.shell.optimize.collect.enable", true) && !sql
                  .contains("order by")) {
              logDebug("execution.printEachPartitionResult():")
            } else {
              logDebug("execution.stringResult():")
            }
            df.queryExecution.analyzed match {
              case c: Command =>
                execution.hiveResultString().foreach(println)
              case u @ Union(children) if children.forall(_.isInstanceOf[Command]) =>
                execution.hiveResultString().foreach(println)
              case _ =>
                SQLExecution.withNewExecutionId(spark, execution) {
                  execution.hiveResultString().foreach(println)
                }
            }
          } else {
            spark.sql(sql).count
          }
        }
      }
      val endTime = System.currentTimeMillis()
      scala.Console.err.println("Time taken: %s s".format((endTime - startTime) / 1000.0))
    }

    /**
     * 处理每次输入字符串，如果是完整的一条或者多条SQL语句则调用run()函数执行，否则继续等待输入
     *
     * @param line 输入的字符串
     * @return 是否需要继续输入
     */
    def process(line: String): Boolean = {
      if (line.trim().startsWith("--")) {
        return true
      }
      wholeLine = wholeLine + " " + line
      if (line.trim() == "" || line.trim().last != ';') {
        return true
      }
      val lines = wholeLine
      wholeLine = ""
      run(lines)
      return false
    }

    /**
     * 循环读取输入的字符串并调用process()函数处理
     */
    def loop() = {
      val reader = new ConsoleReader()
      reader.setExpandEvents(false)
      /*if (sc.getConf.getBoolean("spark.hive.completor.enable", true)) {
        try {
          val startTime = System.currentTimeMillis()
          /* 采用反射的方式获取hive中ConsoleReader的Completor */
          val hiveCliDriverClass = Class.forName("org.apache.hadoop.hive.cli.CliDriver")
          val method = hiveCliDriverClass.getMethod("getCommandCompletor")
          val completors = method.invoke(null).asInstanceOf[Array[Completor]]
          /* 将从hive中获取的Completor添加到本地reader中 */
          completors.foreach(reader.addCompletor(_))
          val endTime = System.currentTimeMillis()
          logDebug("add completor taken: %s s".format((endTime - startTime) / 1000.0))
        } catch {
          case e: Exception => logWarning("Add completor failed.")
        }
      }*/

      /* 设置histroy文件，开启history功能 */
      val HISTORYFILE = ".spark_hive_history"
      val historyFile = System.getProperty("user.home") + File.separator + HISTORYFILE
      reader.setHistory(new FileHistory(new File(historyFile)))
      val prompt = "spark-xsql"

      while (true) {
        try {
          val line =
            if (isContinue) {
              reader.readLine(" " * prompt.length + "> ")
            } else {
              reader.readLine(prompt + "> ")
            }
          isContinue = process(line)
        } catch {
          case e: Exception => {
            //scala.Console.err.println("Failed: Error " + e.getMessage)
            logError("Failed: Error ", e)
          }
        }
      }
    }

    def exit(code: Int = 0) = {
      try {
        val catalog = spark.sessionState.catalog.asInstanceOf[XSQLSessionCatalog]
        catalog.stop()
        sc.stop()
      } catch {
        case e: Exception => None
      }
      System.exit(code)
    }

    exit()
  }
}
