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

package org.apache.spark.sql.hive.xitong.shell

/**
 * Created by liyuance on 15/8/31.
 */
import java.io.File

import scala.collection.mutable
import scala.io.Source

import jline.console._
import jline.console.history._
import sun.misc.BASE64Decoder

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

case class Config(file: String = "", batch: String = "")

object SparkHiveShell extends Logging {
  //scalastyle:off
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    val conf = spark.sparkContext.getConf
    lazy val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    var wholeLine = ""
    var isContinue = false
    val appNameMaxLength = 30
    val appNameDefault = "SparkSQLShell"
    val HIVERCFILE = ".hiverc"
    val regex = """set\s+spark\.app\.name\s*=\s*(.+)""".r
    var initFininshed = false
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
          if (conf.get("spark.master") == "yarn" && conf
                .get("spark.submit.deployMode") == "cluster") {
            logDebug(s"batchContent base64 is ${config.batch}")
            new String(new BASE64Decoder().decodeBuffer(config.batch));
          } else {
            config.batch
          }
        initialize(batchContent, conf)
        run(dropComment(batchContent))
      } else {
        // initialize(appNameDefault, conf)
        loop()
      }
    } getOrElse {
      // arguments are bad, error message will have been displayed
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
      conf.set("spark.sql.string", java.net.URLEncoder.encode(sqlString))
    }

    def processInitFile(conf: SparkConf) = {
      val hivercFile = System.getProperty("user.home") + File.separator + HIVERCFILE;
      val enableHiverc = conf.getBoolean("spark.enable.hiverc", true)
      logInfo("spark.enable.hiverc:" + enableHiverc)
      if (new File(hivercFile).exists() && enableHiverc) {
        val fileContent = Source.fromFile(hivercFile).mkString
        run(fileContent, true)
      }
    }

    def run(commands: String, silent: Boolean = false) = {
      var startTime = System.currentTimeMillis()
      for (command <- commands.split(";")) {
        var sql = command.trim()
        if (sql != "") {
          if (sql.equals("exit") || sql.equals("quit")) {
            exit()
          }
          logInfo("current SQL: " + sql)
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
            // hiveContext.sql(sql).collect.foreach(c => println(c.mkString("\t")))
            // val execution = hiveContext.executePlan(hiveContext.sql(sql).logicalPlan)
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
              execution.hiveResultString().foreach(println);
            } else {
              logDebug("execution.stringResult():")
              execution.hiveResultString().foreach(println);
            }
          } else {
            spark.sql(sql).count
          }
        }
      }
      val endTime = System.currentTimeMillis()
      scala.Console.err.println("Time taken: %s s".format((endTime - startTime) / 1000.0))
    }

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
      if (initFininshed == false) {
        initialize(appNameDefault, conf)
        initFininshed = true
      }
      run(lines)
      return false
    }

    def loop() = {
      val reader = new ConsoleReader()
      reader.setExpandEvents(false)
      val HISTORYFILE = ".spark_hive_history"
      val historyFile = System.getProperty("user.home") + File.separator + HISTORYFILE
      reader.setHistory(new FileHistory(new File(historyFile)))
      val prompt = "spark-hive"

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
          case e: NullPointerException =>
            exit(-1)
          case e: Exception =>
            // scala.Console.err.println("Failed: Error " + e.getMessage)
            logError("Failed: Error ", e)
        }
      }
    }

    def exit(code: Int = 0) = {
      try {
        sc.stop()
      } catch {
        case e: Exception => None
      }
      System.exit(code)
    }

    exit()
  }
}
