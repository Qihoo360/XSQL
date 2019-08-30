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
package org.apache.spark.monitor.application

import java.sql.{Connection, Timestamp}
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration

import org.apache.spark.alarm.AlertMessage
import org.apache.spark.alarm.AlertType._
import org.apache.spark.monitor.Monitor
import org.apache.spark.monitor.MonitorItem.MonitorItem

abstract class ApplicationMonitor extends Monitor {
  override val alertType = Seq(Application)
}

class ApplicationInfo(
    title: MonitorItem,
    appName: String,
    appId: String,
    md5: String,
    startTime: Date,
    duration: Long,
    appUiUrl: String,
    historyUrl: String,
    eventLogDir: String,
    minExecutor: Int,
    maxExecutor: Int,
    executorCore: Int,
    executorMemoryMB: Long,
    executorAccu: Double,
    user: String)
  extends AlertMessage(title) {
  override def toCsv(): String = {
    s"${user},${appId}," +
      s"${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(startTime)}," +
      s"${Duration(duration, TimeUnit.MILLISECONDS).toSeconds}," +
      s"${executorMemoryMB},${executorCore},${executorAccu.formatted("%.2f")},${appName}"
  }
  // scalastyle:off
  override def toHtml(): String = {
    val html = <h1>任务完成! </h1>
        <h2>任务信息 </h2>
        <ul>
          <li>作业名：{appName}</li>
          <li>作业ID：{appId}</li>
          <li>开始时间：{new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(startTime)}</li>
          <li>任务用时：{Duration(duration, TimeUnit.MILLISECONDS).toSeconds} s</li>
        </ul>
        <h2>资源用量</h2>
        <ul>
          <li>Executor个数：{minExecutor}~{maxExecutor}</li>
          <li>Executor内存：{executorMemoryMB} MB</li>
          <li>Executor核数：{executorCore}</li>
          <li>Executor累积用量：{executorAccu.formatted("%.2f")} executor*min</li>
        </ul>
        <h2>调试信息</h2>
        <ul>
          <li>回看链接1：<a href={appUiUrl.split(",").head}>{appUiUrl.split(",").head}</a></li>
          <li>回看链接2：<a href={historyUrl}>{historyUrl}</a></li>
          <li>日志文件所在目录：{eventLogDir}</li>
        </ul>
    html.mkString
  }

  override def toJdbc(conn: Connection, appId: String): Unit = {
    val query = "INSERT INTO `xsql_monitor`.`spark_history`(" +
      "`user`, `md5`, `appId`, `startTime`, `duration`, " +
      "`yarnURL`, `sparkHistoryURL`, `eventLogDir`, `coresPerExecutor`, `memoryPerExecutorMB`," +
      " `executorAcc`, `appName`, `minExecutors`, `maxExecutors`)" +
      " SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? FROM DUAL" +
      " WHERE NOT EXISTS (SELECT * FROM `xsql_monitor`.`spark_history` WHERE `appId` = ?);"

    val preparedStmt = conn.prepareStatement(query)
    preparedStmt.setString(1, user)
    preparedStmt.setString(2, md5)
    preparedStmt.setString(3, appId)
    preparedStmt.setTimestamp(4, new Timestamp(startTime.getTime))
    preparedStmt.setLong(5, Duration(duration, TimeUnit.MILLISECONDS).toSeconds)
    preparedStmt.setString(6, appUiUrl)
    preparedStmt.setString(7, historyUrl)
    preparedStmt.setString(8, eventLogDir)
    preparedStmt.setInt(9, executorCore)
    preparedStmt.setLong(10, executorMemoryMB)
    preparedStmt.setDouble(11, executorAccu)
    preparedStmt.setString(12, appName)
    preparedStmt.setInt(13, minExecutor)
    preparedStmt.setInt(14, maxExecutor)
    preparedStmt.setString(15, appId)
    preparedStmt.execute
  }
}
