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
package org.apache.spark.monitor.sql

import java.util.Date
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Function

import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.lang3.time.{DateFormatUtils, DateUtils}

import org.apache.spark.alarm.AlertMessage
import org.apache.spark.monitor.MonitorItem
import org.apache.spark.monitor.MonitorItem.MonitorItem
import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerEvent}
import org.apache.spark.sql.execution.ui.{
  SparkListenerSQLAdaptiveExecutionUpdate,
  SparkListenerSQLExecutionEnd,
  SparkListenerSQLExecutionStart
}

class SQLPlanModifiedMonitor extends SQLMonitor {
  override val item: MonitorItem = MonitorItem.SQL_CHANGE_NOTIFIER

  private val liveExecutions = new ConcurrentHashMap[Long, LiveExecutionData]()

  private val blacklist = Seq(
    "SetCommand",
    "LocalRelation",
    "SetDatabaseCommand",
    "XSQLSetDatabaseCommand",
    "DropTableCommand",
    "XSQLShowColumnsCommand",
    "XSQLShowTablesCommand")

  private val dateFormats = Seq("yyyy-MM-dd", "yyyy/MM/dd", "yyyyMMdd")

  private lazy val appid = appStore.applicationInfo().id

  def getMd5(physicalPlanDescription: String, time: Long): Option[String] = {
    val today = new Date(time)
    val yesterday = DateUtils.addDays(new Date(time), -1)
    val parsedLogicalPlan = physicalPlanDescription
      .substring("== Parsed Logical Plan ==\n".length)
    if (blacklist.exists(parsedLogicalPlan.startsWith(_))) {
      None
    } else {
      var cleanedPlan = parsedLogicalPlan
        .substring(0, physicalPlanDescription.indexOf("Analyzed Logical Plan"))
        .replaceAll("#[0-9]*", "")
      dateFormats.foreach(
        format =>
          cleanedPlan = cleanedPlan
            .replaceAll(DateFormatUtils.format(yesterday, format), "yesterday")
            .replaceAll(DateFormatUtils.format(today, format), "today"))
      Option(DigestUtils.md5Hex(cleanedPlan))
    }
  }

  override def watchOut(event: SparkListenerEvent): Option[AlertMessage] = {
    event match {
      case e: SparkListenerSQLExecutionStart =>
        val SparkListenerSQLExecutionStart(
          executionId,
          description,
          details,
          physicalPlanDescription,
          sparkPlanInfo,
          time) = event
        val exec = getOrCreateExecution(executionId)
        exec.description = description
        exec.details = details
        exec.physicalPlanDescription = physicalPlanDescription
        exec.submissionTime = time

        Option.empty
      case e: SparkListenerSQLExecutionEnd =>
        val SparkListenerSQLExecutionEnd(executionId, time) = event
        val exec = getOrCreateExecution(executionId)
        getMd5(exec.physicalPlanDescription, time).map(sqlId => {
          new SQLInfo(
            item,
            sqlId,
            exec.ae_work,
            appid,
            executionId,
            new Date(exec.submissionTime),
            time - exec.submissionTime)
        })
      case e: SparkListenerSQLAdaptiveExecutionUpdate =>
        val SparkListenerSQLAdaptiveExecutionUpdate(
          executionId,
          physicalPlanDescription,
          sparkPlanInfo) = event
        val exec = getOrCreateExecution(executionId)
        if ((physicalPlanDescription
              .substring(physicalPlanDescription.indexOf("Physical Plan"))
              .split("BroadcastHashJoin", -1)
              .length - 1) != exec.initBroadcastJoin) {
          exec.ae_work = true
        }
        Option.empty
      case e: SparkListenerApplicationEnd =>
        Option.empty
    }
  }
  private def getOrCreateExecution(executionId: Long): LiveExecutionData = {
    liveExecutions.computeIfAbsent(executionId, new Function[Long, LiveExecutionData]() {
      override def apply(key: Long): LiveExecutionData = new LiveExecutionData(executionId)
    })
  }
}

private class LiveExecutionData(val executionId: Long) {
  var description: String = null
  var details: String = null
  var physicalPlanDescription: String = null
  var submissionTime = -1L
  lazy val initBroadcastJoin = {
    physicalPlanDescription
      .substring(physicalPlanDescription.indexOf("Physical Plan"))
      .split("BroadcastHashJoin", -1)
      .length - 1
  }
  var ae_work: Boolean = false
}
