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

import java.util.Date

import scala.collection.mutable.HashMap

import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.lang3.time.{DateFormatUtils, DateUtils}

import org.apache.spark.SparkConf
import org.apache.spark.alarm.{AlertMessage, AlertType}
import org.apache.spark.alarm.AlertType._
import org.apache.spark.internal.config.{DYN_ALLOCATION_MAX_EXECUTORS, EXECUTOR_INSTANCES}
import org.apache.spark.monitor.{Monitor, MonitorItem}
import org.apache.spark.monitor.MonitorItem.MonitorItem
import org.apache.spark.scheduler.{
  SparkListenerApplicationEnd,
  SparkListenerApplicationStart,
  SparkListenerEvent,
  SparkListenerExecutorAdded,
  SparkListenerExecutorRemoved
}
import org.apache.spark.scheduler.cluster.SchedulerBackendUtils
import org.apache.spark.util.Utils

class ApplicationEndMonitor extends ApplicationMonitor {

  private class LiveExecutor(val executorId: String, val addTime: Long)
  private val liveExecutors = new HashMap[String, LiveExecutor]()

  var sparkConf: SparkConf = null
  override val alertType: Seq[AlertType.Value] = Seq(Application, Executor)
  override val item: MonitorItem = MonitorItem.APP_FINISH_NOTIFIER
  var executorAccu: Double = 0

  def getMd5(sparkProperties: Map[String, String], time: Long): String = {
    var javaCommand = sparkProperties("sun.java.command")
    val commands = javaCommand.split("\\s+")
    val mainClass = commands(commands.indexOf("--class") + 1)
    val today = new Date(time)
    val yesterday = DateUtils.addDays(new Date(time), -1)
    if (mainClass.equals("*********(redacted)")) {
      "redacted"
    } else if (Monitor.commonClasses.contains(mainClass) && !commands.exists(_.equals("-e"))) {
      mainClass.split('.').last
    } else {
      Monitor.dateFormats.foreach(
        format =>
          javaCommand = javaCommand
            .replaceAll(DateFormatUtils.format(yesterday, format), "yesterday")
            .replaceAll(DateFormatUtils.format(today, format), "today"))
      DigestUtils.md5Hex(javaCommand)
    }
  }

  def getMaxExecutors(): Int = {
    if (Utils.isDynamicAllocationEnabled(sparkConf)) {
      sparkConf.get(DYN_ALLOCATION_MAX_EXECUTORS)
    } else {
      sparkConf.get(EXECUTOR_INSTANCES).getOrElse(SchedulerBackendUtils.DEFAULT_NUMBER_EXECUTORS)
    }
  }

  def getInitialExecutors(): Int = {
    if (Utils.isDynamicAllocationEnabled(sparkConf)) {
      Utils.getDynamicAllocationInitialExecutors(sparkConf)
    } else {
      sparkConf.get(EXECUTOR_INSTANCES).getOrElse(SchedulerBackendUtils.DEFAULT_NUMBER_EXECUTORS)
    }
  }

  override def watchOut(event: SparkListenerEvent): Option[AlertMessage] = {
    event match {
      case e: SparkListenerApplicationStart =>
        sparkConf = new SparkConf(false).setAll(appStore.environmentInfo().sparkProperties)
        Option.empty
      case e: SparkListenerExecutorAdded =>
        getOrCreateExecutor(e.executorId, e.time)
        Option.empty
      case e: SparkListenerExecutorRemoved =>
        liveExecutors.remove(e.executorId).foreach { exec =>
          executorAccu += (e.time - exec.addTime) / 1000D / 60
        }
        Option.empty
      case e: SparkListenerApplicationEnd =>
        liveExecutors.values.foreach { exec =>
          executorAccu += (e.time - exec.addTime) / 1000D / 60
        }
        val appInfo = appStore.applicationInfo()
        val appEnv = appStore.environmentInfo()
        val appAttempt = appInfo.attempts.head
        val username = Option(appInfo.attempts.head.sparkUser)
          .orElse(appEnv.systemProperties.toMap.get("user.name"))
          .getOrElse("<unknown>")
        Option(
          new ApplicationInfo(
            item,
            appInfo.name,
            appInfo.id,
            getMd5(appEnv.systemProperties.toMap, e.time),
            appAttempt.startTime,
            appAttempt.duration,
            sparkConf.get(
              "spark.org.apache.hadoop" +
                ".yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASES",
              ""),
            s"http://${sparkConf.get("spark.yarn.historyServer.address", "")}" +
              s"/history/${sparkConf.get("spark.app.id")}",
            sparkConf.get("spark.eventLog.dir"),
            getInitialExecutors,
            getMaxExecutors,
            sparkConf.get("spark.executor.cores", "1").toInt,
            sparkConf.getSizeAsMb("spark.executor.memory", "1024m"),
            executorAccu,
            username))
    }
  }

  private def getOrCreateExecutor(executorId: String, addTime: Long): LiveExecutor = {
    liveExecutors.getOrElseUpdate(executorId, {
      new LiveExecutor(executorId, addTime)
    })
  }

}
