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
package org.apache.spark.monitor.executor

import java.io.File
import java.util.Date

import scala.xml._

import org.apache.spark.alarm.{AlertMessage, EmailAlarm, HtmlMessage}
import org.apache.spark.monitor.{Monitor, MonitorItem}
import org.apache.spark.monitor.MonitorItem.MonitorItem
import org.apache.spark.painter.TimeSeriesChartPainter
import org.apache.spark.scheduler._
import org.apache.spark.status.ExecutorSummaryWrapper

class ExecutorNumMonitor extends ExecutorMonitor {
  override val item: MonitorItem = MonitorItem.EXECUTOR_NUM_NOTIFIER
  lazy val dataPath = s"/tmp/${item}-${conf.get("spark.app.id")}.csv"
  lazy val picturePath = s"/tmp/${item}-${conf.get("spark.app.id")}.jpg"
  lazy val eventMinInterval =
    conf.getTimeAsMs(s"${Monitor.PREFIX}.${item.toString.toLowerCase}.granularity", "60s")
  var lastPointTime: Long = new Date().getTime
  var recentEventTime: Long = new Date().getTime

  lazy private val painter = new TimeSeriesChartPainter(dataPath, picturePath)

  def executorNum(): Long = {
    kvStore.count(classOf[ExecutorSummaryWrapper], "active", true)
  }

  def addPoint(executorNum: Long, time: Long): Unit = {
    painter.addPoint(executorNum, recentEventTime)
  }
  // scalastyle:off
  override def watchOut(event: SparkListenerEvent): Option[AlertMessage] = {
    event match {
      case env: SparkListenerExecutorAdded =>
        // try to coarse num change in 60s into one point, so that we can keep graph clean and readable
        if (env.time - lastPointTime > eventMinInterval) {
          addPoint(executorNum, recentEventTime)
          addPoint(executorNum, env.time)
          lastPointTime = env.time
        }
        recentEventTime = env.time
        Option.empty
      case env: SparkListenerExecutorRemoved =>
        if (env.time - lastPointTime > eventMinInterval) {
          addPoint(executorNum, recentEventTime)
          addPoint(executorNum, env.time)
          lastPointTime = env.time
        }
        recentEventTime = env.time
        Option.empty
      case e: SparkListenerApplicationEnd =>
        addPoint(executorNum, recentEventTime)
        addPoint(executorNum, new Date().getTime)
        painter.paint(600, 400, "executor num curve", "datetime", "executor num")
        if (EmailAlarm.get().isDefined) {
          val pic = EmailAlarm.get().get.embed(new File(picturePath))
          val a = <h2>动态调度情况：</h2>
            <img src={"cid:"+pic}></img>
            <br/>
          Option(new HtmlMessage(title = item, content = a.mkString))
        } else {
          Option.empty
        }
    }
  }
  // scalastyle:on
}
