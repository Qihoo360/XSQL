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
package org.apache.spark.monitor.job

import java.util.concurrent.{Executors, ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._

import org.apache.spark.JobExecutionStatus
import org.apache.spark.alarm.{AlertMessage, HtmlMessage}
import org.apache.spark.monitor.{Monitor, MonitorItem}
import org.apache.spark.monitor.MonitorItem.MonitorItem
import org.apache.spark.scheduler.{SparkListenerEvent, SparkListenerJobEnd, SparkListenerJobStart}
import org.apache.spark.status.JobDataWrapper

class ApplicationIdleMonitor extends JobMonitor {

  override val item: MonitorItem = MonitorItem.APP_IDLE_WARNER
  val delayThread = Executors.newScheduledThreadPool(1)
  lazy val endureLimit =
    conf.getTimeAsMs(s"${Monitor.PREFIX}.${item.toString.toLowerCase}.timeout", "1h")
  private var idleTimeout: AtomicReference[ScheduledFuture[_]] = new AtomicReference()

  private def getActiveJobNum(): Int = {
//    appStore.count(classOf[JobDataWrapper], "completionTime", -1L)
    kvStore
      .view(classOf[JobDataWrapper])
      .reverse()
      .asScala
      .map(_.info)
      .filter(_.status == JobExecutionStatus.RUNNING)
      .size
  }

  private def stopIdleTimeout(): Unit = {
    val idleTimeout = this.idleTimeout.getAndSet(null)
    if (idleTimeout != null) {
      idleTimeout.cancel(false)
    }
  }

  private def setupIdleTimeout(): Unit = {
    if (getActiveJobNum > 0) return
    val timeoutTask = new Runnable() {
      override def run(): Unit = {
        // scalastyle:off
        val driverlUrl = conf
          .get(
            "spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASES")
          .split(",")
          .head
        val a = <h2>您的Spark应用</h2>
            <a href={driverlUrl}>{driverlUrl}</a>
            <h2>空闲已超过 {conf.get(
              s"${Monitor.PREFIX}.${item}.timeout", "1h")}</h2>
            <h2>请及时关闭</h2>
        val message = new HtmlMessage(title = item, content = a.mkString)
        alarms.foreach(_.alarm(message))
        // scalastyle:on
      }
    }

    val timeout = delayThread
      .scheduleWithFixedDelay(timeoutTask, endureLimit, endureLimit, TimeUnit.MILLISECONDS)
    // If there's already an idle task registered, then cancel the new one.
    if (!this.idleTimeout.compareAndSet(null, timeout)) {
      timeout.cancel(false)
    }
    // If a new client connected while the idle task was being set up, then stop the task.
    if (getActiveJobNum > 0) stopIdleTimeout()
  }

  override def watchOut(event: SparkListenerEvent): Option[AlertMessage] = {
    event match {
      case env: SparkListenerJobStart =>
        stopIdleTimeout
        Option.empty
      case env: SparkListenerJobEnd =>
        setupIdleTimeout
        Option.empty
      case _ =>
        Option.empty
    }
  }
}
