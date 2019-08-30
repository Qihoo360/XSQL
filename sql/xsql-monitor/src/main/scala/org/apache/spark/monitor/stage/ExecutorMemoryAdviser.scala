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
package org.apache.spark.monitor.stage

import java.io.File
import java.util.Scanner

import scala.collection.JavaConverters._

import org.apache.spark.alarm.{AlertMessage, EmailAlarm, HtmlMessage}
import org.apache.spark.monitor.{Monitor, MonitorItem}
import org.apache.spark.monitor.MonitorItem.MonitorItem
import org.apache.spark.painter.BarChartPainter
import org.apache.spark.scheduler._
import org.apache.spark.status.{ExecutorSummaryWrapper, TaskDataWrapper}
import org.apache.spark.util.Utils

class ExecutorMemoryAdviser extends StageMonitor {

  override val item: MonitorItem = MonitorItem.EXECUTOR_MEMORY_ADVISER
  lazy val dataPath = s"/tmp/${item}-${conf.get("spark.app.id")}.csv"
  lazy val picturePath = s"/tmp/${item}-${conf.get("spark.app.id")}.jpg"
  lazy private val painter = new BarChartPainter(dataPath, picturePath)

  lazy val majorityCriterion =
    conf.getDouble(s"${Monitor.PREFIX}.${item.toString.toLowerCase}.majority.criterion", 0.8)
  lazy val lowerBound =
    conf.getDouble(s"${Monitor.PREFIX}.${item.toString.toLowerCase}.range.lb", 0.5)
  lazy val upperBound =
    conf.getDouble(s"${Monitor.PREFIX}.${item.toString.toLowerCase}.range.ub", 0.9)

  lazy val executeTotalMemory =
    kvStore.view(classOf[ExecutorSummaryWrapper]).skip(1).max(1).asScala.head.info.maxMemory

  private def getStagePeakAndMajorityMemory(stageInfo: StageInfo): Unit = {
    val stageKey = Array(stageInfo.stageId, stageInfo.attemptId)
    val peakMemory = kvStore
      .view(classOf[TaskDataWrapper])
      .index("pem")
      .parent(stageKey)
      .reverse
      .skip(0)
      .max(1)
      .asScala
      .head
      .peakExecutionMemory
    val skipTask = (stageInfo.numTasks * (1 - majorityCriterion)).toLong
    val majorityMemory = kvStore
      .view(classOf[TaskDataWrapper])
      .index("pem")
      .parent(stageKey)
      .reverse
      .skip(skipTask)
      .max(1)
      .asScala
      .head
      .peakExecutionMemory
    painter.addPoint(stageKey.mkString("_"), peakMemory, majorityMemory)
  }
  private def computeMaxMemoryUtility(): (Double, Double) = {
    val scaner = new Scanner(new File(dataPath))
    var maxPeakMemory = 0L
    var maxMajorityMemory = 0L
    while (scaner.hasNext()) {
      val cols = scaner.next().split(",")
      maxPeakMemory = Math.max(cols(1).toLong, maxPeakMemory)
      maxMajorityMemory = Math.max(cols(2).toLong, maxMajorityMemory)
    }
    (maxPeakMemory / executeTotalMemory, maxMajorityMemory / executeTotalMemory)
  }
  // scalastyle:off
  override def watchOut(event: SparkListenerEvent): Option[AlertMessage] = {
    event match {
      case e: SparkListenerStageCompleted =>
        getStagePeakAndMajorityMemory(e.stageInfo)
        Option.empty
      case _: SparkListenerApplicationEnd =>
        painter.paint(
          600,
          400,
          "stage memory usage columnar",
          "stageId",
          "memory usage(MB)",
          0,
          Utils.byteStringAsMb(executeTotalMemory.toLong + "b"))
        if (EmailAlarm.get().isDefined) {
          val pic = EmailAlarm.get().get.embed(new File(picturePath))
          val (peakUtility, majorityUtility) = computeMaxMemoryUtility
          val a = <h2>Stage 运行内存使用量概况：</h2>
              <img src={"cid:"+pic}></img>
                <br/>
          val userDefinedMemory =
            conf.getSizeAsBytes("spark.executor.memory") > (3L * 1024 * 1024 * 1024)
          val b = if (peakUtility < lowerBound && userDefinedMemory) {
            <h2>内存建议：调低executor-memory</h2>
          } else if (majorityUtility < lowerBound && userDefinedMemory) {
            <h2>内存建议：解决数据倾斜后，调低executor-memory</h2>
          } else if (majorityUtility > upperBound) {
            <h2>内存建议：调高executor-memory，或者调低spark.memory.storageFraction</h2>
          } else {
            <h2>executor内存利用率正常</h2>
          }
          a.append(b)
          Option(new HtmlMessage(title = item, content = a.mkString))
        } else {
          Option.empty
        }
    }
  }
  // scalastyle:on
}
