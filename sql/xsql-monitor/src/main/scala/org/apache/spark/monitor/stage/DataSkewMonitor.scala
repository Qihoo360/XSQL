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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.alarm.AlertMessage
import org.apache.spark.monitor.Monitor
import org.apache.spark.monitor.MonitorItem.{DATASKEW_NOTIFIER, MonitorItem}
import org.apache.spark.scheduler.{
  SparkListenerApplicationEnd,
  SparkListenerEvent,
  SparkListenerStageCompleted
}
import org.apache.spark.status.{TaskDataWrapper, TaskIndexNames}
import org.apache.spark.status.api.v1.TaskMetricDistributions

class DataSkewMonitor extends StageMonitor {
  override val item: MonitorItem = DATASKEW_NOTIFIER
  private val skewed_stages = new ArrayBuffer[Int]()
  private val metrics = new ArrayBuffer[TaskMetricDistributions]()
  private val expectReduce = new ArrayBuffer[Double]()
  lazy val factorThreshold =
    conf.getDouble(s"${Monitor.PREFIX}.${item.toString.toLowerCase}.threshold.factor", 2)
  lazy val mbThreshold =
    conf.getSizeAsBytes(s"${Monitor.PREFIX}.${item.toString.toLowerCase}.threshold.mb", "200m")

  private def isSkew(indexedSeq: IndexedSeq[Double]): Boolean = {
    indexedSeq(4) > indexedSeq(3) * factorThreshold && indexedSeq(4) > mbThreshold
  }

  private def isSkew(metric: TaskMetricDistributions): Boolean = {
    val shuffleReadSize = metric.shuffleReadMetrics.readBytes
    val inputSize = metric.inputMetrics.bytesRead
    val executeTime = metric.executorRunTime
    isSkew(shuffleReadSize) || isSkew(inputSize)
//     || isSkew(executeTime)
  }

  private def getExpectReduce(
      stageId: Int,
      attemptId: Int,
      metric: TaskMetricDistributions): Double = {
    val shuffleReadSize = metric.shuffleReadMetrics.readBytes
    val inputSize = metric.inputMetrics.bytesRead
    val stageKey = Array(stageId, attemptId)
    val expectDuration = metric.executorRunTime(3)
    if (isSkew(shuffleReadSize)) {
      kvStore
        .view(classOf[TaskDataWrapper])
        .parent(stageKey)
        .index(TaskIndexNames.SHUFFLE_TOTAL_READS)
        .first(shuffleReadSize(3).toLong)
        .asScala
        .filter { _.status == "SUCCESS" } // Filter "SUCCESS" tasks
        .toIndexedSeq
        .map(m => (m.executorRunTime - expectDuration) / 1000D / 60)
        .sum
        .max(0D)
    } else if (isSkew(inputSize)) {
      kvStore
        .view(classOf[TaskDataWrapper])
        .parent(stageKey)
        .index(TaskIndexNames.INPUT_SIZE)
        .first(inputSize(3).toLong)
        .asScala
        .filter { _.status == "SUCCESS" } // Filter "SUCCESS" tasks
        .toIndexedSeq
        .map(m => (m.executorRunTime - expectDuration) / 1000D / 60)
        .sum
        .max(0D)
    } else {
      0D
    }
  }

  override def watchOut(event: SparkListenerEvent): Option[AlertMessage] = {
    event match {
      case e: SparkListenerStageCompleted =>
        val stageId = e.stageInfo.stageId
        val metric =
          appStore.taskSummary(stageId, e.stageInfo.attemptId, Array(0, 0.25, 0.5, 0.75, 1.0))
        if (metric.exists(isSkew)) {
          skewed_stages += (stageId)
          expectReduce += getExpectReduce(stageId, e.stageInfo.attemptId, metric.get)
          metrics += metric.get
        }
        Option.empty
      case _: SparkListenerApplicationEnd =>
        if (skewed_stages.size > 0) {
          Option(
            new DataSkewMessage(
              skewed_stages.toArray,
              metrics.toArray,
              expectReduce.toArray,
              title = item))
        } else {
          Option.empty
        }
    }
  }
}
