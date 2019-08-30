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

import java.sql.Connection

import scala.xml.Node

import org.apache.spark.alarm.AlertMessage
import org.apache.spark.alarm.AlertType._
import org.apache.spark.monitor.Monitor
import org.apache.spark.monitor.MonitorItem.MonitorItem
import org.apache.spark.status.api.v1.TaskMetricDistributions
import org.apache.spark.ui.{ToolTips, UIUtils}
import org.apache.spark.ui.jobs.TaskDetailsClassNames
import org.apache.spark.util.Utils

abstract class StageMonitor extends Monitor {
  override val alertType = Seq(Stage)

}
class DataSkewMessage(
    skewed_stages: Array[Int],
    metrics: Array[TaskMetricDistributions],
    expectReduce: Array[Double],
    title: MonitorItem)
  extends AlertMessage(title) {
  // scalastyle:off
  private def makeHtmlTable(
      stageId: Int,
      metrics: TaskMetricDistributions,
      expectReduce: Double): Seq[Node] = {
    def timeQuantiles(data: IndexedSeq[Double]): Seq[Node] = {
      data.map { millis =>
        <td>{UIUtils.formatDuration(millis.toLong)}</td>
      }
    }

    def sizeQuantiles(data: IndexedSeq[Double]): Seq[Node] = {
      data.map { size =>
        <td>{Utils.bytesToString(size.toLong)}</td>
      }
    }

    def sizeQuantilesWithRecords(
        data: IndexedSeq[Double],
        records: IndexedSeq[Double]): Seq[Node] = {
      data.zip(records).map {
        case (d, r) =>
          <td>{s"${Utils.bytesToString(d.toLong)} / ${r.toLong}"}</td>
      }
    }

    def titleCell(title: String, tooltip: String): Seq[Node] = {
      <td>
        <span>
          {title}
        </span>
      </td>
    }

    def simpleTitleCell(title: String): Seq[Node] = <td>{title}</td>

    val serviceQuantiles = simpleTitleCell("Duration") ++ timeQuantiles(metrics.executorRunTime)

    val gcQuantiles = titleCell("GC Time", ToolTips.GC_TIME) ++ timeQuantiles(metrics.jvmGcTime)

    val peakExecutionMemoryQuantiles = titleCell(
      "Peak Execution Memory",
      ToolTips.PEAK_EXECUTION_MEMORY) ++ sizeQuantiles(metrics.peakExecutionMemory)

    def inputQuantiles: Seq[Node] = {
      simpleTitleCell("Input Size / Records") ++
        sizeQuantilesWithRecords(metrics.inputMetrics.bytesRead, metrics.inputMetrics.recordsRead)
    }

    def shuffleReadTotalQuantiles: Seq[Node] = {
      titleCell("Shuffle Read Size / Records", ToolTips.SHUFFLE_READ) ++
        sizeQuantilesWithRecords(
          metrics.shuffleReadMetrics.readBytes,
          metrics.shuffleReadMetrics.readRecords)
    }

    def memoryBytesSpilledQuantiles: Seq[Node] = {
      simpleTitleCell("Shuffle spill (memory)") ++ sizeQuantiles(metrics.memoryBytesSpilled)
    }

    def diskBytesSpilledQuantiles: Seq[Node] = {
      simpleTitleCell("Shuffle spill (disk)") ++ sizeQuantiles(metrics.diskBytesSpilled)
    }
    val quantileHeaders =
      Seq("Metric", "Min", "25th percentile", "Median", "75th percentile", "Max")
    val headerRow: Seq[Node] = {
      quantileHeaders.view.map { x =>
        <th>{x}</th>
      }
    }
    <h2>统计信息：Stage {stageId} 约额外占用 {expectReduce.formatted("%.2f")} executor*min </h2>
        <table border="3" bordercolor="#CCCCCC">
          <thead>{headerRow}</thead>
          <tbody>
            <tr>{serviceQuantiles}</tr>
            <tr>{gcQuantiles}</tr>
            <tr class={TaskDetailsClassNames.PEAK_EXECUTION_MEMORY}>
              {peakExecutionMemoryQuantiles}
            </tr>
            <tr>{inputQuantiles}</tr>
            <tr>{shuffleReadTotalQuantiles}</tr>
            <tr>{memoryBytesSpilledQuantiles}</tr>
            <tr>{diskBytesSpilledQuantiles}</tr>
          </tbody>
        </table>
  }

  override def toHtml(): String = {
    val summary = <h2>
      <span style="color:#E53333;">Stage
        {skewed_stages.mkString(",")}
        发生数据倾斜</span>
    </h2>
    val details = skewed_stages
      .zip(metrics)
      .zip(expectReduce)
      .map {
        case ((stageId, metric), expectReduce) =>
          makeHtmlTable(stageId, metric, expectReduce)
      }
      .flatten
    (summary ++ details).toString
  }

  override def toJdbc(conn: Connection, appId: String): Unit = {
    val query = "UPDATE `xsql_monitor`.`spark_history` " +
      "SET `skewStages` = ?, `expectExecutorAccReduce` = ?  WHERE `appId` = ? "
    val preparedStmt = conn.prepareStatement(query)
    preparedStmt.setInt(1, skewed_stages.size)
    preparedStmt.setDouble(2, expectReduce.sum)
    preparedStmt.setString(3, appId)
    preparedStmt.execute
  }
}
