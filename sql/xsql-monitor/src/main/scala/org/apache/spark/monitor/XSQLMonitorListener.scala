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
package org.apache.spark.monitor

import java.sql.Connection

import scala.collection.mutable
import scala.collection.mutable.HashMap

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.alarm.{Alarm, AlarmFactory, AlertMessage, AlertType}
import org.apache.spark.internal.Logging
import org.apache.spark.monitor.stage.DataSkewMessage
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.ui.{
  SparkListenerSQLAdaptiveExecutionUpdate,
  SparkListenerSQLExecutionEnd,
  SparkListenerSQLExecutionStart
}
import org.apache.spark.status.ApplicationInfoWrapper
import org.apache.spark.util.kvstore.KVStore

class XSQLMonitorListener(conf: SparkConf, kvstore: KVStore) extends SparkListener with Logging {

  def this(conf: SparkConf) = {
    this(conf, SparkContext.getActive.get.statusStore.store)
  }

  var _alarms: Array[Alarm] = _
  var monitors: HashMap[AlertType.AlertType, mutable.Buffer[Monitor]] =
    new mutable.HashMap[AlertType.AlertType, mutable.Buffer[Monitor]]()

  initialize

  def initialize: Unit = {
    val alarmNames = conf.get("spark.xsql.alarm.items", "email").split(",")
    _alarms = alarmNames.map {
      case alarmName =>
        AlarmFactory.create(
          alarmName,
          conf.getAllWithPrefix(s"spark.xsql.alarm.${alarmName}.").toMap)
    }
    conf.get(Monitor.MONITOR_ITEMS).foreach { monitorName =>
      val monitor = MonitorFactory.create(monitorName, _alarms, kvstore, conf)
      monitor.alertType.foreach(alertType =>
        monitors.getOrElseUpdate(alertType, new mutable.ArrayBuffer[Monitor]()).append(monitor))
    }
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    val summary = monitors.values.flatten
      .toSet[Monitor]
      .map(_.watchOut(applicationEnd))
      .filter(_.isDefined)
      .map(_.get)
      .toSeq
      .sortBy(_.title)
    _alarms.foreach(
      _.finalAlarm(
        new ApplicationSummary(
          summary,
          kvstore.view(classOf[ApplicationInfoWrapper]).max(1).iterator().next().info.id)))
  }

  override def onApplicationStart(event: SparkListenerApplicationStart): Unit = {
    monitors.getOrElse(AlertType.Application, Seq()).foreach(_.onEvent(event))
  }

  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit = {
    monitors.getOrElse(AlertType.Executor, Seq()).foreach(_.onEvent(event))
  }

  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = {
    monitors.getOrElse(AlertType.Executor, Seq()).foreach(_.onEvent(event))
  }

  override def onJobStart(event: SparkListenerJobStart): Unit = {
    monitors.getOrElse(AlertType.Job, Seq()).foreach(_.onEvent(event))
  }
  override def onJobEnd(event: SparkListenerJobEnd): Unit = {
    monitors.getOrElse(AlertType.Job, Seq()).foreach(_.onEvent(event))
  }
  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    monitors.getOrElse(AlertType.Stage, Seq()).foreach(_.onEvent(event))
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionStart =>
      monitors.getOrElse(AlertType.SQL, Seq()).foreach(_.onEvent(event))
    case e: SparkListenerSQLExecutionEnd =>
      monitors.getOrElse(AlertType.SQL, Seq()).foreach(_.onEvent(event))
    case e: SparkListenerSQLAdaptiveExecutionUpdate =>
      monitors.getOrElse(AlertType.SQL, Seq()).foreach(_.onEvent(event))
    case _ => // Ignore
  }

}
class ApplicationSummary(messages: Seq[AlertMessage], appId: String)
  extends AlertMessage(title = MonitorItem.SPARK_APPLICATION_SUMMARY) {
  override def toCsv(): String = {
    messages.map(_.toCsv()).mkString(",")
  }

  override def toHtml(): String = {
    messages.map(_.toHtml()).mkString
  }

  override def toJdbc(conn: Connection, appId: String = appId): Unit = {
    messages.foreach(_.toJdbc(conn, appId))
    if (messages.exists(_.isInstanceOf[DataSkewMessage])) {
      val query = "UPDATE `xsql_monitor`.`spark_history` SET `htmlContent` = ? WHERE `appId` = ? "
      val preparedStmt = conn.prepareStatement(query)
      preparedStmt.setString(1, toHtml())
      preparedStmt.setString(2, appId)
      preparedStmt.execute
    }
  }
}
