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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkConf
import org.apache.spark.alarm.{Alarm, AlertMessage}
import org.apache.spark.alarm.AlertType.AlertType
import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.monitor.MonitorItem.MonitorItem
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.status.AppStatusStore
import org.apache.spark.util.kvstore.KVStore

trait Monitor {

  val alertType: Seq[AlertType]
  val item: MonitorItem
  val alarms: ArrayBuffer[Alarm] = ArrayBuffer()
  var kvStore: KVStore = null
  var appStore: AppStatusStore = null
  var conf: SparkConf = null

  def watchOut(event: SparkListenerEvent): Option[AlertMessage]
  def bind(alarm: Alarm): Monitor = {
    alarms.append(alarm)
    this
  }
  def bind(alarms: Seq[Alarm]): Monitor = {
    this.alarms.appendAll(alarms)
    this
  }
  def bind(kvStore: KVStore): Monitor = {
    this.kvStore = kvStore
    this.appStore = new AppStatusStore(kvStore)
    this
  }
  def bind(conf: SparkConf): Monitor = {
    this.conf = conf
    this
  }
  def onEvent(event: SparkListenerEvent): Unit = {
    val message = watchOut(event)
    if (message.isDefined) {
      alarms.foreach(_.alarm(message.get))
    }
  }
}
object Monitor {
  val commonClasses = Seq(
    "org.apache.spark.sql.xsql.shell.SparkXSQLShell",
    "org.apache.spark.repl.Main",
    "org.apache.spark.sql.hive.xitong.shell.SparkHiveShell",
    "org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver")
  val dateFormats = Seq("yyyy-MM-dd", "yyyy/MM/dd", "yyyyMMdd")
  val PREFIX = "spark.monitor"
  private[spark] val MONITOR_ITEMS =
    ConfigBuilder("spark.monitor.items")
      .internal()
      .doc("choose monitors to open, split with `,`")
      .stringConf
      .transform(_.toUpperCase)
      .toSequence
      .checkValue(
        _.toSet.subsetOf(MonitorItem.values.map(_.toString)),
        s"must be one of ${MonitorItem.values.map(_.toString)}")
      .createWithDefault(Seq.empty)
}
object MonitorItem extends Enumeration {
  type MonitorItem = Value
  val SQL_CHANGE_NOTIFIER = Value
  val APP_FINISH_NOTIFIER, EXECUTOR_NUM_NOTIFIER, DATASKEW_NOTIFIER, EXECUTOR_MEMORY_ADVISER =
    Value
  val SPARK_APPLICATION_SUMMARY, APP_IDLE_WARNER = Value
}
