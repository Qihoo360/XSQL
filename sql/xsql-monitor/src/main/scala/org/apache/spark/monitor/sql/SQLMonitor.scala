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

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration

import org.apache.spark.alarm.AlertMessage
import org.apache.spark.alarm.AlertType._
import org.apache.spark.monitor.Monitor
import org.apache.spark.monitor.MonitorItem.MonitorItem

abstract class SQLMonitor extends Monitor {
  override val alertType = Seq(SQL)

}

class SQLInfo(
    title: MonitorItem,
    sqlId: String,
    aeFlag: Boolean,
    appId: String,
    executionId: Long,
    submissionTime: Date,
    duration: Long)
  extends AlertMessage(title) {
  override def toCsv(): String = {
    s"${sqlId},${aeFlag},${appId},${executionId}," +
      s"${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(submissionTime)}," +
      s"${Duration(duration, TimeUnit.MILLISECONDS).toSeconds}"
  }

}
