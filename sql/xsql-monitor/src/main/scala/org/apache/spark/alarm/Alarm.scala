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
package org.apache.spark.alarm

import java.sql.Connection

import org.apache.spark.monitor.MonitorItem.MonitorItem

trait Alarm {
  val name: String
  var options: Map[String, String] = _
  def bind(options: Map[String, String]): Alarm = {
    this.options = options
    this
  }

  /**
   * Send the alert message to possible external SMS, EMAIL, Phone system.
   *
   * @param msg the alert message to send
   * @return a [[AlertResp]] with status and an optional message
   */
  def alarm(msg: AlertMessage): AlertResp

  def finalAlarm(msg: AlertMessage): AlertResp
}

/**
 * Message used to be sent by [[Alarm]]
 * @param title message title
 */
class AlertMessage(val title: MonitorItem) {
  def toCsv(): String = {
    throw new Exception("can not treat as csv")
  }
  def toHtml(): String = {
    ""
  }
  def toJdbc(conn: Connection, appId: String = ""): Unit = {
    // do nothing
  }
}

class HtmlMessage(title: MonitorItem, content: String) extends AlertMessage(title) {
  override def toHtml(): String = {
    content
  }
}

case class AlertResp(status: Boolean, ret: String)

object AlertResp {
  def success(ret: String): AlertResp = apply(status = true, ret)
  def failure(ret: String): AlertResp = apply(status = false, ret)
}
object AlertType extends Enumeration {
  type AlertType = Value
  val Application, Job, Stage, Task, Executor, SQL = Value
}
object JobType extends Enumeration {
  type JobType = Value
  val CORE, SQL, STREAMING, MLLIB, GRAPHX = Value
}
