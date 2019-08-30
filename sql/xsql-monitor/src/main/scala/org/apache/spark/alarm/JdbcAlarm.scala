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

import java.sql.{Connection, DriverManager}

class JdbcAlarm extends Alarm {
  override val name: String = "mysql"

  private val conn: Connection = getConnect

  private def getConnect(): Connection = {
    org.apache.spark.util.Utils.classForName("com.mysql.jdbc.Driver")
    DriverManager.getConnection(
      "jdbc:mysql://localhost:3306/xsql_monitor?useSSL=true",
      "xsql_monitor",
      "xsql_monitor")
  }

  /**
   * Send the alert message to possible external SMS, EMAIL, Phone system.
   *
   * @param msg the alert message to send
   * @return a [[AlertResp]] with status and an optional message
   */
  override def alarm(msg: AlertMessage): AlertResp = {
    msg.toJdbc(conn)
    AlertResp.success("")
  }

  override def finalAlarm(msg: AlertMessage): AlertResp = {
    msg.toJdbc(conn)
    AlertResp.success("")
  }
}
