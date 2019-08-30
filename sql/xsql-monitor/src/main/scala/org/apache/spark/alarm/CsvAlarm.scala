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

import java.io.FileWriter

class CsvAlarm extends Alarm {
  import CsvAlarm._
  override val name: String = "csv"
  private lazy val path = options.getOrElse(PATH, "test.csv")
  lazy val fw = new FileWriter(path, true)

  /**
   * Send the alert message to possible external SMS, EMAIL, Phone system.
   *
   * @param msg the alert message to send
   * @return a [[AlertResp]] with status and an optional message
   */
  override def alarm(msg: AlertMessage): AlertResp = {
    fw.write(msg.toCsv() + "\n")
    AlertResp.success("")
  }

  override def finalAlarm(msg: AlertMessage): AlertResp = {
    val content = msg.toCsv()
    if (!content.isEmpty) {
      fw.write(content + "\n")
    }
    fw.flush()
    fw.close()
    AlertResp.success("")
  }
}

object CsvAlarm {
  val PATH = "path"
}
