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

package org.apache.spark.sql.execution.datasources.druid

import org.json4s.JsonAST._
import org.json4s.JsonDSL._

case class ColumnOrder(
    columnName: String,
    direction: String,
    dimensionOrder: String = "lexicographic")
  extends Expression {
  def toJson: JValue =
    JObject(
      "dimension" -> columnName,
      "direction" -> direction,
      "dimensionOrder" -> dimensionOrder)
}
case class OrderBy(cols: Seq[ColumnOrder], limit: Option[Int] = None) extends Expression {
  def toJson: JValue =
    if (cols.size > 0) {
      JObject(
        "type" -> "default",
        "columns" -> cols.map(_.toJson),
        "limit" -> limit.map(i => JInt(BigInt(i))).getOrElse(JNull))
    } else {
      JObject("type" -> "default", "limit" -> limit.map(i => JInt(BigInt(i))).getOrElse(JNull))
    }
}
