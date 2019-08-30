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

case class Aggregation(typeName: String, fieldName: String, outputName: String)
  extends Expression {
  def toJson: JObject =
    JObject("type" -> typeName, "name" -> outputName, "fieldName" -> fieldName)

  def as(outputName: String): Aggregation = copy(outputName = outputName)
}

case class MultiFieldAggregation(typeName: String, fieldNames: Seq[String], outputName: String)
  extends Expression {
  def toJson: JObject =
    JObject("type" -> typeName, "name" -> outputName, "fieldNames" -> fieldNames)

  def as(outputName: String): MultiFieldAggregation = copy(outputName = outputName)
}

class CardinalityAggregation(fields: Seq[String], override val outputName: String)
  extends Aggregation("cardinality", "", outputName) {
  override def toJson: JObject =
    JObject("type" -> typeName, "name" -> outputName, "fields" -> fields, "byRow" -> true)
}
