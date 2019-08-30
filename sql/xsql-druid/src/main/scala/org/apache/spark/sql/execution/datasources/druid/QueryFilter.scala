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

import org.json4s.JsonAST.{JArray, JNull, JObject, JValue}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

sealed trait QueryFilter extends Expression {
  def and(other: QueryFilter): QueryFilter = And(Seq(this, other))

  def or(other: QueryFilter): QueryFilter = Or(Seq(this, other))
}

case class And(filters: Seq[Expression]) extends QueryFilter {

  override def and(other: QueryFilter): QueryFilter = copy(other +: filters)

  def toJson: JValue = JObject("type" -> "and", "fields" -> JArray(filters.toList.map(_.toJson)))
}

case class Or(filters: Seq[Expression]) extends QueryFilter {

  override def or(other: QueryFilter): QueryFilter = copy(other +: filters)

  def toJson: JValue = JObject("type" -> "or", "fields" -> JArray(filters.toList.map(_.toJson)))
}

case class Not(filter: Expression) extends QueryFilter {

  def toJson: JValue = JObject("type" -> "not", "field" -> filter.toJson)
}

case class IsNotNull(attributeNotNull: String) extends QueryFilter {

//  {
//    "field": {
//      "type": "selector",
//      "dimension": "added",
//      "value": ""
//    },
//    "type": "not"
//  }
  def toJson: JValue =
    JObject(
      "field" -> JObject("type" -> "selector", "dimension" -> attributeNotNull, "value" -> ""),
      "type" -> "not")
}

case class ExprQueryFilter(typeName: String, dimension: String, value: String)
  extends QueryFilter {
  def toJson: JValue = JObject("type" -> typeName, "dimension" -> dimension, "value" -> value)
}

case class SelectorQueryFilter(dimension: String, value: String) extends QueryFilter {
  def toJson: JValue = JObject("type" -> "selector", "dimension" -> dimension, "value" -> value)
}

case class RegexQueryFilter(dimension: String, pattern: String) extends QueryFilter {
  def toJson: JValue = JObject("type" -> "regex", "dimension" -> dimension, "pattern" -> pattern)
}

case class AllQueryFilter(condition: java.util.HashMap[String, Any]) extends QueryFilter {
  //  val json = JSONObject.fromObject(condition.get("filter")).toString
  //  def toJson: JValue = parse(json)
  def toJson: JValue = fromJsonNode(mapper.valueToTree(condition.get("filter")))
}

object QueryFilter {

  def custom(typeName: String, dimension: String, value: String): ExprQueryFilter =
    ExprQueryFilter(typeName, dimension, value)

  def where(dimension: String, value: String): SelectorQueryFilter =
    SelectorQueryFilter(dimension, value)

  def regex(dimension: String, pattern: String): RegexQueryFilter =
    RegexQueryFilter(dimension, pattern)

  val All = new QueryFilter {
    def toJson: JValue = JNull
  }
}
