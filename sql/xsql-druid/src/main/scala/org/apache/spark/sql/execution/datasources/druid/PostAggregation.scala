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

import org.json4s.JsonAST.{JObject, JValue}
import org.json4s.JsonDSL._

trait PostAggregationFieldSpec extends Expression {
  private def arith(rhs: PostAggregationFieldSpec, fn: String): PostAggregation =
    ArithmeticPostAggregation("n/a", fn, Seq(this, rhs))

  def *(rhs: PostAggregationFieldSpec): PostAggregation = arith(rhs, "*")

  def /(rhs: PostAggregationFieldSpec): PostAggregation = arith(rhs, "/")

  def +(rhs: PostAggregationFieldSpec): PostAggregation = arith(rhs, "+")

  def -(rhs: PostAggregationFieldSpec): PostAggregation = arith(rhs, "-")
}

trait PostAggregation extends PostAggregationFieldSpec {
  def as(outputName: String): PostAggregation
}

object PostAggregation {
  def constant(value: Double): ConstantPostAggregation =
    ConstantPostAggregation("constant", value)

  case class FieldAccess(fieldName: String) extends PostAggregationFieldSpec {
    def toJson: JValue = JObject("type" -> "fieldAccess", "fieldName" -> fieldName)
  }

}

case class ConstantPostAggregation(outputName: String, value: Double) extends PostAggregation {
  def toJson: JValue = JObject("type" -> "constant", "name" -> outputName, "value" -> value)

  def as(outputName: String): PostAggregation = copy(outputName = outputName)
}

case class ArithmeticPostAggregation(
    outputName: String,
    fn: String,
    fields: Seq[PostAggregationFieldSpec])
  extends PostAggregation {
  def toJson: JValue =
    JObject(
      "type" -> "arithmetic",
      "name" -> outputName,
      "fn" -> fn,
      "fields" -> fields.map(_.toJson))

  def as(outputName: String): ArithmeticPostAggregation = copy(outputName = outputName)
}
