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

object DSL {

  import PostAggregation._
  import QueryFilter._

  case class FilterOps(dimension: String) {
    def ===(value: String): SelectorQueryFilter = where(dimension, value)

    def =*=(pattern: String): RegexQueryFilter = regex(dimension, pattern)
  }

  implicit def string2FilterOps(s: String): FilterOps = FilterOps(s)

  case class PostAggStringOps(lhs: String) {
    def /(rhs: String): ArithmeticPostAggregation =
      ArithmeticPostAggregation(
        "%s_by_%s".format(lhs, rhs),
        "/",
        Seq(FieldAccess(lhs), FieldAccess(rhs)))

    def *(rhs: String): ArithmeticPostAggregation =
      ArithmeticPostAggregation(
        "%s_times_%s".format(lhs, rhs),
        "*",
        Seq(FieldAccess(lhs), FieldAccess(rhs)))

    def -(rhs: String): ArithmeticPostAggregation =
      ArithmeticPostAggregation(
        "%s_minus_%s".format(lhs, rhs),
        "-",
        Seq(FieldAccess(lhs), FieldAccess(rhs)))

    def +(rhs: String): ArithmeticPostAggregation =
      ArithmeticPostAggregation(
        "%s_plus_%s".format(lhs, rhs),
        "-",
        Seq(FieldAccess(lhs), FieldAccess(rhs)))
  }

  implicit def string2PostAggOps(s: String): PostAggStringOps = PostAggStringOps(s)

  implicit def string2PostAgg(s: String): PostAggregationFieldSpec =
    ArithmeticPostAggregation("no_name", "*", Seq(FieldAccess(s), constant(1)))

  implicit def numericToConstant[T](n: T)(implicit num: Numeric[T]): ConstantPostAggregation =
    constant(num.toDouble(n))

  case class OrderByStringOps(col: String) {
    def asc: ColumnOrder = ColumnOrder(col, "ASCENDING")

    def desc: ColumnOrder = ColumnOrder(col, "DESCENDING")
  }

  implicit def string2OrderByOps(s: String): OrderByStringOps = OrderByStringOps(s)

  def sum(fieldName: String, alias: String = ""): Aggregation =
    Aggregation("longSum", fieldName, if (alias.length > 0) alias else fieldName + "_sum")

  def doubleSum(fieldName: String, alias: String = ""): Aggregation =
    Aggregation("doubleSum", fieldName, if (alias.length > 0) alias else fieldName + "_sum")

  def min(fieldName: String, alias: String = ""): Aggregation =
    Aggregation("longMin", fieldName, if (alias.length > 0) alias else fieldName + "_min")

  def doubleMin(fieldName: String, alias: String = ""): Aggregation =
    Aggregation("doubleMin", fieldName, if (alias.length > 0) alias else fieldName + "_min")

  def max(fieldName: String, alias: String = ""): Aggregation =
    Aggregation("longMax", fieldName, if (alias.length > 0) alias else fieldName + "_max")

  def doubleMax(fieldName: String, alias: String = ""): Aggregation =
    Aggregation("doubleMax", fieldName, if (alias.length > 0) alias else fieldName + "_max")

  def count(alias: String = "row_count"): Aggregation = Aggregation("count", "na", alias)

  def countMultDistinct(fields: Seq[String], alias: String = "count_distinct"): Aggregation =
    new CardinalityAggregation(fields, alias)

  def countdistinct(
      fieldName: String,
      disType: String,
      alias: String = "count_distinct"): Aggregation = Aggregation(disType, fieldName, alias)

}
