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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.CatalystTypeConverters.convertToScala
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  Expression => SExpression,
  Literal,
  NamedExpression,
  SortOrder
}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

/**
 *
 */
object DruidRule extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case Aggregate(ges, aes, p @ Project(_, _)) =>
      ProjectAndAggregate(ges, aes, p)

    case s @ Sort(orders, _, child) =>
      if (child.isInstanceOf[ProjectAndAggregate]) {
        child.asInstanceOf[ProjectAndAggregate].copy(orders = orders)
      } else {
        s
      }

    case l @ LocalLimit(Literal(v, t), child) =>
      val value: Any = convertToScala(v, t)
      val limit = value.asInstanceOf[Int]
      if (limit < 0) {
        throw new SparkException(s"Aggregate limit must great than zero!")
      }
      if (child.isInstanceOf[ProjectAndAggregate]) {
        child.asInstanceOf[ProjectAndAggregate].copy(limit = limit)
      } else {
        l
      }

    case g @ GlobalLimit(_, child) =>
      if (child.isInstanceOf[ProjectAndAggregate]) {
        child
      } else {
        g
      }
  }
}
case class ProjectAndAggregate(
    groupingExpressions: Seq[SExpression],
    aggregateExpressions: Seq[NamedExpression],
    child: LogicalPlan,
    orders: Seq[SortOrder] = null,
    limit: Int = 20)
  extends UnaryNode {
  override def output: Seq[Attribute] = aggregateExpressions.map(_.toAttribute)
}
