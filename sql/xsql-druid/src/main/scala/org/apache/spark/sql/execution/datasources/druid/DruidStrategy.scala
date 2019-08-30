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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{execution, Row, Strategy}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  AttributeReference,
  AttributeSet,
  Expression => DruidExpression,
  NamedExpression
}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{RowDataSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy.selectFilters
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.{Filter, PushDownAggregateScan}

class DruidStrategy extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(
        projects,
        filters,
        l @ LogicalRelation(t: PushDownAggregateScan, _, _, _)) =>
      pruneFilterProject4Druid(
        l,
        projects,
        filters,
        (a, f) => toCatalystRDD(l, a, t.buildScan(a.map(_.name).toArray, f.toArray))) :: Nil

    case ProjectAndAggregate(ges, aes, child, orders, limit) =>
      child match {
        case PhysicalOperation(
            projects,
            filters,
            l @ LogicalRelation(t: PushDownAggregateScan, _, _, _)) =>
          t.setGroupingExpressions(ges)
          t.setAggregateExpressions(aes)
          t.setOrders(orders)
          t.setLimit(limit)
          pruneFilterProjectAndAggRaw(
            l,
            aes,
            filters,
            (a, f) => toCatalystRDD(l, a, t.buildScan(a.map(_.name).toArray, f.toArray))) :: Nil
      }
    case _ => Nil
  }

  /**
   * handle druid query result is empty because the time format is not uniform
   * Can be optimized
   */
  private def pruneFilterProject4Druid(
      relation: LogicalRelation,
      projects: Seq[NamedExpression],
      filterPredicates: Seq[DruidExpression],
      scanBuilder: (Seq[Attribute], Seq[Filter]) => RDD[InternalRow]): SparkPlan = {

    val projectSet = AttributeSet(projects.flatMap(_.references))
    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))

    val candidatePredicates = filterPredicates.map {
      _ transform {
        case a: AttributeReference =>
          relation.attributeMap(a) // Match original case of attributes.
      }
    }

    val (unhandledPredicates, pushedFilters, handledFilters) =
      selectFilters(relation.relation, candidatePredicates)

    if (pushedFilters.size == 0) {
      throw new UnsupportedOperationException(
        "Druid must have startTime and endTime" +
          " like (__time>'2015-09-01' and __time<'2015-09-03')")
    }
    val handledSet = {
      val handledPredicates = filterPredicates.filterNot(unhandledPredicates.contains)
      val unhandledSet = AttributeSet(unhandledPredicates.flatMap(_.references))
      AttributeSet(handledPredicates.flatMap(_.references)) --
        (projectSet ++ unhandledSet).map(relation.attributeMap)
    }
    // Don't request columns that are only referenced by pushed filters.
    val requestedColumns =
      (projectSet ++ filterSet -- handledSet).map(relation.attributeMap).toSeq

    val scan = RowDataSourceScanExec(
      relation.output,
      requestedColumns.map(relation.output.indexOf),
      pushedFilters.toSet,
      handledFilters,
      scanBuilder(requestedColumns, pushedFilters),
      relation.relation,
      relation.catalogTable.map(_.identifier))
    // skip filter
    execution.ProjectExec(projects, scan)
  }

  protected def pruneFilterProjectAndAggRaw(
      relation: LogicalRelation,
      projects: Seq[NamedExpression],
      filterPredicates: Seq[DruidExpression],
      scanBuilder: (Seq[Attribute], Seq[Filter]) => RDD[InternalRow]) = {

    val candidatePredicates = filterPredicates.map {
      _ transform {
        case a: AttributeReference =>
          relation.attributeMap(a) // Match original case of attributes.
      }
    }

    val (_, pushedFilters, _) =
      selectFilters(relation.relation, candidatePredicates)

    val requestedColumns = projects.map(_.toAttribute)
//    val requestedColumns = projects.map { r =>
//        if (r.children.size > 0) {
//          AttributeReference(r.name, LongType)(r.exprId, r.qualifier)
//        } else {
//          r.toAttribute
//        }
//      r.toAttribute
//    }
    execution.RDDScanExec(
      requestedColumns,
      scanBuilder(requestedColumns, pushedFilters),
      relation.relation.toString)
  }

  private[this] def toCatalystRDD(
      relation: LogicalRelation,
      output: Seq[Attribute],
      rdd: RDD[Row]): RDD[InternalRow] = {
    if (relation.relation.needConversion) {
      execution.RDDConversions.rowToRowRdd(rdd, output.map(_.dataType))
    } else {
      rdd.asInstanceOf[RDD[InternalRow]]
    }
  }
}
