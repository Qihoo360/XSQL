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

package org.apache.spark.sql.xsql.execution.datasources.redis

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, LinkedHashSet}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{expressions, TableIdentifier}
import org.apache.spark.sql.catalyst.CatalystTypeConverters.convertToScala
import org.apache.spark.sql.catalyst.analysis.{
  UnresolvedAttribute,
  UnresolvedRelation,
  UnresolvedStar
}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, UnresolvedCatalogRelation}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.xsql.DataSourceType
import org.apache.spark.sql.xsql.XSQLSessionCatalog
import org.apache.spark.sql.xsql.execution.command.ScanTableCommand
import org.apache.spark.sql.xsql.manager.RedisManager.{KEY, RANGE, SCORE, SUFFIX}

/**
 * Author: weiwenda Date: 2018-08-27 12:24
 * Description: this strategy must after FindDataSourceTable
 */
class RedisSpecialStrategy(session: SparkSession) extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // INLINE: process select * where key = 'key'
    // INLINE: this strategy must placed before ResolveScanSingleTable
    val catalog = session.sessionState.catalog.asInstanceOf[XSQLSessionCatalog]
    val catalogDB: Option[CatalogDatabase] = catalog.getCurrentCatalogDatabase
    val ds = catalog.getDataSourceType(catalogDB.get.dataSourceName)
    plan resolveOperators {
      case filter @ Filter(condition, relation: OneRowRelation)
          if (ds.equalsIgnoreCase(DataSourceType.REDIS.toString)) =>
        val filters = splitConjunctivePredicates(condition)
          .map(RedisSpecialStrategy.getAttr)
        if (filters.map(_._1).contains(KEY)) {
          val key = filters.filter(_._1.equals(KEY)).head._2
          val realKey = if (key.isInstanceOf[Seq[String]]) {
            key.asInstanceOf[Seq[String]].head
          } else {
            key.toString
          }
          val tableName = if (realKey.contains(":")) {
            realKey.substring(0, realKey.lastIndexOf(":"))
          } else {
            realKey.substring(0, realKey.length - 1)
          }
          filter.copy(
            child = UnresolvedRelation(
              TableIdentifier(tableName, Option(catalogDB.get.name), Option.empty)))
        } else {
          filter
        }
    }
  }
}
object RedisSpecialStrategy extends PredicateHelper {
  // INLINE: similiar to DataSourceStrategy.translateFilter
  def getAttr(predicate: Expression): (String, Any) = {
    predicate match {
      case expressions.EqualTo(a: Attribute, Literal(v, t)) => (a.name, convertToScala(v, t))
      case expressions.EqualTo(Literal(v, t), a: Attribute) => (a.name, convertToScala(v, t))
      case expressions.GreaterThanOrEqual(a: Attribute, Literal(v, t)) =>
        (a.name, convertToScala(v, t))
      case expressions.LessThanOrEqual(a: Attribute, Literal(v, t)) =>
        (a.name, convertToScala(v, t))
      case expressions.In(a: Attribute, set: Seq[Literal]) =>
        (a.name, set.map(_.toString))
      case _ => ("unsupported", 0)
    }
  }
  // INLINE: similiar to DataSourceStrategy.selectFilters
  def selectFilters(expressions: Seq[Expression]): (Seq[Expression], Seq[Expression]) = {
    expressions.partition(exp =>
      exp match {
        case e: EqualTo
            if (e.references.forall(attribute => Seq(KEY, SUFFIX).contains(attribute.name))) =>
          true
        case g: GreaterThanOrEqual
            if (g.references.forall(attribute => Seq(RANGE, SCORE).contains(attribute.name))) =>
          true
        case l: LessThanOrEqual
            if (l.references.forall(attribute => Seq(RANGE, SCORE).contains(attribute.name))) =>
          true
        case i @ In(attr, set: Seq[Literal])
            if (attr.references.forall(attribute => Seq(KEY, SUFFIX).contains(attribute.name))) =>
          true
        case _ => false
    })
  }
  def processSingleTableRedis(plan: LogicalPlan): LogicalPlan = {
    plan resolveOperatorsUp {
      case p @ Project(projectList, scanTableCommand: ScanTableCommand) =>
        // INLINE: projectList don't have func means can push down
        if (projectList.forall { project =>
              project match {
                case u: UnresolvedStar => true
                case a @ Alias(child: UnresolvedAttribute, name) => true
                case u: UnresolvedAttribute => true
                case _ => false
              }
            }) {
          val names: LinkedHashSet[String] = LinkedHashSet.empty
          val tableMeta = scanTableCommand.tableMeta
          val fields: ArrayBuffer[StructField] = ArrayBuffer.empty
          val dataCols = scanTableCommand.dataCols
          val partitionCols = scanTableCommand.partitionCols
          projectList.foreach { x =>
            x match {
              case u: UnresolvedStar =>
                val cols = dataCols ++ partitionCols
                cols.foreach { col =>
                  names.add(col.name)
                  fields += tableMeta.dataSchema(col.name)
                }
              case a @ Alias(attr: UnresolvedAttribute, aliasname) =>
                val realName = attr.name.split("\\.").last
                names.add(realName)
                fields += tableMeta.dataSchema(realName).copy(name = aliasname)
              case attr: UnresolvedAttribute =>
                // Table alias need remove.
                val realName = attr.name.split("\\.").last
                names.add(realName)
                fields += tableMeta.dataSchema(realName)
            }
          }
          val structType = new StructType(fields.toArray)
          val newDataCols: Seq[AttributeReference] = structType.asNullable.toAttributes
          val newPartitionCols: Seq[AttributeReference] = partitionCols
          val columns = names.toSeq.asJava
          scanTableCommand.copy(
            dataCols = newDataCols,
            partitionCols = newPartitionCols,
            columns = columns)
        } else {
          p
        }
      case filter @ Filter(condition, command: ScanTableCommand) =>
        // INLINE: only a part of filters can be push down to redis relation
        val (pushedFilters, unhandledPredicates) =
          selectFilters(splitConjunctivePredicates(condition))
        val filterCondition = unhandledPredicates.reduceLeftOption(expressions.And)
        val filters = new util.HashMap[String, Any](
          pushedFilters
            .map(getAttr)
            .groupBy(_._1)
            .map(tup => (tup._1, tup._2.map(_._2)))
            .toMap
            .asJava)
        val newCommand = command.copy(condition = filters)
        if (filterCondition.isEmpty) {
          newCommand
        } else {
          filter.copy(condition = filterCondition.get, child = newCommand)
        }
      case subqueryAlias @ SubqueryAlias(alias, relation: UnresolvedCatalogRelation) =>
        ScanTableCommand(
          relation.tableMeta,
          relation.tableMeta.dataSchema.asNullable.toAttributes,
          relation.tableMeta.partitionSchema.asNullable.toAttributes,
          columns = relation.tableMeta.dataSchema.map(_.name).asJava)
    }
  }

}
