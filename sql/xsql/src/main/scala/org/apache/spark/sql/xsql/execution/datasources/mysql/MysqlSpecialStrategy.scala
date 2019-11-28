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

package org.apache.spark.sql.xsql.execution.datasources.mysql

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, _}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.execution.datasources.{CreateTable, LogicalRelation}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation
import org.apache.spark.sql.types._
import org.apache.spark.sql.xsql.DataSourceManager.MAX_LIMIT
import org.apache.spark.sql.xsql.XSQLSessionCatalog
import org.apache.spark.sql.xsql.execution.command.PushDownQueryCommand
import org.apache.spark.sql.xsql.manager.MysqlManager._

class TransmitOriginalQuery(session: SparkSession) extends Rule[LogicalPlan] {

  val catalog = session.sessionState.catalog.asInstanceOf[XSQLSessionCatalog]
  private def alreadyResolved(plan: LogicalPlan): Boolean = {
    plan match {
      case CreateTable(_, _, Some(query)) =>
        query.resolved
      case InsertIntoTable(_, _, query, _, _) =>
        query.resolved
      case ExplainCommand(plan, _, _, _) =>
        plan.resolved
      case p =>
        p.resolved
    }
  }
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (alreadyResolved(plan) && !plan.isInstanceOf[SubqueryAlias]) {
      val leafs = plan.collectLeaves()
      // Collect all names and isPush of JDBC datasource
      val dsName2pushdown = leafs.map { leaf =>
        leaf match {
          case LogicalRelation(_: JDBCRelation, _, catalogTable: Some[CatalogTable], _) =>
            catalogTable.get.identifier.dataSource.get ->
              catalog.isPushDown(catalogTable.get.identifier)
          case _ => null
        }
      }.distinct
      // If the leaves of the tree are the same JDBC datasource
      val wholePushdown = dsName2pushdown.size == 1 &&
        dsName2pushdown.head != null &&
        dsName2pushdown.head._2 == true &&
        allSubqueryAreSameDS(dsName2pushdown.head._1, plan)
      val partPushdown = dsName2pushdown.exists(_ != null) ||
        (leafs.size > 1 && plan.find(_.subqueries.nonEmpty).nonEmpty)
      if (wholePushdown) {
        transform(plan, leafs)
      } else if (partPushdown) {
        transformPartPlan(plan, dsName2pushdown.filter(_ != null).toMap)
      } else {
        plan
      }
    } else {
      plan
    }
  }

  /**
   * Pushdown part of Tree that the relation is JDBCRelation
   */
  private def transformPartPlan(
      plan: LogicalPlan,
      dataSourceToIsPush: Map[String, Boolean]): LogicalPlan = plan resolveOperatorsUp {
    case join @ Join(left, right, _, _) =>
      @inline def transfromJoinChild(joinChild: LogicalPlan): LogicalPlan = {
        joinChild match {
          case s @ SubqueryAlias(_, child)
              if child.isInstanceOf[LogicalRelation] ||
                child.isInstanceOf[SubqueryAlias] =>
            s
          case SubqueryAlias(alias, child) =>
            val childLeaf = child.collectLeaves()
            val newChild = childLeaf.head match {
              case LogicalRelation(_: JDBCRelation, _, catalogTable, _)
                  if allSubqueryAreSameDS(catalogTable.get.identifier.dataSource.get, child) &&
                    dataSourceToIsPush(catalogTable.get.identifier.dataSource.get) =>
                transform(child, childLeaf)
              case _ => child
            }
            SubqueryAlias(alias, newChild)
          case other => other
        }
      }
      val leftTransformed = transfromJoinChild(left)
      val rightTransformed = transfromJoinChild(right)
      join.copy(left = leftTransformed, right = rightTransformed)
    case union @ Union(children) =>
      val newChildren = children.map { child =>
        child match {
          case p: Project if p.collectLeaves().exists(_.isInstanceOf[PushDownQueryCommand]) =>
            p
          case other =>
            val leafs = other.collectLeaves()
            leafs.head match {
              case LogicalRelation(_: JDBCRelation, _, catalogTable, _)
                  if allSubqueryAreSameDS(catalogTable.get.identifier.dataSource.get, other) &&
                    dataSourceToIsPush(catalogTable.get.identifier.dataSource.get) =>
                transform(other, leafs)
              case _ => other
            }
        }
      }
      union.copy(children = newChildren)
  }

  /**
   * Determine the datasource of subquery is same as the whole plan and their provider are JDBC
   */
  def allSubqueryAreSameDS(dataSource: String, plan: LogicalPlan): Boolean = {
    val datasources = new HashSet[String]
    @inline def allDSOfSubquery(plan: LogicalPlan) {
      plan foreachUp {
        case currentNode =>
          currentNode transformExpressions {
            case s: SubqueryExpression =>
              if (s.plan.find(_.subqueries.nonEmpty).nonEmpty) {
                s.plan.collectLeaves.foreach {
                  case PushDownQueryCommand(tableMeta, _, _, _, _, _, _) =>
                    datasources.add(tableMeta.identifier.dataSource.get)
                  case LogicalRelation(_: JDBCRelation, _, catalogTable, _) =>
                    datasources.add(catalogTable.get.identifier.dataSource.get)
                  case _ => datasources.add("")
                }
                allDSOfSubquery(s.plan)
              } else {
                s.plan.collectLeaves.foreach {
                  case PushDownQueryCommand(tableMeta, _, _, _, _, _, _) =>
                    datasources.add(tableMeta.identifier.dataSource.get)
                  case LogicalRelation(_: JDBCRelation, _, catalogTable, _) =>
                    datasources.add(catalogTable.get.identifier.dataSource.get)
                  case _ => datasources.add("")
                }
              }
              s
          }
      }
    }
    allDSOfSubquery(plan)
    datasources.forall(_.equals(dataSource))
  }

  /**
   * Pushdown the plan if satifies these following condition
   */
  private def transform(
      plan: LogicalPlan,
      leaf: Seq[LogicalPlan],
      innerSubquery: Boolean = false): LogicalPlan = {
    val push = if (innerSubquery) {
      // if the plan is a subquery that belongs to PushDownQueryCommand, then just PushDown
      true
    } else {
      // Before pushdown,consider the following conditions:
      // 1.Consider outerReference， If exists don't pushDown this plan
      // 2.Consider the number of rows and index of the table for the plan
      // 3.Consider the number of row and index of the table for the subqueries
      // that they aren't tansformed PushDownQueryCommand
      // 4.Prevent ResolveAggregateFunctions.executeSameContext() enter this function
      // 5.If found any spark-provided function, don't pushDown this path
      !SubExprUtils.hasOuterReferences(plan) &&
      considerFunctionCanPushdownOrNot(plan) &&
      considerTableRowsAndIndexToPushdown(plan, leaf) &&
      plan.find { s =>
        s.subqueries.exists { sub =>
          val leafs = sub.collectLeaves()
          if (leafs.exists(_.isInstanceOf[PushDownQueryCommand])) {
            false
          } else {
            !considerTableRowsAndIndexToPushdown(sub, leafs)
          }
        }
      }.isEmpty &&
      plan.find {
        case Aggregate(_, agg, _)
            if !agg.exists(expr =>
              !expr.name.equals("aggOrder") &&
                !expr.name.equals("havingCondition")) =>
          true
        case _ =>
          false
      }.isEmpty
    }
    if (push) {
      plan resolveOperatorsUp process
    } else {
      plan
    }
  }

  private def process: PartialFunction[LogicalPlan, LogicalPlan] = {
    case SubqueryAlias(_, LogicalRelation(_: JDBCRelation, output, catalogtable, _)) =>
      val tableIdentifier = catalogtable.get.identifier
      PushDownQueryCommand(
        catalogtable.get,
        output,
        false,
        false,
        false,
        false,
        s"${tableIdentifier.database.get}.${tableIdentifier.table}")
    case SubqueryAlias(alias, child: PushDownQueryCommand) =>
      if (child.queryContent.contains("select ")) {
        // Deal with the alias of subquery
        child.copy(
          havingFiltered = false,
          sorted = false,
          queryContent = s"(${child.queryContent}) as ${alias.identifier}")
      } else {
        child.copy(queryContent = s"${child.queryContent} as ${alias.identifier}")
      }
    case Join(left: PushDownQueryCommand, right: PushDownQueryCommand, joinType, condition) =>
      if (joinType.sql.equalsIgnoreCase("FULL OUTER")) {
        throw new SparkException(
          "mysql don't support full outer join," +
            s" please set spark.xsql.datasource." +
            s"${left.tableMeta.identifier.dataSource.get}.pushdown = false")
      }
      val conditionStr = if (condition.isEmpty) {
        ""
      } else {
        "on " +
          substituteExpression(condition.get).sql
      }
      val newQuery = s"${left.queryContent} ${joinType.sql} " +
        s"join ${right.queryContent} ${conditionStr}"
      left.copy(
        dataCols = left.tableMeta.dataSchema.toAttributes ++
          right.tableMeta.dataSchema.toAttributes,
        queryContent = newQuery)
    case Filter(condition, child @ PushDownQueryCommand(_, _, _, _, _, _, oldQuery)) =>
      if (oldQuery.endsWith("`")) {
        // Deal with having filter
        val pattern = "cast\\(([^#]*)#\\d+ as \\w+\\)".r
        val condi = pattern.replaceAllIn(substituteExpression(condition).sql, "$1")
        child.copy(havingFiltered = true, queryContent = s"${oldQuery} having ${condi}")
      } else {
        // Deal with where filter
        child.copy(queryContent = s"${oldQuery} where ${substituteExpression(condition).sql}")
      }
    case a: Aggregate if a.child.analyzed =>
      a transformUp process
    case Aggregate(group, agg, child: PushDownQueryCommand) =>
      val newDataCols: ArrayBuffer[AttributeReference] = ArrayBuffer.empty
      agg.foreach { x =>
        x match {
          case Alias(ScalarSubquery(plan: PushDownQueryCommand, _, _), _) =>
            newDataCols ++= plan.dataCols
          case alias: Alias =>
            newDataCols.append(alias.toAttribute.asInstanceOf[AttributeReference])
          case attr: AttributeReference =>
            if (group.isEmpty) {
              throw new SparkException("cann't mix full group column with single column")
            } else {
              if (!group.contains(attr)) {
                throw new SparkException("cann't select single column not in group by domain")
              } else {
                newDataCols += attr
              }
            }
        }
      }
      val newGroup = group.map(expr =>
        expr match {
          case Alias(child, name) =>
            AttributeReference(name, child.dataType, child.nullable)()
          case a => a
      })
      val groupString =
        if (group.isEmpty) ""
        else {
          s"group by ${newGroup.map(_.sql).mkString(",")}"
        }

      child.copy(
        dataCols = newDataCols,
        limit = false,
        queryContent = s"select ${mkQuery(cleanUp(agg))} " +
          s"from ${child.queryContent} ${groupString}")
    case Project(
        projectList,
        child @ PushDownQueryCommand(_, _, false, false, false, _, oldQuery)) =>
      if (!oldQuery.startsWith("select")) {
        // Prevent a whole query enter this function
        val newDataCols: ArrayBuffer[AttributeReference] = ArrayBuffer.empty
        projectList.foreach { x =>
          x match {
            // Deal with ScalarSubquery
            case Alias(ScalarSubquery(plan, _, _), _) =>
              val newPlan = plan match {
                case p: PushDownQueryCommand => p
                case _ =>
                  transform(plan, plan.collectLeaves(), true)
                    .asInstanceOf[PushDownQueryCommand]
              }
              newDataCols ++= newPlan.dataCols
            case alias: Alias =>
              newDataCols.append(alias.toAttribute.asInstanceOf[AttributeReference])
            case attr: AttributeReference =>
              newDataCols += attr
          }
        }
        child.copy(
          dataCols = newDataCols,
          limit = false,
          queryContent = s"select ${mkQuery(projectList)} from ${oldQuery}")
      } else {
        child
      }
    // 1.aggregate expressions that are not in an aggregate operator
    // 2.For some union query, there is a Project node on the top of Union node
    case project @ Project(
          projectList,
          child @ PushDownQueryCommand(_, _, havingFiltered, sorted, union, _, oldQuery))
        if sorted || havingFiltered || union =>
      if (projectList.exists(!_.isInstanceOf[AttributeReference])) {
        project
      } else {
        val project = projectList.map(_.asInstanceOf[AttributeReference])
        // Every derived table must have its own alias
        val tableName = if (project.head.qualifier.isEmpty) {
          "Alias"
        } else {
          project.head.qualifier.last
        }
        val pattern = "CAST\\(([^\\s]*) AS \\w+\\)".r
        val newProject = pattern.replaceAllIn(mkQuery(projectList), "$1")
        val newQuery = s"select ${newProject} from (${oldQuery}) as ${tableName}"
        child.copy(
          dataCols = projectList.map(_.asInstanceOf[AttributeReference]),
          queryContent = newQuery)
      }
    case Sort(orders, _, child @ PushDownQueryCommand(_, _, _, _, _, _, oldQuery)) =>
      if (oldQuery.startsWith("(")) {
        // Deal with the case that the plan has order after union
        // For example (SQL):
        // {{{
        //  (SELECT  id
        //  FROM    a)
        //  UNION
        //  (SELECT  id
        //  FROM b)
        //  Order by id
        // }}}
        child.copy(
          sorted = true,
          queryContent = s"${oldQuery} " +
            s"order by ${orders
              .map(order => s"${toPrettySQL(order.child)} ${order.direction.sql}")
              .mkString(",")}")
      } else {
        child.copy(
          sorted = true,
          queryContent = s"${oldQuery} " +
            s"order by ${orders
              .map(order => s"${substituteExpression(order.child).sql} ${order.direction.sql}")
              .mkString(",")}")
      }
    case GlobalLimit(_, LocalLimit(expr2, child: PushDownQueryCommand)) =>
      val limitNum = Integer.parseInt(toPrettySQL(expr2))
      if (limitNum < 0) {
        throw new SparkException(s"Aggregate limit must great than zero!")
      }
      if (limitNum > MAX_LIMIT) {
        throw new SparkException(
          s"The value that XSQL Pushdown " +
            s"returned exceeds the $MAX_LIMIT limit")
      }
      child.copy(limit = true, queryContent = s"${child.queryContent} limit $limitNum")
    case Distinct(child @ PushDownQueryCommand(_, _, _, _, _, _, oldQuery)) =>
      var newQuery = ""
      if (oldQuery.startsWith("(")) {
        // Deal with the case when the plan contains union
        // For example (SQL):
        // {{{
        //  (SELECT  id
        //  FROM    a)
        //  UNION
        //  (SELECT  id
        //  FROM b)
        // }}}
        newQuery = s"${oldQuery.substring(0, oldQuery.lastIndexOf("union all"))}" +
          s" union ${oldQuery.substring(oldQuery.lastIndexOf("union all") + 9)}"
      } else {
        // Deal with the case when the plan contains distinct
        // For example (SQL):
        // {{{
        //  SELECT DISTINCT id
        //  FROM  a
        // }}}
        newQuery = "select distinct" + oldQuery.substring(6)
      }
      child.copy(queryContent = newQuery)
    case Union(children: Seq[LogicalPlan])
        if children.forall(_.isInstanceOf[PushDownQueryCommand]) =>
      val sb = new StringBuilder()
      children.foreach(child => {
        val newChild = child.asInstanceOf[PushDownQueryCommand]
        if (newChild.queryContent.startsWith("(")) {
          sb.append(s" union all ${newChild.queryContent}")
        } else {
          sb.append(s" union all (${newChild.queryContent})")
        }
      })
      PushDownQueryCommand(
        children.head.asInstanceOf[PushDownQueryCommand].tableMeta,
        children.head.asInstanceOf[PushDownQueryCommand].dataCols,
        false,
        false,
        true,
        false,
        sb.substring(11))
  }

  /**
   * If there are some spark-provided functions, don't pushdown
   */
  private def considerFunctionCanPushdownOrNot(plan: LogicalPlan): Boolean = {
    val pushdownFunctions = Seq(
      "avg",
      "count",
      "min",
      "max",
      "sum",
      "add",
      "",
      "year",
      "month",
      "week",
      "dayofmonth",
      "dayofweek",
      "dayofyear",
      "current_date",
      "current_timestamp",
      "ceil",
      "floor",
      "round",
      "abs",
      "ltrim",
      "rtrim",
      "reverse",
      "length",
      "lower",
      "replace",
      "substring",
      "trim",
      "upper",
      "rand",
      "cast",
      "like")

    @inline def functionNotPushdown(plan: LogicalPlan): Boolean = {
      plan.expressions.exists {
        case a: Alias =>
          if (a.child.isInstanceOf[AttributeReference] || a.child.isInstanceOf[ScalarSubquery]
            || a.child.isInstanceOf[BinaryArithmetic]) {
            false
          } else {
            val func = if (a.child.isInstanceOf[AggregateExpression]) {
              a.child.asInstanceOf[AggregateExpression].aggregateFunction
            } else {
              a.child
            }
            !pushdownFunctions.exists(_.equals(func.prettyName))
          }
        case _ => false
      }
    }
    plan.find(functionNotPushdown).isEmpty
  }

  /**
   * Consider the number of rows and index of the table to determine the pushdown
   */
  private def considerTableRowsAndIndexToPushdown(
      plan: LogicalPlan,
      leafs: Seq[LogicalPlan]): Boolean = {
    val tableIdentifierToInfo = new HashMap[String, HashMap[String, Any]]
    val tableIdentifierToJoinColumn = new HashMap[String, Seq[String]]
    // Get table's info and put in a map
    leafs.foreach { leaf =>
      leaf match {
        case LogicalRelation(_: JDBCRelation, _, catalogTable, _) =>
          val identifier = catalogTable.get.identifier
          val ds = identifier.dataSource.get
          val db = identifier.database.get
          val tbName = identifier.table
          val considerTableRows = catalog.isConsiderTablesRowsToPushdown(identifier)
          val considerTableIndex = catalog.isConsiderTablesIndexToPushdown(identifier)
          val (tableRows, tableIndex) = catalog.getTableRowsAndIndex(identifier)
          val tableInfo = new HashMap[String, Any]
          tableInfo.put(CONSIDER_TABLE_ROWS_TO_PUSHDOWN, considerTableRows)
          tableInfo.put(CONSIDER_TABLE_INDEX_TO_PUSHDOWN, considerTableIndex)
          tableInfo.put(TABLEROWS, tableRows)
          tableInfo.put(TABLEINDEX, tableIndex)
          tableIdentifierToInfo += s"${ds}_${db}_${tbName}" -> tableInfo
        case _ =>
      }
    }
    // Get tableIdentifier and join field
    plan foreach {
      case Join(left, right, _, condition) =>
        if (condition.nonEmpty) {
          val aliasToTableIdentifier = new HashMap[String, String]
          val aliasToColumnName = new HashMap[String, Seq[String]]
          @inline def getAliasToIdentifierOfJoinChild(plan: LogicalPlan): Unit = {
            plan match {
              case SubqueryAlias(alias, child) =>
                child.collectLeaves().head match {
                  case LogicalRelation(_: JDBCRelation, _, catalogTable, _) =>
                    val identifier = catalogTable.get.identifier
                    val ds = identifier.dataSource.get
                    val db = identifier.database.get
                    val tbName = identifier.table
                    aliasToTableIdentifier += alias.identifier -> s"${ds}_${db}_${tbName}"
                }
            }
          }
          // Save alias and tableIdentifier for the join child
          getAliasToIdentifierOfJoinChild(left)
          getAliasToIdentifierOfJoinChild(right)
          // Collect alias and column_name on the conditon of join
          condition.get foreachUp {
            case attr: AttributeReference =>
              val oldColumnSeq = aliasToColumnName.getOrElse(attr.qualifier.last, Seq.empty)
              aliasToColumnName += attr.qualifier.last -> (oldColumnSeq ++ Seq(attr.name))
            case _ =>
          }
          aliasToColumnName.keySet.foreach { alias =>
            tableIdentifierToJoinColumn += aliasToTableIdentifier(alias) -> aliasToColumnName(
              alias)
          }
        }
      case _ =>
    }
    // Determine the pushdown based on the number of Rows and the index for the table
    if (leafs.size == 1) {
      // There is only one table
      val tableInfo = tableIdentifierToInfo.head._2
      tableInfo(CONSIDER_TABLE_ROWS_TO_PUSHDOWN).asInstanceOf[Boolean] match {
        case true =>
          // Whether tableRows of the table is less than 10000000, if it is satisfied then pushdown
          tableInfo(TABLEROWS).asInstanceOf[Int] < DEFAULT_PUSHDOWN_SINGLE_TABLE_ROWS
        case false =>
          // Don't consider tableRows, just pushdown
          true
      }
    } else {
      // There are more than one table
      val considerTableRows =
        tableIdentifierToInfo.head._2(CONSIDER_TABLE_ROWS_TO_PUSHDOWN).asInstanceOf[Boolean]
      val considerTableIndex =
        tableIdentifierToInfo.head._2(CONSIDER_TABLE_INDEX_TO_PUSHDOWN).asInstanceOf[Boolean]
      (considerTableIndex, considerTableRows) match {
        case (true, a) =>
          // Determine if the join condition contains an index field
          val containIndex = tableIdentifierToInfo.keySet.exists { table =>
            val tableIndex = tableIdentifierToInfo(table).get(TABLEINDEX) match {
              case Some(_) =>
                tableIdentifierToInfo(table).get(TABLEINDEX).get.asInstanceOf[Seq[String]]
              case None => Seq.empty[String]
            }
            val joinColumn = tableIdentifierToJoinColumn.get(table) match {
              case Some(_) => tableIdentifierToJoinColumn(table)
              case None => Seq.empty[String]
            }
            tableIndex.intersect(joinColumn).nonEmpty
          }
          (containIndex, a) match {
            case (true, _) => true
            case (false, true) =>
              tableIdentifierToInfo.keySet.exists { table =>
                tableIdentifierToInfo(table).get(TABLEROWS).get.asInstanceOf[Int] <
                  DEFAULT_PUSHDOWN_MULTI_TABLE_ROWS
              }
            case (false, false) => true
          }
        case (false, true) =>
          tableIdentifierToInfo.keySet.exists { table =>
            tableIdentifierToInfo(table).get(TABLEROWS).get.asInstanceOf[Int] <
              DEFAULT_PUSHDOWN_MULTI_TABLE_ROWS
          }
        case (false, false) => true
      }
    }
  }

  /**
   * Deal with scalarsubquery in Project and Aggregate
   */
  private def mkQuery(expr: Seq[Expression]): String = {
    expr
      .map { x =>
        x match {
          case alias @ Alias(ScalarSubquery(plan, _, _), name) =>
            val newPlan = plan match {
              case p: PushDownQueryCommand => p
              case _ =>
                transform(plan, plan.collectLeaves(), true).asInstanceOf[PushDownQueryCommand]
            }
            if (name.startsWith("scalarsubquery(")) {
              s"(${newPlan.queryContent}) as scalarsubquery${alias.exprId.id}"
            } else {
              s"(${newPlan.queryContent}) as ${name}"
            }
          case attr @ AttributeReference(name, _, _, _) if name.startsWith("scalarsubquery(") =>
            s"scalarsubquery${attr.exprId.id}"
          case a => a.sql
        }
      }
      .distinct
      .mkString(",")
  }

  private def cleanUp(exprs: Seq[NamedExpression]): Seq[NamedExpression] = {
    exprs.map(substituteExpression(_).asInstanceOf[NamedExpression])
  }

  /**
   * Replace some expressions
   */
  private def substituteExpression(expr: Expression): Expression = {
    expr transformUp {
      // Deal with cast
      case Cast(child, _, _) =>
        child
      case Alias(child, name) if name.toLowerCase.contains("cast(") =>
        Alias(child, toPrettySQL(child))()
      // Deal with the condition of the filter as though it is in the aggregate clause
      case Alias(child, name) if name.equals("havingCondition") =>
        child
      // Deal with the ordering as though it is in the aggregate clause.
      case alias @ Alias(expre, name) if name.equals("aggOrder") =>
        Alias(expre, s"${name}${alias.exprId.id}")()
      case attr @ AttributeReference(name, dataType, nullable, metadata)
          if name.equals("aggOrder") =>
        AttributeReference(s"${name}${attr.exprId.id}", dataType, nullable, metadata)()
      // The following cases are used to deal with subquery
      case OuterReference(e) =>
        e match {
          case a @ AttributeReference(name, _, _, _) =>
            val newQualifier = a.qualifier.last
            Literal(s"$newQualifier.`$name`", StringType)
          case other => other
        }
      case in @ InSubquery(_, query: ListQuery) =>
        val pushDownQueryCommand = query.plan match {
          case p: PushDownQueryCommand => p
          case plan =>
            transform(plan, plan.collectLeaves(), true)
              .asInstanceOf[PushDownQueryCommand]
        }
        In(in.value, Seq(Literal(pushDownQueryCommand.queryContent, StringType)))
      case exists @ Exists(plan, _, _) =>
        val newPlan = plan match {
          case p: PushDownQueryCommand => p
          case _ => transform(plan, plan.collectLeaves(), true).asInstanceOf[PushDownQueryCommand]
        }
        val literal = Literal(newPlan.queryContent, StringType)
        exists.copy(children = literal :: Nil)
      case greaterThan @ GreaterThan(_, _ @ScalarSubquery(plan, _, _)) =>
        val newPlan = plan match {
          case p: PushDownQueryCommand => p
          case _ => transform(plan, plan.collectLeaves(), true).asInstanceOf[PushDownQueryCommand]
        }
        val literal = Literal(s"(${newPlan.queryContent})", StringType)
        greaterThan.copy(right = literal)
      case lessThan @ LessThan(_, _ @ScalarSubquery(plan, _, _)) =>
        val newPlan = plan match {
          case p: PushDownQueryCommand => p
          case _ => transform(plan, plan.collectLeaves(), true).asInstanceOf[PushDownQueryCommand]
        }
        val literal = Literal(s"(${newPlan.queryContent})", StringType)
        lessThan.copy(right = literal)
      case equalTo @ EqualTo(_, _ @ScalarSubquery(plan, _, _)) =>
        val newPlan = plan match {
          case p: PushDownQueryCommand => p
          case _ => transform(plan, plan.collectLeaves(), true).asInstanceOf[PushDownQueryCommand]
        }
        val literal = Literal(s"(${newPlan.queryContent})", StringType)
        equalTo.copy(right = literal)
      case greaterThanOrEqual @ GreaterThanOrEqual(_, _ @ScalarSubquery(plan, _, _)) =>
        val newPlan = plan match {
          case p: PushDownQueryCommand => p
          case _ => transform(plan, plan.collectLeaves(), true).asInstanceOf[PushDownQueryCommand]
        }
        val literal = Literal(s"(${newPlan.queryContent})", StringType)
        greaterThanOrEqual.copy(right = literal)
      case lessThanOrEqual @ LessThanOrEqual(_, _ @ScalarSubquery(plan, _, _)) =>
        val newPlan = plan match {
          case p: PushDownQueryCommand => p
          case _ => transform(plan, plan.collectLeaves(), true).asInstanceOf[PushDownQueryCommand]
        }
        val literal = Literal(s"(${newPlan.queryContent})", StringType)
        lessThanOrEqual.copy(right = literal)
      case equalNullSafe @ EqualNullSafe(_, _ @ScalarSubquery(plan, _, _)) =>
        val newPlan = plan match {
          case p: PushDownQueryCommand => p
          case _ => transform(plan, plan.collectLeaves(), true).asInstanceOf[PushDownQueryCommand]
        }
        val literal = Literal(s"(${newPlan.queryContent})", StringType)
        equalNullSafe.copy(right = literal)
    }
  }

}
