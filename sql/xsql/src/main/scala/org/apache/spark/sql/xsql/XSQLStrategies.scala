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

package org.apache.spark.sql.xsql

import java.util.{HashMap, HashSet, Optional}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, LinkedHashSet}

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.CatalystTypeConverters.convertToScala
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{
  EliminateEventTimeWatermark,
  UnresolvedAlias,
  UnresolvedAttribute,
  UnresolvedFunction,
  UnresolvedRelation,
  UnresolvedStar
}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.{CacheTableCommand, RunnableCommand}
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.execution.streaming.{
  BaseStreamingSink,
  StreamingRelation,
  StreamingRelationV2
}
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.sources.v2.{
  ContinuousReadSupport,
  DataSourceOptions,
  MicroBatchReadSupport
}
import org.apache.spark.sql.types._
import org.apache.spark.sql.xsql.DataSourceManager._
import org.apache.spark.sql.xsql.DataSourceType._
import org.apache.spark.sql.xsql.execution.command.{ScanTableCommand, StreamingIncrementCommand}
import org.apache.spark.sql.xsql.execution.datasources.redis.RedisSpecialStrategy
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.util.Utils

/**
 * Resolve single table one query, translate to scan table directly.
 */
// INLINE: this strategy must between ResolveRelations and FindDataSourceTable
class ResolveScanSingleTable(session: SparkSession) extends Rule[LogicalPlan] {

  val catalog = session.sessionState.catalog.asInstanceOf[XSQLSessionCatalog]

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.isInstanceOf[SubqueryAlias]
        && plan.collectLeaves().exists(leaf => leaf.isInstanceOf[UnresolvedCatalogRelation])
        && plan.find(_.subqueries.size > 0).isEmpty) {
      findSingleTable(plan)
    } else {
      plan
    }
  }

  /**
   * Find single table in query.
   */
  def findSingleTable(plan: LogicalPlan): LogicalPlan = {
    var splited = false
    val tmpPlan = plan resolveOperatorsUp {
      case innerplan =>
        if (innerplan.children.size > 1) {
          splited = true
          innerplan.withNewChildren(innerplan.children.map(child =>
            if (child.collectLeaves.size > 1) {
              child
            } else {
              processSingleTable(child)
          }))
        } else {
          innerplan
        }
    }
    if (!splited) {
      processSingleTable(tmpPlan)
    } else {
      tmpPlan
    }
  }

  /**
   * Process single table.
   */
  def processSingleTable(plan: LogicalPlan): LogicalPlan = {
    plan.collectLeaves.head match {
      // The basic rules of push down as follows:
      // First, enabled push down for some data source
      //        (e.g when spark.xsql.datasource.xxx.pushdown=true)
      // Second, the data source must support push down.
      // Built-in data source support push down contains MySQL, Redis, HBASE, MongoDB, Druid,
      // Elasticsearch, but Hive and KAFKA.
      case relation: UnresolvedCatalogRelation =>
        val tableMeta = relation.tableMeta
        val tableIdentifier = tableMeta.identifier
        val ds = catalog.getDataSourceName(tableIdentifier)
        val dsType = DataSourceType.withName(catalog.getDataSourceType(ds))
        val isPushdown = catalog.isPushDown(tableIdentifier)
        (dsType, isPushdown) match {
          // MySQL has a dedicated pushdown rule, so let it go here.
          case (MYSQL, _) => plan
          // Hive and KAFKA not support pushdown.
          case (HIVE | KAFKA, _) => plan
          case (_, false) => plan
          // REDIS has different process function.
          case (REDIS, true) => RedisSpecialStrategy.processSingleTableRedis(plan)
          case (HBASE | MONGO | DRUID | ELASTICSEARCH, true) =>
            if (checkSubQueryOuterRef(plan)) {
              plan
            } else {
              val newPlan = filterInnerSubqueryAlias(plan)
              processSingleTableStandard(newPlan, dsType)
            }
          case _ =>
            plan
        }
      case _: OneRowRelation =>
        plan
      case _: HiveTableRelation =>
        plan
      case _: LogicalRelation =>
        plan
      case _: LocalRelation =>
        plan
      case _: StreamingRelationV2 =>
        plan
      case other =>
        throw new SparkException(s"Unexpected Relation: ${other}")
    }
  }

  /**
   * Check sub query reference outer alias.
   */
  private def checkSubQueryOuterRef(plan: LogicalPlan): Boolean = {
    var flag = false
    plan resolveOperatorsUp {
      case f @ Filter(condition: BinaryExpression, subqueryAlias: SubqueryAlias) =>
        val leftAlias = findAlias(condition.left)
        val rightAlias = findAlias(condition.right)
        val currentAlias = subqueryAlias.alias
        if (doCheck(currentAlias, leftAlias) || doCheck(currentAlias, rightAlias)) {
          flag = true
        }
        f
      case other => other
    }
    def doCheck(alias: String, otherAlias: String): Boolean = {
      if (otherAlias == null || alias.endsWith(otherAlias)) {
        false
      } else {
        true
      }
    }
    flag
  }

  /**
   * Find alias
   */
  private def findAlias(expression: Expression): String = {
    expression match {
      case UnresolvedAttribute(nameParts: Seq[String]) =>
        if (nameParts.length > 1) {
          nameParts.head
        } else {
          null
        }
      case _ => null
    }
  }

  def filterInnerSubqueryAlias(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsUp {
    case outerSubqueryAlias @ SubqueryAlias(alias, subqueryAlias: SubqueryAlias) =>
      SubqueryAlias(alias, subqueryAlias.child)
    case other => other
  }

  /**
   * Pushdown single table.
   */
  def processSingleTableStandard(plan: LogicalPlan, dsType: DataSourceType): LogicalPlan =
    plan resolveOperatorsUp {
      case relation: UnresolvedCatalogRelation =>
        ScanTableCommand(
          relation.tableMeta,
          relation.tableMeta.dataSchema.asNullable.toAttributes,
          relation.tableMeta.partitionSchema.asNullable.toAttributes,
          columns = relation.tableMeta.dataSchema.map(_.name).asJava)

      case filter @ Filter(
            condition,
            subqueryAlias @ SubqueryAlias(alias, command: ScanTableCommand)) =>
        val searchArg = catalog.buildFilterArgument(command.tableMeta, condition)
        subqueryAlias.copy(alias, command.copy(condition = searchArg))

      case p @ Project(
            projectList,
            subqueryAlias @ SubqueryAlias(alias, command: ScanTableCommand)) =>
        if (dsType == DataSourceType.HBASE && projectList.exists(e =>
              e.find {
                case u @ UnresolvedFunction(funcId, children, isDistinct) => true
                case a @ AggregateExpression(aggregateFunction, mode, isDistinct, resultId) =>
                  true
                case ScalarSubquery(plan, children, exprId) => true
                case _ => false
              }.isDefined)) {
          p
        } else {
          val names: LinkedHashSet[String] = LinkedHashSet.empty
          val tableMeta = command.tableMeta
          val tableType = tableMeta.tableType.name
          val fields: ArrayBuffer[StructField] = ArrayBuffer.empty
          val innerAggsMap = new HashMap[String, Any]
          val dataCols = command.dataCols
          val partitionCols = command.partitionCols
          projectList.foreach { x =>
            x match {
              case u: UnresolvedStar =>
                val cols = dataCols ++ partitionCols
                cols.foreach { col =>
                  names.add(col.name)
                  fields += tableMeta.dataSchema(col.name)
                }
              case UnresolvedAlias(child, aliasFunc) =>
                parseUnresolvedAlias(tableMeta, child, innerAggsMap, fields, names, None)
              case Alias(child, name) =>
                parseUnresolvedAlias(tableMeta, child, innerAggsMap, fields, names, Some(name))
              case attr: UnresolvedAttribute =>
                // Table alias need remove.
                val realName = attr.name.split("\\.").last
                if (!innerAggsMap.isEmpty) {
                  throw new SparkException(s"""Not allowed select Field $realName
                     | when using Aggregate Function without group!""".stripMargin)
                }
                names.add(realName)
                fields += tableMeta.dataSchema(realName)
            }
          }
          val structType = new StructType(fields.toArray)
          val newDataCols: Seq[AttributeReference] = structType.asNullable.toAttributes
          val newPartitionCols: Seq[AttributeReference] = partitionCols
          val columns = names.toSeq.asJava
          val groupMap = innerAggsMap.get(AGGS).asInstanceOf[HashMap[String, Any]]
          val newScanTableCommand = command.copy(
            dataCols = newDataCols,
            partitionCols = newPartitionCols,
            columns = columns,
            aggs = if (tableType.equalsIgnoreCase("datasource")) innerAggsMap else groupMap)
          subqueryAlias.copy(child = newScanTableCommand)
        }

      case a @ Aggregate(
            groupingExpressions,
            aggregateExpressions,
            subqueryAlias @ SubqueryAlias(alias, command: ScanTableCommand)) =>
        if (dsType.equals(DataSourceType.HBASE)) {
          a
        } else {
          val names: LinkedHashSet[String] = LinkedHashSet.empty
          val tableMeta = command.tableMeta
          val tableType = tableMeta.tableType.name
          val fields: ArrayBuffer[StructField] = ArrayBuffer.empty
          var groupMap = new HashMap[String, Any]
          val innerAggsMap = new HashMap[String, Any]
          val groupByKeys: ArrayBuffer[String] = ArrayBuffer.empty
          val globalGroupFieldMap = new HashSet[String]
          groupingExpressions.reverse.foreach { x =>
            x match {
              case attr: UnresolvedAttribute =>
                val realName = attr.name.split("\\.").last
                globalGroupFieldMap.add(realName)
                groupMap = catalog.buildGroupArgument(
                  tableMeta,
                  realName,
                  groupByKeys,
                  innerAggsMap,
                  groupMap)
            }
          }
          aggregateExpressions.foreach { x =>
            x match {
              case UnresolvedAlias(child, aliasFunc) =>
                parseUnresolvedAlias(tableMeta, child, innerAggsMap, fields, names, None)
              case Alias(child, name) =>
                parseUnresolvedAlias(tableMeta, child, innerAggsMap, fields, names, Some(name))
              case attr: UnresolvedAttribute =>
                val realName = attr.name.split("\\.").last
                if (!globalGroupFieldMap.contains(realName)) {
                  throw new SparkException(s"Aggregate Field $realName is not in Group Field!")
                }
                fields += tableMeta.dataSchema(realName)
                names.add(realName)
              case s: UnresolvedStar =>
                throw new SparkException(s"expression * is not allowed in group by statement")
            }
          }
          val structType = new StructType(fields.toArray)
          val newDataCols: Seq[AttributeReference] = structType.asNullable.toAttributes
          val columns = names.toSeq.asJava
          val aggs =
            if (groupMap.isEmpty) innerAggsMap.get(AGGS).asInstanceOf[HashMap[String, Any]]
            else groupMap
          val newScanTableCommand = command.copy(
            dataCols = newDataCols,
            columns = columns,
            aggs = if (tableType.equalsIgnoreCase("datasource")) innerAggsMap else aggs,
            groupByKeys = groupByKeys.seq)
          subqueryAlias.copy(child = newScanTableCommand)
        }
      case s @ Sort(
            order,
            global,
            subqueryAlias @ SubqueryAlias(alias, command: ScanTableCommand)) =>
        logInfo(s"sort is $global")
        val tableMeta = command.tableMeta
        val aggs = command.aggs
        if (aggs == null) {
          val sortArg = catalog.buildSortArgument(tableMeta, order)
          if (sortArg == null) {
            s
          } else {
            val newScanTableCommand = command.copy(sort = sortArg)
            subqueryAlias.copy(child = newScanTableCommand)
          }
        } else {
          catalog.buildSortArgumentInAggregate(tableMeta, order, aggs)
          subqueryAlias
        }

      case l @ LocalLimit(
            Literal(v, t),
            subqueryAlias @ SubqueryAlias(alias, command: ScanTableCommand)) =>
        val value: Any = convertToScala(v, t)
        val limit = value.asInstanceOf[Int]
        if (limit < 0) {
          throw new SparkException(s"Aggregate limit must great than zero!")
        }
        if (limit > MAX_LIMIT) {
          throw new SparkException(s"XSQL Pushdown is bigger than $MAX_LIMIT limit")
        }
        subqueryAlias.copy(child = command.copy(limit = limit))

      case g @ GlobalLimit(
            Literal(v, t),
            subqueryAlias @ SubqueryAlias(alias, command: ScanTableCommand)) =>
        subqueryAlias

      case d @ Distinct(subqueryAlias @ SubqueryAlias(alias, command: ScanTableCommand)) =>
        val columns = command.columns
        val (groupMap, groupByKeys) =
          catalog.buildDistinctArgument(command.tableMeta.identifier, columns)
        if (groupMap == null || groupByKeys == null) {
          d
        } else {
          val newScanTableCommand = command.copy(aggs = groupMap, groupByKeys = groupByKeys)
          subqueryAlias.copy(child = newScanTableCommand)
        }
      case other => other
    }

  /**
   * Build aggregate argument in query.
   */
  private def buildAggregateArgument(
      tableDefinition: CatalogTable,
      child: Expression,
      aggregatefuncName: String,
      aggsMap: HashMap[String, Any],
      aggregateAliasOpts: Option[String]): StructField = {
    child match {
      case attr: UnresolvedAttribute =>
        val realName = attr.name.split("\\.").last
        catalog.buildAggregateArgument(
          tableDefinition,
          realName,
          aggregatefuncName,
          aggsMap,
          aggregateAliasOpts)
    }
  }

  /**
   * Parse UnresolvedAlias.
   */
  private def parseUnresolvedAlias(
      tableDefinition: CatalogTable,
      child: Expression,
      innerAggsMap: HashMap[String, Any],
      fields: ArrayBuffer[StructField],
      names: LinkedHashSet[String],
      aggregateAliasOpts: Option[String]): Unit = {
    var stringField: StructField = null
    child match {
      case a @ AggregateExpression(aggregateFunction, mode, isDistinct, resultId) =>
        aggregateFunction match {
          case count @ Count(children) =>
            children.foreach { child =>
              child match {
                case l @ Literal(v, t) =>
                  val value: Any = convertToScala(v, t)
                  if (value == 1) {
                    val adapt = new UnresolvedAttribute(Seq(STAR))
                    stringField = buildAggregateArgument(
                      tableDefinition,
                      adapt,
                      COUNT,
                      innerAggsMap,
                      aggregateAliasOpts)
                  }
                case attr: UnresolvedAttribute =>
                  stringField = buildAggregateArgument(
                    tableDefinition,
                    attr,
                    COUNT,
                    innerAggsMap,
                    aggregateAliasOpts)
              }
            }
        }
      case u @ UnresolvedFunction(funcId, children, isDistinct) =>
        val func = catalog.lookupFunction(funcId, children)
        func match {
          case sum @ Sum(child) =>
            stringField = buildAggregateArgument(
              tableDefinition,
              child,
              sum.prettyName,
              innerAggsMap,
              aggregateAliasOpts)
          case avg @ Average(child) =>
            stringField = buildAggregateArgument(
              tableDefinition,
              child,
              avg.prettyName,
              innerAggsMap,
              aggregateAliasOpts)
          case max @ Max(child) =>
            stringField = buildAggregateArgument(
              tableDefinition,
              child,
              max.prettyName,
              innerAggsMap,
              aggregateAliasOpts)
          case min @ Min(child) =>
            stringField = buildAggregateArgument(
              tableDefinition,
              child,
              min.prettyName,
              innerAggsMap,
              aggregateAliasOpts)
          case count @ Count(children) =>
            children.foreach { child =>
              if (isDistinct) {
                stringField = buildAggregateArgument(
                  tableDefinition,
                  child,
                  COUNT_DISTINCT,
                  innerAggsMap,
                  aggregateAliasOpts)
              } else {
                stringField = buildAggregateArgument(
                  tableDefinition,
                  child,
                  COUNT,
                  innerAggsMap,
                  aggregateAliasOpts)
              }
            }
        }
      case ScalarSubquery(plan, children, exprId) =>
        throw new SparkException(
          "XSQL not support pushdown sub query except MySQL." +
            "Please using --conf spark.xsql.datasource." +
            s"${tableDefinition.identifier.dataSource.get}.pushdown=false.")

//      case attr: UnresolvedAttribute =>
//        stringField = new StructField(aggregateAliasOpts.get,
//          StringType)
//          tableDefinition.schema(attr.name.split("\\.").last).dataType)
    }
    fields += stringField
    names.add(stringField.name)
  }

  /**
   * Exists other relation except Hive.
   */
  def existsNonHiveRelation(plan: LogicalPlan): Boolean = {
    var exists = false
    val function: PartialFunction[LogicalPlan, LogicalPlan] = {
      case r @ UnresolvedRelation(tableIdentifier) =>
        val newTableIdentifier = catalog.getUsedTableIdentifier(tableIdentifier)
        val dsType =
          DataSourceType.withName(catalog.getDataSourceType(newTableIdentifier.dataSource.get))
        dsType match {
          case DataSourceType.HIVE =>
          case _ => exists = true
        }
        r
      case other => other
    }
    plan resolveOperators function
    exists
  }

  /**
   * Used for SparkXSQLShell register AM later.
   */
  def isRunOnYarn(plan: LogicalPlan): Boolean = {
    var isRun = false
    val dsToTableIdentifier = new HashMap[String, HashSet[TableIdentifier]]
    val innerPlans: ArrayBuffer[LogicalPlan] = ArrayBuffer.empty

    val collectLogicPlanInCommand: PartialFunction[LogicalPlan, LogicalPlan] = {
      case c: CacheTableCommand =>
        innerPlans += c.plan.get
        c
      case p =>
        p.subqueries.foreach { subquery =>
          innerPlans += subquery
        }
        p
    }

    val collectDatasources: PartialFunction[LogicalPlan, LogicalPlan] = {
      case r @ UnresolvedRelation(tableIdentifier) =>
        val newTableIdentifier = catalog.getUsedTableIdentifier(tableIdentifier)
        val ds = newTableIdentifier.dataSource.get
        val identifierSet = dsToTableIdentifier.getOrDefault(ds, new HashSet[TableIdentifier])
        identifierSet.add(newTableIdentifier)
        dsToTableIdentifier.put(ds, identifierSet)
        r
      case other => other
    }
    plan resolveOperators collectLogicPlanInCommand
    val allPlans = Seq(plan).++:(innerPlans)
    allPlans.foreach { plan =>
      plan resolveOperators collectDatasources
    }
    dsToTableIdentifier.asScala.foreach { kv =>
      val selectedTableIdentifier = kv._2.asScala.toSeq.last
      val dsType =
        DataSourceType.withName(catalog.getDataSourceType(selectedTableIdentifier.dataSource.get))
      dsType match {
        case DataSourceType.MYSQL =>
          isRun = isRun |
            !catalog.isPushDown(selectedTableIdentifier, dsToTableIdentifier, plan)
        case _ => isRun = isRun | !catalog.isPushDown(selectedTableIdentifier)
      }
    }
    isRun
  }

  /**
   * Get yarn cluster. There exists a priority rule:
   * 1.Hive data location have the highest priority.
   * 2.Last selected cluster have the higher priority in tree.
   */
  private def getYarnCluster(plan: LogicalPlan): Option[String] = {
    var selectedCluster: Option[String] = None
    var hiveCluster: Option[String] = None
    val function: PartialFunction[LogicalPlan, LogicalPlan] = {
      case r @ UnresolvedRelation(tableIdentifier) =>
        val newTableIdentifier = catalog.getUsedTableIdentifier(tableIdentifier)
        val dsType =
          DataSourceType.withName(catalog.getDataSourceType(newTableIdentifier.dataSource.get))
        dsType match {
          case DataSourceType.HIVE => hiveCluster = catalog.getDefaultCluster(newTableIdentifier)
          case _ => selectedCluster = catalog.getDefaultCluster(newTableIdentifier)
        }
        r
      case other => other
    }
    plan resolveOperators function
    hiveCluster.orElse(selectedCluster)
  }

  /**
   * Select suitable yarn cluster.
   * Notice: Only used once on restart Scheduler.
   */
  def selectYarnCluster(plan: LogicalPlan): Map[String, String] = {
    getYarnCluster(plan)
      .map { clusterName =>
        catalog.getClusterOptions(clusterName)
      }
      .getOrElse(Map.empty)
  }
}

/**
 * Resolve input data stream with structured streaming.
 */
class ResolveInputDataStream(session: SparkSession) extends Rule[LogicalPlan] {
  val catalog = session.sessionState.catalog.asInstanceOf[XSQLSessionCatalog]
  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case relation @ UnresolvedCatalogRelation(tableMeta) =>
      val tableIdentifier = tableMeta.identifier
      val catalogDB =
        catalog.getUsedCatalogDatabase(tableIdentifier.dataSource, tableIdentifier.database)
      val ds = catalogDB.get.dataSourceName
      val dsType = DataSourceType.withName(catalog.getDataSourceType(ds))
      val isStructuredStream = catalog.isStream(tableIdentifier)
      (dsType, isStructuredStream) match {
        case (KAFKA, true) =>
          val source = tableMeta.provider.get
          val ds = DataSource.lookupDataSource(source, session.sqlContext.conf).newInstance()
          val extraOptions = tableMeta.storage.properties
          val options = new DataSourceOptions(extraOptions.asJava)
          // We need to generate the V1 data source so we can pass it to the V2 relation as a shim.
          // We can't be sure at this point whether we'll actually want to use V2,
          // since we don't know the writer or whether the query is continuous.
          val v1DataSource = DataSource(
            session,
            userSpecifiedSchema = None,
            className = source,
            options = extraOptions.toMap)
          val v1Relation = ds match {
            case _: StreamSourceProvider => Some(StreamingRelation(v1DataSource))
            case _ => None
          }
          val streamingRelation = ds match {
            case s: MicroBatchReadSupport =>
              val tempReader = s.createMicroBatchReader(
                Optional.ofNullable(null),
                Utils.createTempDir(namePrefix = s"temporaryReader").getCanonicalPath,
                options)
              StreamingRelationV2(
                s,
                source,
                extraOptions.toMap,
                tempReader.readSchema().toAttributes,
                v1Relation)(session)
            case s: ContinuousReadSupport =>
              val tempReader = s.createContinuousReader(
                Optional.ofNullable(null),
                Utils.createTempDir(namePrefix = s"temporaryReader").getCanonicalPath,
                options)
              StreamingRelationV2(
                s,
                source,
                extraOptions.toMap,
                tempReader.readSchema().toAttributes,
                v1Relation)(session)
            case _ =>
              // Code path for data source v1.
              StreamingRelation(v1DataSource)
          }

          // Parse water mark
          val watermark = extraOptions.get(STREAMING_WATER_MARK)
          if (watermark == None) {
            streamingRelation
          } else {
            val delayThreshold = watermark.get
            val parsedDelay =
              Option(CalendarInterval.fromString("interval " + delayThreshold)).getOrElse(
                throw new AnalysisException(s"Unable to parse time delay '$delayThreshold'"))
            require(
              parsedDelay.milliseconds >= 0 && parsedDelay.months >= 0,
              s"delay threshold ($delayThreshold) should not be negative.")
            EliminateEventTimeWatermark(
              EventTimeWatermark(
                UnresolvedAttribute(KAFKA_WATER_MARK),
                parsedDelay,
                streamingRelation))
          }
        case _ =>
          relation
      }
  }
}

/**
 * Construct query of structured streaming.
 */
class ConstructStreamingQuery(session: SparkSession) extends Rule[LogicalPlan] {
  import org.apache.spark.sql.xsql.execution.command.ConstructedStreaming
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (plan.isInstanceOf[ConstructedStreaming]) {
      plan.asInstanceOf[ConstructedStreaming].child
    } else if (plan.collectLeaves().exists(leaf => leaf.isInstanceOf[StreamingRelationV2])) {
      StreamingIncrementCommand(plan)
    } else {
      plan
    }
  }

}

private[xsql] trait XSQLStrategies {}
