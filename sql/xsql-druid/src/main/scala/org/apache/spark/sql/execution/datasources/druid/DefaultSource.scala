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

import java.util

import com.google.common.collect.ImmutableList
import io.druid.data.input.InputRow
import io.druid.hll.HyperLogLogCollector
import io.druid.query.filter.{Filter => _, _}
import org.apache.hadoop.io.NullWritable
import org.joda.time.{DateTime, Interval}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, GenericRow}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.sources.{
  And => SAnd,
  IsNotNull => SIsNotNull,
  Not => SNot,
  Or => SOr,
  _
}
import org.apache.spark.sql.types._
import org.apache.spark.sql.xsql.types._

private[sql] class DefaultSource
  extends DataSourceRegister
  with RelationProvider
  with SchemaRelationProvider {

  override def shortName(): String = "druid"

  /**
   * Returns a new base relation with the given parameters.
   *
   * @note The parameters' keywords are case insensitive and this insensitivity is enforced
   *       by the Map that is passed to the function.
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    DruidRelation(sqlContext, parameters)
  }

  def addPhysicalRules(sqlContext: SQLContext): Unit = {
    sqlContext.sparkSession.experimental.extraStrategies ++=
      Seq(new DruidStrategy())
    sqlContext.sparkSession.experimental.extraOptimizations ++=
      Seq(DruidRule)
  }

  /**
   * Returns a new base relation with the given parameters and user defined schema.
   * @note The parameters' keywords are case insensitive and this insensitivity is enforced
   *       by the Map that is passed to the function.
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    val relation = DruidRelation(sqlContext, parameters, Some(schema))
    // add experimental rules
    addPhysicalRules(sqlContext)
    relation
  }
}

private[sql] case class DruidRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    userSchema: Option[StructType] = None)
  extends BaseRelation
  with PushDownAggregateScan
  with Logging {

  var startTime: String = null
  var endTime: String = null
  var granularity: Granularity = SimpleGranularity.All
  val url = parameters.getOrElse("url", null)
  val coordinator = parameters.getOrElse("coordinator", null)
  val datasource = parameters.getOrElse("datasource", null)
  if (datasource == null) {
    throw new IllegalArgumentException("datasource must set when create table")
  }
  val timestampcolumn = parameters.getOrElse("timestampcolumn", null)
  if (timestampcolumn == null) {
    throw new IllegalArgumentException("timestampcolumn must set when create table")
  }

  override def schema: StructType = userSchema.get

  def buildScan(): RDD[Row] = {
    buildScan(Array.empty)
  }

  def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    buildScan(requiredColumns, Array.empty)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val otherfilters = getPushDownFilters(filters)
    if (startTime == null) {
      throw new IllegalArgumentException(s"the $timestampcolumn lowerbound must be set in query")
    }
    if (endTime == null) {
      throw new IllegalArgumentException(s"the $timestampcolumn uperbound must be set in query")
    }
//    execScanQuery(requiredColumns, otherfilters)
//    if (aggregateExpressions == null && groupingExpressions == null && !otherfilters.isEmpty) {

    if (aggregateExpressions == null && groupingExpressions == null && otherfilters.isEmpty) {
      logInfo("execute SelectQueries")
      execSelectQuery(requiredColumns, otherfilters)
    } else if (aggregateExpressions == null
               && groupingExpressions == null && !otherfilters.isEmpty) {
      logInfo("execute ScanQueries")
      execScanQuery(requiredColumns, otherfilters)
    } else {
      val groupFields =
        AttributeSet(groupingExpressions.flatMap(_.references)).map(_.name).toArray
      if (groupFields.size == 1 && groupFields(0) == timestampcolumn) {
        logInfo("execute TimeSeriesQuery")
        execTimeSeriesQuery(requiredColumns, otherfilters)
      } else {
        logInfo("execute GroupByQuery")
        execGroupByQuery(requiredColumns, otherfilters)
      }
    }
  }

  def getGranularity(granularity: String): Option[Granularity] = {
    val sampleGranularityValue = Set(
      "all",
      "none",
      "second",
      "minute",
      "fifteen_minute",
      "thirty_minute",
      "hour",
      "day",
      "week",
      "month",
      "quarter",
      "year")
    val strValue = granularity.trim
    val v = strValue.split(",")
    if (v.size == 1) {
      if (sampleGranularityValue.contains(strValue)) {
        Some(SimpleGranularity(strValue))
      } else {
        throw new IllegalArgumentException(s"can not analysis $granularity")
      }
    } else {
      val vMap = v.map { x =>
        val a = x.split(":")
        if (a.size != 2) throw new IllegalArgumentException(s"can not analysis $granularity")
        (a(0).trim, a(1).trim)
      }.toMap

      if (vMap.contains("type")) {
        vMap.getOrElse("type", null) match {
          case "period" =>
            Some(
              PeriodGranularity(
                vMap.getOrElse("period", null),
                vMap.getOrElse("timeZone", null),
                vMap.getOrElse("origin", null)))
          case "duration" =>
            Some(
              DurationGranularity(
                vMap.getOrElse("duration", null),
                vMap.getOrElse("timeZone", null),
                vMap.getOrElse("duration", null)))
          case _ => throw new IllegalArgumentException(s"can not analysis $granularity")
        }
      } else {
        throw new IllegalArgumentException(s"can not analysis $granularity")
      }
    }
  }

  def getPushDownFilters(filters: Array[Filter]): Array[Filter] = {
    val (_, otherfilters) = filters.partition { f =>
      f match {
        case EqualTo(attribute, value) =>
          if (attribute == "granularity") {
            val tempGranularity = getGranularity(value.toString)
            if (tempGranularity.isDefined) {
              granularity = tempGranularity.get
            }
            true
          } else {
            false
          }
        case LessThan(attribute, value) =>
          if (attribute == timestampcolumn) {
            endTime = value.toString
            true
          } else {
            false
          }
        case LessThanOrEqual(attribute, value) =>
          if (attribute == timestampcolumn) {
            endTime = value.toString
            true
          } else {
            false
          }
        case GreaterThan(attribute, value) =>
          if (attribute == timestampcolumn) {
            startTime = value.toString
            true
          } else {
            false
          }
        case GreaterThanOrEqual(attribute, value) =>
          if (attribute == timestampcolumn) {
            startTime = value.toString
            true
          } else {
            false
          }
        case _ => false
      }
    }
    otherfilters
  }

  def execSelectQuery(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    implicit val executionContext = ExecutionContext.Implicits.global
    val client = DruidClient(url)
    val fieldMap = Map.empty[String, String]
    val query = SelectQuery(
      source = datasource,
      interval = new Interval(new DateTime(startTime), new DateTime(endTime)),
      granularity = granularity,
      dimensions = requiredColumns,
      filter = if (filters.isEmpty) QueryFilter.All else createDruidFilters(filters))
    val future = client(query)
    var data: Seq[Map[String, Any]] = null
    future.onComplete {
      case Success(resp) => data = resp.data
      case Failure(ex) => ex.printStackTrace()
    }
    while (!future.isCompleted) {
      logInfo("sleep 500ms")
      Thread.sleep(500)
    }
    logInfo("get selectByQuery result and the size is " + data.size)
    client.close()
    converedToRow(data, requiredColumns, fieldMap)
  }

  def createDruidFilters(filters: Array[Filter]): QueryFilter = {
    val f = filters.filter(!_.isInstanceOf[SIsNotNull])
    if (f.length > 0) {
      And(f.map(filter => translateFilter(filter)))
    } else {
      QueryFilter.All
    }
  }

  def createDruidDimFilters(filters: Array[Filter]): DimFilter = {
    val list = new util.ArrayList[DimFilter](filters.size)
    for (filter <- filters) {
      list.add(translateToDruidFilter(filter))
    }
    new AndDimFilter(list)
  }

  def translateFilter(filter: Filter): QueryFilter = {
    filter match {
      case EqualTo(attribute, value) => SelectorQueryFilter(attribute, value.toString)
      case SAnd(left, right) => And(Seq(translateFilter(left), translateFilter(right)))
      case SOr(left, right) => Or(Seq(translateFilter(left), translateFilter(right)))
      case SNot(filterToNeg) => Not(translateFilter(filterToNeg))
//      case SIsNotNull(attributeNotNull) => IsNotNull(attributeNotNull)
      case f: Product if isClass(f, "org.apache.spark.sql.sources.StringStartsWith") =>
        val arg = f.productElement(1).toString()
        RegexQueryFilter(f.productElement(0).toString(), "$arg*")

      case f: Product if isClass(f, "org.apache.spark.sql.sources.StringEndsWith") =>
        val arg = f.productElement(1).toString()
        RegexQueryFilter(f.productElement(0).toString(), "*$arg")

      case f: Product if isClass(f, "org.apache.spark.sql.sources.StringContains") =>
        val arg = f.productElement(1).toString()
        RegexQueryFilter(f.productElement(0).toString(), "*$arg*")
    }
  }

  def scalaArrayToJavaCollection(values: Array[Any]): util.Collection[String] = {
    val list = new util.ArrayList[String](values.size)
    values.map { value =>
      list.add(value.toString)
    }
    list
  }

  def translateToDruidFilter(filter: Filter): DimFilter = {
    filter match {
      case EqualTo(attribute, value) => new SelectorDimFilter(attribute, value.toString, null)
      case SAnd(left, right) =>
        new AndDimFilter(
          ImmutableList
            .of[DimFilter](translateToDruidFilter(left), translateToDruidFilter(right)))
      case SOr(left, right) =>
        new OrDimFilter(
          ImmutableList
            .of[DimFilter](translateToDruidFilter(left), translateToDruidFilter(right)))
      case SNot(filterToNeg) => new NotDimFilter(translateToDruidFilter(filterToNeg))

      case In(attribute, values) =>
        new InDimFilter(attribute, scalaArrayToJavaCollection(values), null)

      case SIsNotNull(attribute) => new NotDimFilter(new SelectorDimFilter(attribute, "", null))
    }
  }

  def isClass(obj: Any, className: String): Boolean = {
    className.equals(obj.getClass().getName())
  }

  def execScanQuery(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val intervals = new util.ArrayList[Interval](1)
    intervals.add(new Interval(new DateTime(startTime), new DateTime(endTime)))
    val columns = new util.ArrayList[String]()
    requiredColumns.map { column =>
      columns.add(column)
    }
    val df = createDruidDimFilters(filters)
    DruidInputFormat.setInputs(
      sqlContext.sparkContext.hadoopConfiguration,
      coordinator,
      datasource,
      intervals,
      df,
      columns)
    val rdd = new NewHadoopRDD[NullWritable, InputRow](
      sqlContext.sparkContext,
      classOf[DruidInputFormat],
      classOf[NullWritable],
      classOf[InputRow],
      sqlContext.sparkContext.hadoopConfiguration)

    rdd.map(_._2).map { row =>
      val size = requiredColumns.size
      val r = new Array[Any](size)
      for (i <- 0 until size) {
        if ("__time".equalsIgnoreCase(requiredColumns(i))) {
          r(i) = row.getTimestamp.toString()
        } else {
          val hll = row.getRaw(requiredColumns(i))
          if (hll.isInstanceOf[HyperLogLogCollector]) {
            r(i) = null
          } else if (hll.isInstanceOf[Float]) {
            r(i) = java.lang.Double.parseDouble(hll + "")
          } else {
            r(i) = hll
          }
        }
      }
      new GenericRow(r)
    }
  }

  def converedToRow(
      data: Seq[Map[String, Any]],
      requiredColumns: Array[String],
      sparkfField2Druid: Map[String, String]): RDD[Row] = {
    val numColumns = requiredColumns.length
    sqlContext.sparkContext.parallelize(data, 2).mapPartitions { data =>
      for (d <- data) yield {
        val row = new Array[Any](numColumns)
        for (id <- 0 until numColumns) {
          val value = d.getOrElse(requiredColumns(id), null)
          if (value == null) {
            row(id) = ""
          } else {
            row(id) = value match {
              case v: BigInt => v.toLong
              case v: Double =>
                if (!sparkfField2Druid
                      .get(requiredColumns(id))
                      .get
                      .isEmpty) { v.longValue() } else v
              case _ => value
            }
          }
        }
        new GenericRow(row)
      }
    }
  }

  def execTimeSeriesQuery(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    implicit val executionContext = ExecutionContext.Implicits.global
    val client = DruidClient(url)

    val (aggDruid, fieldMap) = converedToDriudAggregate

    val query = TimeSeriesQuery(
      source = datasource,
      interval = new Interval(new DateTime(startTime), new DateTime(endTime)),
      granularity = granularity,
      aggregate = aggDruid,
      postAggregate = Seq(),
      filter = if (filters.isEmpty) QueryFilter.All else createDruidFilters(filters))

    val future = client(query)
    var data: Seq[(DateTime, Map[String, Any])] = null
    future.onComplete {
      case Success(resp) => data = resp.data
      case Failure(ex) => ex.printStackTrace()
    }

    while (!future.isCompleted) {
      logInfo("sleep 500ms")
      Thread.sleep(500)
    }
    logInfo("get TimeSeriesQuery result and the size is " + data.size)
    client.close()
    converedToTsRow(data, requiredColumns, fieldMap)
  }

  def converedToDriudAggregate(): (Seq[Aggregation], Map[String, String]) = {
    val aggExpr = aggregateExpressions.flatMap { expr =>
      expr.collect {
        case agg: AggregateExpression => (expr.toAttribute.name, agg)
      }
    }.distinct

    val aggregateFunction = aggExpr.map {
      case (name, agg) =>
        var aggFunc = ""
        var field = ""
        var nameDruid = ""
        var t = ""
        val aggregateFunction = agg.aggregateFunction
        aggregateFunction match {
          case s: Sum =>
            s.dataType match {
              case LongType | IntegerType =>
                aggFunc = "longSum"
                field = s.references.map(_.name).mkString(" ")
              case DoubleType | FloatType =>
                aggFunc = "doubleSum"
                field = s.references.map(_.name).mkString(" ")
              case _ => None
            }
          case s: Max =>
            s.dataType match {
              case LongType | IntegerType =>
                aggFunc = "longMax"
                field = s.references.map(_.name).mkString(" ")
              case DoubleType | FloatType =>
                aggFunc = "doubleMax"
                field = s.references.map(_.name).mkString(" ")
              case _ => None
            }
          case s: Min =>
            s.dataType match {
              case LongType | IntegerType =>
                aggFunc = "longMin"
                field = s.references.map(_.name).mkString(" ")
              case DoubleType | FloatType =>
                aggFunc = "doubleMax"
                field = s.references.map(_.name).mkString(" ")
              case _ => None
            }
          case c: Count =>
            aggFunc = "count"
            if (agg.isDistinct) {
              aggFunc = "countdistinct"
              t = c.references.baseSet.head.a.metadata.getString(DRUID_TYPE_STRING)
            }
            field = c.references.map(_.name).mkString(" ")
          case _ => None
        }
        (name, (aggFunc, field, t))
    }
    val fieldMap = aggregateFunction.map(x => (x._1, x._2._3)).toMap
    logInfo("fieldMap is " + fieldMap)

    import org.apache.spark.sql.execution.datasources.druid.DSL._
    val aggDruid = aggregateFunction.map {
      case (nameDruid, (f, n, t)) =>
        f match {
          case "longSum" => sum(n, nameDruid)
          case "doubleSum" => doubleSum(n, nameDruid)
          case "longMax" => max(n, nameDruid)
          case "doubleMax" => doubleMax(n, nameDruid)
          case "longMin" => min(n, nameDruid)
          case "doubleMin" => doubleMin(n, nameDruid)
          case "count" => count(nameDruid)
          case "countdistinct" => countdistinct(n, t, nameDruid)
        }
    }.toSeq
    (aggDruid, fieldMap)
  }

  def converedToTsRow(
      data: Seq[(DateTime, Map[String, Any])],
      requiredColumns: Array[String],
      sparkfField2Druid: Map[String, String]): RDD[Row] = {
    val numColumns = requiredColumns.length
    sqlContext.sparkContext.parallelize(data, 1).mapPartitions { data =>
      for ((t, d) <- data) yield {
        val row = new Array[Any](numColumns)
        for (id <- 0 until numColumns) {
          if (requiredColumns(id) == timestampcolumn) {
            row(id) = t.toString
          } else {
            val value = d.getOrElse(requiredColumns(id), null)
            row(id) = value match {
              case v: BigInt => v.toLong
              case v: Double =>
                if (!sparkfField2Druid.get(requiredColumns(id)).get.isEmpty) {
                  v.longValue
                } else {
                  v
                }
              case _ => value
            }
          }
        }
        new GenericRow(row)
      }
    }
  }

  def converedToTsRow2(
      data: Seq[(DateTime, Seq[Map[String, Any]])],
      requiredColumns: Array[String],
      sparkfField2Druid: Map[String, String]): RDD[Row] = {
    val numColumns = requiredColumns.length
    sqlContext.sparkContext.parallelize(data, 1).mapPartitions { data =>
      val list = ArrayBuffer.empty[Row]
      for ((t, d) <- data) {
        for (dd <- d) {
          val row = new Array[Any](numColumns)
          for (id <- 0 until numColumns) {
            if (requiredColumns(id) == timestampcolumn) {
              row(id) = t.toString
            } else {
              val value = dd.getOrElse(requiredColumns(id), null)
              row(id) = value match {
                case v: BigInt => v.toLong
                case v: Double =>
                  if (!sparkfField2Druid.get(requiredColumns(id)).get.isEmpty) {
                    v.longValue
                  } else {
                    v
                  }
                case _ => value
              }
            }
          }
          list.append(new GenericRow(row))
        }
      }
      list.iterator
    }
  }

  def execGroupByQuery(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    implicit val executionContext = ExecutionContext.Implicits.global
    val client = new DruidClient(url)

    var groupFields = AttributeSet(groupingExpressions.flatMap(_.references)).map(_.name).toSeq
    val (aggDruid, fieldMap) = converedToDriudAggregate
    var order = new ArrayBuffer[ColumnOrder]
    if (orders != null) {
      orders.map { o =>
        val t = o.child.dataType
        if (t.isInstanceOf[NumericType]) {
          order += ColumnOrder(o.child.references.head.name, o.direction.sql, "numeric")
        } else {
          order += ColumnOrder(o.child.references.head.name, o.direction.sql)
        }
      }
    }
    if (groupFields.size == 1 && orders != null && orders.size == 1 && limit > 0) {
      // topN
      var m: Metric = null
      if (orders(0).dataType.isInstanceOf[NumericType]) {
        if (orders(0).direction.sql.equalsIgnoreCase("descending")) {
          m = Metric("numberic", orders(0).child.references.head.name)
        } else {
          m = Metric("inverted", orders(0).child.references.head.name)
        }
      } else {
        m = Metric(metric = orders(0).child.references.head.name)
      }

      val query = TopNSelectQuery(
        source = datasource,
        dimension = groupFields(0),
        metric = m,
        interval = new Interval(new DateTime(startTime), new DateTime(endTime)),
        granularity = granularity,
        aggregate = aggDruid,
        filter = if (filters.isEmpty) QueryFilter.All else createDruidFilters(filters),
        limit = limit)
      if (granularity.toString.equalsIgnoreCase("all")) {
        var data: Seq[Map[String, Any]] = null
        val future = client.queryTopN(query)
        future.onComplete {
          case Success(resp) => data = resp.data
          case Failure(ex) => throw new RuntimeException(ex)
        }
        while (!future.isCompleted) {
          logInfo("sleep 100ms")
          Thread.sleep(100)
        }
        client.close()
        converedToRow(data, requiredColumns, fieldMap)
      } else {
        val future = client.queryTopN2(query)
        var data: Seq[(DateTime, Seq[Map[String, Any]])] = null
        future.onComplete {
          case Success(resp) => data = resp.data
          case Failure(ex) => throw new RuntimeException(ex)
        }
        while (!future.isCompleted) {
          logInfo("sleep 100ms")
          Thread.sleep(100)
        }
        logInfo("get TimeSeriesQuery result and the size is " + data.size)
        client.close()
        converedToTsRow2(data, requiredColumns, fieldMap)
      }
    } else {
      groupFields = groupFields.filter(!_.equalsIgnoreCase("__time"))
      val query = GroupByQuery(
        source = datasource,
        interval = new Interval(new DateTime(startTime), new DateTime(endTime)),
        granularity = granularity,
        dimensions = groupFields,
        aggregate = aggDruid,
        postAggregate = Seq(),
        filter = if (filters.isEmpty) QueryFilter.All else createDruidFilters(filters),
        orderBy = order,
        limit = Some(if (limit > 0) limit else 20))

      val future = client(query)
      var data: Seq[(DateTime, Map[String, Any])] = null
      future.onComplete {
        case Success(resp) => data = resp.data
        case Failure(ex) => ex.printStackTrace()
      }

      while (!future.isCompleted) {
        logInfo("sleep 500ms")
        Thread.sleep(500)
      }
      logInfo("get GroupByQuery result and the size is " + data.size)
      client.close()
      converedToTsRow(data, requiredColumns, fieldMap)
    }
  }
}
