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
package org.apache.spark.sql.xsql.manager

import java.util
import java.util.LinkedList

import scala.collection.convert.decorateAsScala._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

import org.joda.time.{DateTime, Interval}

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.{expressions, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{SortDirection, SortOrder}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.execution.datasources.druid._
import org.apache.spark.sql.types._
import org.apache.spark.sql.xsql._
import org.apache.spark.sql.xsql.DataSourceType.DRUID
import org.apache.spark.sql.xsql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.xsql.manager.DruidManager._
import org.apache.spark.sql.xsql.types._

private[spark] class DruidManager(conf: SparkConf) extends DataSourceManager with Logging {
  import DataSourceManager._

  def this() = {
    this(null)
  }

  override def shortName(): String = DataSourceType.DRUID.toString

  private var restClient: DruidClient = _

  /**
   * Get the Spark SQL native DataType from Druid's property.
   */
  private def getSparkSQLDataType(druidType: String): DataType = {
    try {
      CatalystSqlParser.parseDataType(druidType)
    } catch {
      case e: ParseException =>
        throw new SparkException("Cannot recognize Druid type string: " + druidType, e)
    }
  }

  /**
   * Converts the native StructField to Druid's name and type.
   */
  def toDruidColumn(c: StructField): (String, String) = {
    val typeString = if (c.metadata.contains(DRUID_TYPE_STRING)) {
      c.metadata.getString(DRUID_TYPE_STRING)
    } else {
      c.dataType.simpleString
    }
    (c.name, typeString)
  }

  /**
   * Converts Druid's name and type to the native StructField.
   */
  def fromDruidColumn(fieldName: String, dataType: String): StructField = {
    val columnType = getSparkSQLDataType(dataType)
    val metadata = if (!dataType.equalsIgnoreCase(columnType.catalogString)) {
      new MetadataBuilder().putString(DRUID_TYPE_STRING, dataType).build()
    } else {
      Metadata.empty
    }

    StructField(name = fieldName, dataType = columnType, nullable = true, metadata = metadata)
  }

  /**
  override def doParse(
      dataSourceName: String,
      infos: Map[String, String],
      dataSourcesByName: HashMap[String, DataSource],
      dataSourceToCatalogDatabase: HashMap[String, HashMap[String, CatalogDatabase]],
      dbToCatalogTable: HashMap[Int, HashMap[String, CatalogTable]]): Unit = {
    val url = infos.get(s"$dataSourceName.uri")
    val coordinator = infos.get(s"$dataSourceName.coordinator.uri")
    if (url == None) {
      throw new SparkException("Data source is druid must have uri!")
    }

    val ds: DataSource = new DruidDataSource(
      dataSourceName,
      DRUID,
      this,
      url.get,
      infos.get(s"$dataSourceName.user").get,
      infos.get(s"$dataSourceName.password").get,
      infos.get(s"$dataSourceName.version").get)

    dataSourcesByName(dataSourceName) = ds

    implicit val executionContext = ExecutionContext.Implicits.global
    val client = DruidClient(url.get)
    dataSourceToRestClient += ((dataSourceName, client))
    val xdatabases = dataSourceToCatalogDatabase.getOrElseUpdate(
      ds.getName, new HashMap[String, CatalogDatabase])
    val db = CatalogDatabase(
      id = newDatabaseId,
      dataSourceName = dataSourceName,
      name = "druid",
      description = null,
      locationUri = null,
      properties = Map.empty)
    xdatabases += ((db.name, db))

    val (whiteTables, blackTables) = getWhiteAndBlackTables(ds.getName, db.name)
    val xtables = dbToCatalogTable.getOrElseUpdate(db.id, new HashMap[String, CatalogTable])
    client.showTables(DATASOURCES).filter {
      tableName => isSelectedTable(whiteTables, blackTables, tableName)
    }.foreach { datasource =>
        // get schema
        // /druid/v2?petty
        // post {
        //  "queryType": "segmentMetadata",
        //  "dataSource": "sys_shbt_abtestmobile_logshare_event_kafka"
        // }
        val fields: ArrayBuffer[StructField] = ArrayBuffer.empty
        client.descTable(datasource).foreach { data =>
          // StructField(data._1, CatalystSqlParser.parseDataType(data._2.toString))
          fields += fromDruidColumn(data._1,
              if (data._1.equals("__time")) "STRING"
              // Druid change doublesum data save as float type
              else if (data._2.toString.equalsIgnoreCase("float")) "double"
              else data._2.toString
          )
        }

        // add granularity column for where granularity='hour'
        fields += StructField("granularity", StringType)
        val schema = StructType(fields)
        val table = CatalogTable(
          // Druid has only one "database" so we call it druid
          identifier = TableIdentifier(datasource, Option("druid"), Option(dataSourceName)),
          tableType = CatalogTableType.DATASOURCE,
          storage = CatalogStorageFormat(
            locationUri = None,
            inputFormat = Some(INPUT_FORMAT),
            outputFormat = Some(OUTPUT_FORMAT),
            serde = None,
            compressed = false,
            properties = Map(
              "url" -> url.get,
              "coordinator" -> coordinator.get,
              "datasource" -> datasource,
              "timestampcolumn" -> "__time"
            )
          ),
          schema = schema,
          provider = Some(PROVIDER)
        )
        xtables += ((datasource, table))
    }

  }
   */
  /**
   * Cache database.
   */
  override protected def cacheDatabase(
      isDefault: Boolean,
      dataSourceName: String,
      infos: Map[String, String],
      dataSourcesByName: HashMap[String, CatalogDataSource],
      dataSourceToCatalogDatabase: HashMap[String, HashMap[String, CatalogDatabase]]): Unit = {
    val url = infos.get(URL)
    val coordinator = infos.get(COORDINATORURL)
    if (url == None) {
      throw new SparkException("Data source is druid must have url!")
    }

    val ds: CatalogDataSource = new DruidDataSource(
      dataSourceName,
      DRUID,
      this,
      url.get,
      infos.get(USER).get,
      infos.get(PASSWORD).get,
      infos.get(VERSION).get)

    dataSourcesByName(dataSourceName) = ds

    implicit val executionContext = ExecutionContext.Implicits.global
    restClient = DruidClient(url.get)
    restClient.coordinator = coordinator.get
    val xdatabases = dataSourceToCatalogDatabase.getOrElseUpdate(
      ds.getName,
      new HashMap[String, CatalogDatabase])

    val db = CatalogDatabase(
      id = newDatabaseId,
      dataSourceName = dataSourceName,
      name = "druid",
      description = null,
      locationUri = null,
      properties = Map.empty)
    xdatabases += ((db.name, db))
  }

  override protected def cacheTable(
      dataSourceName: String,
      dataSourceToCatalogDatabase: HashMap[String, HashMap[String, CatalogDatabase]],
      dbToCatalogTable: HashMap[Int, HashMap[String, CatalogTable]]): Unit = {
    val xdatabases = dataSourceToCatalogDatabase(dataSourceName)
    // Druid has only one "database" named druid
    val db = xdatabases("druid")
    val (whiteTables, blackTables) = getWhiteAndBlackTables(db.name)
    val xtables = dbToCatalogTable.getOrElseUpdate(db.id, new HashMap[String, CatalogTable])
    restClient
      .showTables(DATASOURCESAPI)
      .filter { tableName =>
        isSelectedTable(whiteTables, blackTables, tableName)
      }
      .foreach { datasource =>
        // get schema
        // /druid/v2?petty
        // post {
        //  "queryType": "segmentMetadata",
        //  "dataSource": "sys_shbt_abtestmobile_logshare_event_kafka"
        // }
        val fields: ArrayBuffer[StructField] = ArrayBuffer.empty
        restClient.descTable(datasource).foreach { data =>
          // StructField(data._1, CatalystSqlParser.parseDataType(data._2.toString))
          fields += fromDruidColumn(
            data._1,
            if (data._1.equals("__time")) "STRING"
            // Druid change doublesum data save as float type
            else if (data._2.toString.equalsIgnoreCase("float")) "double"
            else data._2.toString)
        }

        // add granularity column for where granularity='hour'
        fields += StructField("granularity", StringType)
        val schema = StructType(fields)
        val table = CatalogTable(
          // Druid has only one "database" named druid
          identifier = TableIdentifier(datasource, Option("druid"), Option(dataSourceName)),
          tableType = CatalogTableType.DATASOURCE,
          storage = CatalogStorageFormat(
            locationUri = None,
            inputFormat = Some(INPUT_FORMAT),
            outputFormat = Some(OUTPUT_FORMAT),
            serde = None,
            compressed = false,
            properties = Map(
              "url" -> restClient.serverUrl,
              "coordinator" -> restClient.coordinator,
              "datasource" -> datasource,
              "timestampcolumn" -> "__time")),
          schema = schema,
          provider = Some(PROVIDER))
        xtables += ((datasource, table))
      }
  }

  /**
   * Druid has only one "database" named druid
   */
  override def listDatabases(dataSourceName: String): Seq[String] = {
    Seq("druid")
  }

  /**
   * Check table exists or not.
   */
  override def tableExists(dbName: String, table: String): Boolean = {
    restClient
      .showTables(DATASOURCESAPI)
      .filter { tableName =>
        tableName.equals(table)
      }
      .size == 1
  }

  override def listTables(dsName: String, dbName: String): Seq[String] = {
    val (whiteTables, blackTables) = getWhiteAndBlackTables(dbName)
    restClient.showTables(DATASOURCESAPI).filter { tableName =>
      isSelectedTable(whiteTables, blackTables, tableName)
    }
  }

  override def doGetRawTable(
      db: String,
      originDB: String,
      table: String): Option[CatalogTable] = {
    val fields: ArrayBuffer[StructField] = ArrayBuffer.empty
    restClient.descTable(table).foreach { data =>
      // StructField(data._1, CatalystSqlParser.parseDataType(data._2.toString))
      fields += fromDruidColumn(
        data._1,
        if (data._1.equals("__time")) "STRING"
        // Druid change doublesum data save as float type
        else if (data._2.toString.equalsIgnoreCase("float")) "double"
        else data._2.toString)
    }
    // add granularity column for where granularity='hour'
    fields += StructField("granularity", StringType)
    val schema = StructType(fields)
    Option(
      CatalogTable(
        // Druid has only one "database" named druid
        identifier = TableIdentifier(table, Option("druid"), Option(dsName)),
        tableType = CatalogTableType.DATASOURCE,
        storage = CatalogStorageFormat(
          locationUri = None,
          inputFormat = Some(INPUT_FORMAT),
          outputFormat = Some(OUTPUT_FORMAT),
          serde = None,
          compressed = false,
          properties = Map(
            "url" -> restClient.serverUrl,
            "coordinator" -> restClient.coordinator,
            "datasource" -> table,
            "timestampcolumn" -> "__time")),
        schema = schema,
        provider = Some(PROVIDER)))
  }

  import DruidManager._

  /**
   * parsing EqualTo filter,such as 'where a=1'
   */
  override def buildEqualToArgument(
      searchArg: util.HashMap[String, Any],
      attribute: String,
      value: Any): Unit = {
    if (attribute.equalsIgnoreCase("granularity")) {
      searchArg.put("granularity", value.toString)
    } else {
      val map = new util.HashMap[String, Any]
      map.put(DRUID_TYPE, SELECTOR)
      map.put(DIMENSION, attribute)
      map.put(VALUE, value)
      searchArg.put(FILTER, map)
    }
  }

  /**
   * parsing AND filter,such as 'where a=1 and c<10'
   */
  override def buildAndArgument(
      searchArg: util.HashMap[String, Any],
      leftSearchArg: util.HashMap[String, Any],
      rightSearchArg: util.HashMap[String, Any]): Unit = {
    val list = new util.ArrayList[Any]()
    searchArg.putAll(leftSearchArg)
    searchArg.putAll(rightSearchArg)
    val leftVal = leftSearchArg.get(FILTER)
    val rightVal = rightSearchArg.get(FILTER)
    if (leftVal != null) list.add(leftVal)
    if (rightVal != null) list.add(rightVal)
    val andArg = new util.HashMap[String, Any]
    if (list.size() == 2) {
      andArg.put(DRUID_TYPE, AND)
      andArg.put(FILEDS, list)
      searchArg.put(FILTER, andArg)
    } else if (list.size() == 1) {
      searchArg.put(FILTER, list.get(0))
    }
  }

  /**
   * parsing OR filter,such as 'where a=1 and c<10'
   */
  override def buildOrArgument(
      searchArg: util.HashMap[String, Any],
      leftSearchArg: util.HashMap[String, Any],
      rightSearchArg: util.HashMap[String, Any]): Unit = {
    val list = new util.ArrayList[Any]()
    searchArg.putAll(leftSearchArg)
    searchArg.putAll(rightSearchArg)
    val leftVal = leftSearchArg.get(FILTER)
    val rightVal = rightSearchArg.get(FILTER)
    if (leftVal != null) list.add(leftVal)
    if (rightVal != null) list.add(rightVal)
    val orArg = new util.HashMap[String, Any]
    if (list.size() == 2) {
      orArg.put(DRUID_TYPE, OR)
      orArg.put(FILEDS, list)
      searchArg.put(FILTER, orArg)
    } else if (list.size() == 1) {
      searchArg.put(FILTER, list.get(0))
    }
  }

  /**
   * parsing LessThan filter,such as 'where c<10'
   */
  override def buildLessThanArgument(
      searchArg: util.HashMap[String, Any],
      attribute: String,
      value: Any): Unit = {
    notEqualQueryFilter(searchArg, attribute, value, "LessThan")
  }

  /**
   * parsing LessThan filter,such as 'where c<=10'
   */
  override def buildLessThanOrEqualArgument(
      searchArg: util.HashMap[String, Any],
      attribute: String,
      value: Any): Unit = {
    notEqualQueryFilter(searchArg, attribute, value, "LessThanOrEqual")
  }

  private def notEqualQueryFilter(
      searchArg: util.HashMap[String, Any],
      attribute: String,
      value: Any,
      symbolType: String) = {
    val map = new util.HashMap[String, Any]
    attribute.toLowerCase match {
      case "__time" =>
        symbolType.toLowerCase match {
          case "lessthan" => searchArg.put("endTime", value)
          case "lessthanorequal" =>
            searchArg.put("endTime", new DateTime(value).getMillis + 1)
          case "greaterthanorequal" => searchArg.put("startTime", value)
          case "greaterthan" => searchArg.put("startTime", new DateTime(value).getMillis + 1)
        }
        searchArg
      case _ =>
        map.put(DRUID_TYPE, BOUND)
        map.put(DIMENSION, attribute)
        if (symbolType.equalsIgnoreCase("LessThan")) {
          map.put(LOWERSTRICT, true)
        } else if (symbolType.equalsIgnoreCase("GreaterThan")) {
          map.put(UPPERSTRICT, true)
        }

        symbolType.toLowerCase match {
          case "lessthan" =>
            map.put(LOWERSTRICT, true)
            map.put(UPPER, value)
            map.put(LOWER, 0)
          case "lessthanorequal" =>
            map.put(UPPER, value)
            map.put(LOWER, 0)
          case "greaterthan" =>
            map.put(UPPERSTRICT, true)
            map.put(UPPER, 0)
            map.put(LOWER, value)
          case "greaterthanorequal" =>
            map.put(UPPER, 0)
            map.put(LOWER, value)
        }

        if (value.isInstanceOf[IntegerType] ||
            value.isInstanceOf[DoubleType] ||
            value.isInstanceOf[FloatType] ||
            value.isInstanceOf[LongType]) {
          map.put(ORDERING, NUMERTIC)
        }
        searchArg.put(FILTER, map)
    }
  }

  /**
   * parsing greaterThan filter,such as 'where c>10'
   */
  override def buildGreaterThanArgument(
      searchArg: util.HashMap[String, Any],
      attribute: String,
      value: Any): Unit = {
    notEqualQueryFilter(searchArg, attribute, value, "GreaterThan")
  }

  /**
   * parsing greaterThanOrEqual filter,such as 'where c>=10'
   */
  override def buildGreaterThanOrEqualArgument(
      searchArg: util.HashMap[String, Any],
      attribute: String,
      value: Any): Unit = {
    notEqualQueryFilter(searchArg, attribute, value, "GreaterThanOrEqual")
  }

  /**
   * parsing not filter,such as 'where c!=10'
   */
  override def buildNotArgument(
      searchArg: util.HashMap[String, Any],
      childSearchArg: util.HashMap[String, Any]): Unit = {
    val map = new util.HashMap[String, Any]
    map.put(DRUID_TYPE, NOT)
    map.put(FILED, childSearchArg.get(FILTER))
    searchArg.put(FILTER, map)
  }

  /**
   * parsing isNull filter,such as 'where c is null'
   */
  override def buildIsNullArgument(
      searchArg: util.HashMap[String, Any],
      childSearchArg: util.HashMap[String, Any]): Unit = {
    val attribute = childSearchArg.get(FILED)
    if (null != attribute && attribute.toString.equalsIgnoreCase("granularity")) {
      searchArg.put("granularity", "")
    } else {
      val map = new util.HashMap[String, Any]
      map.put(DRUID_TYPE, SELECTOR)
      map.put(DIMENSION, attribute)
      map.put(VALUE, "")
      searchArg.put(FILTER, map)
    }
  }

  /**
   * parsing isNotNull filter,such as 'where c is not null'
   */
  override def buildIsNotNullArgument(
      searchArg: util.HashMap[String, Any],
      childSearchArg: util.HashMap[String, Any]): Unit = {
    val attribute = childSearchArg.get(FILED)
    if (null != attribute && attribute.toString.equalsIgnoreCase("granularity")) {
      searchArg.put("granularity", "")
    } else {
      val map = new util.HashMap[String, Any]
      val map2 = new util.HashMap[String, Any]
      map.put(DRUID_TYPE, SELECTOR)
      map.put(DIMENSION, attribute)
      map.put(VALUE, "")
      map2.put(DRUID_TYPE, NOT)
      map2.put(FILED, map)
      searchArg.put(FILTER, map2)
    }
  }

  /**
   * parsing like filter,such as 'where c like "%abc%"'
   */
  override def buildLikeArgument(
      searchArg: util.HashMap[String, Any],
      attribute: String,
      value: Any): Unit = {
    val map = new util.HashMap[String, Any]
    map.put(DRUID_TYPE, LIKE)
    map.put(DIMENSION, attribute)
    map.put(PATTERN, value)
    searchArg.put(FILTER, map)
  }

  /**
   * parsing regex filter,such as 'where c REGEXP "ok$"'
   */
  override def buildRLikeArgument(
      searchArg: util.HashMap[String, Any],
      attribute: String,
      value: Any): Unit = {
    val map = new util.HashMap[String, Any]
    map.put(DRUID_TYPE, REGEX)
    map.put(DIMENSION, attribute)
    map.put(VALUE, value)
    searchArg.put(FILTER, map)
  }

  /**
   * parsing in filter,such as 'where c in (1,2,3)'
   */
  override def buildInArgument(
      searchArg: util.HashMap[String, Any],
      attribute: String,
      values: util.LinkedList[Any]): Unit = {
    val map = new util.HashMap[String, Any]
    map.put(DRUID_TYPE, IN)
    map.put(DIMENSION, attribute)
    map.put(VALUES, values)
    searchArg.put(FILTER, map)
  }

  override def buildAttributeArgument(
      searchArg: util.HashMap[String, Any],
      attribute: String): Unit = {
    searchArg.put(FILED, attribute)
  }

  /**
   * Build group argument in query.
   */
  override def buildGroupArgument(
      fieldName: String,
      groupByKeys: ArrayBuffer[String],
      innerAggsMap: util.HashMap[String, Any],
      groupMap: util.HashMap[String, Any]): util.HashMap[String, Any] = {
    groupMap.put(fieldName, fieldName)
    groupByKeys += fieldName
    groupMap
  }

  /**
   * add aggs to aggsMap
   */
  private def addAggsItem(
      aggsMap: util.HashMap[String, Any],
      aggregateAliasName: String,
      tuple: (String, String),
      functionName: String = "count_distinct"): String = {
    if (!aggsMap.containsKey(aggregateAliasName)) {
      aggsMap.put(aggregateAliasName, tuple)
      aggregateAliasName
    } else {
      if (functionName.equals("count_distinct")) {
        val t = aggsMap.get(aggregateAliasName).asInstanceOf[Tuple2[String, String]]
        aggsMap.put(aggregateAliasName, (t._1.toString + "|" + tuple._1, t._2))
        aggregateAliasName
      } else {
        var alias = ""
        var numStr = ""
        var preStr = ""
        if (aggregateAliasName.lastIndexOf("_") > 0) {
          numStr = aggregateAliasName.substring(aggregateAliasName.lastIndexOf("_") + 1)
          preStr = aggregateAliasName.substring(0, aggregateAliasName.lastIndexOf("_"))
          if (numStr.isEmpty || numStr.isInstanceOf[String]) {
            alias += 2
          } else {
            alias = preStr + (Integer.parseInt(numStr) + 1)
          }
        } else {
          preStr = aggregateAliasName
          alias = preStr + "_2"
        }
        addAggsItem(aggsMap, alias, tuple)
        alias
      }
    }
  }

  /**
   * parsing agg args
   */
  override def buildAggregateArgument(
      structField: StructField,
      aggregatefuncName: String,
      aggsMap: util.HashMap[String, Any],
      aggregateAliasOpts: Option[String]): StructField = {
    val fieldName = structField.name
    val aggfuncName = aggregatefuncName match {
      case "value_count" => "count"
      case _ => aggregatefuncName
    }

//    val dataType = toDruidColumn(c)._2
//    val aggAliasName = dataType.toLowerCase match {
//      case "hyperunique" => "hyperUnique"
//      case "cardinality" => "cardinality"
//      case "thetasketch" => "thetaSketch"
//      case _ => aggregatefuncName
//    }

    var aggregateAliasName = aggregateAliasOpts.getOrElse(fieldName + "_" + aggregatefuncName)

    aggfuncName match {
      case "count_distinct" =>
        aggregateAliasName = addAggsItem(
          aggsMap,
          aggregateAliasOpts.getOrElse("count_distinct"),
          (fieldName, aggfuncName))
        new StructField(aggregateAliasName, LongType)
      case "count" =>
        aggregateAliasName = addAggsItem(aggsMap, aggregateAliasName, (fieldName, aggfuncName))
        new StructField(aggregateAliasName, LongType)
      case _ =>
        aggregateAliasName = addAggsItem(aggsMap, aggregateAliasName, (fieldName, aggfuncName))
        new StructField(aggregateAliasName, structField.dataType)
    }
  }

  /**
   * Build sort argument in query without aggregate.
   */
  override def buildSortArgument(order: Seq[SortOrder]): util.List[util.HashMap[String, Any]] = {
    new LinkedList[java.util.HashMap[String, Any]]
  }

  /**
   * Build sort argument in query with aggregate.
   */
  override def buildSortArgumentInAggregate(
      parentGroupMap: util.HashMap[String, Any],
      aggs: util.HashMap[String, Any],
      expr: expressions.Expression,
      direction: SortDirection,
      isDistinct: Boolean): Unit = {
    expr match {
      case sum @ Sum(child) =>
        addOrderByColumn(aggs, direction.sql, child, "sum")
      case max @ Max(child) =>
        addOrderByColumn(aggs, direction.sql, child, "max")
      case min @ Min(child) =>
        addOrderByColumn(aggs, direction.sql, child, "min")
      case attr: UnresolvedAttribute =>
        addOrderByColumn(aggs, direction.sql, attr, "")
    }
  }

  private def addOrderByColumn(
      aggs: util.HashMap[String, Any],
      direction: String,
      child: expressions.Expression,
      functionName: String) = {
    val name = child.asInstanceOf[UnresolvedAttribute].name
    val orderByColumn =
      if (functionName.isEmpty) name
      else name + "_" + functionName
    if (aggs.containsKey(orderByColumn) || aggs.containsKey(name)) {
      if (aggs.containsKey("SORT")) {
        aggs
          .get("SORT")
          .asInstanceOf[util.ArrayList[Tuple3[String, String, String]]]
          .add(Tuple3(orderByColumn, direction, "numeric"))
      } else {
        val list = new util.ArrayList[Tuple3[String, String, String]]()
        list.add(Tuple3(orderByColumn, direction, "numeric"))
        aggs.put("SORT", list)
      }
    } else {
      if (aggs.containsKey("SORT")) {
        aggs
          .get("SORT")
          .asInstanceOf[util.ArrayList[Tuple3[String, String, String]]]
          .add(Tuple3(name, direction, "lexicographic"))
      } else {
        val list = new util.ArrayList[Tuple3[String, String, String]]()
        list.add(Tuple3(name, direction, "lexicographic"))
        aggs.put("SORT", list)
      }
    }
  }

  override def scanXSQLTable(
      dataSourcesByName: HashMap[String, CatalogDataSource],
      tableDefinition: CatalogTable,
      columns: java.util.List[String],
      condition: java.util.HashMap[String, Any],
      sort: java.util.List[java.util.HashMap[String, Any]],
      aggs: java.util.HashMap[String, Any],
      groupByKeys: Seq[String],
      limit: Int): Seq[Seq[Any]] = {

    val orderBy = if (null != aggs && aggs.containsKey("SORT")) {
      aggs.get("SORT").asInstanceOf[util.ArrayList[Tuple3[String, String, String]]]
      aggs.remove("SORT")
    } else {
      new util.ArrayList[Tuple3[String, String, String]]()
    }

    var groupByKeysClone = groupByKeys
    try {
      if (groupByKeys == null && null != aggs && aggs.size() > 0) {
        // select sum(l_extendedprice) from mydruid.druid.lineitem10 transform to
        // select sum(l_extendedprice) from mydruid.druid.lineitem10 group by __time
        groupByKeysClone = new ArrayBuffer[String](1)
        groupByKeysClone.asInstanceOf[ArrayBuffer[String]].append("__time")
      }
      val ds = dataSourcesByName(tableDefinition.identifier.dataSource.get)
      val result: ArrayBuffer[Seq[Any]] = ArrayBuffer.empty
      implicit val executionContext = ExecutionContext.Implicits.global
      if (null == condition) {
        throw new RuntimeException(
          "Durid must have startime and endtime,__time>'2010-09-01' and " +
            "__time <='2010-09-03'")
      }
      val startTime = condition.get("startTime")
      val endTime = condition.get("endTime")
      var granularity = condition.get("granularity")
      if (null == granularity) {
        granularity = "all"
      }
      val filter =
        if (null == condition.get("filter")) QueryFilter.All
        else AllQueryFilter(condition)
      val aggregates = ArrayBuffer[Aggregation]()
      if (null != aggs) {
        aggs.asScala.foreach { agg =>
          val groupMap = agg._2.asInstanceOf[Tuple2[String, String]]
          var column = groupMap._1
          val funcName = groupMap._2
          var schemas: StructField = null
          var dataType: DataType = null
          if (!column.contains("|")) {
            if (column.toString.equalsIgnoreCase("*")) {
              column = "__time"
              dataType = StringType
            } else {
              schemas = tableDefinition.schema(column.toString)
              dataType = schemas.dataType
            }
          }

          funcName match {
            case "sum" =>
              dataType match {
                case LongType => aggregates += DSL.sum(column.toString, agg._1)
                case DoubleType => aggregates += DSL.doubleSum(column.toString, agg._1)
              }
            case "max" =>
              dataType match {
                case LongType => aggregates += DSL.max(column.toString, agg._1)
                case DoubleType => aggregates += DSL.doubleMax(column.toString, agg._1)
              }
            case "min" =>
              dataType match {
                case LongType => aggregates += DSL.min(column.toString, agg._1)
                case DoubleType => aggregates += DSL.doubleMin(column.toString, agg._1)
              }
            case "count" => aggregates += DSL.count(agg._1)
            case _ =>
              if (funcName.contains("count_distinct")) {
                val columns = column.split("\\|")
                // (count111,(user_unique,count_distinct))
                if (columns.length > 1) {
                  aggregates += DSL.countMultDistinct(columns, agg._1)
                } else {
                  schemas = tableDefinition.schema(column.toString)
                  val dataType = toDruidColumn(schemas)._2
                  aggregates += DSL.countdistinct(column.toString, dataType, agg._1)
                }
              }
          }
        }
      }
      if (groupByKeysClone == null) {
        val desc =
          orderBy.asInstanceOf[util.ArrayList[Tuple3[String, String, String]]].asScala.filter {
            t =>
              t._1 match {
                case "__time" => true
                case _ => false
              }
          }
        var dir = "false"
        if (desc.size > 0) {
          dir = desc(0)._2.equalsIgnoreCase("desc").toString
        }
        // Druid select query
        val query = SelectQuery(
          source = tableDefinition.identifier.table,
          descending = dir,
          interval = new Interval(new DateTime(startTime), new DateTime(endTime)),
          granularity = getGranularity(granularity.toString).get,
          dimensions = columns.asScala.toArray,
          filter = filter,
          limit = PagingSpec(null, if (limit > 0) limit else 20))
        var data: Seq[Map[String, Any]] = null
        val future = restClient.querySelect(query)
        future.onComplete {
          case Success(resp) => data = resp.data
          case Failure(ex) => throw new RuntimeException(ex)
        }
        while (!future.isCompleted) {
          logInfo("sleep 100ms")
          Thread.sleep(100)
        }
        converedData(result, data, columns)
      } else if (groupByKeysClone.size == 1
                 && groupByKeysClone(0).equalsIgnoreCase("__time")) {
        val desc =
          orderBy.asInstanceOf[util.ArrayList[Tuple3[String, String, String]]].asScala.filter {
            t =>
              t._1 match {
                case "__time" => true
                case _ => false
              }
          }
        var dir = "false"
        if (desc.size > 0) {
          dir = desc(0)._2.equalsIgnoreCase("desc").toString
        }
        // Druid timeseries query
        val query = TimeSeriesQuery(
          source = tableDefinition.identifier.table,
          interval = new Interval(new DateTime(startTime), new DateTime(endTime)),
          descending = dir,
          granularity = getGranularity(granularity.toString).get,
          aggregate = aggregates,
          filter = filter)
        val future = restClient.queryTimeSeries(query)
        var data: Seq[(DateTime, Map[String, Any])] = null
        future.onComplete {
          case Success(resp) => data = resp.data
          case Failure(ex) => throw new RuntimeException(ex)
        }
        while (!future.isCompleted) {
          logInfo("sleep 100ms")
          Thread.sleep(100)
        }
        logInfo("get TimeSeriesQuery result and the size is " + data.size)

        val druidTypeToConver =
          aggregates.map(a => (a.outputName, a.typeName)).toMap[String, String]

        converedToTsRow(result, data, columns, druidTypeToConver)
      } else {
        if (groupByKeysClone.size == 1 &&
            orderBy.asInstanceOf[util.ArrayList[Tuple3[String, String, String]]].size() == 1 &&
            limit > 0) {
          // topN
          var m: Metric = null
          val orderByValue = orderBy
            .asInstanceOf[util.ArrayList[Tuple3[String, String, String]]]
            .get(0)
          val orderByName = orderByValue._1
          val direction = orderByValue._2
          val orderType = orderByValue._3
          if (orderType.equalsIgnoreCase("lexicographic")) {
            m = Metric(metric = orderByName)
          } else if (direction.equalsIgnoreCase("desc")) {
            m = Metric("numberic", orderByName)
          } else {
            m = Metric("inverted", orderByName)
          }
          // Druid topN query
          val query = TopNSelectQuery(
            source = tableDefinition.identifier.table,
            dimension = groupByKeysClone(0),
            metric = m,
            interval = new Interval(new DateTime(startTime), new DateTime(endTime)),
            granularity = getGranularity(granularity.toString).get,
            aggregate = aggregates,
            filter = filter,
            limit = limit)

          if (granularity.toString.equalsIgnoreCase("all")) {
            val future = restClient.queryTopN(query)
            var data: Seq[Map[String, Any]] = null
            future.onComplete {
              case Success(resp) => data = resp.data
              case Failure(ex) => throw new RuntimeException(ex)
            }
            while (!future.isCompleted) {
              logInfo("sleep 100ms")
              Thread.sleep(100)
            }
            val druidTypeToConver =
              aggregates.map(a => (a.outputName, a.typeName)).toMap[String, String]
            converedData(result, data, columns, druidTypeToConver)
          } else {
            val future = restClient.queryTopN2(query)
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

            val druidTypeToConver =
              aggregates.map(a => (a.outputName, a.typeName)).toMap[String, String]

            converedToTsRow4TopN(result, data, columns, druidTypeToConver)
          }
        } else {
          // group by multiple columns
          var order = new ArrayBuffer[ColumnOrder]
          orderBy.asInstanceOf[util.ArrayList[Tuple3[String, String, String]]].asScala.map { r =>
            val orderByName = r._1
            val direction =
              if (r._2.equalsIgnoreCase("desc")) "descending"
              else "ascending"
            val orderType = r._3
            if (orderType.equalsIgnoreCase("lexicographic")) {
              order += ColumnOrder(orderByName, direction)
            } else {
              order += ColumnOrder(orderByName, direction, "numeric")
            }
          }

          groupByKeysClone = groupByKeysClone.filter(!_.equalsIgnoreCase("__time"))
          // Druid groupby query
          val query = GroupByQuery(
            source = tableDefinition.identifier.table,
            granularity = getGranularity(granularity.toString).get,
            dimensions = groupByKeysClone,
            aggregate = aggregates,
            interval = new Interval(new DateTime(startTime), new DateTime(endTime)),
            filter = filter,
            orderBy = order,
            limit = Some(if (limit > 0) limit else 20))
          val druidTypeToConver =
            aggregates.map(a => (a.outputName, a.typeName)).toMap[String, String]

          var data: Seq[(DateTime, Map[String, Any])] = null
          val future = restClient.queryGroupBy(query)
          future.onComplete {
            case Success(resp) => data = resp.data
            case Failure(ex) => throw new RuntimeException(ex)
          }
          while (!future.isCompleted) {
            logInfo("sleep 100ms")
            Thread.sleep(100)
          }
          converedToTsRow(result, data, columns, druidTypeToConver)
        }
      }
      result
    } catch {
      case ex: Throwable => throw new RuntimeException(ex)
    }
  }

  /**
   * convered query result to ArrayBuffer
   */
  def converedData(
      result: ArrayBuffer[Seq[Any]],
      data: Seq[Map[String, Any]],
      columns: util.List[String],
      druidTypeToConver: Map[String, Any] = null): ArrayBuffer[Seq[Any]] = {
    val numColumns = columns.size()
    if (null != data) {
      for (d <- data) {
        val row = new Array[Any](numColumns)
        for (id <- 0 until numColumns) {
          val value = d.getOrElse(columns.get(id), null)
          if (columns.get(id) == "__time") {
            row(id) = d.getOrElse("timestamp", null)
          } else {
            row(id) = value match {
              case v: BigInt => v.toLong
              case v: Double =>
                if (druidTypeToConver != null) {
                  val t = druidTypeToConver.get(columns.get(id)).get.toString
                  if ((t.equalsIgnoreCase("hyperunique") ||
                      t.equalsIgnoreCase("cardinality") ||
                      t.equalsIgnoreCase("thetasketch"))) {
                    v.longValue() + 1
                  } else v
                } else v
              case _ => value
            }
          }
        }
        result += row
      }
    }
    result
  }

  /**
   * convered query result with timeseries to ArrayBuffer
   */
  def converedToTsRow(
      result: ArrayBuffer[Seq[Any]],
      data: Seq[(DateTime, Map[String, Any])],
      columns: util.List[String],
      druidTypeToConver: Map[String, Any] = null): ArrayBuffer[Seq[Any]] = {
    val numColumns = columns.size()
    if (null != data) {
      for ((t, d) <- data) {
        val row = new Array[Any](numColumns)
        for (id <- 0 until numColumns) {
          val value = d.getOrElse(columns.get(id), null)
          if (columns.get(id) == "__time") {
            row(id) = t.toString
          } else {
            row(id) = value match {
              case v: BigInt => v.toLong
              case v: Double =>
                if (druidTypeToConver != null) {
                  val t = druidTypeToConver.get(columns.get(id)).get.toString
                  if ((t.equalsIgnoreCase("hyperunique") ||
                      t.equalsIgnoreCase("cardinality") ||
                      t.equalsIgnoreCase("thetasketch"))) {
                    v.longValue() + 1
                  } else v
                } else v
              case _ => value
            }
          }
        }
        result += row
      }
    }
    result
  }

  /**
   * convered TopN query result to ArrayBuffer
   */
  def converedToTsRow4TopN(
      result: ArrayBuffer[Seq[Any]],
      data: Seq[(DateTime, Seq[Map[String, Any]])],
      columns: util.List[String],
      druidTypeToConver: Map[String, Any] = null): ArrayBuffer[Seq[Any]] = {
    val numColumns = columns.size()
    if (null != data) {
      for ((t, d) <- data) {
        for (dd <- d) {
          val row = new Array[Any](numColumns)
          for (id <- 0 until numColumns) {
            val value = dd.getOrElse(columns.get(id), null)
            if (columns.get(id) == "__time") {
              row(id) = t.toString
            } else {
              row(id) = value match {
                case v: BigInt => v.toLong
                case v: Double =>
                  if (druidTypeToConver != null) {
                    val t = druidTypeToConver.get(columns.get(id)).get.toString
                    if ((t.equalsIgnoreCase("hyperunique") ||
                        t.equalsIgnoreCase("cardinality") ||
                        t.equalsIgnoreCase("thetasketch"))) {
                      v.longValue() + 1
                    } else v
                  } else v
                case _ => value
              }
            }
          }
          result += row
        }
      }
    }
    result
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

  /**
   * Get default options from CatalogTable.
   */
  override def getDefaultOptions(table: CatalogTable): Map[String, String] =
    Map.empty[String, String]

  override def stop(): Unit = {
    restClient.close()
  }

}

object DruidManager {
  val DATASOURCESAPI = "/druid/v2/datasources"
  val SCHEMAS = "/druid/v2/?pretty"
  val PROVIDER = "org.apache.spark.sql.execution.datasources.druid"
  val INPUT_FORMAT = "org.apache.spark.sql.execution.datasources.druid.DruidInputFormat"
  val OUTPUT_FORMAT = ""
  val DRUID_TYPE = "type"
  val SELECTOR = "selector"
  val DIMENSION = "dimension"
  val VALUE = "value"
  val FILTER = "filter"
  val FILEDS = "fields"
  val FILED = "field"
  val AND = "and"
  val OR = "or"
  val NOT = "not"
  val BOUND = "bound"
  val LOWER = "lower"
  val UPPER = "upper"
  val LOWERSTRICT = "lowerStrict"
  val UPPERSTRICT = "upperStrict"
  val ORDERING = "ordering"
  val NUMERTIC = "numeric"
  val LIKE = "like"
  val REGEX = "regex"
  val IN = "in"
  val VALUES = "values"
  val PATTERN = "pattern"
}
