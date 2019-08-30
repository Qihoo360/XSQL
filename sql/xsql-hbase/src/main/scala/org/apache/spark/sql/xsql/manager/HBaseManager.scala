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

import java.io.{File, IOException}
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}

import net.sf.json.{JSONArray, JSONObject}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.util.Bytes

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.{expressions, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.{
  CatalogDatabase,
  CatalogStorageFormat,
  CatalogTable,
  CatalogTableType
}
import org.apache.spark.sql.catalyst.expressions.{
  AttributeReference,
  Contains,
  EndsWith,
  Expression,
  Length,
  Like,
  Literal,
  PredicateHelper,
  StartsWith
}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.datasources.hbase.{
  HBaseRelation,
  HBaseRelationImpl,
  HBaseTableCatalog,
  HBaseTableScanUtil
}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.xsql._
import org.apache.spark.sql.xsql.types._
import org.apache.spark.sql.xsql.util.Utils.getPropertiesFile

/**
 * extend PredicateHelper to mixin splitConjunctivePredicates method
 */
private[spark] class HBaseManager extends DataSourceManager with Logging with PredicateHelper {

  import DataSourceManager._
  import HBaseManager._

  override def shortName(): String = DataSourceType.HBASE.toString

  override lazy val schemaReader: (String, HashMap[String, HashMap[String, JSONArray]]) => Unit =
    HBaseManager.schemaReader

  private def getHBaseConfiguration(): String = {
    val JSONConfig = new JSONObject()
    JSONConfig.put(HOST, cachedProperties(SHORT_HOST))
    JSONConfig.put(PORT, cachedProperties.getOrElse(SHORT_PORT, DEFAULT_PORT))
    JSONConfig.put(ZK_PARENT, cachedProperties.getOrElse(ZK_PARENT, DEFAULT_PARENT))
    JSONConfig.put("hbase.client.pause", "1000")
    JSONConfig.put("hbase.client.retries.number", "2")
    JSONConfig.toString
  }

  /**
   * Cache database.
   */
  override protected def cacheDatabase(
      isDefault: Boolean,
      dataSourceName: String,
      infos: Map[String, String],
      dataSourcesByName: mutable.HashMap[String, CatalogDataSource],
      dataSourceToCatalogDatabase: HashMap[String, HashMap[String, CatalogDatabase]]): Unit = {
    val host = cachedProperties.getOrElse(SHORT_HOST, {
      throw new SparkException("HBase DataSource must config master like 127.0.0.1 !")
    })
    val port = cachedProperties.getOrElse(SHORT_PORT, DEFAULT_PORT).toInt
    val ds = new HBaseDataSource(
      dataSourceName,
      DataSourceType.HBASE,
      this,
      host,
      port,
      cachedProperties(VERSION))
    dataSourcesByName.put(dataSourceName, ds)
    val xdatabases = dataSourceToCatalogDatabase.getOrElseUpdate(
      dataSourceName,
      new HashMap[String, CatalogDatabase])
    for ((dbName, dbHash) <- schemasMap(dataSourceName)) {
      val db = CatalogDatabase(
        id = newDatabaseId,
        dataSourceName = dataSourceName,
        name = dbName,
        description = null,
        locationUri = null,
        properties = Map.empty)
      xdatabases.put(dbName, db)
    }
  }

  /**
   * Cache table.
   */
  override protected def cacheTable(
      dataSourceName: String,
      dataSourceToCatalogDatabase: HashMap[String, HashMap[String, CatalogDatabase]],
      dbToCatalogTable: mutable.HashMap[Int, mutable.HashMap[String, CatalogTable]]): Unit = {
    val xdatabases = dataSourceToCatalogDatabase(dataSourceName)
    xdatabases.foreach {
      case (dbName, db) =>
        val xtables = dbToCatalogTable.getOrElseUpdate(db.id, new HashMap[String, CatalogTable])
        val dbHash = schemasMap.get(dbName).get
        for ((tbName, tbAny) <- dbHash) {
          val tabArr = tbAny.asInstanceOf[JSONArray]
          val tbHash = tabArr.get(0).asInstanceOf[JSONObject]
          val schema = schemaTranslator(tbHash)
          val table = CatalogTable(
            identifier = TableIdentifier(tbName, Option(dbName), Option(dataSourceName)),
            tableType = CatalogTableType.HBASE,
            // properties should contains conf, schema
            storage = CatalogStorageFormat.empty.copy(
              properties = cachedProperties.toMap ++ Map(
                HBaseTableCatalog.tableCatalog -> tbHash.toString,
                HBaseRelation.HBASE_CONFIGURATION -> getHBaseConfiguration())),
            schema = schema,
            provider = Some(HBASE_PROVIDER))
          xtables.put(tbName, table)
        }
    }
  }

  /**
   * Drop 'table' in some data source.
   */
  override def dropTable(
      dbName: String,
      table: String,
      ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = {
    val runAnyway = cachedProperties.getOrElse(RUNANYWAY, "false").toBoolean
    if (runAnyway) {
      new HBaseRelationImpl(
        Map(HBaseRelation.HBASE_CONFIGURATION -> getHBaseConfiguration()),
        None).dropTableIfExist(table)
      schemasMap(dbName).remove(table)
      if (cachedProperties.get(WRITE_SCHEMAS).exists(_.equals("true"))) {
        schemaWriter(getPropertiesFile(file = cachedProperties(SCHEMAS)), schemasMap)
      }
    } else {
      throw new SparkException(
        "drop HBase table from XSQL is dangerous," +
          "configure ds.force = true if necessary!")
    }
  }

  /**
   * Build filter argument in query. similar to LikeSimplification
   */
  override def buildFilterArgument(
      tableDefinition: CatalogTable,
      condition: Expression): util.HashMap[String, Any] = {
    // if guards below protect from escapes on trailing %.
    // Cases like "something\%" are not optimized, but this does not affect correctness.
    val startsWith = "([^_%]+)%".r
    val endsWith = "%([^_%]+)".r
    val startsAndEndsWith = "([^_%]+)%([^_%]+)".r
    val contains = "%([^_%]+)%".r
    val equalTo = "([^_%]*)".r
    val cleanedCondition = condition transformDown {
      case expressions.Like(input, expressions.Literal(pattern, StringType)) =>
        if (pattern == null) {
          // If pattern is null, return null value directly, since "col like null" == null.
          Literal(null, BooleanType)
        } else {
          pattern.toString match {
            case startsWith(prefix) if !prefix.endsWith("\\") =>
              StartsWith(input, Literal(prefix))
            case endsWith(postfix) =>
              EndsWith(input, Literal(postfix))
            // 'a%a' pattern is basically same with 'a%' && '%a'.
            // However, the additional `Length` condition is required to prevent 'a' match 'a%a'.
            case startsAndEndsWith(prefix, postfix) if !prefix.endsWith("\\") =>
              expressions.And(
                expressions
                  .GreaterThanOrEqual(Length(input), Literal(prefix.length + postfix.length)),
                expressions
                  .And(StartsWith(input, Literal(prefix)), EndsWith(input, Literal(postfix))))
            case contains(infix) if !infix.endsWith("\\") =>
              Contains(input, Literal(infix))
            case equalTo(str) =>
              expressions.EqualTo(input, Literal(str))
            case _ =>
              Like(input, Literal.create(pattern, StringType))
          }
        }
      case u @ UnresolvedAttribute(nameParts) =>
        val f = tableDefinition.dataSchema.apply(nameParts.last)
        AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()
      case others => others
    }
    val filters = splitConjunctivePredicates(cleanedCondition).zipWithIndex
      .map(tup => (tup._2.toString, DataSourceStrategy.translateFilter(tup._1).get))
    val result = new util.HashMap[String, Any]()
    result.putAll(filters.toMap.asJava)
    result
  }

  /**
   * Create 'table' in some data source.
   */
  override def createTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit = {
    val runAnyway = cachedProperties.getOrElse(RUNANYWAY, "false").toBoolean
    if (runAnyway) {
      val hbaseSchema = schemaTranslator(tableDefinition)
      val checkedtableDefinition = tableDefinition.copy(
        tableType = CatalogTableType.HBASE,
        storage = CatalogStorageFormat.empty.copy(
          properties = cachedProperties.toMap ++ Map(
            HBaseTableCatalog.tableCatalog -> hbaseSchema.toString,
            HBaseRelation.HBASE_CONFIGURATION -> getHBaseConfiguration(),
            HBaseTableCatalog.newTable -> "5")),
        provider = Some(HBASE_PROVIDER))
      new HBaseRelationImpl(
        checkedtableDefinition.storage.properties,
        Option(checkedtableDefinition.schema)).createTableIfNotExist(ignoreIfExists)
      val dbName = checkedtableDefinition.database
      val tbName = checkedtableDefinition.identifier.table
      val schemas = schemasMap.getOrElseUpdate(dbName, new HashMap[String, JSONArray])
      val hbaseSchemaArr = new JSONArray()
      hbaseSchemaArr.add(hbaseSchema)
      schemas.update(tbName, hbaseSchemaArr)
      if (cachedProperties.get(WRITE_SCHEMAS).exists(_.equals("true"))) {
        schemaWriter(getPropertiesFile(file = cachedProperties(SCHEMAS)), schemasMap)
      }
    } else {
      throw new SparkException(
        "create HBase table from XSQL is on trial," +
          "configure ds.force = true if necessary!")
    }
  }

  /**
   * Scan data in Specified table.
   */
  override def scanXSQLTable(
      dataSourcesByName: mutable.HashMap[String, CatalogDataSource],
      tableDefinition: CatalogTable,
      columns: util.List[String],
      condition: util.HashMap[String, Any],
      sort: util.List[util.HashMap[String, Any]],
      aggs: util.HashMap[String, Any],
      groupByKeys: Seq[String],
      limit: Int): Seq[Seq[Any]] = {
    // relation contains client information
    val relation =
      new HBaseRelationImpl(tableDefinition.storage.properties, Option(tableDefinition.schema))
    val filters = if (condition == null) {
      Array[Filter]()
    } else {
      Array() ++ condition.values().asScala.map(_.asInstanceOf[Filter])
    }
    val realColumns = if (columns == null) {
      Array[String]()
    } else {
      Array() ++ columns.asScala
    }
    // tableScanUtil contains scan method
    val tableScanUtil = HBaseTableScanUtil(relation, realColumns, filters, limit)
    val partition = tableScanUtil.getHBaseScanPartition
    val res = tableScanUtil.computeForCommand(partition).toArray
    tableScanUtil.close()
    if (res.size > limit) {
      res.slice(0, limit)
    } else {
      res
    }
  }

  /**
   * Get default options from CatalogTable.
   */
  override def getDefaultOptions(table: CatalogTable): Map[String, String] = {
    Map(HBaseTableCatalog.tableCatalog -> schemaTranslator(table).toString())
  }

  /**
   * Check table exists or not.
   */
  override def tableExists(dbName: String, table: String): Boolean =
    schemasMap.contains(dbName) && schemasMap(dbName).contains(table)

  override protected def doGetRawTable(
      db: String,
      originDB: String,
      table: String): Option[CatalogTable] = {
    val tbAny = schemasMap(db)(table)
    val tabArr = tbAny.asInstanceOf[JSONArray]
    val tbHash = tabArr.get(0).asInstanceOf[JSONObject]
    val schema = schemaTranslator(tbHash)
    Option(
      CatalogTable(
        identifier = TableIdentifier(table, Option(db), Option(dsName)),
        tableType = CatalogTableType.HBASE,
        // properties should contains conf, schema
        storage = CatalogStorageFormat.empty.copy(
          properties = cachedProperties.toMap ++ Map(
            HBaseTableCatalog.tableCatalog -> tbHash.toString,
            HBaseRelation.HBASE_CONFIGURATION -> getHBaseConfiguration())),
        schema = schema,
        provider = Some(HBASE_PROVIDER)))
  }

  override def listTables(dbName: String): Seq[String] = {
    schemasMap(dbName).keys.toSeq
  }

  override def listDatabases(): Seq[String] = {
    schemasMap.keys.toSeq
  }
}
object HBaseManager {

  val DEFAULT_CF_STRING = "cf"
  val DEFAULT_QUALIFIER_STRING = "col"
  val ROWKEY = "rowkey"
  val TYPE = "type"
  val NAMESPACE = "namespace"
  val NAME = "name"
  val COLUMNS = "columns"
  val TABLE = "table"

  val SHORT_HOST = "host"
  val SHORT_PORT = "port"
  val RUNANYWAY = "force"

  val DEFAULT_PORT = "2181"
  val DEFAULT_PARENT = "/hbase"
  val HBASE_PROVIDER = "hbase"
  val ZK_PARENT = "zookeeper.znode.parent"
  val HOST = "hbase.zookeeper.quorum"
  val PORT = "hbase.zookeeper.property.clientPort"
  val METADATA = TableName.valueOf("metadata")
  val DEFAULT_CF = Bytes.toBytes("cf")
  val DEFAULT_QUALIFIER = Bytes.toBytes("col")

  private def schemaTranslator(tbHash: JSONObject): StructType = {
    var fields: mutable.Buffer[StructField] = ArrayBuffer.empty
    val cols = tbHash.getJSONObject(COLUMNS)
    val keys = cols.keys()
    while (keys.hasNext()) {
      val fieldName = keys.next().asInstanceOf[String]
      val propertyObj = cols.getJSONObject(fieldName)
      fields += StructField(
        fieldName,
        DataSourceManager.getSparkSQLDataType(propertyObj.get(TYPE).asInstanceOf[String]))
    }
    StructType(fields)
  }

  private def schemaTranslator(catalogTable: CatalogTable): JSONObject = {
    val namespace = catalogTable.database
    val tableName = catalogTable.identifier.table
    val (primarykey, normalkey) = catalogTable.schema.fields
      .partition(_.metadata.contains(PRIMARY_KEY))
    assert(primarykey.size > 0)
    val rowkey = primarykey.map(_.name).mkString(":")
    val options = catalogTable.storage.properties
    val columns = primarykey.map(pk =>
      (pk.name, HBaseTableCatalog.rowKey, pk.name, pk.dataType.simpleString)) ++ normalkey.map(
      pk => {
        val cfcol =
          options.get(pk.name).getOrElse(s"${DEFAULT_CF_STRING}:${pk.name}").split(":")
        assert(cfcol.size == 2)
        (pk.name, cfcol(0), cfcol(1), pk.dataType.simpleString)
      })
    val columnsJSONObject = new JSONObject()
    columns.foreach {
      case (colName, cf, col, dt) =>
        val innerJSONObject = new JSONObject()
        innerJSONObject.put(DEFAULT_CF_STRING, cf)
        innerJSONObject.put(DEFAULT_QUALIFIER_STRING, col)
        innerJSONObject.put(TYPE, dt)
        columnsJSONObject.put(colName, innerJSONObject)
    }
    val resultJSONObject = new JSONObject()
    val tableJSONObject = new JSONObject()
    tableJSONObject.put(NAMESPACE, namespace)
    tableJSONObject.put(NAME, tableName)
    resultJSONObject.put(TABLE, tableJSONObject)
    resultJSONObject.put(ROWKEY, rowkey)
    resultJSONObject.put(COLUMNS, columnsJSONObject)
    resultJSONObject
  }
  private def schemaReader(
      content: String,
      schemaMap: HashMap[String, HashMap[String, JSONArray]]): Unit = {
    val jsonArr = JSONArray.fromObject(content)
    val itr = jsonArr.iterator()
    while (itr.hasNext) {
      val obj = itr.next().asInstanceOf[JSONObject]
      val dbtb = obj.getJSONObject(TABLE)
      val db = dbtb.getString(NAMESPACE)
      val tb = dbtb.getString(NAME)
      val tables = schemaMap.getOrElseUpdate(db, new HashMap[String, JSONArray])
      val jsonArray = new JSONArray
      jsonArray.add(obj)
      tables.put(tb, jsonArray)
    }
  }

  private def schemaWriter(
      filename: String,
      schemasMap: HashMap[String, HashMap[String, JSONArray]]) = {
    val file = new File(filename)
    require(file.exists(), s"Schemas file $file does not exist")
    require(file.isFile(), s"Schemas file $file is not a normal file")
    try {
      val jsonArray = new JSONArray()
      for (jsonObject <- schemasMap.values.flatMap(_.values).map(_.get(0))) {
        jsonArray.add(jsonObject)
      }
      FileUtils.writeStringToFile(file, jsonArray.toString(2))
    } catch {
      case e: IOException =>
        throw new SparkException(s"Failed when loading Spark properties from $filename", e)
    }
  }

}
