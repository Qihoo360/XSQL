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

import java.net.URI
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.util.matching.Regex

import net.sf.json.{JSONArray, JSONObject}
import redis.clients.jedis.{Jedis, ScanParams}
import redis.clients.util.JedisURIHelper

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{
  CatalogDatabase,
  CatalogStorageFormat,
  CatalogTable,
  CatalogTableType
}
import org.apache.spark.sql.execution.datasources.redis._
import org.apache.spark.sql.types._
import org.apache.spark.sql.xsql.{CatalogDataSource, DataSourceManager, RedisDataSource}
import org.apache.spark.sql.xsql.DataSourceType.REDIS

private[spark] class RedisManager extends DataSourceManager with Logging {

  import DataSourceManager._
  import RedisManager._

  override def shortName(): String = REDIS.toString

  var redisConfig: RedisConfig = _
  var keysBuffer = mutable.Map[String, mutable.Set[String]]()

  /**
   * determine the cache level of schema cache
   */
  override val defaultCacheLevel: String = "2"

  /**
   * Cache database.
   */
  override protected def cacheDatabase(
      isDefault: Boolean,
      dataSourceName: String,
      infos: Map[String, String],
      dataSourcesByName: mutable.HashMap[String, CatalogDataSource],
      dataSourceToCatalogDatabase: HashMap[String, HashMap[String, CatalogDatabase]]): Unit = {
    val url = cachedProperties.getOrElse(URL, {
      throw new SparkException("Redis DataSource must have host and port!")
    })
    val auth = JedisURIHelper.getPassword(URI.create(url))
    val ds =
      new RedisDataSource(dataSourceName, REDIS, this, url, auth, cachedProperties(VERSION))
    dataSourcesByName.put(ds.getName, ds)
    // INLINE: get RedisEndpoint, get database count
    redisConfig = new RedisConfig(new RedisEndpoint(url))
    val xdatabases =
      dataSourceToCatalogDatabase.getOrElseUpdate(
        ds.getName,
        new HashMap[String, CatalogDatabase])
    for (i <- Range(0, redisConfig.dbAmount)) {
      val db = CatalogDatabase(
        id = newDatabaseId,
        dataSourceName = dataSourceName,
        name = i.toString,
        description = null,
        locationUri = null,
        properties = Map.empty)
      xdatabases.put(db.name, db)
    }
  }

  /**
   * Cache table.
   */
  override protected def cacheTable(
      dataSourceName: String,
      dataSourceToCatalogDatabase: HashMap[String, mutable.HashMap[String, CatalogDatabase]],
      dbToCatalogTable: mutable.HashMap[Int, mutable.HashMap[String, CatalogTable]]): Unit = {
    val xdatabases = dataSourceToCatalogDatabase(dataSourceName)
    xdatabases.foreach {
      case (dbName, db) =>
        val xtables = dbToCatalogTable.getOrElseUpdate(db.id, new HashMap[String, CatalogTable])
        schemasMap.get(db.name).foreach { tables =>
          val tableNames = tables.keySet
          for (tableName <- tableNames) {
            val (schema, tableType) =
              schemaTranslator(tables(tableName), redisConfig, db.name.toInt)
            val table = CatalogTable(
              identifier = TableIdentifier(tableName, Option(db.name), Option(dataSourceName)),
              tableType = CatalogTableType.REDIS,
              // properties should contains url, version, tableName, tableType, dbNum
              storage = CatalogStorageFormat.empty.copy(
                properties = cachedProperties.toMap ++ Map(
                  TABLE -> tableName,
                  REDIS_TYPE -> tableType,
                  DATABASE -> db.name)),
              schema = schema,
              provider = Some(REDIS_PROVIDER))
            xtables.put(tableName, table)
          }
        }
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
    val relation = RedisRelationImpl(tableDefinition.storage.properties, tableDefinition.schema)
    val conditions = if (condition == null) Map[String, Any]() else Map() ++ condition.asScala
    // tableScanUtil contains scan method
    val tableScanUtil =
      new RedisTableScanUtil(relation, columns.asScala.toArray[String], conditions)
    tableScanUtil.getKeyValue(
      tableScanUtil.getRowkeys(0, 16383),
      redisConfig.getConnect(tableDefinition.database.toInt))
  }

  /**
   * check if there is any key with specific suffix
   * Note: this method will cache keys of table to accelerate table scan
   */
  override def tableExists(dbName: String, table: String): Boolean = {
    if (keysBuffer.isDefinedAt(table)) {
      true
    } else {
      val conn = redisConfig.getConnect(dbName.toInt)
      val params = new ScanParams().`match`(s"${table}*").count(1)
      val res = scanKeys(conn, params).asScala
      if (res.size > 0) {
        keysBuffer.put(table, res)
      }
      res.size > 0
    }
  }

  /**
   * make use of keysBuffer, if absent scanKeys at once
   * Note: this method don't throw Exception when values have different type.
   */
  override def doGetRawTable(
      dbName: String,
      originDB: String,
      table: String): Option[CatalogTable] = {
    val conn = redisConfig.getConnect(dbName.toInt)
    if (!keysBuffer.isDefinedAt(table)) {
      val params = new ScanParams().`match`(s"${table}*")
      keysBuffer.put(table, scanKeys(conn, params).asScala)
    }
    val firstType = conn.`type`(keysBuffer(table).head)
    val schema = getSchema(conn, keysBuffer(table).head, firstType)
    Option(
      CatalogTable(
        identifier = TableIdentifier(table, Option(dbName), Option(dsName)),
        tableType = CatalogTableType.REDIS,
        storage = CatalogStorageFormat.empty.copy(properties = cachedProperties.toMap ++
          Map(TABLE -> table, REDIS_TYPE -> firstType, DATABASE -> dbName)),
        schema = schema,
        provider = Some(REDIS_PROVIDER)))
  }

  override def listDatabases(dataSourceName: String): Seq[String] = {
    Range(0, redisConfig.dbAmount).map(_.toString)
  }

  override def listTables(dbName: String): Seq[String] = {
    logWarning("show tables in RedisDataSource is time-consuming, be patient please!")
    val conn = redisConfig.getConnect(dbName.toInt)
    val params = new ScanParams().`match`(s"*")
    val res = scanKeys(conn, params).asScala
    val delimiterSet = Seq(":", "-", "/", "#", "|", "\\")
    res.toSeq
      .flatMap(key => {
        val index = delimiterSet.map(delimiter => key.lastIndexOf(delimiter)).max
        if (index != -1) {
          Iterator(key.substring(0, index))
        } else Nil
      })
      .groupBy(identity)
      .filter(_._2.size > 1)
      .map(_._1)
      .toSeq
  }

  /**
   * Get redaction pattern
   */
  override def getRedactionPattern: Option[Regex] = Some(s"(?i)${URL}".r)

  /**
   * this method is replace by schema file
   */
  override def listAndUpdateTables(
      tableHash: mutable.HashMap[String, CatalogTable],
      dsName: String,
      dbName: String): Seq[String] = {
    val conn = redisConfig.getConnect(dbName.toInt)
    val params = new ScanParams().`match`(s"*")
    val res = scanKeys(conn, params).asScala
    val delimiterSet = Seq(":", "-", "/", "#", "|", "\\")
    val possibleTable = res.toSeq
      .flatMap(key => {
        val (index, delimiter) = delimiterSet
          .map(delimiter => (key.lastIndexOf(delimiter), delimiter))
          .maxBy(_._1)
        if (index != -1) {
          Iterator((key.substring(0, index), delimiter))
        } else Nil
      })
      .groupBy(identity)
      .map(e => (e._1, e._2.size))
      .filter(_._2 > 1)
      .map {
        case ((prefix, delimiter), count) =>
          val params = new ScanParams().`match`(s"${prefix}${delimiter}*").count(1)
          val sample = scanKeys(conn, params).asScala.head
          val firstType = conn.`type`(sample)
          val schema = getSchema(conn, sample, firstType)
          CatalogTable(
            identifier = TableIdentifier(prefix, Option(dbName), Option(dsName)),
            tableType = CatalogTableType.REDIS,
            storage = CatalogStorageFormat.empty.copy(
              properties = cachedProperties.toMap ++ Map(
                SUFFIX_DELIMITER -> delimiter,
                TABLE -> prefix,
                REDIS_TYPE -> firstType,
                TBSIZE -> count.toString,
                DATABASE -> dbName)),
            schema = schema,
            provider = Some(REDIS_PROVIDER))
      }
    for (catalogTable <- possibleTable) {
      tableHash.put(catalogTable.identifier.table, catalogTable)
    }
    possibleTable.map(_.identifier.table).toSeq
  }

}

object RedisManager {

  import org.apache.spark.sql.xsql.DataSourceManager.SCHEMA_TYPE

  val TIMEOUT = "timeout"
  val REDIS_PROVIDER = "redis"
  val KEY = "key"
  val SUFFIX = "suffix"
  val RANGE = "range"
  val SCORE = "score"
  val VALUE = "value"
  val REDIS_TYPE = "type"
  val TABLE = "table"
  val DATABASE = "database"
  val SUFFIX_DELIMITER = "suffix_delimiter"
  val TBSIZE = "tbsize"

  private def schemaTranslator(
      tbHash: JSONArray,
      redisClient: RedisConfig,
      dbNum: Int): (StructType, String) = {
    val itr = tbHash.iterator()
    var fields: mutable.Buffer[StructField] = ArrayBuffer.empty
    var tableType = "hash"
    while (itr.hasNext()) {
      val propertyObj = itr.next().asInstanceOf[JSONObject]
      val fieldName = propertyObj.getString(DataSourceManager.NAME)
      if (!fieldName.equals(SCHEMA_TYPE)) {
        if (propertyObj.get(SCHEMA_TYPE) != null) {
          fields += StructField(
            fieldName,
            DataSourceManager.getSparkSQLDataType(
              propertyObj.get(SCHEMA_TYPE).asInstanceOf[String]))
        }
      } else {
        tableType = propertyObj.get(SCHEMA_TYPE).asInstanceOf[String]
        fields = getSchema(redisClient.getConnect(dbNum), "", tableType).fields.toBuffer
      }
    }
    (StructType(fields), tableType)
  }

  def getSchema(conn: Jedis, key: String, firstType: String): StructType = {
    val defaultType = StringType
    firstType match {
      case "string" =>
        StructType(Seq(KEY, VALUE).map(StructField(_, defaultType)))
      case "list" | "set" =>
        StructType(Seq(StructField(KEY, defaultType), StructField(VALUE, ArrayType(defaultType))))
      case "zset" =>
        StructType(
          Seq(
            StructField(KEY, defaultType),
            StructField(VALUE, MapType(defaultType, DoubleType))))
      case "hash" =>
        val cols = Seq(KEY) ++ conn.hgetAll(key).asScala.keys
        StructType(cols.map(StructField(_, defaultType)))
    }
  }

  def scanKeys(jedis: Jedis, params: ScanParams): util.HashSet[String] = {
    val keys = new util.HashSet[String]
    var cursor = "0"
    do {
      val scan = jedis.scan(cursor, params)
      keys.addAll(scan.getResult)
      cursor = scan.getStringCursor
    } while (cursor != "0")
    keys
  }
}
