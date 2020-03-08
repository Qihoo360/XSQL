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

import java.util.LinkedList
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.util.matching.Regex

import net.sf.json.JSONArray

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.CatalystTypeConverters.convertToScala
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.catalog.CatalogTypes._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.xsql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.xsql.util.Utils._
import org.apache.spark.util.Utils

/**
 * Each DataSourceManager will deal with one real datasource when running, we just override all
 * [[ExternalCatalog]]'s abstract methods with default implementation which means specific kind of
 * operation on this datasource is not supported for now.
 */
trait DataSourceManager extends ExternalCatalog with Logging {
  import DataSourceManager._

  def shortName(): String

  protected var dsName: String = ""

  /**
   * The default cluster that schedules and executes jobs.
   */
  private var defaultCluster: Option[String] = None

  /**
   * If true, XSQL will allow queries using the API of the underlying data source.
   */
  private var enablePushDown: Boolean = true

  /**
   * If true, XSQL will discover the schema of tables in the data source.
   */
  private var discoverSchema: Boolean = false

  /**
   * If true, XSQL will allow queries by structured stream API.
   */
  private var enableStream: Boolean = true

  /**
   * The default cache level of metadata for each data source.
   */
  protected val defaultCacheLevel = "1"

  /**
   * The user who submits the current job.
   */
  protected lazy val userName = Some(Utils.getCurrentUserName())

  /**
   * The special properties for each data source.
   */
  protected val specialProperties = new HashMap[String, HashMap[String, String]]

  /**
   * The cached properties for each data source.
   */
  protected val cachedProperties = new HashMap[String, String]

  /**
   * The schemas present in the specified file.
   */
  protected lazy val schemasMap = new HashMap[String, HashMap[String, JSONArray]]

  /**
   * A reader for read schemas from a JSON string.
   */
  protected lazy val schemaReader: (String, HashMap[String, HashMap[String, JSONArray]]) => Unit =
    getSchemasFromStr

  /**
   * The white list defined in the specified file.
   */
  protected val whiteListMap = new HashMap[String, HashSet[String]]

  /**
   * The black list defined in the specified file.
   */
  protected val blackListMap = new HashMap[String, HashSet[String]]

  /**
   * Get all cachedProperties, used when refresh datasource.
   */
  def getProperties(): Map[String, String] = {
    cachedProperties.toMap
  }

  /**
   * Get specific configuration of datasource.
   */
  def getProperty(key: String): String = {
    cachedProperties.getOrElse(key, "")
  }

  /**
   * Load and update schema.
   */
  @deprecated("Use schema defined file instead")
  def listAndUpdateTables(
      tableHash: HashMap[String, CatalogTable],
      dsName: String,
      dbName: String): Seq[String] = Nil

  /**
   * Get default cluster.
   */
  def getDefaultCluster(
      clusterNames: HashSet[String],
      dataSourcesByName: HashMap[String, CatalogDataSource],
      identifier: TableIdentifier): Option[String] = {
    defaultCluster.orElse(getClusterBasedOnData(clusterNames, dataSourcesByName, identifier))
  }

  /**
   * Get cluster based on data location.
   */
  def getClusterBasedOnData(
      clusterNames: HashSet[String],
      dataSourcesByName: HashMap[String, CatalogDataSource],
      identifier: TableIdentifier): Option[String] = None

  /**
   * Get the cache level of the metadata for the specified data source.
   */
  def getCacheLevel(): Integer = {
    Integer.valueOf(cachedProperties.getOrElse(CACHE_LEVEL, defaultCacheLevel))
  }

  /**
   * Get the redaction pattern of the specified data source for displaying metadata.
   */
  def getRedactionPattern: Option[Regex] = None

  /**
   * Parse the file that defines the metadata that needs to be discovered.
   */
  def parseDiscoverFile(dataSourceName: String, discoverFile: String): Unit = ()

  /**
   * Create client to pull metadata from specified data source, then put it into cache.
   */
  def parse(
      isDefault: Boolean,
      dataSourceName: String,
      infos: Map[String, String],
      dataSourcesByName: HashMap[String, CatalogDataSource],
      dataSourceToCatalogDatabase: HashMap[String, HashMap[String, CatalogDatabase]],
      dbToCatalogTable: HashMap[Int, HashMap[String, CatalogTable]]): Unit = {
    dsName = dataSourceName
    defaultCluster = infos.get(CLUSTER)
    enablePushDown = java.lang.Boolean.valueOf(infos.getOrElse(PUSHDOWN, TRUE))
    discoverSchema = java.lang.Boolean.valueOf(infos.getOrElse(SCHEMAS_DISCOVER, FALSE))
    enableStream = java.lang.Boolean.valueOf(infos.getOrElse(STREAM, FALSE))
    val whitelistFile = infos.get(WHITELIST)
    if (whitelistFile.isDefined) {
      loadWhiteAndBlackList(getPropertiesFile(file = whitelistFile.get))
    }
    cachedProperties ++= infos
    val schemasFile = cachedProperties.get(SCHEMAS)
    if (schemasFile.isDefined) {
      val schemasFilePath = getPropertiesFile(file = schemasFile.get)
      getSettingsFromFile(schemasFilePath, schemasMap, schemaReader)
    } else if (cachedProperties.contains("schemas.str")) {
      schemaReader(cachedProperties("schemas.str"), schemasMap)
    }
    val discoverFile = cachedProperties.get(SCHEMAS_DISCOVER_CONFIG)
    if (discoverFile.isDefined) {
      parseDiscoverFile(dataSourceName, discoverFile.get)
    }
    val metadataCacheLevel = getCacheLevel
    assert(metadataCacheLevel > 0, "cache level of datasource must great than zero!")
    cacheDatabase(
      isDefault,
      dataSourceName,
      infos,
      dataSourcesByName,
      dataSourceToCatalogDatabase)
    if (metadataCacheLevel == 2) {
      cacheTable(dataSourceName, dataSourceToCatalogDatabase, dbToCatalogTable)
    }
  }

  /**
   * Parse schema.
   */
  @deprecated("Use cacheDatabase and cacheTable instead.", "0.1.0")
  def doParse(
      dataSourceName: String,
      infos: Map[String, String],
      dataSourcesByName: HashMap[String, CatalogDataSource],
      dataSourceToCatalogDatabase: HashMap[String, HashMap[String, CatalogDatabase]],
      dbToCatalogTable: HashMap[Int, HashMap[String, CatalogTable]]): Unit = {}

  /**
   * Get default options from CatalogTable.
   * You can use this method to avoid display secrecy info in CatalogTable's storage properties.
   */
  def getDefaultOptions(table: CatalogTable): Map[String, String] = Map.empty

  /**
   * Cache metadata about database.
   */
  protected def cacheDatabase(
      isDefault: Boolean,
      dataSourceName: String,
      infos: Map[String, String],
      dataSourcesByName: HashMap[String, CatalogDataSource],
      dataSourceToCatalogDatabase: HashMap[String, HashMap[String, CatalogDatabase]]): Unit = {}

  /**
   * Cache metadata about table.
   */
  protected def cacheTable(
      dataSourceName: String,
      dataSourceToCatalogDatabase: HashMap[String, HashMap[String, CatalogDatabase]],
      dbToCatalogTable: HashMap[Int, HashMap[String, CatalogTable]]): Unit = {}

  /**
   * Load whitelist and blacklist from file.
   */
  protected def loadWhiteAndBlackList(whitelistFilePath: String): Unit =
    getWhitelistFromFile(whitelistFilePath, whiteListMap, blackListMap, getWhitelistFromStr)

  /**
   * Check if the specified database in whitelist.
   */
  protected def isSelectedDatabase(
      isDefault: Boolean,
      dbName: String,
      defaultDatabaseName: String): Boolean = {
    whiteListMap.isEmpty ||
    whiteListMap.contains(dbName) ||
    isDefault && defaultDatabaseName.equalsIgnoreCase(dbName)
  }

  /**
   * Get table rows and index of relational database.
   */
  def getXSQLTableRowsAndIndex(identifier: TableIdentifier): (Int, Seq[String]) = (-1, null)

  /**
   * Check if the pushdown feature is enabled.
   */
  def isPushDown: Boolean = enablePushDown

  /**
   * Check if the pushdown feature is enabled for relational database.
   */
  def isPushDown(runtimeConf: SQLConf, dataSourceName: String, plan: LogicalPlan): Boolean =
    enablePushDown

  /**
   * Check if the specified table contains fields need to discover.
   */
  def isDiscover: Boolean = discoverSchema

  /**
   * Structured streaming enabled or not.
   */
  def isStream: Boolean = enableStream

  /**
   * Shut down the DataSourceManager.
   */
  def stop(): Unit = ()

  // --------------------------------------------------------------------------
  // Databases
  // --------------------------------------------------------------------------

  /**
   * Create database in specified data source.
   */
  override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {
    throw new UnsupportedOperationException(s"Create ${shortName()} database not supported!")
  }

  /**
   * Drop database in specified data source.
   */
  override def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = {
    throw new UnsupportedOperationException(s"Drop ${shortName()} database not supported!")
  }

  /**
   * Alter database in specified data source.
   */
  override def alterDatabase(dbDefinition: CatalogDatabase): Unit = {
    throw new UnsupportedOperationException(s"Alter ${shortName()} database not supported!")
  }

  /**
   * Desc database in specified data source.
   */
  override def getDatabase(db: String): CatalogDatabase = {
    throw new UnsupportedOperationException(s"Get ${shortName()} database not supported!")
  }

  /**
   * Check if database exists in specified data source.
   */
  override def databaseExists(db: String): Boolean = {
    throw new UnsupportedOperationException(
      s"Check ${shortName()} database exists not supported!")
  }

  /**
   * Display the databases of specified data source.
   */
  override def listDatabases(): Seq[String] = {
    throw new UnsupportedOperationException(s"List ${shortName()} database exists not supported!")
  }

  /**
   * Display the databases of specified data source with pattern.
   */
  override def listDatabases(pattern: String): Seq[String] = {
    StringUtils.filterPattern(listDatabases(), pattern)
  }

  /**
   * Set current database, won't be called actually.
   */
  override def setCurrentDatabase(db: String): Unit = {
    throw new UnsupportedOperationException(s"Set ${shortName()} current database not supported!")
  }

  // --------------------------------------------------------------------------
  // Tables
  // --------------------------------------------------------------------------

  /**
   * Create table in specified database.
   */
  override def createTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit = {
    throw new UnsupportedOperationException(s"Create ${shortName()} table not supported!")
  }

  /**
   * Drop table in specified data source.
   */
  override def dropTable(
      db: String,
      table: String,
      ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = {
    throw new UnsupportedOperationException(s"Drop ${shortName()} table not supported!")
  }

  /**
   * Truncate table in specified data source.
   */
  def truncateTable(table: CatalogTable, partitionSpec: Option[TablePartitionSpec]): Unit = {
    throw new UnsupportedOperationException(s"Truncate ${shortName()} table not supported!")
  }

  /**
   * Rename table in specified data source.
   */
  override def renameTable(db: String, oldName: String, newName: String): Unit = {
    throw new UnsupportedOperationException(s"Rename ${shortName()} table stats not supported!")
  }

  /**
   * Alter table in specified data source.
   */
  override def alterTable(tableDefinition: CatalogTable): Unit = {
    throw new UnsupportedOperationException(s"Alter ${shortName()} table not supported!")
  }

  /**
   * Alter schema of specified table.
   */
  override def alterTableDataSchema(
      db: String,
      table: String,
      newDataSchema: StructType): Unit = {
    throw new UnsupportedOperationException(s"Alter ${shortName()} table schema not supported!")
  }

  /**
   * Alter schema of specified table in relational database.
   */
  def alterTableDataSchema(dbName: String, queryContent: String): Unit = {
    throw new UnsupportedOperationException(s"Alter ${shortName()} table schema not supported!")
  }

  /**
   * Alter statistics of specified table.
   */
  override def alterTableStats(
      db: String,
      table: String,
      stats: Option[CatalogStatistics]): Unit = {
    throw new UnsupportedOperationException(s"Alter ${shortName()} table stats not supported!")
  }

  /**
   * Get the metadata of specified table.
   */
  override def getTable(db: String, table: String): CatalogTable = {
    getRawTable(db, db, table)
  }

  /**
   * Display the tables of specified database.
   */
  override def listTables(db: String): Seq[String] = {
    throw new UnsupportedOperationException(s"List ${shortName()} table not supported!")
  }

  /**
   * Display the tables of specified database with pattern.
   */
  override def listTables(dbName: String, pattern: String): Seq[String] = {
    StringUtils.filterPattern(listTables(dbName), pattern)
  }

  /**
   * Load data from specified table.
   */
  override def loadTable(
      db: String,
      table: String,
      loadPath: String,
      isOverwrite: Boolean,
      isSrcLocal: Boolean): Unit = {
    throw new UnsupportedOperationException(s"Load ${shortName()} table not supported!")
  }

  /**
   * Get raw table in some data source.
   * originDB is real name of database, and 'db' is a valid name in XSQL.
   * originDB is used for query raw table from specified data source.
   */
  def getRawTable(db: String, originDB: String, table: String): CatalogTable = {
    doGetRawTable(db, originDB, table).getOrElse(
      throw new NoSuchTableException(db, table = table))
  }

  /**
   * Get whitelist or blacklist of tables.
   */
  protected def getWhiteOrBlackTables(
      dbName: String,
      listMap: HashMap[String, HashSet[String]]): HashSet[String] = {
    val listOps = listMap.get(dbName)
    listOps.getOrElse(HashSet.empty)
  }

  /**
   * Get whitelist and blacklist of tables.
   */
  protected def getWhiteAndBlackTables(dbName: String): (HashSet[String], HashSet[String]) =
    (getWhiteOrBlackTables(dbName, whiteListMap), getWhiteOrBlackTables(dbName, blackListMap))

  /**
   * Check if the specified table in the whitelist.
   */
  protected def isSelectedTable(
      whiteTables: HashSet[String],
      blackTables: HashSet[String],
      tableName: String): Boolean = {
    (whiteTables.isEmpty || whiteTables.contains(tableName)) && !blackTables.contains(tableName)
  }

  protected def doGetRawTable(
      db: String,
      originDB: String,
      table: String): Option[CatalogTable] = {
    if (isDiscover) {
      logDebug(s"Discovering $dsName.$db.$table, $dsName.$originDB.$table in fact.")
      discoverRawTable(db, originDB, table)
    } else {
      fastGetRawTable(db, originDB, table)
    }
  }

  /**
   * Discover the schema of the specified table.
   */
  protected def discoverRawTable(
      db: String,
      originDB: String,
      table: String): Option[CatalogTable] = {
    throw new UnsupportedOperationException(s"Discover ${shortName()} raw table not supported!")
  }

  /**
   * Get the schema of the specified table in fast way.
   */
  protected def fastGetRawTable(
      db: String,
      originDB: String,
      table: String): Option[CatalogTable] = {
    throw new UnsupportedOperationException(s"Get ${shortName()} raw table fast not supported!")
  }

  /**
   * Scan data in specified table.
   */
  def scanXSQLTable(
      dataSourcesByName: HashMap[String, CatalogDataSource],
      tableDefinition: CatalogTable,
      columns: java.util.List[String],
      condition: java.util.HashMap[String, Any],
      sort: java.util.List[java.util.HashMap[String, Any]],
      aggs: java.util.HashMap[String, Any],
      groupByKeys: Seq[String],
      limit: Int): Seq[Seq[Any]] = Seq.empty

  /**
   * Scan data in table of relational database.
   */
  def scanXSQLTables(
      dataSourcesByName: HashMap[String, CatalogDataSource],
      tableDefinition: CatalogTable,
      dataCols: Seq[AttributeReference],
      limit: Boolean,
      query: String): Seq[Seq[Any]] = Seq.empty

  /**
   * Build argument of and to pushdown.
   */
  def buildAndArgument(
      searchArg: java.util.HashMap[String, Any],
      leftSearchArg: java.util.HashMap[String, Any],
      rightSearchArg: java.util.HashMap[String, Any]): Unit = {}

  /**
   * Build argument of or to pushdown.
   */
  def buildOrArgument(
      searchArg: java.util.HashMap[String, Any],
      leftSearchArg: java.util.HashMap[String, Any],
      rightSearchArg: java.util.HashMap[String, Any]): Unit = {}

  /**
   * Build argument of not to pushdown.
   */
  def buildNotArgument(
      searchArg: java.util.HashMap[String, Any],
      childSearchArg: java.util.HashMap[String, Any]): Unit = {}

  /**
   * Build argument of equal to to pushdown.
   */
  def buildEqualToArgument(
      searchArg: java.util.HashMap[String, Any],
      attribute: String,
      value: Any): Unit = {}

  /**
   * Build argument of less than to to pushdown.
   */
  def buildLessThanArgument(
      searchArg: java.util.HashMap[String, Any],
      attribute: String,
      value: Any): Unit = {}

  /**
   * Build argument of less than or equal to to pushdown.
   */
  def buildLessThanOrEqualArgument(
      searchArg: java.util.HashMap[String, Any],
      attribute: String,
      value: Any): Unit = {}

  /**
   * Build argument of greater than to to pushdown.
   */
  def buildGreaterThanArgument(
      searchArg: java.util.HashMap[String, Any],
      attribute: String,
      value: Any): Unit = {}

  /**
   * Build argument of greater than or equal to pushdown.
   */
  def buildGreaterThanOrEqualArgument(
      searchArg: java.util.HashMap[String, Any],
      attribute: String,
      value: Any): Unit = {}

  /**
   * Build argument of is null to pushdown.
   */
  def buildIsNullArgument(
      searchArg: java.util.HashMap[String, Any],
      childSearchArg: java.util.HashMap[String, Any]): Unit = {}

  /**
   * Build argument of is not null to pushdown.
   */
  def buildIsNotNullArgument(
      searchArg: java.util.HashMap[String, Any],
      childSearchArg: java.util.HashMap[String, Any]): Unit = {}

  /**
   * Build argument of like to pushdown.
   */
  def buildLikeArgument(
      searchArg: java.util.HashMap[String, Any],
      attribute: String,
      value: Any): Unit = {}

  /**
   * Build argument of REGEX like to pushdown.
   */
  def buildRLikeArgument(
      searchArg: java.util.HashMap[String, Any],
      attribute: String,
      value: Any): Unit = {}

  /**
   * Build argument of in to pushdown.
   */
  def buildInArgument(
      searchArg: java.util.HashMap[String, Any],
      attribute: String,
      values: LinkedList[Any]): Unit = {}

  /**
   * Build argument of attribute to pushdown.
   */
  def buildAttributeArgument(
      searchArg: java.util.HashMap[String, Any],
      attribute: String): Unit = {}

  /**
   * Build filter argument in query to pushdown.
   */
  def buildFilterArgument(
      tableDefinition: CatalogTable,
      condition: Expression): java.util.HashMap[String, Any] = {
    val searchArg = new java.util.HashMap[String, Any]
    condition match {
      case And(left, right) =>
        val leftSearchArg = buildFilterArgument(tableDefinition, left)
        val rightSearchArg = buildFilterArgument(tableDefinition, right)
        buildAndArgument(searchArg, leftSearchArg, rightSearchArg)
      case Or(left, right) =>
        val leftSearchArg = buildFilterArgument(tableDefinition, left)
        val rightSearchArg = buildFilterArgument(tableDefinition, right)
        buildOrArgument(searchArg, leftSearchArg, rightSearchArg)
      case Not(child) =>
        val childSearchArg = buildFilterArgument(tableDefinition, child)
        buildNotArgument(searchArg, childSearchArg)
      case EqualTo(a: Attribute, Literal(v, t)) =>
        // Table alias need remove.
        val attribute: String = a.name.split("\\.").last
        val value: Any = convertToScala(v, t)
        buildEqualToArgument(searchArg, attribute, value)
      case LessThan(a: Attribute, Literal(v, t)) =>
        // Table alias need remove.
        val attribute: String = a.name.split("\\.").last
        val value: Any = convertToScala(v, t)
        buildLessThanArgument(searchArg, attribute, value)
      case LessThanOrEqual(a: Attribute, Literal(v, t)) =>
        // Table alias need remove.
        val attribute: String = a.name.split("\\.").last
        val value: Any = convertToScala(v, t)
        buildLessThanOrEqualArgument(searchArg, attribute, value)
      case GreaterThan(a: Attribute, Literal(v, t)) =>
        // Table alias need remove.
        val attribute: String = a.name.split("\\.").last
        val value: Any = convertToScala(v, t)
        buildGreaterThanArgument(searchArg, attribute, value)
      case GreaterThanOrEqual(a: Attribute, Literal(v, t)) =>
        // Table alias need remove.
        val attribute: String = a.name.split("\\.").last
        val value: Any = convertToScala(v, t)
        buildGreaterThanOrEqualArgument(searchArg, attribute, value)
      case IsNull(child) =>
        val childSearchArg = buildFilterArgument(tableDefinition, child)
        buildIsNullArgument(searchArg, childSearchArg)
      case IsNotNull(child) =>
        val childSearchArg = buildFilterArgument(tableDefinition, child)
        buildIsNotNullArgument(searchArg, childSearchArg)
      case Like(a: Attribute, Literal(v, t)) =>
        // Table alias need remove.
        val attribute: String = a.name.split("\\.").last
        val value: Any = convertToScala(v, t)
        buildLikeArgument(searchArg, attribute, value)
      case RLike(a: Attribute, Literal(v, t)) =>
        val attribute: String = a.name.split("\\.").last
        val value: Any = convertToScala(v, t)
        buildRLikeArgument(searchArg, attribute, value)
      case In(a: Attribute, list) =>
        // Table alias need remove.
        val attribute: String = a.name.split("\\.").last
        val values = new LinkedList[Any]
        list.foreach { x =>
          x match {
            case Literal(v, t) =>
              val value: Any = convertToScala(v, t)
              values.add(value)
          }
        }
        buildInArgument(searchArg, attribute, values)
      case attr: UnresolvedAttribute =>
        val attribute = attr.name.split("\\.").last
        buildAttributeArgument(searchArg, attribute)
    }
    searchArg
  }

  /**
   * Build group argument in query to pushdown.
   */
  def buildGroupArgument(
      fieldName: String,
      groupByKeys: ArrayBuffer[String],
      innerAggsMap: java.util.HashMap[String, Any],
      groupMap: java.util.HashMap[String, Any]): java.util.HashMap[String, Any] = null

  /**
   * Build aggregate argument in query to pushdown.
   */
  def buildAggregateArgument(
      structField: StructField,
      aggregatefuncName: String,
      aggsMap: java.util.HashMap[String, Any],
      aggregateAliasOpts: Option[String]): StructField = null

  /**
   * Build sort argument in query with aggregate to pushdown.
   */
  def buildSortArgument(order: Seq[SortOrder]): java.util.List[java.util.HashMap[String, Any]] =
    null

  /**
   * Build sort argument in query with aggregate to pushdown.
   */
  def buildSortArgumentInAggregate(
      parentGroupMap: java.util.HashMap[String, Any],
      aggs: java.util.HashMap[String, Any],
      expr: Expression,
      direction: SortDirection,
      isDistinct: Boolean): Unit = {}

  /**
   * Build distinct argument to pushdown.
   */
  def buildDistinctArgument(
      columns: java.util.List[String]): (java.util.HashMap[String, Any], Seq[String]) =
    (null, null)

  /**
   * Load partition information from the specified table.
   */
  override def loadPartition(
      db: String,
      table: String,
      loadPath: String,
      partition: TablePartitionSpec,
      isOverwrite: Boolean,
      inheritTableSpecs: Boolean,
      isSrcLocal: Boolean): Unit = {
    throw new UnsupportedOperationException(s"Load ${shortName()} partition not supported!")
  }

  /**
   * Load dynamic partition information from the specified table.
   */
  override def loadDynamicPartitions(
      db: String,
      table: String,
      loadPath: String,
      partition: TablePartitionSpec,
      replace: Boolean,
      numDP: Int): Unit = {
    throw new UnsupportedOperationException(s"Load ${shortName()} partition not supported!")
  }

  // --------------------------------------------------------------------------
  // Partitions
  // --------------------------------------------------------------------------

  /**
   * Create function in specified database.
   */
  override def createPartitions(
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = {
    throw new UnsupportedOperationException(s"Create ${shortName()} partition not supported!")
  }

  /**
   * Drop function in specified database.
   */
  override def dropPartitions(
      db: String,
      table: String,
      parts: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean,
      purge: Boolean,
      retainData: Boolean): Unit = {
    throw new UnsupportedOperationException(s"Drop ${shortName()} partition not supported!")
  }

  /**
   * Override the specs of one or many existing table partitions, assuming they exist.
   * This assumes index i of `specs` corresponds to index i of `newSpecs`.
   */
  override def renamePartitions(
      db: String,
      table: String,
      specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit = {
    throw new UnsupportedOperationException(s"Rename ${shortName()} partition not supported!")
  }

  /**
   * Alter function in specified database.
   */
  override def alterPartitions(
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition]): Unit = {
    throw new UnsupportedOperationException(s"Alter ${shortName()} partition not supported!")
  }

  /**
   * Get function in specified database.
   */
  override def getPartition(
      db: String,
      table: String,
      spec: TablePartitionSpec): CatalogTablePartition = {
    throw new UnsupportedOperationException(s"Get ${shortName()} partition not supported!")
  }

  /**
   * Get partition option in specified table.
   */
  override def getPartitionOption(
      db: String,
      table: String,
      spec: TablePartitionSpec): Option[CatalogTablePartition] = {
    throw new UnsupportedOperationException(s"Get ${shortName()} partition not supported!")
  }

  /**
   * Get name of partitions in specified table.
   */
  override def listPartitionNames(
      db: String,
      table: String,
      partialSpec: Option[TablePartitionSpec] = None): Seq[String] = {
    throw new UnsupportedOperationException(s"List ${shortName()} partition not supported!")
  }

  /**
   * Display the partitions of specified table.
   */
  override def listPartitions(
      db: String,
      table: String,
      partialSpec: Option[TablePartitionSpec] = None): Seq[CatalogTablePartition] = {
    throw new UnsupportedOperationException(s"List ${shortName()} partition not supported!")
  }

  /**
   * Display the partitions of specified table which need to satisfy provided predicates.
   */
  override def listPartitionsByFilter(
      db: String,
      table: String,
      predicates: Seq[Expression],
      defaultTimeZoneId: String): Seq[CatalogTablePartition] = {
    throw new UnsupportedOperationException(s"List ${shortName()} partition not supported!")
  }

  // --------------------------------------------------------------------------
  // Functions
  // --------------------------------------------------------------------------

  /**
   * Create partitions in specified table.
   */
  override def createFunction(db: String, funcDefinition: CatalogFunction): Unit = {
    throw new UnsupportedOperationException(s"Create ${shortName()} function not supported!")
  }

  /**
   * Drop partitions in specified table.
   */
  override def dropFunction(db: String, funcName: String): Unit = {
    throw new UnsupportedOperationException(s"Drop ${shortName()} function not supported!")
  }

  /**
   * Alter function in specified database.
   */
  override def alterFunction(db: String, funcDefinition: CatalogFunction): Unit = {
    throw new UnsupportedOperationException(s"Alter ${shortName()} function not supported!")
  }

  /**
   * Rename function in specified database.
   */
  override def renameFunction(db: String, oldName: String, newName: String): Unit = {
    throw new UnsupportedOperationException(s"Rename ${shortName()} function not supported!")
  }

  /**
   * Get the metadata of specified function.
   */
  override def getFunction(db: String, funcName: String): CatalogFunction = {
    throw new UnsupportedOperationException(s"Get ${shortName()} function not supported!")
  }

  /**
   * Check if function exists in specified database.
   */
  override def functionExists(db: String, funcName: String): Boolean = {
    throw new UnsupportedOperationException(
      s"Check ${shortName()} function exists not supported!")
  }

  /**
   * Display functions in specified database.
   */
  override def listFunctions(db: String, pattern: String): Seq[String] = {
    throw new UnsupportedOperationException(s"List ${shortName()} function not supported!")
  }

}

object DataSourceManager {
  val UNDERLINE = "_"
  val MIDDLELINE = "-"
  val PERCENT_MARK = "%"
  val COMMA = ","
  val COLON = ":"
  val AGGS = "aggs"
  val INNER_GROUP_ALIAS = "group_by_"
  val INNER_AGGS_ALIAS = "inner_aggs_alias_"
  val INNER_DISTINCT_ALIAS = "inner_distinct_alias"
  val DEFAULT_DATA_TYPE: DataType = StringType
  val STAR = "*"
  val WRITE_SCHEMAS = "write_schemas"
  val NAME = "name"
  val COUNT = "count"
  val COUNT_DISTINCT = "count_distinct"
  val TRUE = "true"
  val FALSE = "false"
  val TABLETYPE = "tableType"
  val SCHEMA_TYPE = "type"

  val TYPE = "type"
  val URL = "url"
  val COORDINATORURL = "coordinator.url"
  val USER = "user"
  val PASSWORD = "password"
  val VERSION = "version"
  val WHITELIST = "whitelist"
  val PUSHDOWN = "pushdown"
  val STREAM = "stream"
  val SCHEMAS = "schemas"
  val SCHEMAS_DISCOVER = "schemas.discover"
  val CACHE_LEVEL = "cache.level"
  val SCHEMAS_DISCOVER_CONFIG = "schemas.discover.config"
  val TEMP_FLAG = "temp_flag"

  val CLUSTER = "cluster"
  val MAX_LIMIT = 10000
  val DEFAULT_lIMIT = 10

  val ORI_DB_NAME = "originDBName"

  val STREAMING_SINK_TYPE = "sink"
  val STREAMING_SINK_PREFIX = STREAMING_SINK_TYPE + "."
  val STREAMING_SINK_PATH = "path"
  val STREAMING_SINK_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers"
  val STREAMING_SINK_TOPIC = "topic"
  val DEFAULT_STREAMING_SINK = StreamingSinkType.CONSOLE.toString()
  val STREAMING_OUTPUT_MODE = "output.mode"
  val DEFAULT_STREAMING_OUTPUT_MODE = "append"
  val STREAMING_CHECKPOINT_LOCATION = "checkpointLocation"
  val STREAMING_TRIGGER_TYPE = "trigger.type"
  val STREAMING_MICRO_BATCH_TRIGGER = "microbatch"
  val STREAMING_ONCE_TRIGGER = "once"
  val STREAMING_CONTINUOUS_TRIGGER = "continuous"
  val DEFAULT_STREAMING_TRIGGER_TYPE = STREAMING_MICRO_BATCH_TRIGGER
  val STREAMING_TRIGGER_DURATION = "trigger.duration"
  val DEFAULT_STREAMING_TRIGGER_DURATION = "1 second"

  val STREAMING_WATER_MARK = "watermark"
  val KAFKA_WATER_MARK = "timestamp"

  private val nextDatabaseId = new AtomicInteger(0)

  def newDatabaseId(): Int = nextDatabaseId.getAndIncrement()

  private val nextXTableId = new AtomicInteger(0)

  def newXTableId(): Int = nextXTableId.getAndIncrement()

  /** Get the `Spark SQL` native `DataType` from the properties of some data source. */
  def getSparkSQLDataType(someType: String): DataType = {
    try {
      CatalystSqlParser.parseDataType(someType)
    } catch {
      case e: ParseException =>
        throw new SparkException("Cannot recognize some type : " + someType, e)
    }
  }

  /**
   * `Spark Catalyst` not allowed middle-line, so transform to underline.
   */
  def lineMiddleToUnder(name: String): String = {
    name.replaceAll(MIDDLELINE, UNDERLINE)
  }

  /**
   * If you have transform middle-line to underline, please not forget restore when display.
   */
  def lineUnderToMiddle(name: String): String = {
    name.replaceAll(UNDERLINE, MIDDLELINE)
  }
}
