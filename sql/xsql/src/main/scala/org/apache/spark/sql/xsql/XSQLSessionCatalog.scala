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

import java.util.{HashMap => JHashMap, HashSet, List => JList}

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, IdentifierWithDatabase, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{
  AttributeReference,
  Expression,
  ExpressionInfo,
  SortOrder
}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.hive.{HiveMetastoreCatalog, HiveSessionCatalog}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.xsql.internal.config.{XSQL_DEFAULT_DATABASE, XSQL_DEFAULT_DATASOURCE}

/**
 * When `spark.sql.catalogImplementation` set to xsql, [[org.apache.spark.sql.SparkSession]] will
 * create XSQLSessionCatalog instead of HiveSessionCatalog. The main difference between
 * HiveSessionCatalog and XSQLSessionCatalog is XSQLSessionCatalog always wrap actions into
 * [[XSQLExternalCatalog]]'s setWorkingDataSource, so that [[XSQLExternalCatalog]] can know which
 * datasource current [[org.apache.spark.sql.catalyst.plans.logical.Command]] is using.
 *
 * @note As [[XSQLExternalCatalog]]'s setWorkingDataSource is wrapped by
 *       workingDataSource.synchronized carefully, multi Command can run in serial mode safely.
 */
private[xsql] class XSQLSessionCatalog(
    externalCatalog: ExternalCatalog,
    globalTempViewManager: GlobalTempViewManager,
    override val metastoreCatalog: HiveMetastoreCatalog,
    functionRegistry: FunctionRegistry,
    conf: SQLConf,
    hadoopConf: Configuration,
    parser: ParserInterface,
    functionResourceLoader: FunctionResourceLoader)
  extends HiveSessionCatalog(
    () => externalCatalog,
    () => globalTempViewManager,
    metastoreCatalog,
    functionRegistry,
    conf,
    hadoopConf,
    parser,
    functionResourceLoader) {

  import CatalogTypes.TablePartitionSpec

  assert(externalCatalog.isInstanceOf[ExternalCatalogWithListener])
  val externalCatalogWithListener = externalCatalog.asInstanceOf[ExternalCatalogWithListener]
  assert(externalCatalogWithListener.unwrapped.isInstanceOf[XSQLExternalCatalog])
  val xsqlExternalCatalog =
    externalCatalogWithListener.unwrapped.asInstanceOf[XSQLExternalCatalog]
  /**
    * Synchronize currentDb of SessionCatalog with XSQLExternalCatalog
    */
  val catalogDB = getCurrentCatalogDatabase.get
  setCurrentDatabase(catalogDB.dataSourceName, catalogDB.name)

  /**
   * Get default cluster.
   */
  def getDefaultCluster(identifier: TableIdentifier): Option[String] = {
    xsqlExternalCatalog.getDefaultCluster(identifier)
  }

  /**
   * Get cluster options.
   */
  def getClusterOptions(clusterName: String): Map[String, String] = {
    xsqlExternalCatalog.getClusterOptions(clusterName)
  }

  /**
   * Get default options from CatalogTable.
   */
  def getDefaultOptions(tableDefinition: CatalogTable): Map[String, String] = {
    val newTableDefinition =
      tableDefinition.copy(identifier = getUsedTableIdentifier(tableDefinition.identifier))
    xsqlExternalCatalog.getDefaultOptions(newTableDefinition)
  }

  def getRedactionPattern(identifier: TableIdentifier): Option[Regex] = {
    xsqlExternalCatalog.getRedactionPattern(identifier)
  }

  // ----------------------------------------------------------------------------
  // DataSources
  // ----------------------------------------------------------------------------

  def getDataSourceType(dsName: String): String = {
    xsqlExternalCatalog.getDataSourceType(dsName)
  }

  def listDatasources(): Seq[String] = {
    xsqlExternalCatalog.listDatasources()
  }

  def listDatasources(pattern: String): Seq[String] = {
    xsqlExternalCatalog.listDatasources(pattern)
  }

  def addDataSource(dataSourceName: String, properties: Map[String, String]): Unit = {
    val defaultSource = conf.getConf(XSQL_DEFAULT_DATASOURCE)
    xsqlExternalCatalog.addDataSource(
      dataSourceName,
      properties,
      dataSourceName.equalsIgnoreCase(defaultSource))
  }

  def removeDataSource(dataSourceName: String, ifExists: Boolean): Unit = {
    xsqlExternalCatalog.removeDataSource(dataSourceName, ifExists)
  }

  def refreshDataSource(dataSourceName: String): Unit = {
    xsqlExternalCatalog.refreshDataSource(dataSourceName)
  }

  def setWorkingDataSource[T](dsNameOpt: Option[String])(body: => T): T = {
    xsqlExternalCatalog.setWorkingDataSource(dsNameOpt)(body)
  }

  // ----------------------------------------------------------------------------
  // Databases
  // ----------------------------------------------------------------------------
  // All methods in this category interact directly with the underlying catalog.
  // ----------------------------------------------------------------------------

  override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {
    setWorkingDataSource(Option(dbDefinition.dataSourceName)) {
      super.createDatabase(dbDefinition, ignoreIfExists)
    }
  }

  def dropDatabase(
      dsName: Option[String] = None,
      dbName: String,
      ignoreIfNotExists: Boolean,
      cascade: Boolean): Unit = {
    setWorkingDataSource(dsName) {
      externalCatalog.dropDatabase(formatDatabaseName(dbName), ignoreIfNotExists, cascade)
    }
  }

  override def alterDatabase(dbDefinition: CatalogDatabase): Unit = {
    setWorkingDataSource(Option(dbDefinition.dataSourceName)) {
      super.alterDatabase(dbDefinition)
    }
  }

  def getDatabaseMetadata(dsName: Option[String], dbName: String): CatalogDatabase = {
    setWorkingDataSource(dsName) {
      externalCatalog.getDatabase(formatDatabaseName(dbName))
    }
  }

  def databaseExists(dsName: Option[String], dbName: String): Boolean = {
    setWorkingDataSource(dsName) {
      externalCatalog.databaseExists(formatDatabaseName(dbName))
    }
  }

  def listDatabasesForXSQL(dsName: String): Seq[String] = {
    setWorkingDataSource(Option(dsName)) {
      externalCatalog.listDatabases()
    }
  }

  def listDatabasesForXSQL(dsName: String, pattern: String): Seq[String] = {
    setWorkingDataSource(Option(dsName)) {
      externalCatalog.listDatabases(pattern)
    }
  }

  override def setCurrentDatabase(db: String): Unit = {
    val catalogDB = getCurrentCatalogDatabase
    if (catalogDB != None) {
      setCurrentDatabase(catalogDB.get.dataSourceName, db)
    }
  }

  def setCurrentDatabase(dsName: String, dbName: String): Unit = {
    synchronized {
      setWorkingDataSource(Option(dsName)) {
        super.setCurrentDatabase(dbName)
        externalCatalog.setCurrentDatabase(dbName)
      }
    }
  }

  def getDatabaseName(identifier: IdentifierWithDatabase): String = {
    identifier.database.getOrElse(getCurrentDatabase)
  }

  def getDataSourceName(identifier: IdentifierWithDatabase): String = {
    identifier.dataSource.getOrElse(getCurrentCatalogDatabase.get.dataSourceName)
  }

  def getCurrentCatalogDatabase: Option[CatalogDatabase] = {
    synchronized {
      xsqlExternalCatalog.getCurrentDatabase
    }
  }

  def getUsedCatalogDatabase(tableName: TableIdentifier): Option[CatalogDatabase] = {
    getUsedCatalogDatabase(tableName.dataSource, tableName.database)
  }

  def getUsedCatalogDatabase(
      dataSource: Option[String],
      database: Option[String]): Option[CatalogDatabase] = {
    if (dataSource == None) {
      if (database == None) {
        getCurrentCatalogDatabase
      } else {
        val catalogDB = getCurrentCatalogDatabase
        Option(getDatabaseMetadata(Option(catalogDB.get.dataSourceName), database.get))
      }
    } else {
      Option(getDatabaseMetadata(dataSource, database.get))
    }
  }

  def getUsedTableIdentifier(tableIdentifier: TableIdentifier): TableIdentifier = {
    val catalogDB: Option[CatalogDatabase] = getUsedCatalogDatabase(tableIdentifier)
    val ds = catalogDB.get.dataSourceName
    val db = catalogDB.get.name
    val table = formatTableName(tableIdentifier.table)
    validateName(table)
    TableIdentifier(table, Some(db), Some(ds))
  }

  // ----------------------------------------------------------------------------
  // Tables
  // ----------------------------------------------------------------------------
  // There are two kinds of tables, temporary views and metastore tables.
  // Temporary views are isolated across sessions and do not belong to any
  // particular database. Metastore tables can be used across multiple
  // sessions as their metadata is persisted in the underlying catalog.
  // ----------------------------------------------------------------------------

  // ----------------------------------------------------
  // | Methods that interact with metastore tables only |
  // ----------------------------------------------------

  override def createTable(
      tableDefinition: CatalogTable,
      ignoreIfExists: Boolean,
      validateLocation: Boolean = true): Unit = {
    setWorkingDataSource(tableDefinition.dataSource) {
      super.createTable(tableDefinition, ignoreIfExists, validateLocation)
    }
  }

  override def validateTableLocation(table: CatalogTable): Unit = {
    // SPARK-19724: the default location of a managed table should be non-existent or empty.
    val allowed = Seq("mongo", "es", "mysql", "hbase", "redis", "druid")
    if (!table.provider.exists(allowed.contains(_)) &&
        table.tableType == CatalogTableType.MANAGED &&
        !conf.allowCreatingManagedTableUsingNonemptyLocation) {
      val tableLocation =
        new Path(table.storage.locationUri.getOrElse(defaultTablePath(table.identifier)))
      val fs = tableLocation.getFileSystem(hadoopConf)

      if (fs.exists(tableLocation) && fs.listStatus(tableLocation).nonEmpty) {
        throw new AnalysisException(
          s"Can not create the managed table('${table.identifier}')" +
            s". The associated location('${tableLocation.toString}') already exists.")
      }
    }
  }

  override def alterTable(tableDefinition: CatalogTable): Unit = {
    setWorkingDataSource(tableDefinition.dataSource) {
      super.alterTable(tableDefinition)
    }
  }

  override def alterTableDataSchema(
      identifier: TableIdentifier,
      newDataSchema: StructType): Unit = {
    setWorkingDataSource(identifier.dataSource) {
      super.alterTableDataSchema(identifier, newDataSchema)
    }
  }

  /**
   * TO SUPPORT MYSQL
   * Alter the data schema of a table identified by the provided table identifier. The new data
   * schema should not have conflict column names with the existing partition columns, and should
   * still contain all the existing data columns.
   *
   * @param identifier    TableIdentifier
   * @param newDataSchema Updated data schema to be used for the table
   */
  def alterTableDataSchema(
      identifier: TableIdentifier,
      newDataSchema: StructType,
      queryContent: String): Unit = {
    setWorkingDataSource(identifier.dataSource) {
      xsqlExternalCatalog.alterTableDataSchema(
        getDatabaseName(identifier),
        identifier.table,
        newDataSchema,
        queryContent)
    }
  }

  override def alterTableStats(
      identifier: TableIdentifier,
      newStats: Option[CatalogStatistics]): Unit = {
    setWorkingDataSource(identifier.dataSource) {
      super.alterTableStats(identifier, newStats)
    }
  }

  override def tableExists(name: TableIdentifier): Boolean = {
    setWorkingDataSource(name.dataSource) {
      super.tableExists(name)
    }
  }

  @throws[NoSuchDatabaseException]
  @throws[NoSuchTableException]
  override def getTableMetadata(name: TableIdentifier): CatalogTable = {
    setWorkingDataSource(name.dataSource) {
      super.getTableMetadata(name)
    }
  }

  /**
   * Load files stored in given path into an existing metastore table.
   * If no database is specified, assume the table is in the current database.
   * If the specified table is not found in the database then a [[NoSuchTableException]] is thrown.
   */
  override def loadTable(
      name: TableIdentifier,
      loadPath: String,
      isOverwrite: Boolean,
      isSrcLocal: Boolean): Unit = {
    setWorkingDataSource(name.dataSource) {
      super.loadTable(name, loadPath, isOverwrite, isSrcLocal)
    }
  }

  /**
   * Load files stored in given path into the partition of an existing metastore table.
   * If no database is specified, assume the table is in the current database.
   * If the specified table is not found in the database then a [[NoSuchTableException]] is thrown.
   */
  override def loadPartition(
      name: TableIdentifier,
      loadPath: String,
      spec: TablePartitionSpec,
      isOverwrite: Boolean,
      inheritTableSpecs: Boolean,
      isSrcLocal: Boolean): Unit = {
    setWorkingDataSource(name.dataSource) {
      super.loadPartition(name, loadPath, spec, isOverwrite, inheritTableSpecs, isSrcLocal)
    }
  }

  @throws[NoSuchDatabaseException]
  @throws[NoSuchTableException]
  def getRawTable(name: TableIdentifier): CatalogTable = {
    setWorkingDataSource(name.dataSource) {
      xsqlExternalCatalog.getRawTable(getDatabaseName(name), formatTableName(name.table))
    }
  }

  // ----------------------------------------------
  // | Methods that interact with temp views only |
  // ----------------------------------------------

  // -------------------------------------------------------------
  // | Methods that interact with temporary and metastore tables |
  // -------------------------------------------------------------

  override def renameTable(oldName: TableIdentifier, newName: TableIdentifier): Unit =
    synchronized {
      val oldDs = getDataSourceName(oldName)
      val newDs = getDataSourceName(newName)
      if (oldDs != newDs) {
        throw new AnalysisException(
          s"RENAME TABLE source and destination datasources do not match: '$oldDs' != '$newDs'")
      }
      setWorkingDataSource(oldName.dataSource) {
        super.renameTable(oldName, newName)
      }
    }

  def truncateTable(
      name: TableIdentifier,
      partitionSpec: Option[TablePartitionSpec]): CatalogTable = {
    val table = getTableMetadata(name)
    setWorkingDataSource(name.dataSource) {
      xsqlExternalCatalog.truncateTable(table, partitionSpec)
      table
    }
  }

  override def dropTable(
      name: TableIdentifier,
      ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = synchronized {
    setWorkingDataSource(name.dataSource) {
      super.dropTable(name, ignoreIfNotExists, purge)
    }
  }

  override def lookupRelation(name: TableIdentifier): LogicalPlan = {
    synchronized {
      setWorkingDataSource(name.dataSource) {
        super.lookupRelation(name)
      }
    }
  }

  def listTablesForXSQL(dsName: String, dbName: String): Seq[TableIdentifier] = {
    setWorkingDataSource(Option(dsName)) {
      super.listTables(dbName, "*")
    }
  }

  def listTablesForXSQL(dsName: String, dbName: String, pattern: String): Seq[TableIdentifier] = {
    setWorkingDataSource(Option(dsName)) {
      super.listTables(dbName, pattern)
    }
  }

  override def refreshTable(name: TableIdentifier): Unit = synchronized {
    setWorkingDataSource(name.dataSource) {
      super.refreshTable(name)
    }
  }

  // --------------------------------------------------------------------------
  // PushDown Execute
  // --------------------------------------------------------------------------

  /**
   * Build filter argument in query.
   */
  def buildFilterArgument(
      tableDefinition: CatalogTable,
      condition: Expression): JHashMap[String, Any] = {
    xsqlExternalCatalog.buildFilterArgument(tableDefinition, condition)
  }

  /**
   * Build group argument in query.
   */
  def buildGroupArgument(
      tableDefinition: CatalogTable,
      fieldName: String,
      groupByKeys: ArrayBuffer[String],
      innerAggsMap: JHashMap[String, Any],
      groupMap: JHashMap[String, Any]): JHashMap[String, Any] = {
    xsqlExternalCatalog.buildGroupArgument(
      tableDefinition,
      fieldName,
      groupByKeys,
      innerAggsMap,
      groupMap)
  }

  /**
   * Build aggregate argument in query.
   */
  def buildAggregateArgument(
      tableDefinition: CatalogTable,
      fieldName: String,
      aggregatefuncName: String,
      aggsMap: JHashMap[String, Any],
      aggregateAliasOpts: Option[String]): StructField = {
    xsqlExternalCatalog.buildAggregateArgument(
      tableDefinition,
      fieldName,
      aggregatefuncName,
      aggsMap,
      aggregateAliasOpts)
  }

  /**
   * Build sort argument in query without aggregate.
   */
  def buildSortArgument(
      tableDefinition: CatalogTable,
      order: Seq[SortOrder]): java.util.List[java.util.HashMap[String, Any]] = {
    xsqlExternalCatalog.buildSortArgument(tableDefinition, order)
  }

  /**
   * Build sort argument in query with aggregate.
   */
  def buildSortArgumentInAggregate(
      tableDefinition: CatalogTable,
      order: Seq[SortOrder],
      aggs: JHashMap[String, Any]): Unit = {
    order.foreach { x =>
      // ES not support null ordering.
      // val nullOrdering = x.nullOrdering
      x.child match {
        case AggregateExpression(aggFun, mode, isDistinct, resultId) =>
          xsqlExternalCatalog.buildSortArgumentInAggregate(
            tableDefinition,
            null,
            aggs,
            aggFun,
            x.direction,
            isDistinct)
        case UnresolvedFunction(funcId, children, isDistinct) =>
          val func = lookupFunction(funcId, children)
          xsqlExternalCatalog.buildSortArgumentInAggregate(
            tableDefinition,
            null,
            aggs,
            func,
            x.direction,
            isDistinct)
        case attr: UnresolvedAttribute =>
          xsqlExternalCatalog.buildSortArgumentInAggregate(
            tableDefinition,
            null,
            aggs,
            attr,
            x.direction,
            false)
      }
    }
  }

  /**
   * Build distinct argument in query without aggregate.
   */
  def buildDistinctArgument(
      identifier: TableIdentifier,
      columns: java.util.List[String]): (java.util.HashMap[String, Any], Seq[String]) = {
    xsqlExternalCatalog.buildDistinctArgument(identifier, columns)
  }

  def isPushDown(identifier: TableIdentifier): Boolean = {
    val tableIdentifier = getUsedTableIdentifier(identifier)
    xsqlExternalCatalog.isPushDown(conf, tableIdentifier)
  }

  def isPushDown(
      identifier: TableIdentifier,
      dsToTableIdentifier: JHashMap[String, HashSet[TableIdentifier]],
      plan: LogicalPlan): Boolean = {
    if (dsToTableIdentifier.keySet.size > 1) {
      false
    } else {
      val newPlan = plan transformUp {
        case UnresolvedRelation(tableIdentifier) =>
          val newTableIdentifier = getUsedTableIdentifier(tableIdentifier)
          UnresolvedRelation(newTableIdentifier)
      }
      val tableIdentifier = getUsedTableIdentifier(identifier)
      xsqlExternalCatalog.isPushDown(conf, tableIdentifier, newPlan)
    }
  }

  def isConsiderTablesRowsToPushdown(identifier: TableIdentifier): Boolean = {
    val tableIdentifier = getUsedTableIdentifier(identifier)
    xsqlExternalCatalog.isConsiderTablesRowsToPushdown(conf, tableIdentifier)
  }

  def isConsiderTablesIndexToPushdown(identifier: TableIdentifier): Boolean = {
    val tableIdentifier = getUsedTableIdentifier(identifier)
    xsqlExternalCatalog.isConsiderTablesIndexToPushdown(conf, tableIdentifier)
  }

  def getTableRowsAndIndex(identifier: TableIdentifier): (Int, Seq[String]) = {
    val tableIdentifier = getUsedTableIdentifier(identifier)
    xsqlExternalCatalog.getTableRowsAndIndex(tableIdentifier)
  }

  def isStream(identifier: TableIdentifier): Boolean = {
    val tableIdentifier = getUsedTableIdentifier(identifier)
    xsqlExternalCatalog.isStream(conf, tableIdentifier)
  }

  def scanTables(
      tableDefinition: CatalogTable,
      dataCols: Seq[AttributeReference],
      limit: Boolean,
      queryContent: String): Seq[Seq[Any]] = {
    val tableIdentifier = getUsedTableIdentifier(tableDefinition.identifier)
    val newTableDefinition = tableDefinition.copy(identifier = tableIdentifier)
    setWorkingDataSource(tableDefinition.dataSource) {
      xsqlExternalCatalog.scanTables(newTableDefinition, dataCols, limit, queryContent)
    }
  }

  def scanSingleTable(
      tableDefinition: CatalogTable,
      columns: JList[String],
      condition: JHashMap[String, Any],
      sort: JList[JHashMap[String, Any]],
      aggs: JHashMap[String, Any],
      groupByKeys: Seq[String],
      limit: Int): Seq[Seq[Any]] = {
    val tableIdentifier = getUsedTableIdentifier(tableDefinition.identifier)
    val newTableDefinition = tableDefinition.copy(identifier = tableIdentifier)
    setWorkingDataSource(tableDefinition.dataSource) {
      xsqlExternalCatalog.scanTable(
        newTableDefinition,
        columns,
        condition,
        sort,
        aggs,
        groupByKeys,
        limit)
    }
  }

  // ----------------------------------------------------------------------------
  // Partitions
  // ----------------------------------------------------------------------------
  // All methods in this category interact directly with the underlying catalog.
  // These methods are concerned with only metastore tables.
  // ----------------------------------------------------------------------------

  // TODO: We need to figure out how these methods interact with our data source
  // tables. For such tables, we do not store values of partitioning columns in
  // the metastore. For now, partition values of a data source table will be
  // automatically discovered when we load the table.

  override def createPartitions(
      tableName: TableIdentifier,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = {
    setWorkingDataSource(tableName.dataSource) {
      super.createPartitions(tableName, parts, ignoreIfExists)
    }
  }

  override def dropPartitions(
      tableName: TableIdentifier,
      specs: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean,
      purge: Boolean,
      retainData: Boolean): Unit = {
    setWorkingDataSource(tableName.dataSource) {
      super.dropPartitions(tableName, specs, ignoreIfNotExists, purge, retainData)
    }
  }

  override def renamePartitions(
      tableName: TableIdentifier,
      specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit = {
    setWorkingDataSource(tableName.dataSource) {
      super.renamePartitions(tableName, specs, newSpecs)
    }
  }

  override def alterPartitions(
      tableName: TableIdentifier,
      parts: Seq[CatalogTablePartition]): Unit = {
    setWorkingDataSource(tableName.dataSource) {
      super.alterPartitions(tableName, parts)
    }
  }

  override def getPartition(
      tableName: TableIdentifier,
      spec: TablePartitionSpec): CatalogTablePartition = {
    setWorkingDataSource(tableName.dataSource) {
      super.getPartition(tableName, spec)
    }
  }

  override def listPartitionNames(
      tableName: TableIdentifier,
      partialSpec: Option[TablePartitionSpec] = None): Seq[String] = {
    setWorkingDataSource(tableName.dataSource) {
      super.listPartitionNames(tableName, partialSpec)
    }
  }

  override def listPartitions(
      tableName: TableIdentifier,
      partialSpec: Option[TablePartitionSpec] = None): Seq[CatalogTablePartition] = {
    setWorkingDataSource(tableName.dataSource) {
      super.listPartitions(tableName, partialSpec)
    }
  }

  override def listPartitionsByFilter(
      tableName: TableIdentifier,
      predicates: Seq[Expression]): Seq[CatalogTablePartition] = {
    setWorkingDataSource(tableName.dataSource) {
      super.listPartitionsByFilter(tableName, predicates)
    }
  }

  // ----------------------------------------------------------------------------
  // Functions
  // ----------------------------------------------------------------------------
  // There are two kinds of functions, temporary functions and metastore
  // functions (permanent UDFs). Temporary functions are isolated across
  // sessions. Metastore functions can be used across multiple sessions as
  // their metadata is persisted in the underlying catalog.
  // ----------------------------------------------------------------------------

  // -------------------------------------------------------
  // | Methods that interact with metastore functions only |
  // -------------------------------------------------------

  /**
   * Create a function in the database specified in `funcDefinition`.
   * If no such database is specified, create it in the current database.
   *
   * @param ignoreIfExists: When true, ignore if the function with the specified name exists
   *                        in the specified database.
   */
  override def createFunction(funcDefinition: CatalogFunction, ignoreIfExists: Boolean): Unit = {
    setWorkingDataSource(Option(getDataSourceName(funcDefinition.identifier))) {
      super.createFunction(funcDefinition, ignoreIfExists)
    }
  }

  /**
   * Drop a metastore function.
   * If no database is specified, assume the function is in the current database.
   */
  override def dropFunction(name: FunctionIdentifier, ignoreIfNotExists: Boolean): Unit = {
    setWorkingDataSource(Option(getDataSourceName(name))) {
      super.dropFunction(name, ignoreIfNotExists)
    }
  }

  /**
   * overwirte a metastore function in the database specified in `funcDefinition`..
   * If no database is specified, assume the function is in the current database.
   */
  override def alterFunction(funcDefinition: CatalogFunction): Unit = {
    setWorkingDataSource(Option(getDataSourceName(funcDefinition.identifier))) {
      super.alterFunction(funcDefinition)
    }
  }

  /**
   * Check if the function with the specified name exists
   */
  override def functionExists(name: FunctionIdentifier): Boolean = {
    setWorkingDataSource(Option(getDataSourceName(name))) {
      super.functionExists(name)
    }
  }

  // ----------------------------------------------------------------
  // | Methods that interact with temporary and metastore functions |
  // ----------------------------------------------------------------

  override def isPersistentFunction(name: FunctionIdentifier): Boolean = {
    setWorkingDataSource(name.dataSource) {
      super.isPersistentFunction(name)
    }
  }

  /**
   * Look up the [[ExpressionInfo]] associated with the specified function, assuming it exists.
   */
  override def lookupFunctionInfo(name: FunctionIdentifier): ExpressionInfo = synchronized {
    setWorkingDataSource(Option(getDataSourceName(name))) {
      super.lookupFunctionInfo(name)
    }
  }

  override def lookupFunction(name: FunctionIdentifier, children: Seq[Expression]): Expression = {
    setWorkingDataSource(Option(getDataSourceName(name))) {
      super.lookupFunction(name, children)
    }
  }

  /**
   * List all functions in the specified database, including temporary functions. This
   * returns the function identifier and the scope in which it was defined (system or user
   * defined).
   */
  override def listFunctions(
      dsName: String,
      dbName: String): Seq[(FunctionIdentifier, String)] = {
    setWorkingDataSource(Option(dsName)) {
      super.listFunctions(dbName, "*")
    }
  }

  /**
   * List all matching functions in the specified database, including temporary functions. This
   * returns the function identifier and the scope in which it was defined (system or user
   * defined).
   */
  def listFunctions(
      dsName: String,
      dbName: String,
      pattern: String): Seq[(FunctionIdentifier, String)] = {
    setWorkingDataSource(Option(dsName)) {
      super.listFunctions(dbName, pattern)
    }
  }

  // -----------------
  // | Other methods |
  // -----------------

  /**
   * Drop all existing databases (except "default"), tables, partitions and functions,
   *  and set the current database to "default".
   * This is mainly used for tests.
   */
  override def reset(): Unit = synchronized {
    val defaultDataSource = conf.getConf(XSQL_DEFAULT_DATASOURCE)
    val defaultDatabase = conf.getConf(XSQL_DEFAULT_DATABASE)
    setCurrentDatabase(defaultDataSource, defaultDatabase)
    listDatabasesForXSQL(defaultDataSource).filter(_ != defaultDatabase).foreach { db =>
      dropDatabase(Some(defaultDataSource), db, ignoreIfNotExists = false, cascade = true)
    }
    listTablesForXSQL(defaultDataSource, defaultDatabase).foreach { table =>
      dropTable(table, ignoreIfNotExists = false, purge = false)
    }
    listFunctions(defaultDataSource, defaultDatabase).map(_._1).foreach { func =>
      if (func.database.isDefined) {
        dropFunction(func, ignoreIfNotExists = false)
      } else {
        dropTempFunction(func.funcName, ignoreIfNotExists = false)
      }
    }
    clearTempTables()
    globalTempViewManager.clear()
    functionRegistry.clear()
    // restore built-in functions
    FunctionRegistry.builtin.listFunction().foreach { f =>
      val expressionInfo = FunctionRegistry.builtin.lookupFunction(f)
      val functionBuilder = FunctionRegistry.builtin.lookupFunctionBuilder(f)
      require(expressionInfo.isDefined, s"built-in function '$f' is missing expression info")
      require(functionBuilder.isDefined, s"built-in function '$f' is missing function builder")
      functionRegistry.registerFunction(f, expressionInfo.get, functionBuilder.get)
    }
  }

  /**
   * Checks if the given name conforms the Hive standard ("[a-zA-Z_0-9]+"),
   * i.e. if this name only contains characters, numbers, and _.
   *
   * This method is intended to have the same behavior of
   * org.apache.hadoop.hive.metastore.MetaStoreUtils.validateName.
   */
  private def validateName(name: String): Unit = {
    val validNameFormat = "([\\w:_-]+)".r
    if (!validNameFormat.pattern.matcher(name).matches()) {
      throw new AnalysisException(
        s"`$name` is not a valid name for tables/databases. " +
          "Valid names only contain alphabet characters, numbers and _.")
    }
  }

  /**
   * Stop XSQLSessionCatalog
   */
  def stop(): Unit = {
    xsqlExternalCatalog.stop()
  }

}
