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

import java.lang.reflect.InvocationTargetException
import java.util.Locale
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.util.control.NonFatal
import scala.util.matching.Regex

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{
  NoSuchDatabaseException,
  TableAlreadyExistsException
}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{
  AttributeReference,
  Expression,
  SortDirection,
  SortOrder
}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.xsql.DataSourceManager.TEMP_FLAG
import org.apache.spark.sql.xsql.DataSourceType._
import org.apache.spark.sql.xsql.internal.config._
import org.apache.spark.sql.xsql.manager._
import org.apache.spark.sql.xsql.util.{Utils => XSQLUtils}
import org.apache.spark.util.Utils

/**
 * When `spark.sql.catalogImplementation` set to xsql, all data/metadata operations of external
 * datasource will be routed by XSQLExternalCatalog and handled by corresponding
 * [[DataSourceManager]].
 *
 * This class will be instantiated by [[org.apache.spark.sql.internal.SharedState]], which means
 * that all loaded ds/db/tb information will be shared between different SparkSession.
 *
 * @note built-in DataSources contains: Hive, ElasticSearch, Mongo, Mysql, Redis
 * @note XSQLExternalCatalog is working above HiveExternalCatalog and other DataSourceManager,
 *       and HiveExternalCatalog can be regard as Hive DataSourceManager in XSQL.
 * @note XSQLExternalCatalog will load and cache [[CatalogDatabase]] when initialize with default
 *       configuration. User should set `cache.level` to 2 rather than 1, if they hope to load and
 *       cache [[CatalogTable]] of particular DataSource in the beginning.
 */
private[xsql] class XSQLExternalCatalog(conf: SparkConf, hadoopConf: Configuration)
  extends ExternalCatalog
  with Logging {

  import CatalogTypes.TablePartitionSpec
  import XSQLExternalCatalog._

  private val yarnClustersByName = new HashMap[String, HashMap[String, String]]

  private lazy val yarnClusterNames = HashSet.empty ++ yarnClustersByName.keySet

  private val dataSourcesByName = new HashMap[String, CatalogDataSource]

  private val dataSourceToCatalogDatabase = new HashMap[String, HashMap[String, CatalogDatabase]]

  private val dbToCatalogTable = new HashMap[Int, HashMap[String, CatalogTable]]

  /**
   * Set by `use [ds.]db`, should be the same with XSQLSessionCatalog's currentDb all the time
   */
  private var currentDataBase: Option[CatalogDatabase] = None

  /**
   * Set by XSQLSessionCatalog when execute some Command, should reset to null after Command
   * finished. We decided not to use currentDataBase.get.dataSourceName by default unless call
   * setWorkingDataSource with None when Command don't have any datasource identifier. All these
   * Consideration is to prevent from routing to wrong datasource, may cause loss of data sometime.
   * {{{
   *    setWorkingDataSource(Command's ds or None) {
   *      operations
   *    }
   *    withWorkingDS { (ds, dsName) =>
   *      operations
   *    }
   *    withWorkingDSDB(db) { (ds, db, dsName, dbName) =>
   *      operations
   *    }
   * }}}
   */
  private val workingDataSource = new AtomicReference[CatalogDataSource]

  /**
   * As setWorkingDataSource can be called nestly in some situation, this attribute indicate if
   * workingDataSource has been setted before. If true, workingDatasource should not reset to null
   * at the moment of inner invoke finished.
   */
  private val setted: AtomicBoolean = new AtomicBoolean(false)

  setupAndInitMetadata
  defaultDataSourceForCurrent

  /**
   * Parse yarn, data source and initialize meta data.
   * The default data source must configured.
   */
  private def setupAndInitMetadata(): Unit = {
    if (!conf.contains(XSQL_DATASOURCES)) {
      val xsqlConf = conf.get("spark.xsql.properties.file", "xsql.conf")
      val xsqlConfFile = XSQLUtils.getPropertiesFile(file = xsqlConf)
      logInfo(s"reading xsql configuration from ${xsqlConfFile}")
      val properties = Utils.getPropertiesFromFile(xsqlConfFile)
      properties.foreach {
        case (k, v) =>
          if (!conf.contains(k)) {
            conf.set(k, v)
          }
      }
    }
    // Parse multiple yarn configuration.
    val yarnClusterInfos = conf.getAllWithPrefix(SPARK_XSQL_YARN_PREFIX).toMap
    yarnClusterInfos.foreach { kv =>
      val clusterName = kv._1
      val clusterConf = kv._2
      val clusterPropertiesFile = XSQLUtils.getPropertiesFile(file = clusterConf)
      val clusterProperties = new HashMap[String, String]()
      Option(clusterPropertiesFile).foreach { filename =>
        val properties = Utils.getPropertiesFromFile(filename)
        properties.foreach {
          case (k, v) =>
            clusterProperties(k) = v
        }
        yarnClustersByName(clusterName) = clusterProperties
      }
    }
    // Parse multiple data source's configuration.
    val dataSourceInfos = conf.getAllWithPrefix(SPARK_XSQL_DATASOURCE_PREFIX).toMap
    val dataSources = conf.get(XSQL_DATASOURCES)
    val defaultSource = conf.get(XSQL_DEFAULT_DATASOURCE)
    if (dataSources == None || !dataSources.contains(defaultSource)) {
      throw new SparkException("default data source must configured!")
    }
    dataSources.foreach { dataSourceName =>
      val infos = dataSourceInfos
        .filter {
          case (key, value) =>
            key.startsWith(s"$dataSourceName.")
        }
        .map {
          case (key, value) =>
            (key.substring(dataSourceName.length + 1), value)
        }
      addDataSource(dataSourceName, infos, dataSourceName.equalsIgnoreCase(defaultSource))
    }
  }

  private def defaultDataSourceForCurrent(): Unit = {
    setWorkingDataSource(Option(conf.get(XSQL_DEFAULT_DATASOURCE))) {
      setCurrentDatabase(conf.get(XSQL_DEFAULT_DATABASE))
    }
  }

  private def yarnClusterExists(clusterName: String): Boolean = {
    yarnClustersByName.contains(clusterName)
  }

  /**
   * Get default cluster.
   */
  def getDefaultCluster(identifier: TableIdentifier): Option[String] = {
    assert(identifier.dataSource.isDefined)
    val dsName = identifier.dataSource
    val ds = dataSourcesByName(dsName.get)
    ds.dsManager.getDefaultCluster(yarnClusterNames, dataSourcesByName, identifier)
  }

  /**
   * Get cluster options.
   */
  def getClusterOptions(clusterName: String): Map[String, String] = {
    if (!yarnClusterExists(clusterName)) {
      throw new SparkException(s"Yarn cluster called $clusterName undefined!")
    }
    yarnClustersByName(clusterName).toMap
  }

  /**
   * Get redaction pattern
   */
  def getRedactionPattern(identifier: TableIdentifier): Option[Regex] = {
    assert(identifier.dataSource.isDefined)
    val dsName = identifier.dataSource.get
    val ds = dataSourcesByName(dsName)
    ds.dsManager.getRedactionPattern
  }

  // --------------------------------------------------------------------------
  // PushDown Execute
  // --------------------------------------------------------------------------

  def buildFilterArgument(
      tableDefinition: CatalogTable,
      condition: Expression): java.util.HashMap[String, Any] = {
    assert(tableDefinition.identifier.dataSource.isDefined)
    val dsName = tableDefinition.identifier.dataSource.get
    val ds = dataSourcesByName(dsName)
    ds.dsManager.buildFilterArgument(tableDefinition, condition)
  }

  def buildGroupArgument(
      tableDefinition: CatalogTable,
      fieldName: String,
      groupByKeys: ArrayBuffer[String],
      innerAggsMap: java.util.HashMap[String, Any],
      groupMap: java.util.HashMap[String, Any]): java.util.HashMap[String, Any] = {
    assert(tableDefinition.identifier.dataSource.isDefined)
    val dsName = tableDefinition.identifier.dataSource.get
    val ds = dataSourcesByName(dsName)
    ds.dsManager.buildGroupArgument(fieldName, groupByKeys, innerAggsMap, groupMap)
  }

  def buildAggregateArgument(
      tableDefinition: CatalogTable,
      fieldName: String,
      aggregatefuncName: String,
      aggsMap: java.util.HashMap[String, Any],
      aggregateAliasOpts: Option[String]): StructField = {
    assert(tableDefinition.identifier.dataSource.isDefined)
    val dsName = tableDefinition.identifier.dataSource.get
    val ds = dataSourcesByName(dsName)
    var structField = new StructField("*", StringType)
    if (!fieldName.equalsIgnoreCase("*")) {
      structField = tableDefinition.schema(fieldName)
    }
    ds.dsManager.buildAggregateArgument(
      structField,
      aggregatefuncName,
      aggsMap,
      aggregateAliasOpts)
  }

  def buildSortArgument(
      tableDefinition: CatalogTable,
      order: Seq[SortOrder]): java.util.List[java.util.HashMap[String, Any]] = {
    assert(tableDefinition.identifier.dataSource.isDefined)
    val dsName = tableDefinition.identifier.dataSource.get
    val ds = dataSourcesByName(dsName)
    ds.dsManager.buildSortArgument(order)
  }

  def buildSortArgumentInAggregate(
      tableDefinition: CatalogTable,
      parentGroupMap: java.util.HashMap[String, Any],
      aggs: java.util.HashMap[String, Any],
      expr: Expression,
      direction: SortDirection,
      isDistinct: Boolean): Unit = {
    assert(tableDefinition.identifier.dataSource.isDefined)
    val dsName = tableDefinition.identifier.dataSource.get
    val ds = dataSourcesByName(dsName)
    ds.dsManager.buildSortArgumentInAggregate(parentGroupMap, aggs, expr, direction, isDistinct)
  }

  def buildDistinctArgument(
      identifier: TableIdentifier,
      columns: java.util.List[String]): (java.util.HashMap[String, Any], Seq[String]) = {
    assert(identifier.dataSource.isDefined)
    val dsName = identifier.dataSource.get
    val ds = dataSourcesByName(dsName)
    ds.dsManager.buildDistinctArgument(columns)
  }

  def getTableRowsAndIndex(identifier: TableIdentifier): (Int, Seq[String]) = {
    assert(
      identifier.dataSource.isDefined &&
        identifier.database.isDefined)
    val dsName = identifier.dataSource.get
    val ds = dataSourcesByName(dsName)
    ds.dsManager.getXSQLTableRowsAndIndex(identifier)
  }

  /**
   * Whether to push down according to the number of rows in the table
   */
  def isConsiderTablesRowsToPushdown(
      runtimeConf: SQLConf,
      identifier: TableIdentifier): Boolean = {
    assert(identifier.dataSource.isDefined)
    val dsName = identifier.dataSource.get
    val ds = dataSourcesByName(dsName)
    runtimeConf
      .getConfString(
        s"${SPARK_XSQL_DATASOURCE_PREFIX}${dsName}." +
          s"${DataSourceManager.PUSHDOWN}.${MysqlManager.CONSIDER_TABLE_ROWS_TO_PUSHDOWN}",
        DataSourceManager.TRUE)
      .toBoolean
  }

  /**
   * Whether to push down according to the index of table
   */
  def isConsiderTablesIndexToPushdown(
      runtimeConf: SQLConf,
      identifier: TableIdentifier): Boolean = {
    assert(identifier.dataSource.isDefined)
    val dsName = identifier.dataSource.get
    val ds = dataSourcesByName(dsName)
    runtimeConf
      .getConfString(
        s"${SPARK_XSQL_DATASOURCE_PREFIX}${dsName}." +
          s"${DataSourceManager.PUSHDOWN}.${MysqlManager.CONSIDER_TABLE_INDEX_TO_PUSHDOWN}",
        DataSourceManager.TRUE)
      .toBoolean
  }

  def isPushDown(runtimeConf: SQLConf, identifier: TableIdentifier): Boolean = {
    assert(identifier.dataSource.isDefined)
    val dsName = identifier.dataSource.get
    val ds = dataSourcesByName(dsName)
    val dsType = ds.getDSType
    dsType match {
      case HIVE | KAFKA => ds.dsManager.isPushDown
      case _ =>
        runtimeConf
          .getConfString(
            s"${SPARK_XSQL_DATASOURCE_PREFIX}${dsName}.${DataSourceManager.PUSHDOWN}",
            ds.dsManager.isPushDown.toString)
          .toBoolean
    }
  }

  /**
   * MySQL dedicated.
   */
  def isPushDown(
      runtimeConf: SQLConf,
      identifier: TableIdentifier,
      plan: LogicalPlan): Boolean = {
    val dsName = identifier.dataSource.get
    val ds = dataSourcesByName(dsName)
    ds.dsManager.isPushDown(runtimeConf, dsName, plan)
  }

  def isStream(runtimeConf: SQLConf, identifier: TableIdentifier): Boolean = {
    assert(identifier.dataSource.isDefined)
    val dsName = identifier.dataSource.get
    val ds = dataSourcesByName(dsName)
    runtimeConf
      .getConfString(
        s"${SPARK_XSQL_DATASOURCE_PREFIX}${dsName}.${DataSourceManager.STREAM}",
        ds.dsManager.isStream.toString)
      .toBoolean
  }

  def scanTables(
      tableDefinition: CatalogTable,
      dataCols: Seq[AttributeReference],
      limit: Boolean,
      query: String): Seq[Seq[Any]] =
    withWorkingDSDB(tableDefinition.database) { (ds, db, dsName, dbName) =>
      requireTableExists(dbName, tableDefinition.identifier.table)
      ds.dsManager.scanXSQLTables(dataSourcesByName, tableDefinition, dataCols, limit, query)
    }

  def scanTable(
      tableDefinition: CatalogTable,
      columns: java.util.List[String],
      condition: java.util.HashMap[String, Any],
      sort: java.util.List[java.util.HashMap[String, Any]],
      aggs: java.util.HashMap[String, Any],
      groupByKeys: Seq[String],
      limit: Int): Seq[Seq[Any]] = {
    withWorkingDSDB(tableDefinition.database) { (ds, db, dsName, dbName) =>
      requireTableExists(dbName, tableDefinition.identifier.table)
      ds.dsManager.scanXSQLTable(
        dataSourcesByName,
        tableDefinition,
        columns,
        condition,
        sort,
        aggs,
        groupByKeys,
        limit)
    }
  }

  // --------------------------------------------------------------------------
  // DataSources
  // --------------------------------------------------------------------------

  def getDataSourceType(dsName: String): String = {
    val ds = dataSourcesByName(dsName)
    ds.getDSType.toString
  }

  def listDatasources(): Seq[String] = {
    if (dataSourcesByName.isEmpty) {
      return Seq.empty
    }
    return Seq.empty[String].++:(dataSourcesByName.keySet)
  }

  def addDataSource(
      dataSourceName: String,
      properties: Map[String, String],
      isDefault: Boolean): Unit = {
    logInfo(s"parse data source $dataSourceName")
    if (dataSourcesByName.contains(dataSourceName)) {
      throw new SparkException(
        "Each data source must have different name," +
          " please check your configuration!")
    }
    val dsType = properties.get(DataSourceManager.TYPE)
    if (dsType == None) {
      throw new SparkException("each data source must have type!")
    }

    val datasourceType = dsType.get.toUpperCase(Locale.ROOT)
    val datasourceManager = DataSourceManagerFactory.create(datasourceType, conf, hadoopConf)
    datasourceManager.parse(
      isDefault,
      dataSourceName,
      properties,
      dataSourcesByName,
      dataSourceToCatalogDatabase,
      dbToCatalogTable)
  }

  def removeDataSource(
      dataSourceName: String,
      ifExists: Boolean,
      checkTemp: Boolean = true): Unit = {
    if (!dataSourcesByName.contains(dataSourceName)) {
      if (!ifExists) {
        throw new SparkException(s"${dataSourceName} do not exists!")
      }
    } else {
      if (checkTemp) {
        val tempFlag =
          dataSourcesByName(dataSourceName).dsManager.getProperty(TEMP_FLAG)
        assert(tempFlag.equals("true"), "cannot remove datatsource defined by xsql.conf")
      }
      // remove table first, then database, datasource least
      val databases = dataSourceToCatalogDatabase(dataSourceName)
      databases.foreach {
        case (dbName, catalogDatabase) =>
          dbToCatalogTable.remove(catalogDatabase.id)
      }
      dataSourceToCatalogDatabase.remove(dataSourceName)
      dataSourcesByName.remove(dataSourceName)
    }
  }

  def refreshDataSource(dataSourceName: String): Unit = {
    assert(dataSourcesByName.contains(dataSourceName), s"${dataSourceName} do not exists!")
    val infos = dataSourcesByName(dataSourceName).dsManager.getProperties()
    removeDataSource(dataSourceName, false, checkTemp = false)
    val defaultSource = conf.get(XSQL_DEFAULT_DATASOURCE)
    addDataSource(dataSourceName, infos, dataSourceName.equalsIgnoreCase(defaultSource))
  }

  def listDatasources(pattern: String): Seq[String] = {
    StringUtils.filterPattern(listDatasources(), pattern)
  }

  /**
   * Get current Used database.
   */
  def getCurrentDatabase(): Option[CatalogDatabase] = {
    currentDataBase
  }

  def getOriginDatabaseName(db: CatalogDatabase): String = {
    db.properties.getOrElse(DataSourceManager.ORI_DB_NAME, db.name)
  }

  /**
   * Get default options from CatalogTable. used for xsql.
   */
  def getDefaultOptions(table: CatalogTable): Map[String, String] = {
    assert(
      table.identifier.dataSource.isDefined &&
        table.identifier.database.isDefined)
    val dsName = table.identifier.dataSource
    val ds = dataSourcesByName(dsName.get)
    ds.dsManager.getDefaultOptions(table)
  }

  // --------------------------------------------------------------------------
  // Databases
  // --------------------------------------------------------------------------

  override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {
    withWorkingDS { (ds, dsName) =>
      if (databaseExists(dbDefinition.name)) {
        if (ignoreIfExists) {
          return
        }
        throw new SparkException(s"Database ${dsName}.${dbDefinition.name} already exists")
      }
      ds.dsManager.createDatabase(dbDefinition, ignoreIfExists)
      val xdatabases =
        dataSourceToCatalogDatabase.getOrElseUpdate(dsName, new HashMap[String, CatalogDatabase])
      xdatabases += ((dbDefinition.name, dbDefinition))
    }
  }

  override def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = {
    withWorkingDSDB(db) { (ds, db, dsName, dbName) =>
      if (dsName.equals(conf.get(XSQL_DEFAULT_DATASOURCE)) &&
          dbName.equals(conf.get(XSQL_DEFAULT_DATABASE))) {
        throw new AnalysisException(s"Can not drop default database")
      }
      ds.dsManager.dropDatabase(dbName, ignoreIfNotExists, cascade)
      val xdatabases =
        dataSourceToCatalogDatabase.getOrElseUpdate(dsName, new HashMap[String, CatalogDatabase])
      xdatabases -= ((dbName))
    }
  }

  override def alterDatabase(dbDefinition: CatalogDatabase): Unit = {
    withWorkingDSDB(dbDefinition.name) { (ds, existingDb, dsName, dbName) =>
      if (existingDb.properties == dbDefinition.properties) {
        logWarning(
          s"Request to alter database ${dbName} is a no-op because " +
            s"the provided database properties are the same as the old ones. Hive does not " +
            s"currently support altering other database fields.")
      }
      ds.dsManager.alterDatabase(dbDefinition)
      dataSourceToCatalogDatabase(dsName).put(dbName, dbDefinition)
    }
  }

  override def getDatabase(dbName: String): CatalogDatabase = {
    withWorkingDS { (ds, dsName) =>
      dataSourceToCatalogDatabase(dsName)
        .get(dbName)
        .getOrElse(throw new NoSuchDatabaseException(s"${dsName}.${dbName}"))
    }
  }

  override def databaseExists(dbName: String): Boolean = {
    withWorkingDS { (ds, dsName) =>
      dataSourceToCatalogDatabase
        .getOrElse(dsName, new HashMap[String, CatalogDatabase])
        .contains(dbName)
    }
  }

  override def listDatabases(): Seq[String] = withWorkingDS { (ds, dsName) =>
    if (dataSourceToCatalogDatabase.get(dsName).isDefined) { // database cached.
      val databases = dataSourceToCatalogDatabase(dsName)
      if (databases.isEmpty) {
        logWarning(s"No Databases cached in data source $dsName")
        return Seq.empty
      }
      return databases.keySet.toSeq
    } else {
      ds.dsManager.listDatabases()
    }
  }

  override def listDatabases(pattern: String): Seq[String] = withWorkingDS { (ds, dsName) =>
    if (dataSourceToCatalogDatabase.get(dsName).isDefined) { // database cached.
      val databases = dataSourceToCatalogDatabase(dsName)
      if (databases.isEmpty) {
        logWarning(s"No Databases cached in data source $dsName")
        return Seq.empty
      }
      return StringUtils.filterPattern(databases.keySet.toSeq, pattern)
    } else {
      ds.dsManager.listDatabases(pattern)
    }
  }

  override def setCurrentDatabase(dbName: String): Unit = {
    withWorkingDSDB(dbName) { (_, db, _, _) =>
      currentDataBase = Option(db)
    }
  }

  def setWorkingDataSource[T](dsNameOpt: Option[String])(body: => T): T =
    workingDataSource.synchronized {
      val shouldClean = setted.compareAndSet(false, true)
      if (dsNameOpt.isDefined) {
        workingDataSource.set(dataSourcesByName(dsNameOpt.get))
      } else if (shouldClean) {
        workingDataSource.set(dataSourcesByName(currentDataBase.get.dataSourceName))
      }
      try {
        body
      } finally {
        if (shouldClean) {
          workingDataSource.set(null)
          setted.compareAndSet(true, false)
        }
      }
    }

  private def withWorkingDS[T](body: (CatalogDataSource, String) => T): T =
    workingDataSource.synchronized {
      if (workingDataSource.get eq null) {
        throw new SparkException("please wrap withWorkingDS with setWorkingDataSource")
      } else {
        try {
          body(workingDataSource.get, workingDataSource.get.getName)
        } catch {
          case NonFatal(exception) =>
            val e = exception match {
              // Since we are using shim, the exceptions thrown by the underlying method of
              // Method.invoke() are wrapped by InvocationTargetException
              case i: InvocationTargetException => i.getCause
              case o => o
            }
            throw new AnalysisException(
              e.getClass.getCanonicalName + ": " + e.getMessage,
              cause = Some(e))
        }
      }
    }

  private def withWorkingDSDB[T](dbName: String)(
      body: (CatalogDataSource, CatalogDatabase, String, String) => T): T =
    workingDataSource.synchronized {
      if (workingDataSource.get eq null) {
        throw new SparkException("please wrap withWorkingDSDB with setWorkingDataSource")
      } else {
        try {
          body(workingDataSource.get, getDatabase(dbName), workingDataSource.get.getName, dbName)
        } catch {
          case NonFatal(exception) =>
            val e = exception match {
              // Since we are using shim, the exceptions thrown by the underlying method of
              // Method.invoke() are wrapped by InvocationTargetException
              case i: InvocationTargetException => i.getCause
              case o => o
            }
            throw new AnalysisException(
              e.getClass.getCanonicalName + ": " + e.getMessage,
              cause = Some(e))
        }
      }
    }

  // --------------------------------------------------------------------------
  // Tables
  // --------------------------------------------------------------------------

  override def createTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit = {
    withWorkingDSDB(tableDefinition.database) { (ds, db, dsName, dbName) =>
      val table = tableDefinition.identifier.table
      if (tableExists(dbName, table) && !ignoreIfExists) {
        throw new TableAlreadyExistsException(db = s"${dsName}.${dbName}", table = table)
      }
      val tableType = ds.getDSType match {
        case ELASTICSEARCH => CatalogTableType.TYPE
        case MONGO => CatalogTableType.COLLECTION
        case HBASE => CatalogTableType.HBASE
        case MYSQL | ORACLE => CatalogTableType.JDBC
        case DRUID => CatalogTableType.JDBC
        case _ => tableDefinition.tableType
      }
      val newTableDefinition = tableDefinition.copy(
        tableType = tableType,
        identifier = tableDefinition.identifier.copy(dataSource = Some(dsName)))
      ds.dsManager.createTable(newTableDefinition, ignoreIfExists)
      refreshCachedTableIfNeeded(ds, db, tableDefinition.identifier.table)
    }
  }

  override def dropTable(
      db: String,
      table: String,
      ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = withWorkingDSDB(db) { (ds, db, dsName, dbName) =>
    ds.dsManager.dropTable(dbName, table, ignoreIfNotExists, purge)
    val xtables = dbToCatalogTable.getOrElseUpdate(db.id, new HashMap[String, CatalogTable])
    xtables -= ((table))
  }

  def truncateTable(table: CatalogTable, partitionSpec: Option[TablePartitionSpec]): Unit = {
    withWorkingDS { (ds, dsName) =>
      ds.dsManager.truncateTable(table, partitionSpec)
    }
  }

  override def renameTable(db: String, oldName: String, newName: String): Unit = {
    withWorkingDSDB(db) { (ds, db, dsName, dbName) =>
      val originDBName = getOriginDatabaseName(db)
      ds.dsManager.renameTable(originDBName, oldName, newName)
      if (shouldCacheTable(ds)) {
        val xtables = dbToCatalogTable.getOrElseUpdate(db.id, new HashMap[String, CatalogTable])
        xtables -= oldName
        xtables(newName) = getRawTable(db.name, newName)
      }
    }
  }

  /**
   * Alter a table whose database and name match the ones specified in `tableDefinition`, assuming
   * the table exists. Note that, even though we can specify database in `tableDefinition`, it's
   * used to identify the table, not to alter the table's database, which is not allowed.
   *
   * Note: If the underlying implementation does not support altering a certain field,
   * this becomes a no-op.
   */
  override def alterTable(tableDefinition: CatalogTable): Unit = {
    withWorkingDSDB(tableDefinition.database) { (ds, db, dsName, dbName) =>
      ds.dsManager.alterTable(tableDefinition)
      refreshCachedTableIfNeeded(ds, db, tableDefinition.identifier.table)
    }
  }

  /**
   * Alter the data schema of a table identified by the provided database and table name. The new
   * data schema should not have conflict column names with the existing partition columns, and
   * should still contain all the existing data columns.
   *
   * @param db Database that table to alter schema for exists in
   * @param table Name of table to alter schema for
   * @param newDataSchema Updated data schema to be used for the table.
   */
  override def alterTableDataSchema(
      db: String,
      table: String,
      newDataSchema: StructType): Unit = {
    withWorkingDSDB(db) { (ds, db, dsName, dbName) =>
      ds.dsManager.alterTableDataSchema(dbName, table, newDataSchema)
      refreshCachedTableIfNeeded(ds, db, table)
    }
  }

  /**
   * Alter the data schema of a table identified by the provided database and table name. The new
   * data schema should not have conflict column names with the existing partition columns, and
   * should still contain all the existing data columns.
   *
   * @param db            Database that table to alter schema for exists in
   * @param table         Name of table to alter schema for
   * @param newDataSchema Updated data schema to be used for the table.
   */
  def alterTableDataSchema(
      db: String,
      table: String,
      newDataSchema: StructType,
      queryContent: String): Unit = {
    withWorkingDSDB(db) { (ds, db, dsName, dbName) =>
      requireTableExists(dbName, table)
      ds.dsManager.alterTableDataSchema(dbName, queryContent)
      refreshCachedTableIfNeeded(ds, db, table)
    }
  }

  /** Alter the statistics of a table. If `stats` is None, then remove all existing statistics. */
  override def alterTableStats(
      db: String,
      table: String,
      stats: Option[CatalogStatistics]): Unit = {
    withWorkingDSDB(db) { (ds, db, dsName, dbName) =>
      ds.dsManager.alterTableStats(dbName, table, stats)
      refreshCachedTableIfNeeded(ds, db, table)
    }
  }

  def refreshCachedTableIfNeeded(
      ds: CatalogDataSource,
      db: CatalogDatabase,
      table: String): Unit = {
    if (shouldCacheTable(ds)) {
      val xtables = dbToCatalogTable.getOrElseUpdate(db.id, new HashMap[String, CatalogTable])
      xtables(table) = getRawTable(db.name, table)
    }
  }

  def shouldCacheTable(ds: CatalogDataSource): Boolean = {
    ds.dsManager.getCacheLevel() == 2
  }

  /**
   * getRawTable never get Table from dbToCatalogTable, while getTable may get Table from cache
   */
  def getRawTable(dbName: String, table: String): CatalogTable = {
    withWorkingDSDB(dbName) { (ds, db, dsName, dbName) =>
      val originDBName = getOriginDatabaseName(db)
      ds.dsManager.getRawTable(dbName, originDBName, table)
    }
  }

  override def getTable(db: String, table: String): CatalogTable = {
    withWorkingDSDB(db) { (ds, db, dsName, dbName) =>
      if (shouldCacheTable(ds)) {
        val tableHash = dbToCatalogTable.getOrElseUpdate(db.id, new HashMap[String, CatalogTable])
        if (tableHash.isDefinedAt(table)) {
          tableHash(table)
        } else {
          val originDBName = getOriginDatabaseName(db)
          val rawTable = ds.dsManager.getRawTable(dbName, originDBName, table)
          tableHash(table) = rawTable
          rawTable
        }
      } else {
        val originDBName = getOriginDatabaseName(db)
        ds.dsManager.getRawTable(dbName, originDBName, table)
      }
    }
  }

  override def tableExists(dbName: String, table: String): Boolean = {
    withWorkingDS { (ds, dsName) =>
      if (databaseExists(dbName)) {
        val db = getDatabase(dbName)
        val tableHash = dbToCatalogTable.getOrElseUpdate(db.id, new HashMap[String, CatalogTable])
        if (tableHash.isDefinedAt(table)) {
          true
        } else {
          val originDBName = getOriginDatabaseName(db)
          ds.dsManager.tableExists(originDBName, table)
        }
      } else {
        false
      }
    }
  }

  override def listTables(dbName: String): Seq[String] = {
    withWorkingDSDB(dbName) { (ds, db, dsName, dbName) =>
      if (shouldCacheTable(ds)) {
        val tables = dbToCatalogTable(db.id)
        if (tables.isEmpty) {
          logWarning(s"No Tables cached in database $dsName.$dbName")
          return Seq.empty
        }
        return tables.keySet.toSeq
      } else {
        val originDBName = getOriginDatabaseName(db)
        ds.dsManager.listTables(originDBName)
      }
    }
  }

  override def listTables(dbName: String, pattern: String): Seq[String] = {
    withWorkingDSDB(dbName) { (ds, db, dsName, dbName) =>
      if (shouldCacheTable(ds)) {
        val tables = dbToCatalogTable(db.id)
        if (tables.isEmpty) {
          logWarning(s"No Tables cached in database $dsName.$dbName")
          return Seq.empty
        }
        return StringUtils.filterPattern(tables.keySet.toSeq, pattern)
      } else {
        val originDBName = getOriginDatabaseName(db)
        ds.dsManager.listTables(originDBName, pattern)
      }
    }
  }

  override def loadTable(
      dbName: String,
      table: String,
      loadPath: String,
      isOverwrite: Boolean,
      isSrcLocal: Boolean): Unit = {
    withWorkingDS { (ds, dsName) =>
      requireTableExists(dbName, table)
      assert(
        getDataSourceType(dsName).equalsIgnoreCase(DataSourceType.HIVE.toString()),
        "try to load hdfs file to no-hive datasource")
      ds.dsManager.loadTable(dbName, table, loadPath, isOverwrite, isSrcLocal)
    }
  }

  override def loadPartition(
      dbName: String,
      table: String,
      loadPath: String,
      partition: TablePartitionSpec,
      isOverwrite: Boolean,
      inheritTableSpecs: Boolean,
      isSrcLocal: Boolean): Unit = {
    withWorkingDS { (ds, dsName) =>
      requireTableExists(dbName, table)
      assert(
        getDataSourceType(dsName).equalsIgnoreCase(DataSourceType.HIVE.toString()),
        "try to load hdfs file to no-hive datasource")
      ds.dsManager.loadPartition(
        dbName,
        table,
        loadPath,
        partition,
        isOverwrite,
        inheritTableSpecs,
        isSrcLocal)
    }
  }

  override def loadDynamicPartitions(
      dbName: String,
      table: String,
      loadPath: String,
      partition: TablePartitionSpec,
      replace: Boolean,
      numDP: Int): Unit = {
    withWorkingDS { (ds, dsName) =>
      requireTableExists(dbName, table)
      assert(
        getDataSourceType(dsName).equalsIgnoreCase(DataSourceType.HIVE.toString()),
        "try to load hdfs file to no-hive datasource")
      ds.dsManager.loadDynamicPartitions(dbName, table, loadPath, partition, replace, numDP)
    }
  }

  // --------------------------------------------------------------------------
  // Partitions
  // --------------------------------------------------------------------------

  override def createPartitions(
      dbName: String,
      table: String,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = {
    withWorkingDS { (ds, dsName) =>
      requireTableExists(dbName, table)
      ds.dsManager.createPartitions(dbName, table, parts, ignoreIfExists)
    }
  }

  override def dropPartitions(
      dbName: String,
      table: String,
      parts: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean,
      purge: Boolean,
      retainData: Boolean): Unit = {
    withWorkingDS { (ds, dsName) =>
      ds.dsManager.dropPartitions(dbName, table, parts, ignoreIfNotExists, purge, retainData)
    }
  }

  override def renamePartitions(
      dbName: String,
      table: String,
      specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit = {
    withWorkingDS { (ds, dsName) =>
      requireTableExists(dbName, table)
      ds.dsManager.renamePartitions(dbName, table, specs, newSpecs)
    }
  }

  override def alterPartitions(
      dbName: String,
      table: String,
      parts: Seq[CatalogTablePartition]): Unit = {
    withWorkingDS { (ds, dsName) =>
      requireTableExists(dbName, table)
      ds.dsManager.alterPartitions(dbName, table, parts)
    }
  }

  override def getPartition(
      db: String,
      table: String,
      spec: TablePartitionSpec): CatalogTablePartition = {
    withWorkingDS { (ds, dsName) =>
      ds.dsManager.getPartition(db, table, spec)
    }
  }

  override def getPartitionOption(
      db: String,
      table: String,
      spec: TablePartitionSpec): Option[CatalogTablePartition] = {
    withWorkingDS { (ds, dsName) =>
      ds.dsManager.getPartitionOption(db, table, spec)
    }
  }

  override def listPartitionNames(
      db: String,
      table: String,
      partialSpec: Option[TablePartitionSpec] = None): Seq[String] = {
    withWorkingDS { (ds, dsName) =>
      ds.dsManager.listPartitionNames(db, table, partialSpec)
    }
  }

  override def listPartitions(
      db: String,
      table: String,
      partialSpec: Option[TablePartitionSpec] = None): Seq[CatalogTablePartition] = {
    withWorkingDS { (ds, dsName) =>
      ds.dsManager.listPartitions(db, table, partialSpec)
    }
  }

  override def listPartitionsByFilter(
      db: String,
      table: String,
      predicates: Seq[Expression],
      defaultTimeZoneId: String): Seq[CatalogTablePartition] = {
    withWorkingDS { (ds, dsName) =>
      ds.dsManager.listPartitionsByFilter(db, table, predicates, defaultTimeZoneId)
    }
  }

  // --------------------------------------------------------------------------
  // Functions
  // --------------------------------------------------------------------------

  override def createFunction(dbName: String, funcDefinition: CatalogFunction): Unit = {
    withWorkingDS { (ds, dsName) =>
      requireDbExists(dbName)
      // Hive's metastore is case insensitive. However, Hive's createFunction does
      // not normalize the function name (unlike the getFunction part). So,
      // we are normalizing the function name.
      val functionName = funcDefinition.identifier.funcName.toLowerCase(Locale.ROOT)
      requireFunctionNotExists(dbName, functionName)
      val functionIdentifier = funcDefinition.identifier.copy(funcName = functionName)
      ds.dsManager.createFunction(dbName, funcDefinition.copy(identifier = functionIdentifier))
    }
  }

  override def dropFunction(dbName: String, funcName: String): Unit = {
    withWorkingDS { (ds, dsName) =>
      requireFunctionExists(dbName, funcName)
      ds.dsManager.dropFunction(dbName, funcName)
    }
  }

  override def alterFunction(dbName: String, funcDefinition: CatalogFunction): Unit = {
    withWorkingDS { (ds, dsName) =>
      requireDbExists(dbName)
      val functionName = funcDefinition.identifier.funcName.toLowerCase(Locale.ROOT)
      requireFunctionExists(dbName, functionName)
      val functionIdentifier = funcDefinition.identifier.copy(funcName = functionName)
      ds.dsManager.alterFunction(dbName, funcDefinition.copy(identifier = functionIdentifier))
    }
  }

  override def renameFunction(db: String, oldName: String, newName: String): Unit = {
    withWorkingDS { (ds, dsName) =>
      ds.dsManager.renameFunction(db, oldName, newName)
    }
  }

  override def getFunction(dbName: String, funcName: String): CatalogFunction = {
    withWorkingDS { (ds, dsName) =>
      requireFunctionExists(dbName, funcName)
      ds.dsManager.getFunction(dbName, funcName)
    }
  }

  override def functionExists(dbName: String, funcName: String): Boolean = {
    withWorkingDS { (ds, dsName) =>
      requireDbExists(dbName)
      ds.dsManager.functionExists(dbName, funcName)
    }
  }

  override def listFunctions(dbName: String, pattern: String): Seq[String] = {
    withWorkingDS { (ds, dsName) =>
      requireDbExists(dbName)
      ds.dsManager.listFunctions(dbName, pattern)
    }
  }

  def stop(): Unit = {
    dataSourcesByName.values.foreach(_.dsManager.stop())
  }
}

object XSQLExternalCatalog {
  val SPARK_XSQL_DATASOURCE_PREFIX = "spark.xsql.datasource."
  val SPARK_XSQL_YARN_PREFIX = "spark.xsql.yarn."
}
