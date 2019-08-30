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

import scala.collection.mutable.{HashMap, HashSet}
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf.ConfVars

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchPartitionException
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.command.DDLUtils.isDatasourceTable
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.hive.HiveExternalCatalog
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.xsql.{CatalogDataSource, DataSourceManager, HiveDataSource}
import org.apache.spark.sql.xsql.DataSourceType.HIVE
import org.apache.spark.sql.xsql.internal.config._

/**
 * Manager for hive.
 */
private[spark] class HiveManager(conf: SparkConf, hadoopConf: Configuration)
  extends HiveExternalCatalog(conf, hadoopConf)
  with DataSourceManager {

  /**
   * Hive not support pushdown.
   */
  override def isPushDown: Boolean = false

  def this() = {
    this(null, null)
  }

  import DataSourceManager._

  override def shortName(): String = HIVE.toString

  override def getClusterBasedOnData(
      clusterNames: HashSet[String],
      dataSourcesByName: HashMap[String, CatalogDataSource],
      identifier: TableIdentifier): Option[String] = {
    val ds = dataSourcesByName(identifier.dataSource.get).asInstanceOf[HiveDataSource]
    val metastoreUrl = ds.getUrl
    var hiveCluster: Option[String] = None
    clusterNames.foreach { clusterName =>
      if (metastoreUrl.contains(clusterName)) {
        hiveCluster = Option(clusterName)
      }
    }
    hiveCluster
  }

  override protected def cacheDatabase(
      isDefault: Boolean,
      dataSourceName: String,
      infos: Map[String, String],
      dataSourcesByName: HashMap[String, CatalogDataSource],
      dataSourceToCatalogDatabase: HashMap[String, HashMap[String, CatalogDatabase]]): Unit = {
    val metastoreUrl = infos.get(s"metastore.${URL}")
    if (metastoreUrl == None) {
      throw new SparkException("Data source is hive must have metastore url!")
    }
    val ds: CatalogDataSource = new HiveDataSource(
      dataSourceName,
      HIVE,
      this,
      metastoreUrl.get,
      infos(USER),
      infos(PASSWORD),
      infos(VERSION))
    dataSourcesByName(ds.getName) = ds
    hadoopConf.set(ConfVars.METASTOREURIS.varname, metastoreUrl.get)
    val databases: Seq[String] = client.listDatabases("*")
    val xdatabases = dataSourceToCatalogDatabase.getOrElseUpdate(
      dataSourceName,
      new HashMap[String, CatalogDatabase])
    databases.filter(isSelectedDatabase(isDefault, _, conf.get(XSQL_DEFAULT_DATABASE))).foreach {
      dbName =>
        logDebug(s"Parse hive db $dbName")
        xdatabases += ((dbName, getDatabase(dbName)))
    }
  }

  override protected def cacheTable(
      dataSourceName: String,
      dataSourceToCatalogDatabase: HashMap[String, HashMap[String, CatalogDatabase]],
      dbToCatalogTable: HashMap[Int, HashMap[String, CatalogTable]]): Unit = withClient {
    val xdatabases = dataSourceToCatalogDatabase(dataSourceName)
    xdatabases.foreach { kv =>
      val dbName = kv._1
      val db = kv._2
      val tables: Seq[String] = client.listTables(dbName)
      val xtables = dbToCatalogTable.getOrElseUpdate(db.id, new HashMap[String, CatalogTable])
      val (whiteTables, blackTables) = getWhiteAndBlackTables(dbName)
      tables.filter(isSelectedTable(whiteTables, blackTables, _)).foreach { tableName =>
        logDebug(s"Parse hive table $tableName")
        xtables += ((tableName, getTable(dbName, tableName)))
      }
    }
  }

  override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit =
    super[HiveExternalCatalog].createDatabase(dbDefinition, ignoreIfExists)

  override def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit =
    super[HiveExternalCatalog].dropDatabase(db, ignoreIfNotExists, cascade)

  override def alterDatabase(dbDefinition: CatalogDatabase): Unit =
    super[HiveExternalCatalog].alterDatabase(dbDefinition)

  override def getDatabase(db: String): CatalogDatabase = {
    super[HiveExternalCatalog].getDatabase(db).copy(dataSourceName = dsName)
  }

  override def databaseExists(db: String): Boolean =
    super[HiveExternalCatalog].databaseExists(db)

  override def listDatabases(): Seq[String] =
    super[HiveExternalCatalog].listDatabases()

  override def listDatabases(pattern: String): Seq[String] =
    super[HiveExternalCatalog].listDatabases(pattern)

  override def setCurrentDatabase(db: String): Unit =
    super[HiveExternalCatalog].setCurrentDatabase(db)

  override def createTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit =
    super[HiveExternalCatalog].createTable(tableDefinition, ignoreIfExists)

  override def dropTable(
      db: String,
      table: String,
      ignoreIfNotExists: Boolean,
      purge: Boolean): Unit =
    super[HiveExternalCatalog].dropTable(db, table, ignoreIfNotExists, purge)

  override def truncateTable(
      table: CatalogTable,
      partitionSpec: Option[TablePartitionSpec]): Unit = {
    val tableIdentWithDB = table.identifier.quotedString
    val ds = table.identifier.dataSource.get
    val db = table.identifier.database.get
    val tableName = table.identifier.table
    if (table.tableType == CatalogTableType.EXTERNAL) {
      throw new AnalysisException(
        s"Operation not allowed: TRUNCATE TABLE on external tables: $tableIdentWithDB")
    }
    if (table.tableType == CatalogTableType.VIEW) {
      throw new AnalysisException(
        s"Operation not allowed: TRUNCATE TABLE on views: $tableIdentWithDB")
    }
    if (table.partitionColumnNames.isEmpty && partitionSpec.isDefined) {
      throw new AnalysisException(
        s"Operation not allowed: TRUNCATE TABLE ... PARTITION is not supported " +
          s"for tables that are not partitioned: $tableIdentWithDB")
    }
    if (partitionSpec.isDefined) {
      if (!SQLConf.get.manageFilesourcePartitions && isDatasourceTable(table)) {
        throw new AnalysisException(
          s"TRUNCATE TABLE ... PARTITION is not allowed on $tableName " +
            s"since filesource partition management is " +
            "disabled (spark.sql.hive.manageFilesourcePartitions = false).")
      }
      if (!table.tracksPartitionsInCatalog && isDatasourceTable(table)) {
        throw new AnalysisException(
          s"TRUNCATE TABLE ... PARTITION is not allowed on $tableName " +
            s"since its partition metadata is not stored in " +
            "the Hive metastore. To import this information into the metastore, run " +
            s"`msck repair table $tableName`")
      }
    }

    val partCols = table.partitionColumnNames
    val locations =
      if (partCols.isEmpty) {
        Seq(table.storage.locationUri)
      } else {
        val normalizedSpec = partitionSpec.map { spec =>
          PartitioningUtils.normalizePartitionSpec(
            spec,
            partCols,
            table.identifier.quotedString,
            SQLConf.get.resolver)
        }
        val partLocations = listPartitions(db, tableName, normalizedSpec)
          .map(_.storage.locationUri)

        // Fail if the partition spec is fully specified (not partial) and the partition does not
        // exist.
        for (spec <- partitionSpec if partLocations.isEmpty && spec.size == partCols.length) {
          throw new NoSuchPartitionException(table.database, table.identifier.table, spec)
        }

        partLocations
      }
    locations.foreach { location =>
      if (location.isDefined) {
        val path = new Path(location.get)
        try {
          val fs = path.getFileSystem(hadoopConf)
          fs.delete(path, true)
          fs.mkdirs(path)
        } catch {
          case NonFatal(e) =>
            throw new AnalysisException(
              s"Failed to truncate table $tableIdentWithDB when removing data of the path: $path " +
                s"because of ${e.toString}")
        }
      }
    }
  }

  override def renameTable(db: String, oldName: String, newName: String): Unit =
    super[HiveExternalCatalog].renameTable(db, oldName, newName)

  override def alterTable(tableDefinition: CatalogTable): Unit =
    super[HiveExternalCatalog].alterTable(tableDefinition)

  override def alterTableDataSchema(db: String, table: String, newDataSchema: StructType): Unit =
    super[HiveExternalCatalog].alterTableDataSchema(db, table, newDataSchema)

  override def alterTableStats(
      db: String,
      table: String,
      stats: Option[CatalogStatistics]): Unit =
    super[HiveExternalCatalog].alterTableStats(db, table, stats)

  override def getTable(db: String, table: String): CatalogTable = {
    val catalogTable = super[HiveExternalCatalog].getTable(db, table)
    catalogTable.copy(identifier = catalogTable.identifier.copy(dataSource = Option(dsName)))
  }

  override def listTables(db: String): Seq[String] =
    super[HiveExternalCatalog].listTables(db)

  override def listTables(dbName: String, pattern: String): Seq[String] =
    super[HiveExternalCatalog].listTables(dbName, pattern)

  override def loadTable(
      db: String,
      table: String,
      loadPath: String,
      isOverwrite: Boolean,
      isSrcLocal: Boolean): Unit =
    super[HiveExternalCatalog].loadTable(db, table, loadPath, isOverwrite, isSrcLocal)

  override def getRawTable(db: String, originDB: String, table: String): CatalogTable =
    this.getTable(db, table)

  override def loadPartition(
      db: String,
      table: String,
      loadPath: String,
      partition: TablePartitionSpec,
      isOverwrite: Boolean,
      inheritTableSpecs: Boolean,
      isSrcLocal: Boolean): Unit =
    super[HiveExternalCatalog]
      .loadPartition(db, table, loadPath, partition, isOverwrite, inheritTableSpecs, isSrcLocal)

  override def loadDynamicPartitions(
      db: String,
      table: String,
      loadPath: String,
      partition: TablePartitionSpec,
      replace: Boolean,
      numDP: Int): Unit =
    super[HiveExternalCatalog].loadDynamicPartitions(
      db,
      table,
      loadPath,
      partition,
      replace,
      numDP)

  override def createPartitions(
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit =
    super[HiveExternalCatalog].createPartitions(db, table, parts, ignoreIfExists)

  override def dropPartitions(
      db: String,
      table: String,
      parts: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean,
      purge: Boolean,
      retainData: Boolean): Unit =
    super[HiveExternalCatalog]
      .dropPartitions(db, table, parts, ignoreIfNotExists, purge, retainData)

  override def renamePartitions(
      db: String,
      table: String,
      specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit =
    super[HiveExternalCatalog].renamePartitions(db, table, specs, newSpecs)

  override def alterPartitions(
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition]): Unit =
    super[HiveExternalCatalog].alterPartitions(db, table, parts)

  override def getPartition(
      db: String,
      table: String,
      spec: TablePartitionSpec): CatalogTablePartition =
    super[HiveExternalCatalog].getPartition(db, table, spec)

  override def getPartitionOption(
      db: String,
      table: String,
      spec: TablePartitionSpec): Option[CatalogTablePartition] =
    super[HiveExternalCatalog].getPartitionOption(db, table, spec)

  override def listPartitionNames(
      db: String,
      table: String,
      partialSpec: Option[TablePartitionSpec]): Seq[String] =
    super[HiveExternalCatalog].listPartitionNames(db, table, partialSpec)

  override def listPartitions(
      db: String,
      table: String,
      partialSpec: Option[TablePartitionSpec]): Seq[CatalogTablePartition] =
    super[HiveExternalCatalog].listPartitions(db, table, partialSpec)

  override def listPartitionsByFilter(
      db: String,
      table: String,
      predicates: Seq[Expression],
      defaultTimeZoneId: String): Seq[CatalogTablePartition] =
    super[HiveExternalCatalog].listPartitionsByFilter(db, table, predicates, defaultTimeZoneId)

  override def createFunction(db: String, funcDefinition: CatalogFunction): Unit =
    super[HiveExternalCatalog].createFunction(db, funcDefinition)

  override def dropFunction(db: String, funcName: String): Unit =
    super[HiveExternalCatalog].dropFunction(db, funcName)

  override def alterFunction(db: String, funcDefinition: CatalogFunction): Unit =
    super[HiveExternalCatalog].alterFunction(db, funcDefinition)

  override def renameFunction(db: String, oldName: String, newName: String): Unit =
    super[HiveExternalCatalog].renameFunction(db, oldName, newName)

  override def getFunction(db: String, funcName: String): CatalogFunction =
    super[HiveExternalCatalog].getFunction(db, funcName)

  override def functionExists(db: String, funcName: String): Boolean =
    super[HiveExternalCatalog].functionExists(db, funcName)

  override def listFunctions(db: String, pattern: String): Seq[String] =
    super[HiveExternalCatalog].listFunctions(db, pattern)
}
