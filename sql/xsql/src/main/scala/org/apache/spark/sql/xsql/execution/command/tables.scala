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

package org.apache.spark.sql.xsql.execution.command

import java.util.{HashMap, Locale}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.catalog.CatalogTableType._
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet}
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.getCommonJDBCType
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.sql.xsql.XSQLSessionCatalog
import org.apache.spark.sql.xsql.types._
import org.apache.spark.util.Utils

/**
 * A command that add columns to a table
 * The syntax of using this command in SQL is:
 * {{{
 *   ALTER TABLE table_identifier
 *   ADD COLUMNS (col_name data_type [COMMENT col_comment], ...);
 * }}}
 */
case class XSQLAlterTableAddColumnsCommand(table: TableIdentifier, colsToAdd: Seq[StructField])
  extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog.asInstanceOf[XSQLSessionCatalog]
    val catalogDB: Option[CatalogDatabase] = catalog.getUsedCatalogDatabase(table)
    val ds = catalogDB.get.dataSourceName
    val db = catalogDB.get.name
    val tableName = table.table
    val newTableIdentifier = TableIdentifier(tableName, Some(db), Some(ds))
    val catalogTable =
      verifyAlterTableAddColumn(sparkSession.sessionState.conf, catalog, newTableIdentifier)

    try {
      sparkSession.catalog.uncacheTable(newTableIdentifier.quotedString)
    } catch {
      case NonFatal(e) =>
        log.warn(s"Exception when attempting to uncache table ${table.quotedString}", e)
    }
    catalog.refreshTable(newTableIdentifier)

    SchemaUtils.checkColumnNameDuplication(
      (colsToAdd ++ catalogTable.schema).map(_.name),
      "in the table definition of " + table.identifier,
      conf.caseSensitiveAnalysis)
    DDLUtils.checkDataColNames(catalogTable, colsToAdd.map(_.name))

    catalog.alterTableDataSchema(
      newTableIdentifier,
      StructType(catalogTable.dataSchema ++ colsToAdd))
    Seq.empty[Row]
  }

  /**
   * ALTER TABLE ADD COLUMNS command does not support temporary view/table,
   * view, or datasource table with text, orc formats or external provider.
   * For datasource table, it currently only supports parquet, json, csv.
   */
  private def verifyAlterTableAddColumn(
      conf: SQLConf,
      catalog: SessionCatalog,
      table: TableIdentifier): CatalogTable = {
    val catalogTable = catalog.getTempViewOrPermanentTableMetadata(table)

    if (catalogTable.tableType == VIEW) {
      throw new AnalysisException(s"""
          |ALTER ADD COLUMNS does not support views.
          |You must drop and re-create the views for adding the new columns. Views: $table
         """.stripMargin)
    }

    if (DDLUtils.isDatasourceTable(catalogTable)) {
      DataSource.lookupDataSource(catalogTable.provider.get, conf).newInstance() match {
        // For datasource table, this command can only support the following File format.
        // TextFileFormat only default to one column "value"
        // Hive type is already considered as hive serde table, so the logic will not
        // come in here.
        case _: JsonFileFormat | _: CSVFileFormat | _: ParquetFileFormat =>
        case s if s.getClass.getCanonicalName.endsWith("OrcFileFormat") =>
        case s
            if s.getClass.getCanonicalName.equals("org.elasticsearch.spark.sql.DefaultSource") =>
        case s =>
          throw new AnalysisException(s"""
              |ALTER ADD COLUMNS does not support datasource table with type $s.
              |You must drop and re-create the table for adding the new columns. Tables: $table
             """.stripMargin)
      }
    }
    catalogTable
  }
}

/**
 * A command that add columns to a table of mysql
 * The syntax of using this command in SQL is:
 * {{{
 *   ALTER TABLE table_identifier
 *   ADD COLUMN? (col_name data_type [COMMENT col_comment], ...);
 * }}}
 */
case class XSQLAlterTableAddColumnsForMysqlCommand(
    table: TableIdentifier,
    colsToAdd: Seq[StructField])
  extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog.asInstanceOf[XSQLSessionCatalog]
    val catalogTable =
      verifyAlterTableAddColumn(sparkSession.sessionState.conf, catalog, table)
    try {
      sparkSession.catalog.uncacheTable(table.quotedString)
    } catch {
      case NonFatal(e) =>
        log.warn(s"Exception when attempting to uncache table ${table.quotedString}", e)
    }
    catalog.refreshTable(table)

    SchemaUtils.checkColumnNameDuplication(
      (colsToAdd ++ catalogTable.schema).map(_.name),
      "in the table definition of " + table.identifier,
      conf.caseSensitiveAnalysis)
    DDLUtils.checkDataColNames(catalogTable, colsToAdd.map(_.name))
    val dialect = JdbcDialects.get(catalogTable.storage.properties.get("url").get)
    val strSchema = schemaString(StructType(colsToAdd), dialect)
    val sql = s"ALTER TABLE ${table.table} ADD ($strSchema)"
    catalog.alterTableDataSchema(table, StructType(catalogTable.dataSchema ++ colsToAdd), sql)
    Seq.empty[Row]
  }

  private def schemaString(schema: StructType, dialect: JdbcDialect): String = {
    val sb = new StringBuilder()
    schema.fields.foreach { field =>
      val name = dialect.quoteIdentifier(field.name)
      val typ = dialect
        .getJDBCType(field.dataType)
        .orElse(getCommonJDBCType(field.dataType))
        .get
        .databaseTypeDefinition
      val nullable = if (typ.equalsIgnoreCase("TIMESTAMP")) {
        "NULL"
      } else {
        if (field.nullable) {
          ""
        } else {
          "NOT NULL"
        }
      }
      sb.append(s", $name $typ $nullable ")
      if (field.metadata.contains(MYSQL_COLUMN_DEFAULT)) {
        sb.append(s"DEFAULT ${field.metadata.getString(MYSQL_COLUMN_DEFAULT)} ")
      }
      if (field.metadata.contains(MYSQL_COLUMN_AUTOINC)) {
        sb.append("AUTO_INCREMENT ")
      }
      if (field.metadata.contains(PRIMARY_KEY)) {
        sb.append("PRIMARY KEY ")
      }
      if (field.metadata.contains("COMMENT".toLowerCase)) {
        sb.append(s"COMMENT '${field.metadata.getString("COMMENT".toLowerCase)}'")
      }
    }
    if (sb.length < 2) "" else sb.substring(2)
  }

  /**
   * ALTER TABLE ADD COLUMN command does not support temporary view/table,
   * it currently only supports jdbc datasource.
   */
  private def verifyAlterTableAddColumn(
      conf: SQLConf,
      catalog: SessionCatalog,
      table: TableIdentifier): CatalogTable = {

    val catalogTable = catalog.getTempViewOrPermanentTableMetadata(table)

    if (catalogTable.tableType == VIEW) {
      throw new AnalysisException(s"""
           |ALTER ADD COLUMN does not support views.
           |You must drop and re-create the views for adding the new columns. Views: $table
         """.stripMargin)
    }

    if (DDLUtils.isDatasourceTable(catalogTable) ||
        catalogTable.provider.get.toLowerCase(Locale.ROOT) == DDLUtils.HIVE_PROVIDER) {
      DataSource.lookupDataSource(catalogTable.provider.get, conf).newInstance() match {
        // For datasource table, this command can only support jdbc
        case s
            if s.getClass.getCanonicalName.equals(
              "org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider") =>
        case s =>
          throw new AnalysisException(s"""
               |ALTER ADD COLUMN? does not support datasource table with type $s.
             """.stripMargin)
      }
    }
    catalogTable
  }
}

case class XSQLAlterTableDropColumnsForMysqlCommand(
    table: TableIdentifier,
    colstoDrop: Seq[String])
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog.asInstanceOf[XSQLSessionCatalog]
    val catalogTable =
      verifyAlterTableDropColumn(sparkSession.sessionState.conf, catalog, table)

    try {
      sparkSession.catalog.uncacheTable(table.quotedString)
    } catch {
      case NonFatal(e) =>
        log.warn(s"Exception when attempting to uncache table ${table.quotedString}", e)
    }
    catalog.refreshTable(table)
    DDLUtils.checkDataColNames(catalogTable, colstoDrop)

    val oldFields = catalogTable.dataSchema.fields

    colstoDrop.foreach(colName => {
      if (!oldFields.map(_.name).contains(colName)) {
        throw new AnalysisException(s"""
             |dropped column ${colName} doesn't exist in table ${table.table}
          """.stripMargin)
      }
    })

    val newDataSchema: Array[StructField] = oldFields.filter(field => {
      !colstoDrop.contains(field.name)
    })

    val colsToDrop = catalogTable.dataSchema.fields.filter(field => {
      colstoDrop.contains(field.name)
    })

    val strField = StructType(colsToDrop).fields
      .map(field => {
        "DROP " + field.name
      })
      .mkString(",")
    val sql = s"ALTER TABLE ${table.table} $strField"

    catalog.alterTableDataSchema(table, StructType(newDataSchema), sql)

    Seq.empty[Row]
  }

  /**
   * ALTER TABLE DROP COLUMN command does not support temporary view/table,
   * it currently only supports jdbc datasource.
   */
  private def verifyAlterTableDropColumn(
      conf: SQLConf,
      catalog: SessionCatalog,
      table: TableIdentifier): CatalogTable = {
    val catalogTable = catalog.getTempViewOrPermanentTableMetadata(table)

    if (catalogTable.tableType == VIEW) {
      throw new AnalysisException(s"""
           |ALTER DROP COLUMN does not support views.
           |You must drop and re-create the views for adding the new columns. Views: $table
         """.stripMargin)
    }
    if (DDLUtils.isDatasourceTable(catalogTable) ||
        catalogTable.provider.get.toLowerCase(Locale.ROOT) == DDLUtils.HIVE_PROVIDER) {
      DataSource.lookupDataSource(catalogTable.provider.get, conf).newInstance() match {
        // For datasource table, this command can only support jdbc
        case s
            if s.getClass.getCanonicalName.equals(
              "org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider") =>
        case s =>
          throw new AnalysisException(s"""
               |ALTER DROP COLUMN does not support datasource table with type $s.
             """.stripMargin)
      }
    }
    catalogTable
  }
}

/**
 * A command to truncate table.
 *
 * The syntax of this command is:
 * {{{
 *   TRUNCATE TABLE tablename [PARTITION (partcol1=val1, partcol2=val2 ...)]
 * }}}
 */
case class XSQLTruncateTableCommand(
    tableName: TableIdentifier,
    partitionSpec: Option[TablePartitionSpec])
  extends RunnableCommand {

  override def run(spark: SparkSession): Seq[Row] = {
    val catalog = spark.sessionState.catalog.asInstanceOf[XSQLSessionCatalog]
    val table = catalog.truncateTable(tableName, partitionSpec)
    val tableIdentWithDB = table.identifier.quotedString
    // After deleting the data, invalidate the table to make sure we don't keep around a stale
    // file relation in the metastore cache.
    catalog.refreshTable(tableName)
    // Also try to drop the contents of the table from the columnar cache
    try {
      spark.sharedState.cacheManager.uncacheQuery(spark.table(table.identifier), cascade = true)
    } catch {
      case NonFatal(e) =>
        log.warn(s"Exception when attempting to uncache table $tableIdentWithDB", e)
    }

    if (table.stats.nonEmpty) {
      // empty table after truncation
      val newStats = CatalogStatistics(sizeInBytes = 0, rowCount = Some(0))
      catalog.alterTableStats(tableName, Some(newStats))
    }
    Seq.empty[Row]
  }
}

/**
 * Command that looks like
 * {{{
 *   DESCRIBE [EXTENDED|FORMATTED] table_name partitionSpec?;
 * }}}
 */
case class XSQLDescribeTableCommand(
    table: TableIdentifier,
    partitionSpec: TablePartitionSpec,
    isExtended: Boolean)
  extends RunnableCommand {

  override val output: Seq[Attribute] = Seq(
    // Column names are based on Hive.
    AttributeReference(
      "col_name",
      StringType,
      nullable = false,
      new MetadataBuilder().putString("comment", "name of the column").build())(),
    AttributeReference(
      "data_type",
      StringType,
      nullable = false,
      new MetadataBuilder().putString("comment", "data type of the column").build())(),
    AttributeReference(
      "comment",
      StringType,
      nullable = true,
      new MetadataBuilder().putString("comment", "comment of the column").build())())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val result = new ArrayBuffer[Row]
    val catalog = sparkSession.sessionState.catalog.asInstanceOf[XSQLSessionCatalog]

    if (catalog.isTemporaryTable(table)) {
      if (partitionSpec.nonEmpty) {
        throw new AnalysisException(
          s"DESC PARTITION is not allowed on a temporary view: ${table.identifier}")
      }
      describeSchema(catalog.lookupRelation(table).schema, result, header = false)
    } else {
      val metadata = catalog.getTableMetadata(table)
      if (metadata.schema.isEmpty) {
        // In older version(prior to 2.1) of Spark, the table schema can be empty and should be
        // inferred at runtime. We should still support it.
        describeSchema(sparkSession.table(metadata.identifier).schema, result, header = false)
      } else {
        describeSchema(metadata.schema, result, header = false)
      }

      describePartitionInfo(metadata, result)

      if (partitionSpec.nonEmpty) {
        // Outputs the partition-specific info for the DDL command:
        // "DESCRIBE [EXTENDED|FORMATTED] table_name PARTITION (partitionVal*)"
        describeDetailedPartitionInfo(sparkSession, catalog, metadata, result)
      } else if (isExtended) {
        val regex = catalog.getRedactionPattern(metadata.identifier)
        val redactProperties = Utils.redact(regex, metadata.storage.properties.toSeq)
        val redactMetadata =
          metadata.copy(storage = metadata.storage.copy(properties = redactProperties.toMap))
        describeFormattedTableInfo(redactMetadata, result)
      }
    }

    result
  }

  private def describePartitionInfo(table: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    if (table.partitionColumnNames.nonEmpty) {
      append(buffer, "# Partition Information", "", "")
      describeSchema(table.partitionSchema, buffer, header = true)
    }
  }

  private def describeFormattedTableInfo(table: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    // The following information has been already shown in the previous outputs
    val excludedTableInfo = Seq("Partition Columns", "Schema")
    append(buffer, "", "", "")
    append(buffer, "# Detailed Table Information", "", "")
    table.toLinkedHashMap.filterKeys(!excludedTableInfo.contains(_)).foreach { s =>
      append(buffer, s._1, s._2, "")
    }
  }

  private def describeDetailedPartitionInfo(
      spark: SparkSession,
      catalog: SessionCatalog,
      metadata: CatalogTable,
      result: ArrayBuffer[Row]): Unit = {
    if (metadata.tableType == CatalogTableType.VIEW) {
      throw new AnalysisException(s"DESC PARTITION is not allowed on a view: ${table.identifier}")
    }
    DDLUtils.verifyPartitionProviderIsHive(spark, metadata, "DESC PARTITION")
    val partition = catalog.getPartition(table, partitionSpec)
    if (isExtended) describeFormattedDetailedPartitionInfo(table, metadata, partition, result)
  }

  private def describeFormattedDetailedPartitionInfo(
      tableIdentifier: TableIdentifier,
      table: CatalogTable,
      partition: CatalogTablePartition,
      buffer: ArrayBuffer[Row]): Unit = {
    append(buffer, "", "", "")
    append(buffer, "# Detailed Partition Information", "", "")
    append(buffer, "Database", table.database, "")
    append(buffer, "Table", tableIdentifier.table, "")
    partition.toLinkedHashMap.foreach(s => append(buffer, s._1, s._2, ""))
    append(buffer, "", "", "")
    append(buffer, "# Storage Information", "", "")
    table.bucketSpec match {
      case Some(spec) =>
        spec.toLinkedHashMap.foreach(s => append(buffer, s._1, s._2, ""))
      case _ =>
    }
    table.storage.toLinkedHashMap.foreach(s => append(buffer, s._1, s._2, ""))
  }

  private def describeSchema(
      schema: StructType,
      buffer: ArrayBuffer[Row],
      header: Boolean): Unit = {
    if (header) {
      append(buffer, s"# ${output.head.name}", output(1).name, output(2).name)
    }
    schema.foreach { column =>
      val typeString = if (column.metadata.contains(ELASTIC_SEARCH_TYPE_STRING)) {
        column.metadata.getString(ELASTIC_SEARCH_TYPE_STRING)
      } else {
        column.dataType.simpleString
      }
      append(buffer, column.name, typeString, column.getComment().orNull)
    }
  }

  private def append(
      buffer: ArrayBuffer[Row],
      column: String,
      dataType: String,
      comment: String): Unit = {
    buffer += Row(column, dataType, comment)
  }
}

/**
 * A command for users to get tables in the given database.
 * If a databaseName is not given, the current database will be used.
 * The syntax of using this command in SQL is:
 * {{{
 *   SHOW TABLES [(IN|FROM) ['ds'.]database_name] [[LIKE] 'identifier_with_wildcards'];
 *   SHOW TABLE EXTENDED [(IN|FROM) database_name] LIKE 'identifier_with_wildcards'
 *   [PARTITION(partition_spec)];
 * }}}
 */
case class XSQLShowTablesCommand(
    dataSourceName: Option[String] = None,
    databaseName: Option[String],
    tableIdentifierPattern: Option[String],
    isExtended: Boolean = false,
    partitionSpec: Option[TablePartitionSpec] = None)
  extends RunnableCommand {

  // The result of SHOW TABLES/SHOW TABLE has three basic columns: database, tableName and
  // isTemporary. If `isExtended` is true, append column `information` to the output columns.
  override val output: Seq[Attribute] = {
    val tableExtendedInfo = if (isExtended) {
      AttributeReference("information", StringType, nullable = false)() :: Nil
    } else {
      Nil
    }
    AttributeReference("tableName", StringType, nullable = false)() ::
      AttributeReference("database", StringType, nullable = false)() ::
      AttributeReference("dataSourceName", StringType, nullable = false)() ::
      AttributeReference("isTemporary", BooleanType, nullable = false)() :: tableExtendedInfo
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // Since we need to return a Seq of rows, we will call getTables directly
    // instead of calling tables in sparkSession.
    val catalog = sparkSession.sessionState.catalog.asInstanceOf[XSQLSessionCatalog]
    val catalogDB = catalog.getUsedCatalogDatabase(dataSourceName, databaseName)
    if (catalogDB == None) {
      return Seq.empty[Row]
    }
    val ds = catalogDB.get.dataSourceName
    val db = catalogDB.get.name
    if (partitionSpec.isEmpty) {
      // Show the information of tables.
      val tables =
        tableIdentifierPattern
          .map(catalog.listTablesForXSQL(ds, db, _))
          .getOrElse(catalog.listTablesForXSQL(ds, db))
      tables.map { tableIdent =>
        val database = tableIdent.database.getOrElse("")
        val tableName = tableIdent.table
        val isTemp = catalog.isTemporaryTable(tableIdent)
        if (isExtended) {
          val metadata = catalog.getTempViewOrPermanentTableMetadata(tableIdent)
          val regex = catalog.getRedactionPattern(metadata.identifier)
          val redactProperties = Utils.redact(regex, metadata.storage.properties.toSeq)
          val redactMetadata =
            metadata.copy(storage = metadata.storage.copy(properties = redactProperties.toMap))
          val information = redactMetadata.simpleString
          Row(tableName, database, ds, isTemp, s"$information\n")
        } else {
          Row(tableName, database, ds, isTemp)
        }
      }
    } else {
      // Show the information of partitions.
      //
      // Note: tableIdentifierPattern should be non-empty, otherwise a [[ParseException]]
      // should have been thrown by the sql parser.
      val tableIdent = TableIdentifier(tableIdentifierPattern.get, Some(db), Some(ds))
      val table = catalog.getTableMetadata(tableIdent).identifier
//      val partition = catalog.getPartition(tableIdent, partitionSpec.get)
      val database = table.database.getOrElse("")
      val tableName = table.table
      val isTemp = catalog.isTemporaryTable(table)
//      val information = partition.simpleString
      Seq(Row(tableName, database, ds, isTemp, "\n"))
    }
  }
}

/**
 * A command to list the column names for a table.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   SHOW COLUMNS (FROM | IN) table_identifier [(FROM | IN) database];
 * }}}
 */
case class XSQLShowColumnsCommand(
    dataSourceName: Option[String] = None,
    databaseName: Option[String],
    tableName: TableIdentifier)
  extends RunnableCommand {
  override val output: Seq[Attribute] = Seq(
    // Column names are based on Hive.
    AttributeReference(
      "col_name",
      StringType,
      nullable = false,
      new MetadataBuilder().putString("col_name", "name of the column").build())(),
    AttributeReference(
      "data_type",
      StringType,
      nullable = false,
      new MetadataBuilder().putString("data_type", "data type of the column").build())(),
    AttributeReference(
      "comment",
      StringType,
      nullable = true,
      new MetadataBuilder().putString("comment", "comment of the column").build())())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog.asInstanceOf[XSQLSessionCatalog]
    val resolver = sparkSession.sessionState.conf.resolver
    var catalogDB: Option[CatalogDatabase] = None
    if (dataSourceName == None) {
      if (databaseName == None) {
        catalogDB = catalog.getUsedCatalogDatabase(tableName)
      } else {
        catalogDB = catalog.getCurrentCatalogDatabase
      }
    } else {
      catalogDB = Option(catalog.getDatabaseMetadata(dataSourceName, databaseName.get))
    }

    if (catalogDB == None) {
      return Seq.empty[Row]
    }

    val ds = catalogDB.get.dataSourceName
    val dbName = Option(catalogDB.get.name)

    val lookupTable = dbName match {
      case None => tableName
      case Some(db) if tableName.database.exists(!resolver(_, db)) =>
        throw new AnalysisException(
          s"SHOW COLUMNS with conflicting databases: '$db' != '${tableName.database.get}'")
      case Some(db) => TableIdentifier(tableName.identifier, Some(db), Some(ds))
    }
    val table = catalog.getTempViewOrPermanentTableMetadata(lookupTable)
    table.schema.map { c =>
      val typeString = if (c.metadata.contains(ELASTIC_SEARCH_TYPE_STRING)) {
        c.metadata.getString(ELASTIC_SEARCH_TYPE_STRING)
      } else {
        c.dataType.simpleString
      }
      Row(c.name, typeString, c.getComment().orNull)
    }
  }
}

/**
 * A command to list the partition names of a table. If the partition spec is specified,
 * partitions that match the spec are returned. [[AnalysisException]] exception is thrown under
 * the following conditions:
 *
 * 1. If the command is called for a non partitioned table.
 * 2. If the partition spec refers to the columns that are not defined as partitioning columns.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   SHOW PARTITIONS [db_name.]table_name [PARTITION(partition_spec)]
 * }}}
 */
case class XSQLShowPartitionsCommand(tableName: TableIdentifier, spec: Option[TablePartitionSpec])
  extends RunnableCommand {
  override val output: Seq[Attribute] = {
    AttributeReference("partition", StringType, nullable = false)() :: Nil
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog.asInstanceOf[XSQLSessionCatalog]
    val table = catalog.getTableMetadata(tableName)
    val tableIdentWithDB = table.identifier.quotedString

    /**
     * Validate and throws an [[AnalysisException]] exception under the following conditions:
     * 1. If the table is not partitioned.
     * 2. If it is a datasource table.
     * 3. If it is a view.
     */
    if (table.tableType == VIEW) {
      throw new AnalysisException(s"SHOW PARTITIONS is not allowed on a view: $tableIdentWithDB")
    }

    if (table.partitionColumnNames.isEmpty) {
      throw new AnalysisException(
        s"SHOW PARTITIONS is not allowed on a table that is not partitioned: $tableIdentWithDB")
    }

    DDLUtils.verifyPartitionProviderIsHive(sparkSession, table, "SHOW PARTITIONS")

    /**
     * Validate the partitioning spec by making sure all the referenced columns are
     * defined as partitioning columns in table definition. An AnalysisException exception is
     * thrown if the partitioning spec is invalid.
     */
    if (spec.isDefined) {
      val badColumns = spec.get.keySet.filterNot(table.partitionColumnNames.contains)
      if (badColumns.nonEmpty) {
        val badCols = badColumns.mkString("[", ", ", "]")
        throw new AnalysisException(
          s"Non-partitioning column(s) $badCols are specified for SHOW PARTITIONS")
      }
    }

    val partNames = catalog.listPartitionNames(tableName, spec)
    partNames.map(Row(_))
  }
}

case class PushDownQueryCommand(
    tableMeta: CatalogTable,
    dataCols: Seq[AttributeReference],
    havingFiltered: Boolean,
    sorted: Boolean,
    union: Boolean,
    limit: Boolean,
    queryContent: String)
  extends RunnableCommand {

  /** Returns a string representing the arguments to this node, minus any children */
  override def argString: String = queryContent

  override def producedAttributes: AttributeSet = outputSet
  // The partition column should always appear after data columns.
  override def output: Seq[AttributeReference] = dataCols

  override def run(sparkSession: SparkSession): Seq[Row] = {
    if (limit == 0) {
      Seq.empty
    } else {
      val result = new ArrayBuffer[Row]
      val catalog = sparkSession.sessionState.catalog.asInstanceOf[XSQLSessionCatalog]
      val midResult = catalog.scanTables(tableMeta, dataCols, limit, queryContent)
      midResult.foreach { arr =>
        result += Row(arr: _*)
      }
      result
    }
  }
}

/**
 * scan table over data engine itself.
 */
case class ScanTableCommand(
    tableMeta: CatalogTable,
    dataCols: Seq[AttributeReference],
    partitionCols: Seq[AttributeReference],
    columns: java.util.List[String] = null,
    condition: HashMap[String, Any] = null,
    sort: java.util.List[HashMap[String, Any]] = null,
    aggs: HashMap[String, Any] = null,
    groupByKeys: Seq[String] = null,
    limit: Int = 10)
  extends RunnableCommand {

  // The partition column should always appear after data columns.
  override def output: Seq[AttributeReference] = dataCols ++ partitionCols

  override def producedAttributes: AttributeSet = outputSet

  override def run(sparkSession: SparkSession): Seq[Row] = {
    if (limit == 0) {
      Seq.empty
    } else {
      val result = new ArrayBuffer[Row]
      val catalog = sparkSession.sessionState.catalog.asInstanceOf[XSQLSessionCatalog]
      val midResult =
        catalog.scanSingleTable(tableMeta, columns, condition, sort, aggs, groupByKeys, limit)
      midResult.foreach { arr =>
        result += Row(arr: _*)
      }
      result
    }
  }
}
