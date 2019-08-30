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

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.catalog.CatalogDatabase
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.CatalogUtils
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.{DDLUtils, RunnableCommand}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.getCommonJDBCType
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.types._
import org.apache.spark.sql.xsql.XSQLSessionCatalog
import org.apache.spark.sql.xsql.manager.MysqlManager
import org.apache.spark.sql.xsql.types._

/**
 * A command for users to create a new database.
 *
 * It will issue an error message when the database with the same name already exists,
 * unless 'ifNotExists' is true.
 * The syntax of using this command in SQL is:
 * {{{
 *   CREATE (DATABASE|SCHEMA) [IF NOT EXISTS] ['ds'.]database_name
 *     [COMMENT database_comment]
 *     [LOCATION database_directory]
 *     [WITH DBPROPERTIES (property_name=property_value, ...)];
 * }}}
 */
case class XSQLCreateDatabaseCommand(
    dataSourceName: Option[String],
    databaseName: String,
    ifNotExists: Boolean,
    path: Option[String],
    comment: Option[String],
    props: Map[String, String])
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog.asInstanceOf[XSQLSessionCatalog]
    var dsName: String = null
    if (dataSourceName == None) {
      val catalogDB = catalog.getCurrentCatalogDatabase
      dsName = catalogDB.get.dataSourceName
    } else {
      dsName = dataSourceName.get
    }
    catalog.createDatabase(
      CatalogDatabase(
        dataSourceName = dsName,
        name = databaseName,
        description = comment.getOrElse(""),
        locationUri =
          path.map(CatalogUtils.stringToURI(_)).getOrElse(catalog.getDefaultDBPath(databaseName)),
        properties = props),
      ifNotExists)
    Seq.empty[Row]
  }
}

/**
 * A command for users to remove a database from the system.
 *
 * 'ifExists':
 * - true, if database_name does't exist, no action
 * - false (default), if database_name does't exist, a warning message will be issued
 * 'cascade':
 * - true, the dependent objects are automatically dropped before dropping database.
 * - false (default), it is in the Restrict mode. The database cannot be dropped if
 * it is not empty. The inclusive tables must be dropped at first.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *    DROP DATABASE [IF EXISTS] database_name [RESTRICT|CASCADE];
 * }}}
 */
case class XSQLDropDatabaseCommand(
    dataSourceName: Option[String],
    databaseName: String,
    ifExists: Boolean,
    cascade: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog.asInstanceOf[XSQLSessionCatalog]
    catalog.dropDatabase(dataSourceName, databaseName, ifExists, cascade)
    Seq.empty[Row]
  }
}

/**
 * A command for users to add new (key, value) pairs into DBPROPERTIES
 * If the database does not exist, an error message will be issued to indicate the database
 * does not exist.
 * The syntax of using this command in SQL is:
 * {{{
 *    ALTER (DATABASE|SCHEMA) database_name SET DBPROPERTIES (property_name=property_value, ...)
 * }}}
 */
case class XSQLAlterDatabasePropertiesCommand(
    dataSourceName: Option[String],
    databaseName: String,
    props: Map[String, String])
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog.asInstanceOf[XSQLSessionCatalog]
    val db: CatalogDatabase = catalog.getDatabaseMetadata(dataSourceName, databaseName)
    catalog.alterDatabase(db.copy(properties = db.properties ++ props))

    Seq.empty[Row]
  }
}

/**
 * A command for users to show the name of the database, its comment (if one has been set), and its
 * root location on the filesystem. When extended is true, it also shows the database's properties
 * If the database does not exist, an error message will be issued to indicate the database
 * does not exist.
 * The syntax of using this command in SQL is
 * {{{
 *    DESCRIBE DATABASE [EXTENDED] db_name
 * }}}
 */
case class XSQLDescribeDatabaseCommand(
    dataSourceName: Option[String],
    databaseName: String,
    extended: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog.asInstanceOf[XSQLSessionCatalog]
    val dbMetadata: CatalogDatabase = catalog.getDatabaseMetadata(dataSourceName, databaseName)
    val dsName = dbMetadata.dataSourceName
    val locationUri =
      Option(dbMetadata.locationUri).map(CatalogUtils.URIToString(_)).getOrElse("")
    val description = Option(dbMetadata.description).getOrElse("")
    val result =
      Row("Data Source Name", dsName) ::
        Row("Database Name", dbMetadata.name) ::
        Row("Description", description) ::
        Row("Location", locationUri) :: Nil

    if (extended) {
      val properties =
        if (dbMetadata.properties.isEmpty) {
          ""
        } else {
          dbMetadata.properties.toSeq.mkString("(", ", ", ")")
        }
      result :+ Row("Properties", properties)
    } else {
      result
    }
  }

  override val output: Seq[Attribute] = {
    AttributeReference("database_description_item", StringType, nullable = false)() ::
      AttributeReference("database_description_value", StringType, nullable = false)() :: Nil
  }
}

/**
 * A command that sets table/view properties.
 *
 * The syntax of this command is:
 * {{{
 *   ALTER TABLE table1 SET TBLPROPERTIES ('key1' = 'val1', 'key2' = 'val2', ...);
 *   ALTER VIEW view1 SET TBLPROPERTIES ('key1' = 'val1', 'key2' = 'val2', ...);
 * }}}
 */
case class XSQLAlterTableSetPropertiesCommand(
    tableName: TableIdentifier,
    properties: Map[String, String],
    isView: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog.asInstanceOf[XSQLSessionCatalog]
    val table = catalog.getTableMetadata(tableName)
    DDLUtils.verifyAlterTableType(catalog, table, isView)
    // This overrides old properties and update the comment parameter of CatalogTable
    // with the newly added/modified comment since CatalogTable also holds comment as its
    // direct property.
    val newTable = table.copy(
      properties = table.properties ++ properties,
      comment = properties.get("comment").orElse(table.comment))
    catalog.alterTable(newTable)
    Seq.empty[Row]
  }

}

/**
 * A command that unsets table/view properties.
 *
 * The syntax of this command is:
 * {{{
 *   ALTER TABLE table1 UNSET TBLPROPERTIES [IF EXISTS] ('key1', 'key2', ...);
 *   ALTER VIEW view1 UNSET TBLPROPERTIES [IF EXISTS] ('key1', 'key2', ...);
 * }}}
 */
case class XSQLAlterTableUnsetPropertiesCommand(
    tableName: TableIdentifier,
    propKeys: Seq[String],
    ifExists: Boolean,
    isView: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog.asInstanceOf[XSQLSessionCatalog]
    val table = catalog.getTableMetadata(tableName)
    DDLUtils.verifyAlterTableType(catalog, table, isView)
    if (!ifExists) {
      propKeys.foreach { k =>
        if (!table.properties.contains(k) && k != "comment") {
          throw new AnalysisException(
            s"Attempted to unset non-existent property '$k' in table '${table.identifier}'")
        }
      }
    }
    // If comment is in the table property, we reset it to None
    val tableComment = if (propKeys.contains("comment")) None else table.comment
    val newProperties = table.properties.filter { case (k, _) => !propKeys.contains(k) }
    val newTable = table.copy(properties = newProperties, comment = tableComment)
    catalog.alterTable(newTable)
    Seq.empty[Row]
  }

}

/**
 * A command to change the column for a table, only support changing the comment of a non-partition
 * column for now.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   ALTER TABLE table_identifier
 *   CHANGE [COLUMN] column_old_name column_new_name column_dataType [COMMENT column_comment]
 *   [FIRST | AFTER column_name];
 * }}}
 */
case class XSQLAlterTableChangeColumnCommand(
    table: TableIdentifier,
    columnName: String,
    newColumn: StructField)
  extends RunnableCommand {

  // TODO: support change column name/dataType/metadata/position.
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog.asInstanceOf[XSQLSessionCatalog]
    val catalogTable = catalog.getTableMetadata(table)
    val resolver = sparkSession.sessionState.conf.resolver
    DDLUtils.verifyAlterTableType(catalog, catalogTable, isView = false)

    // Find the origin column from dataSchema by column name.
    val originColumn = findColumnByName(catalogTable.dataSchema, columnName, resolver)
    // Throw an AnalysisException if the column name/dataType is changed.
    if (catalogTable.provider.get != MysqlManager.FULL_PROVIDER) {
      if (!columnEqual(originColumn, newColumn, resolver)) {
        throw new AnalysisException(
          "ALTER TABLE CHANGE COLUMN is not supported for changing column " +
            s"'${originColumn.name}' with type '${originColumn.dataType}' to " +
            s"'${newColumn.name}' with type '${newColumn.dataType}'")
      }
    }

    val newDataSchema = catalogTable.dataSchema.fields.map { field =>
      if (field.name == originColumn.name) {
        // Create a new column from the origin column with the new comment.
        addComment(field, newColumn.getComment).copy(
          name = newColumn.name,
          dataType = newColumn.dataType,
          nullable = newColumn.nullable,
          metadata = newColumn.metadata)
      } else {
        field
      }
    }
    if (catalogTable.provider.get == MysqlManager.FULL_PROVIDER) {
      val dialect = JdbcDialects.get(catalogTable.storage.properties.get("url").get)
      val strSchema = schemaString(StructType(Seq(newColumn)), dialect)
      val sql = s"ALTER TABLE ${table.table} CHANGE $columnName $strSchema"
      catalog.alterTableDataSchema(table, StructType(newDataSchema), sql)
    } else {
      catalog.alterTableDataSchema(table, StructType(newDataSchema))
    }
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
        sb.append(s"AUTO_INCREMENT ")
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
  // Find the origin column from schema by column name, throw an AnalysisException if the column
  // reference is invalid.
  private def findColumnByName(
      schema: StructType,
      name: String,
      resolver: Resolver): StructField = {
    schema.fields
      .collectFirst {
        case field if resolver(field.name, name) => field
      }
      .getOrElse(
        throw new AnalysisException(s"Can't find column `$name` given table data columns " +
          s"${schema.fieldNames.mkString("[`", "`, `", "`]")}"))
  }

  // Add the comment to a column, if comment is empty, return the original column.
  private def addComment(column: StructField, comment: Option[String]): StructField = {
    comment.map(column.withComment(_)).getOrElse(column)
  }

  // Compare a [[StructField]] to another, return true if they have the same column
  // name(by resolver) and dataType.
  private def columnEqual(field: StructField, other: StructField, resolver: Resolver): Boolean = {
    resolver(field.name, other.name) && field.dataType == other.dataType
  }
}

/**
 * A command that sets the serde class and/or serde properties of a table/view.
 *
 * The syntax of this command is:
 * {{{
 *   ALTER TABLE table [PARTITION spec] SET SERDE serde_name [WITH SERDEPROPERTIES props];
 *   ALTER TABLE table [PARTITION spec] SET SERDEPROPERTIES serde_properties;
 * }}}
 */
case class XSQLAlterTableSerDePropertiesCommand(
    tableName: TableIdentifier,
    serdeClassName: Option[String],
    serdeProperties: Option[Map[String, String]],
    partSpec: Option[TablePartitionSpec])
  extends RunnableCommand {

  // should never happen if we parsed things correctly
  require(
    serdeClassName.isDefined || serdeProperties.isDefined,
    "ALTER TABLE attempted to set neither serde class name nor serde properties")

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog.asInstanceOf[XSQLSessionCatalog]
    val table = catalog.getTableMetadata(tableName)
    DDLUtils.verifyAlterTableType(catalog, table, isView = false)
    // For datasource tables, disallow setting serde or specifying partition
    if (partSpec.isDefined && DDLUtils.isDatasourceTable(table)) {
      throw new AnalysisException(
        "Operation not allowed: ALTER TABLE SET " +
          "[SERDE | SERDEPROPERTIES] for a specific partition is not supported " +
          "for tables created with the datasource API")
    }
    if (serdeClassName.isDefined && DDLUtils.isDatasourceTable(table)) {
      throw new AnalysisException(
        "Operation not allowed: ALTER TABLE SET SERDE is " +
          "not supported for tables created with the datasource API")
    }
    if (partSpec.isEmpty) {
      val newTable = table.withNewStorage(
        serde = serdeClassName.orElse(table.storage.serde),
        properties = table.storage.properties ++ serdeProperties.getOrElse(Map()))
      catalog.alterTable(newTable)
    } else {
      val spec = partSpec.get
      val part = catalog.getPartition(table.identifier, spec)
      val newPart = part.copy(
        storage = part.storage.copy(
          serde = serdeClassName.orElse(part.storage.serde),
          properties = part.storage.properties ++ serdeProperties.getOrElse(Map())))
      catalog.alterPartitions(table.identifier, Seq(newPart))
    }
    Seq.empty[Row]
  }

}
