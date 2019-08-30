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

package org.apache.spark.sql.xsql.execution.datasources

import java.util.Locale
import java.util.concurrent.Callable

import org.apache.hadoop.fs.Path

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.QualifiedTableName
import org.apache.spark.sql.catalyst.analysis.{CastSupport, Resolver}
import org.apache.spark.sql.catalyst.catalog.{
  CatalogTable,
  CatalogUtils,
  HiveTableRelation,
  UnresolvedCatalogRelation
}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{
  InsertIntoDir,
  InsertIntoTable,
  LogicalPlan,
  Project
}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.{
  CreateTableCommand,
  DDLUtils,
  InsertIntoDataSourceDirCommand
}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.hive.execution.InsertIntoHiveDirCommand
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.xsql.execution.command.{
  XSQLCreateDataSourceTableAsSelectCommand,
  XSQLCreateDataSourceTableCommand,
  XSQLCreateHiveTableAsSelectCommand,
  XSQLInsertIntoHiveTable
}

/**
 * Replaces generic operations with specific variants that are designed to work with Spark
 * SQL Data Sources.
 *
 * Note that, this rule must be run after `PreprocessTableCreation` and
 * `PreprocessTableInsertion`.
 */
case class XSQLDataSourceAnalysis(conf: SQLConf) extends Rule[LogicalPlan] with CastSupport {

  def resolver: Resolver = conf.resolver

  // Visible for testing.
  def convertStaticPartitions(
      sourceAttributes: Seq[Attribute],
      providedPartitions: Map[String, Option[String]],
      targetAttributes: Seq[Attribute],
      targetPartitionSchema: StructType): Seq[NamedExpression] = {

    assert(providedPartitions.exists(_._2.isDefined))

    val staticPartitions = providedPartitions.flatMap {
      case (partKey, Some(partValue)) => (partKey, partValue) :: Nil
      case (_, None) => Nil
    }

    // The sum of the number of static partition columns and columns provided in the SELECT
    // clause needs to match the number of columns of the target table.
    if (staticPartitions.size + sourceAttributes.size != targetAttributes.size) {
      throw new AnalysisException(s"The data to be inserted needs to have the same number of " +
        s"columns as the target table: target table has ${targetAttributes.size} " +
        s"column(s) but the inserted data has ${sourceAttributes.size + staticPartitions.size} " +
        s"column(s), which contain ${staticPartitions.size} partition column(s) having " +
        s"assigned constant values.")
    }

    if (providedPartitions.size != targetPartitionSchema.fields.size) {
      throw new AnalysisException(
        s"The data to be inserted needs to have the same number of " +
          s"partition columns as the target table: target table " +
          s"has ${targetPartitionSchema.fields.size} partition column(s) but the inserted " +
          s"data has ${providedPartitions.size} partition columns specified.")
    }

    staticPartitions.foreach {
      case (partKey, partValue) =>
        if (!targetPartitionSchema.fields.exists(field => resolver(field.name, partKey))) {
          throw new AnalysisException(
            s"$partKey is not a partition column. Partition columns are " +
              s"${targetPartitionSchema.fields.map(_.name).mkString("[", ",", "]")}")
        }
    }

    val partitionList = targetPartitionSchema.fields.map { field =>
      val potentialSpecs = staticPartitions.filter {
        case (partKey, partValue) => resolver(field.name, partKey)
      }
      if (potentialSpecs.isEmpty) {
        None
      } else if (potentialSpecs.size == 1) {
        val partValue = potentialSpecs.head._2
        Some(Alias(cast(Literal(partValue), field.dataType), field.name)())
      } else {
        throw new AnalysisException(
          s"Partition column ${field.name} have multiple values specified, " +
            s"${potentialSpecs.mkString("[", ", ", "]")}. Please only specify a single value.")
      }
    }

    // We first drop all leading static partitions using dropWhile and check if there is
    // any static partition appear after dynamic partitions.
    partitionList.dropWhile(_.isDefined).collectFirst {
      case Some(_) =>
        throw new AnalysisException(
          s"The ordering of partition columns is " +
            s"${targetPartitionSchema.fields.map(_.name).mkString("[", ",", "]")}. " +
            "All partition columns having constant values need to appear before other " +
            "partition columns that do not have an assigned constant value.")
    }

    assert(partitionList.take(staticPartitions.size).forall(_.isDefined))
    val projectList =
      sourceAttributes.take(targetAttributes.size - targetPartitionSchema.fields.size) ++
        partitionList.take(staticPartitions.size).map(_.get) ++
        sourceAttributes.takeRight(targetPartitionSchema.fields.size - staticPartitions.size)

    projectList
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {

    case CreateTable(tableDesc, mode, None) if DDLUtils.isHiveTable(tableDesc) =>
      DDLUtils.checkDataColNames(tableDesc)
      CreateTableCommand(tableDesc, ignoreIfExists = mode == SaveMode.Ignore)

    case CreateTable(tableDesc, mode, Some(query)) if DDLUtils.isHiveTable(tableDesc) =>
      DDLUtils.checkDataColNames(tableDesc)
      XSQLCreateHiveTableAsSelectCommand(tableDesc, query, query.output.map(_.name), mode)

    case CreateTable(tableDesc, mode, None) if DDLUtils.isDatasourceTable(tableDesc) =>
      DDLUtils.checkDataColNames(tableDesc)
      XSQLCreateDataSourceTableCommand(tableDesc, ignoreIfExists = mode == SaveMode.Ignore)

    case CreateTable(tableDesc, mode, Some(query))
        if query.resolved && DDLUtils.isDatasourceTable(tableDesc) =>
      DDLUtils.checkDataColNames(tableDesc.copy(schema = query.schema))
      XSQLCreateDataSourceTableAsSelectCommand(tableDesc, mode, query, query.output.map(_.name))

    case InsertIntoTable(r: HiveTableRelation, partSpec, query, overwrite, ifPartitionNotExists)
        if DDLUtils.isHiveTable(r.tableMeta) =>
      XSQLInsertIntoHiveTable(
        r.tableMeta,
        partSpec,
        query,
        overwrite,
        ifPartitionNotExists,
        query.output.map(_.name))

    case InsertIntoDir(isLocal, storage, provider, child, overwrite)
        if DDLUtils.isHiveTable(provider) =>
      val outputPath = new Path(storage.locationUri.get)
      if (overwrite) DDLUtils.verifyNotReadPath(child, outputPath)
      InsertIntoHiveDirCommand(isLocal, storage, child, overwrite, child.output.map(_.name))

    case InsertIntoTable(
        l @ LogicalRelation(_: InsertableRelation, _, _, _),
        parts,
        query,
        overwrite,
        false) if parts.isEmpty =>
      InsertIntoDataSourceCommand(l, query, overwrite)

    case InsertIntoDir(_, storage, provider, query, overwrite)
        if provider.isDefined && provider.get
          .toLowerCase(Locale.ROOT) != DDLUtils.HIVE_PROVIDER =>
      val outputPath = new Path(storage.locationUri.get)
      if (overwrite) DDLUtils.verifyNotReadPath(query, outputPath)

      InsertIntoDataSourceDirCommand(storage, provider.get, query, overwrite)

    case i @ InsertIntoTable(
          l @ LogicalRelation(t: HadoopFsRelation, _, table, _),
          parts,
          query,
          overwrite,
          _) =>
      // If the InsertIntoTable command is for a partitioned HadoopFsRelation and
      // the user has specified static partitions, we add a Project operator on top of the query
      // to include those constant column values in the query result.
      //
      // Example:
      // Let's say that we have a table "t", which is created by
      // CREATE TABLE t (a INT, b INT, c INT) USING parquet PARTITIONED BY (b, c)
      // The statement of "INSERT INTO TABLE t PARTITION (b=2, c) SELECT 1, 3"
      // will be converted to "INSERT INTO TABLE t PARTITION (b, c) SELECT 1, 2, 3".
      //
      // Basically, we will put those partition columns having a assigned value back
      // to the SELECT clause. The output of the SELECT clause is organized as
      // normal_columns static_partitioning_columns dynamic_partitioning_columns.
      // static_partitioning_columns are partitioning columns having assigned
      // values in the PARTITION clause (e.g. b in the above example).
      // dynamic_partitioning_columns are partitioning columns that do not assigned
      // values in the PARTITION clause (e.g. c in the above example).
      val actualQuery = if (parts.exists(_._2.isDefined)) {
        val projectList = convertStaticPartitions(
          sourceAttributes = query.output,
          providedPartitions = parts,
          targetAttributes = l.output,
          targetPartitionSchema = t.partitionSchema)
        Project(projectList, query)
      } else {
        query
      }

      // Sanity check
      if (t.location.rootPaths.size != 1) {
        throw new AnalysisException("Can only write data to relations with a single path.")
      }

      val outputPath = t.location.rootPaths.head
      if (overwrite) DDLUtils.verifyNotReadPath(actualQuery, outputPath)

      val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Append

      val partitionSchema =
        actualQuery.resolve(t.partitionSchema, t.sparkSession.sessionState.analyzer.resolver)
      val staticPartitions = parts.filter(_._2.nonEmpty).map { case (k, v) => k -> v.get }

      InsertIntoHadoopFsRelationCommand(
        outputPath,
        staticPartitions,
        i.ifPartitionNotExists,
        partitionSchema,
        t.bucketSpec,
        t.fileFormat,
        t.options,
        actualQuery,
        mode,
        table,
        Some(t.location),
        actualQuery.output.map(_.name))
  }
}

/**
 * Replaces [[UnresolvedCatalogRelation]] with concrete relation logical plans.
 *
 * TODO: we should remove the special handling for hive tables after completely making hive as a
 * data source.
 */
class XSQLFindDataSourceTable(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  private def readDataSourceTable(table: CatalogTable): LogicalPlan = {
    val qualifiedTableName =
      QualifiedTableName(table.database, table.identifier.table, table.identifier.dataSource)
    val catalog = sparkSession.sessionState.catalog
    catalog.getCachedPlan(
      qualifiedTableName,
      new Callable[LogicalPlan]() {
        override def call(): LogicalPlan = {
          val pathOption = table.storage.locationUri.map("path" -> CatalogUtils.URIToString(_))
          val dataSource =
            DataSource(
              sparkSession,
              // In older version(prior to 2.1) of Spark, the table schema can be empty and should
              // be inferred at runtime. We should still support it.
              userSpecifiedSchema = if (table.schema.isEmpty) None else Some(table.schema),
              partitionColumns = table.partitionColumnNames,
              bucketSpec = table.bucketSpec,
              className = table.provider.get,
              options = table.storage.properties ++ pathOption,
              catalogTable = Some(table))

          LogicalRelation(dataSource.resolveRelation(checkFilesExist = false), table)
        }
      })
  }

  private def readHiveTable(table: CatalogTable): LogicalPlan = {
    HiveTableRelation(
      table,
      // Hive table columns are always nullable.
      table.dataSchema.asNullable.toAttributes,
      table.partitionSchema.asNullable.toAttributes)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case i @ InsertIntoTable(UnresolvedCatalogRelation(tableMeta), _, _, _, _)
        if DDLUtils.isDatasourceTable(tableMeta) =>
      i.copy(table = readDataSourceTable(tableMeta))

    case i @ InsertIntoTable(UnresolvedCatalogRelation(tableMeta), _, _, _, _) =>
      i.copy(table = readHiveTable(tableMeta))

    case UnresolvedCatalogRelation(tableMeta) if DDLUtils.isDatasourceTable(tableMeta) =>
      readDataSourceTable(tableMeta)

    case UnresolvedCatalogRelation(tableMeta) =>
      readHiveTable(tableMeta)
  }
}
