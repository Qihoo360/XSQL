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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.xsql.XSQLSessionCatalog

/**
 * A command for users to list the databases/schemas of specified data source.
 * If a databasePattern is supplied then the databases that only match the
 * pattern would be listed.
 * The syntax of using this command in SQL is:
 * {{{
 *   SHOW (DATABASES|SCHEMAS) [FROM|IN 'ds'] [LIKE 'identifier_with_wildcards'];
 * }}}
 */
case class XSQLShowDatabasesCommand(
    dataSourceName: Option[String],
    databasePattern: Option[String])
  extends RunnableCommand {

  // The result of SHOW ds.DATABASES has one column called 'databaseName'
  override val output: Seq[Attribute] = {
    AttributeReference("databaseName", StringType, nullable = false)() ::
      AttributeReference("dataSourceName", StringType, nullable = false)() ::
      AttributeReference("dataSourceType", StringType, nullable = false)() :: Nil
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog.asInstanceOf[XSQLSessionCatalog]
    var dsName: String = null
    var dsType: String = null
    if (dataSourceName == None) {
      val catalogDB = catalog.getCurrentCatalogDatabase
      dsName = catalogDB.get.dataSourceName
      dsType = catalog.getDataSourceType(dsName)
    } else {
      dsName = dataSourceName.get
      dsType = catalog.getDataSourceType(dsName)
    }
    val databases =
      databasePattern
        .map { pattern =>
          catalog.listDatabasesForXSQL(dsName, pattern)
        }
        .getOrElse(catalog.listDatabasesForXSQL(dsName))
    databases.map { d =>
      Row(d, dsName, dsType)
    }
  }
}

/**
 * Command for setting the current database.
 * {{{
 *   USE ['ds'.]database_name;
 * }}}
 */
case class XSQLSetDatabaseCommand(dataSourceName: Option[String], databaseName: String)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog.asInstanceOf[XSQLSessionCatalog]
    if (dataSourceName.isEmpty) {
      catalog.setCurrentDatabase(databaseName)
    } else {
      catalog.setCurrentDatabase(dataSourceName.get, databaseName)
    }
    Seq.empty[Row]
  }
}
