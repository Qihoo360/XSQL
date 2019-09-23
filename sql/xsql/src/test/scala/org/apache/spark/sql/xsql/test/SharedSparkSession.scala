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

package org.apache.spark.sql.xsql.test

import java.io.{File, FileInputStream}
import java.net.URLDecoder
import java.util.Properties

import scala.collection.JavaConverters._

import org.scalatest.Suite

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.test.TestSparkSession
import org.apache.spark.sql.xsql.XSQLSessionCatalog
import org.apache.spark.sql.xsql.execution.command.{PushDownQueryCommand, ScanTableCommand}
import org.apache.spark.sql.xsql.util.Utils

trait SharedSparkSession extends org.apache.spark.sql.test.SharedSparkSession { self: Suite =>

  def getResourceFile(path: String): File = {
    new File(Thread.currentThread().getContextClassLoader.getResource(path).getFile)
  }

  /**
   * Similar to SQLTestUtilsBase's activateDatabase, but have `ds` parameter in addition.
   * Activates database `ds`.`db` before executing `f`, then switches back to previous database
   * after `f` returns.
   */
  protected def activateDatabase(ds: String, db: String)(f: => Unit): Unit = {
    val catalog = spark.sessionState.catalog.asInstanceOf[XSQLSessionCatalog]
    val catalogDB = catalog.getCurrentCatalogDatabase
    val currentDS = catalogDB.get.dataSourceName
    val currentDB = catalogDB.get.name
    catalog.setCurrentDatabase(ds, db)
    try f
    finally catalog.setCurrentDatabase(currentDS, currentDB)
  }

  /**
   * Check whether the [[LogicalPlan]] of [[DataFrame]] contains PushDown operation as SubQuery
   * for fast.
   */
  def assertSubQueryPushDown(df: DataFrame): Unit = {
    val analyzed = df.queryExecution.analyzed
    val expressions = analyzed.flatMap(_.subqueries)
    assert(
      expressions
        .exists(e => e.isInstanceOf[PushDownQueryCommand] || e.isInstanceOf[ScanTableCommand]))
  }

  /**
   * Check whether the [[LogicalPlan]] of [[DataFrame]] contains PushDown operation for fast.
   */
  def assertContainsPushDown(df: DataFrame, num: Int = 1): Unit = {
    val analyzed = df.queryExecution.analyzed
    val pds = analyzed.collect {
      case e: ScanTableCommand =>
        e
      case e: PushDownQueryCommand =>
        e
    }
    assert(pds.size == num)
  }

  /**
   * Check whether the [[LogicalPlan]] of [[DataFrame]] is wholly PushDown operation for fast.
   */
  def assertPushDown(df: DataFrame): Unit = {
    val analyzed = df.queryExecution.analyzed
    assert(analyzed.isInstanceOf[ScanTableCommand] || analyzed.isInstanceOf[PushDownQueryCommand])
  }

  /**
   * Check whether the result of [[DataFrame]] is non-empty for fast.
   */
  def assertResultNonEmpty(df: DataFrame): Unit = {
    df.show()
    assert(df.count() > 0)
  }

  /**
   * Check whether the result of [[DataFrame]] is empty for fast.
   */
  def assertResultEmpty(df: DataFrame): Unit = {
    df.show()
    assert(df.count() == 0)
  }

  /**
   * Start a local[2] SparkSession with XSQL support, and load configuration from `xsql.conf`
   * found in classpath.
   */
  override protected def createSparkSession: TestSparkSession = {
    val properties = new Properties()
    val path = Utils.getPropertiesFile(file = "xsql.conf")
    properties.load(new FileInputStream(URLDecoder.decode(path, "utf-8")))
    val conf = new SparkConf()
    val propertiesMap = properties
      .stringPropertyNames()
      .asScala
      .map(key => (key, properties.getProperty(key)))
      .toMap
    for ((key, value) <- propertiesMap if key.startsWith("spark.")) {
      conf.set(key, value, false)
    }
    conf.set("spark.sql.caseSensitive", "true")
    new XSQLTestSparkSession(conf)
  }

  /**
   * Stop the underlying [[org.apache.spark.SparkContext]], if any.
   */
  protected override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }
}
