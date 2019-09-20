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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.test.TestSparkSession
import org.apache.spark.sql.xsql.XSQLSessionStateBuilder

/**
 * A special `SparkSession` prepared for XSQL testing.
 */
class XSQLTestSparkSession(sc: SparkContext) extends TestSparkSession(sc) { self =>
  def this(sparkConf: SparkConf) {
    this(
      new SparkContext(
        "local[2]",
        "test-sql-context",
        sparkConf.set("spark.sql.testkey", "true").set(CATALOG_IMPLEMENTATION, "xsql")))
  }

  @transient
  override lazy val sessionState: SessionState = {
    new XSQLSessionStateBuilder(this, None).build()
  }
}
