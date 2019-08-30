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

package org.apache.spark.sql.xsql.internal

import org.apache.spark.sql.internal.SQLConf.buildConf

object config {

  private[xsql] val XSQL_DATASOURCES = buildConf("spark.xsql.datasources")
    .doc("User defined name of datasource for use.")
    .stringConf
    .toSequence
    .createWithDefault(Seq.empty)

  private[xsql] val XSQL_DEFAULT_DATASOURCE = buildConf("spark.xsql.default.datasource")
    .doc("User defined name of default datasource.")
    .stringConf
    .createWithDefault("default")

  private[xsql] val XSQL_DEFAULT_DATABASE = buildConf("spark.xsql.default.database")
    .doc("User defined name of default database.")
    .stringConf
    .createWithDefault("default")

  private[xsql] val EXTRA_DATASOURCE_MANAGERS = buildConf("spark.xsql.extraDatasourceManagers")
    .doc("Class names of datasource manager to add to XSQLExternalCatalog during initialization.")
    .stringConf
    .toSequence
    .createOptional

  private[xsql] val XSQL_ELASTICSEARCH_AGGREGATION_CARDINALITY_THRESHOLD =
    buildConf("spark.xsql.elasticsearch.cardinalityAggregationThreshold")
      .doc("A single-value metrics aggregation that calculates an approximate count of distinct " +
        "values. Computing exact counts requires loading values into a hash set and returning " +
        "its size. This doesn't scale when working on high-cardinality sets and/or large " +
        "values as the required memory usage and the need to communicate those per-shard sets " +
        "between nodes would utilize too many resources of the cluster.")
      .intConf
      .createWithDefault(30)
}
