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
package org.apache.spark.sql.execution.datasources.hbase

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

class CustomedDefaultSource
  extends DefaultSource
  with DataSourceRegister
  with SchemaRelationProvider {

  override def shortName(): String = "hbase"

  /**
   * Returns a new base relation with the given parameters and user defined schema.
   *
   * @note The parameters' keywords are case insensitive and this insensitivity is enforced
   *       by the Map that is passed to the function.
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    new CustomedHBaseRelation(parameters, Option(schema))(sqlContext)
  }
}
