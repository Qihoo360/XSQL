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
package org.apache.spark.sql.execution.datasources.redis

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.xsql.DataSourceManager._
import org.apache.spark.sql.xsql.execution.datasources.redis.RedisSpecialStrategy

trait RedisRelationTrait {
  val parameters: Map[String, String]
  val schema: StructType
  lazy val redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(parameters.get(URL).get))
}
case class RedisRelationImpl(val parameters: Map[String, String], val schema: StructType)
  extends RedisRelationTrait

case class RedisRelation(
    parameters: Map[String, String],
    schema: StructType,
    filter: Seq[Expression] = Nil)(@transient val sqlContext: SQLContext)
  extends BaseRelation
  with PrunedScan
  with RedisRelationTrait {

  override def toString: String = s"RedisRelation(${filter.mkString(",")})"

  val partitionNum: Int = parameters.getOrElse("partitionNum", "1").toInt

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val filters = filter
      .map(RedisSpecialStrategy.getAttr)
      .groupBy(_._1)
      .map(tup => (tup._1, tup._2.map(_._2)))
    new RedisRDD(sqlContext.sparkContext, this, filters, requiredColumns, partitionNum)
  }
}
