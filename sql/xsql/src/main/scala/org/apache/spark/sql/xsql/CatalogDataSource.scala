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

package org.apache.spark.sql.xsql

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.xsql.DataSourceType.DataSourceType

@DeveloperApi
abstract class CatalogDataSource(
    private val name: String,
    private val dsType: DataSourceType,
    private val version: String,
    val dsManager: DataSourceManager) {
  def getName: String = name
  def getDSType: DataSourceType = dsType
  def getUrl: String
  def getUser: String
  def getPwd: String
  def getVersion: String = version
}

@DeveloperApi
class HiveDataSource(
    private val name: String,
    private val dsType: DataSourceType,
    override val dsManager: DataSourceManager,
    private val url: String,
    private val username: String,
    private val passwd: String,
    private val version: String)
  extends CatalogDataSource(name, dsType, version, dsManager) {
  def getUrl: String = url
  def getUser: String = username
  def getPwd: String = passwd
}

@DeveloperApi
class ElasticSearchDataSource(
    private val name: String,
    private val dsType: DataSourceType,
    override val dsManager: DataSourceManager,
    private val url: String,
    private val username: String,
    private val passwd: String,
    private val version: String)
  extends CatalogDataSource(name, dsType, version, dsManager) {
  def getUrl: String = url
  def getUser: String = username
  def getPwd: String = passwd
}

class MysqlDataSource(
    private val name: String,
    private val dsType: DataSourceType,
    override val dsManager: DataSourceManager,
    private val url: String,
    private val username: String,
    private val passwd: String,
    private val version: String)
  extends CatalogDataSource(name, dsType, version, dsManager) {
  def getUrl: String = url
  def getUser: String = username
  def getPwd: String = passwd
}

@DeveloperApi
class RedisDataSource(
    private val name: String,
    private val dsType: DataSourceType,
    override val dsManager: DataSourceManager,
    private val url: String,
    private val auth: String,
    private val version: String)
  extends CatalogDataSource(name, dsType, version, dsManager) {
  def getUrl: String = url
  def getUser: String = ""
  def getPwd: String = auth
}

@DeveloperApi
class MongoDataSource(
    private val name: String,
    private val dsType: DataSourceType,
    override val dsManager: DataSourceManager,
    private val url: String,
    private val username: String,
    private val passwd: String,
    private val version: String)
  extends CatalogDataSource(name, dsType, version, dsManager) {
  def getUrl: String = url
  def getUser: String = username
  def getPwd: String = passwd
}

@DeveloperApi
class KafkaDataSource(
    private val name: String,
    private val dsType: DataSourceType,
    override val dsManager: DataSourceManager,
    private val url: String,
    private val version: String)
  extends CatalogDataSource(name, dsType, version, dsManager) {
  def getUrl: String = url
  def getUser: String = "Unknown"
  def getPwd: String = "Unknown"
}

@DeveloperApi
class DruidDataSource(
    private val name: String,
    private val dsType: DataSourceType,
    override val dsManager: DataSourceManager,
    private val url: String,
    private val username: String,
    private val passwd: String,
    private val version: String)
  extends CatalogDataSource(name, dsType, version, dsManager) {
  def getUrl: String = url
  def getUser: String = username
  def getPwd: String = passwd
}

@DeveloperApi
class HBaseDataSource(
    private val name: String,
    private val dsType: DataSourceType,
    override val dsManager: DataSourceManager,
    private val host: String,
    private val port: Int,
    private val version: String)
  extends CatalogDataSource(name, dsType, version, dsManager) {
  def getUrl: String = s"${host}:${port}"
  def getUser: String = ""
  def getPwd: String = ""
}
