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

package org.apache.spark.sql.xsql.manager

import java.{util => ju}
import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap}

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{
  CatalogDatabase,
  CatalogStorageFormat,
  CatalogTable,
  CatalogTableType
}
import org.apache.spark.sql.kafka010.KafkaUtils
import org.apache.spark.sql.xsql.{CatalogDataSource, DataSourceManager, KafkaDataSource}
import org.apache.spark.sql.xsql.DataSourceType.KAFKA

/**
 * Manager for KAFKA.
 */
class KafkaManager extends DataSourceManager with Logging {
  import DataSourceManager._
  import KafkaManager._

  override def shortName(): String = KAFKA.toString

  private val dataSourceToKafkaConsumer = new HashMap[String, KafkaConsumer[String, Object]]

  private val deserClassName = classOf[StringDeserializer].getName

  override protected def cacheDatabase(
      isDefault: Boolean,
      dataSourceName: String,
      infos: Map[String, String],
      dataSourcesByName: HashMap[String, CatalogDataSource],
      dataSourceToCatalogDatabase: HashMap[String, HashMap[String, CatalogDatabase]]): Unit = {
    val bootstrapServers = infos.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)
    if (bootstrapServers == None) {
      throw new SparkException(
        "Data source is Kafka," +
          s" ${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG} must be configured!")
    }
    val ds =
      new KafkaDataSource(dataSourceName, KAFKA, this, bootstrapServers.get, infos(VERSION))
    dataSourcesByName(ds.getName) = ds
    val kafkaParams: ju.Map[String, Object] = new ju.HashMap[String, Object](infos.asJava)
    val uniqueGroupId = s"xsql-kafka-manager-${UUID.randomUUID}"
    kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, uniqueGroupId)
    kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserClassName)
    kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserClassName)
    val kafkaConsumer = new KafkaConsumer[String, Object](kafkaParams)
    dataSourceToKafkaConsumer += ((dataSourceName, kafkaConsumer))
    val xdatabases = dataSourceToCatalogDatabase.getOrElseUpdate(
      ds.getName,
      new HashMap[String, CatalogDatabase])
    val db = CatalogDatabase(
      id = newDatabaseId,
      dataSourceName = dataSourceName,
      name = DATABASE_NAME,
      description = null,
      locationUri = null,
      properties = Map.empty)
    xdatabases += ((DATABASE_NAME, db))
  }

  override protected def cacheTable(
      dataSourceName: String,
      dataSourceToCatalogDatabase: HashMap[String, HashMap[String, CatalogDatabase]],
      dbToCatalogTable: HashMap[Int, HashMap[String, CatalogTable]]): Unit = {
    val kafkaConsumer = dataSourceToKafkaConsumer(dataSourceName)
    val xdatabases = dataSourceToCatalogDatabase(dataSourceName)
    xdatabases.foreach { kv =>
      val dbName = kv._1
      val db = kv._2
      val xtables = dbToCatalogTable.getOrElseUpdate(db.id, new HashMap[String, CatalogTable])
      val topics = kafkaConsumer.listTopics().asScala
      val (whiteTables, blackTables) = getWhiteAndBlackTables(dbName)
      val filteredTopics = topics.filter(
        topic =>
          !CONSUMER_OFFSETS.equalsIgnoreCase(topic._1) &&
            isSelectedTable(whiteTables, blackTables, topic._1))
      filteredTopics.foreach { topic =>
        val topicName = topic._1
        val partitionInfos = topic._2
        val tableOpt =
          getTableOption(dbName, dbName, topicName, partitionInfos.asScala)
        tableOpt.foreach { table =>
          xtables += ((topicName, table))
        }
      }
    }
  }

  override def getDefaultOptions(table: CatalogTable): Map[String, String] = {
    val identifier = table.identifier
    val dsName = identifier.dataSource.get
    val topic = identifier.table
    getDefaultOptions(dsName, topic, None)
  }

  private def getDefaultOptions(
      dsName: String,
      topic: String,
      partitionOpts: Option[Seq[PartitionInfo]]): Map[String, String] = {
    val options = new HashMap[String, String]
    val bootstrapServers = cachedProperties(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)
    val consumerStrategy =
      cachedProperties.getOrElse(CONSUMER_STRATEGY, DEFAULT_CONSUMER_STRATEGY)
    options += ((s"kafka.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}", bootstrapServers))
    options += ((consumerStrategy, topic))
    partitionOpts.map { partitionInfos =>
      options += ((KAFKA_PARTITIONS, partitionInfos.toString))
    }
    (options ++: cachedProperties).toMap
  }

  override def listTables(dsName: String, dbName: String): Seq[String] = {
    val kafkaConsumer = dataSourceToKafkaConsumer(dsName)
    val topics = kafkaConsumer.listTopics().asScala
    val (whiteTables, blackTables) = getWhiteAndBlackTables(dbName)
    val filteredTopics = topics.filter(
      topic =>
        !CONSUMER_OFFSETS.equalsIgnoreCase(topic._1) &&
          isSelectedTable(whiteTables, blackTables, topic._1))
    val result = ArrayBuffer.empty[String]
    filteredTopics.foreach(topic => result += topic._1)
    result.seq
  }

  override def tableExists(dbName: String, table: String): Boolean = {
    val kafkaConsumer = dataSourceToKafkaConsumer(dsName)
    val topics = kafkaConsumer.listTopics().asScala
    topics.contains(table)
  }

  override def doGetRawTable(
      db: String,
      originDB: String,
      table: String): Option[CatalogTable] = {
    val kafkaConsumer = dataSourceToKafkaConsumer(dsName)
    val partitionInfos = kafkaConsumer.partitionsFor(table)
    getTableOption(db, originDB, table, partitionInfos.asScala)
  }

  private def getTableOption(
      dbName: String,
      originDB: String,
      topicName: String,
      partitionInfos: Seq[PartitionInfo]): Option[CatalogTable] = {
    logDebug(s"Parse kafka topic $topicName")
    val parameters: Map[String, String] =
      getDefaultOptions(dsName, topicName, Some(partitionInfos))
    val table = CatalogTable(
      identifier = TableIdentifier(topicName, Option(dbName), Option(dsName)),
      tableType = CatalogTableType.TOPIC,
      storage = CatalogStorageFormat(
        locationUri = None,
        // To avoid ClassNotFound exception, we try our best to not get the format class,
        // but get the class name directly. However, for non-native tables,
        // there is no interface to get the format class name,
        // so we may still throw ClassNotFound in this case.
        inputFormat = None,
        outputFormat = None,
        serde = None,
        compressed = false,
        properties = parameters),
      schema = KafkaUtils.kafkaSchema,
      provider = Some(FULL_PROVIDER),
      properties = Map(ORI_DB_NAME -> originDB))
    Option(table)
  }

  override def isPushDown: Boolean = false

  override def stop(): Unit = {
    dataSourceToKafkaConsumer.values.foreach(_.close())
  }

}

object KafkaManager {
  val DATABASE_NAME = "kafka"
  val TOPIC_NAME = "topic"
  val FULL_PROVIDER = "kafka"
  val CONSUMER_OFFSETS = "__consumer_offsets"
  val KAFKA_PARTITIONS = "kafka_partitions"
  val CONSUMER_STRATEGY = "consumer.strategy"
  val DEFAULT_CONSUMER_STRATEGY = "subscribe"
  val DEFAULT_STREAM_SINK = "console"
}
