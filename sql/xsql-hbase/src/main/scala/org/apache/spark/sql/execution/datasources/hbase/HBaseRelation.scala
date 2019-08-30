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

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor
import org.apache.hadoop.hbase.client.TableDescriptorBuilder.ModifyableTableDescriptor
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.util.Bytes
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.execution.datasources.hbase
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.xsql.manager.HBaseManager

class CustomedHBaseRelation(
    parameters: Map[String, String],
    userSpecifiedschema: Option[StructType])(@transient override val sqlContext: SQLContext)
  extends HBaseRelation(parameters, userSpecifiedschema)(sqlContext)
  with HBaseRelationTrait {
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    new CustomedHBaseTableScanRDD(this, requiredColumns, filters)
  }
}

trait HBaseRelationTrait {
  val runAnyway: Boolean = false
  val timestamp: Option[Long]
  val minStamp: Option[Long]
  val maxStamp: Option[Long]
  val maxVersions: Option[Int]
  val mergeToLatest: Boolean
  val catalog: HBaseTableCatalog
  def hbaseConf: Configuration
  val serializedToken: Array[Byte]
  def dropTableIfExist(table: String): Unit = {
    val connection = HBaseConnectionCache.getConnection(hbaseConf)
    val admin = connection.getAdmin
    val tableName = TableName.valueOf(table)
    admin.disableTable(tableName)
    admin.deleteTable(tableName)
  }
  def createTableIfNotExist(ignoreIfExists: Boolean = false) {
    val cfs = catalog.getColumnFamilies
    val connection = HBaseConnectionCache.getConnection(hbaseConf)
    // Initialize hBase table if necessary
    val admin = connection.getAdmin
    val isNameSpaceExist = try {
      admin.getNamespaceDescriptor(catalog.namespace)
      true
    } catch {
      case e: NamespaceNotFoundException => false
      case NonFatal(e) =>
        false
    }
    if (!isNameSpaceExist) {
      admin.createNamespace(NamespaceDescriptor.create(catalog.namespace).build)
    }
    val tName = TableName.valueOf(s"${catalog.namespace}:${catalog.name}")
    // INLINE: we will throw exception if user do have IF NOT EXIST in their ddl
    if (admin.isTableAvailable(tName)) {
      if (ignoreIfExists) {
        // scalastyle:off
        println("hbase table has exist, only register table schema here")
        // scalastyle:on
      } else {
        throw new SparkException("table has exist, please edit schema file instead")
      }
    }

    if (!admin.isTableAvailable(tName)) {
      if (catalog.numReg <= 3) {
        throw new InvalidRegionNumberException(
          "Creating a new table should " +
            "specify the number of regions which must be greater than 3.")
      }
      val tableDesc = new ModifyableTableDescriptor(tName)
      cfs.foreach { x =>
        val cf = new ModifyableColumnFamilyDescriptor(x.getBytes())
        maxVersions.foreach(v => cf.setMaxVersions(v))
        cf.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
        tableDesc.setColumnFamily(cf)
      }
      val startKey = catalog.shcTableCoder.toBytes("aaaaaaa")
      val endKey = catalog.shcTableCoder.toBytes("zzzzzzz")
      val splitKeys = Bytes.split(startKey, endKey, catalog.numReg - 3)
      admin.createTable(tableDesc, splitKeys)
      val r = connection.getRegionLocator(tName).getAllRegionLocations
      while (r == null || r.size() == 0) {
        Thread.sleep(1000)
      }
    }

    admin.close()
    connection.close()
  }
  def rows: RowKey
  def singleKey: Boolean
  def getField(name: String): Field
  def isPrimaryKey(c: String): Boolean
  def isComposite(): Boolean
  def isColumn(c: String): Boolean
  def getIndexedProjections(requiredColumns: Array[String]): Seq[(Field, Int)]
  def splitRowKeyColumns(requiredColumns: Array[String]): (Seq[Field], Seq[Field])
}

class HBaseRelationImpl(parameters: Map[String, String], userSpecifiedschema: Option[StructType])
  extends HBaseRelationTrait {

  override val runAnyway: Boolean =
    parameters.get(HBaseManager.RUNANYWAY).map(_.toBoolean).getOrElse(false)
  val timestamp = parameters.get(hbase.HBaseRelation.TIMESTAMP).map(_.toLong)
  val minStamp = parameters.get(hbase.HBaseRelation.MIN_STAMP).map(_.toLong)
  val maxStamp = parameters.get(hbase.HBaseRelation.MAX_STAMP).map(_.toLong)
  val maxVersions = parameters.get(hbase.HBaseRelation.MAX_VERSIONS).map(_.toInt)
  val mergeToLatest =
    parameters.get(hbase.HBaseRelation.MERGE_TO_LATEST).map(_.toBoolean).getOrElse(true)

  val catalog = HBaseTableCatalog(parameters)

  private val wrappedConf = {
    implicit val formats = DefaultFormats
    val hConf = {
      val hBaseConfiguration =
        parameters
          .get(hbase.HBaseRelation.HBASE_CONFIGURATION)
          .map(parse(_).extract[Map[String, String]])

      val conf = HBaseConfiguration.create
      hBaseConfiguration.foreach(_.foreach(e => conf.set(e._1, e._2)))
      conf
    }
    // task is already broadcast; since hConf is per HBaseRelation (currently), broadcast'ing
    // it again does not help - it actually hurts. When we add support for
    // caching hConf across HBaseRelation, we can revisit broadcast'ing it (with a caching
    // mechanism in place)
    new SerializableConfiguration(hConf)
  }

  def hbaseConf: Configuration = wrappedConf.value

  val serializedToken = SHCCredentialsManager.manager.getTokenForCluster(hbaseConf)

  def rows: RowKey = catalog.row

  def singleKey: Boolean = {
    rows.fields.size == 1
  }

  def getField(name: String): Field = {
    catalog.getField(name)
  }

  // check whether the column is the first key in the rowkey
  def isPrimaryKey(c: String): Boolean = {
    val f1 = catalog.getRowKey(0)
    val f2 = getField(c)
    f1 == f2
  }

  def isComposite(): Boolean = {
    catalog.getRowKey.size > 1
  }
  def isColumn(c: String): Boolean = {
    !catalog.getRowKey.map(_.colName).contains(c)
  }

  def getIndexedProjections(requiredColumns: Array[String]): Seq[(Field, Int)] = {
    requiredColumns.map(catalog.sMap.getField(_)).zipWithIndex
  }
  // Retrieve all columns we will return in the scanner
  def splitRowKeyColumns(requiredColumns: Array[String]): (Seq[Field], Seq[Field]) = {
    val (l, r) =
      requiredColumns.map(catalog.sMap.getField(_)).partition(_.cf == HBaseTableCatalog.rowKey)
    (l, r)
  }
}
