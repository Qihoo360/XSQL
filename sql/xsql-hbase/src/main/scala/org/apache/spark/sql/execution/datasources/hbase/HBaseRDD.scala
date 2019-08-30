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

import java.util.ArrayList

import scala.collection.mutable

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{Filter => HFilter, FirstKeyOnlyFilter, KeyOnlyFilter}

import org.apache.spark.{Partition, SparkException, TaskContext}
import org.apache.spark.sql.{sources, Row}
import org.apache.spark.sql.execution.datasources.hbase
import org.apache.spark.sql.execution.datasources.hbase.types.{SHCDataType, SHCDataTypeFactory}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.util.ShutdownHookManager

// INLINE: HBaseTableScanRDD is private[hbase], so we make a fake one here
private[hbase] class CustomedHBaseTableScanRDD(
    override val relation: CustomedHBaseRelation,
    override val requiredColumns: Array[String],
    override val filters: Array[Filter])
  extends HBaseTableScanRDD(relation, requiredColumns, filters)
  with HBaseTableScanTrait {

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split
      .asInstanceOf[CustomedHBaseScanPartition]
      .regions
      .server
      .map {
        identity
      }
      .toSeq
  }
  override def getPartitions: Array[Partition] = super.getPartitions

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    context.addTaskCompletionListener(context => close())
    compute(split)
  }
}

case class CustomedHBaseRegion(
    override val index: Int,
    start: Option[HBaseType] = None,
    end: Option[HBaseType] = None,
    server: Option[String] = None)
  extends Partition

case class CustomedHBaseScanPartition(
    override val index: Int,
    regions: CustomedHBaseRegion,
    scanRanges: Array[ScanRange[Array[Byte]]],
    tf: SerializedTypedFilter)
  extends Partition

trait HBaseTableScanTrait {
  val relation: HBaseRelationTrait
  val requiredColumns: Array[String]
  val filters: Array[Filter]
  import hbase.order
  import HBaseTableScanUtil._

  val columnFields: Seq[Field]
  val serializedToken: Array[Byte]
  lazy val columnSet = mutable.HashSet(columnFields: _*)

  implicit class FilterChecker(val attrs: Array[Filter]) {
    def getAttr(filter: Filter): Seq[String] = {
      filter match {
        case a: sources.EqualTo =>
          Seq(a.attribute)
        case a: sources.GreaterThan =>
          Seq(a.attribute)
        case a: sources.LessThan =>
          Seq(a.attribute)
        case a: sources.GreaterThanOrEqual =>
          Seq(a.attribute)
        case a: sources.LessThanOrEqual =>
          Seq(a.attribute)
        case a: sources.In =>
          Seq(a.attribute)
        case a: sources.IsNull =>
          Seq(a.attribute)
        case a: sources.IsNotNull =>
          Seq(a.attribute)
        case sources.And(left, right) =>
          getAttr(left) ++ getAttr(right)
        case sources.Or(left, right) =>
          getAttr(left) ++ getAttr(right)
        case sources.Not(child) =>
          getAttr(child)
        case a: sources.StringStartsWith =>
          Seq(a.attribute)
        case a: sources.StringEndsWith =>
          Seq(a.attribute)
        case a: sources.StringContains =>
          Seq(a.attribute)
        case _ => Seq.empty
      }
    }
    def isNotSafe(): Boolean = {
      attrs.isEmpty || attrs.flatMap(getAttr).exists(relation.isColumn(_))
    }
  }

  private def checkRowKeyOnly(): Unit = {
    if (filters.isNotSafe && !relation.runAnyway) {
      throw new SparkException(
        "only rowkey query is allowed to relieve cluster's workload," +
          "configure ds.force = true if necessary!")
    }
  }

  def getHBaseScanPartition: CustomedHBaseScanPartition = {
    checkRowKeyOnly()
    val hbaseFilter = CustomedHBaseFilter.buildFilters(filters, relation, columnSet)
    CustomedHBaseScanPartition(
      0,
      CustomedHBaseRegion(0),
      hbaseFilter.ranges,
      CustomedTypedFilter.toSerializedTypedFilter(hbaseFilter.tf))
  }

  def getPartitions: Array[Partition] = {
    checkRowKeyOnly()
    val hbaseFilter = CustomedHBaseFilter.buildFilters(filters, relation, columnSet)
    var idx = 0
    val r = CustomedRegionResource(relation)
    val ps = r.flatMap { x =>
      // HBase take maximum as empty byte array, change it here.
      val pScan = ScanRange(
        Some(Bound(x.start.get, true)),
        if (x.end.get.size == 0) None else Some(Bound(x.end.get, false)))
      // INLINE: "and" will get ranges should compute in this paritition
      val ranges = ScanRange.and(pScan, hbaseFilter.ranges)(hbase.ord)
      if (ranges.size > 0) {
        val p = Some(
          CustomedHBaseScanPartition(
            idx,
            x,
            ranges,
            CustomedTypedFilter.toSerializedTypedFilter(hbaseFilter.tf)))
        idx += 1
        p
      } else {
        None
      }
    }.toArray
    r.release()
    ShutdownHookManager.addShutdownHook { () =>
      HBaseConnectionCache.close()
    }
    ps.asInstanceOf[Array[Partition]]
  }

  private def buildRow(fields: Seq[(Field, SHCDataType)], result: Result): Seq[Any] = {
    val r = result.getRow
    val keySeq = {
      if (relation.isComposite()) {
        relation.catalog.shcTableCoder
          .decodeCompositeRowKey(r, relation.catalog.getRowKey)
      } else {
        val f = relation.catalog.getRowKey.head
        Seq((f, r)).toMap
      }
    }

    val valueSeq = fields.map {
      case (field, deser) =>
        if (field.isRowKey) {
          val b = keySeq.get(field).get
          if (b.isInstanceOf[Array[Byte]]) {
            deser.fromBytes(b.asInstanceOf[Array[Byte]])
          } else {
            b
          }
        } else {
          val kv = result.getColumnLatestCell(
            relation.catalog.shcTableCoder.toBytes(field.cf),
            relation.catalog.shcTableCoder.toBytes(field.col))
          if (kv == null || kv.getValueLength == 0) {
            null
          } else {
            deser.fromBytes(CellUtil.cloneValue(kv))
          }
        }
    }
    // Return the row ordered by the requested order
//    Row.fromSeq(valueSeq)
    valueSeq
  }

  def buildScan(
      start: Option[Bound[HBaseType]],
      end: Option[Bound[HBaseType]],
      columns: Seq[Field],
      filter: Option[HFilter]): Scan = {
    val scan = {
      (start, end) match {
        case (Some(Bound(lb, lbinc)), Some(Bound(ub, ubinc))) =>
          new Scan().withStartRow(lb, lbinc).withStopRow(ub, ubinc)
        case (Some(Bound(lb, lbinc)), None) => new Scan().withStartRow(lb, lbinc)
        case (None, Some(Bound(ub, ubinc))) => new Scan().withStopRow(ub, ubinc)
        case _ => new Scan
      }
    }
    handleTimeSemantics(scan)

    columns.foreach { c =>
      scan.addColumn(
        relation.catalog.shcTableCoder.toBytes(c.cf),
        relation.catalog.shcTableCoder.toBytes(c.col))
    }
    val size = SparkHBaseConf.defaultCachingSize
    scan.setCaching(size)
    filter.foreach(scan.setFilter(_))
    scan
  }

  def handleTimeSemantics(query: Query): Unit = {
    // Set timestamp related values if present
    (query, relation.timestamp, relation.minStamp, relation.maxStamp) match {
      case (q: Scan, Some(ts), None, None) => q.setTimeStamp(ts)
      case (q: Get, Some(ts), None, None) => q.setTimeStamp(ts)

      case (q: Scan, None, Some(minStamp), Some(maxStamp)) => q.setTimeRange(minStamp, maxStamp)
      case (q: Get, None, Some(minStamp), Some(maxStamp)) => q.setTimeRange(minStamp, maxStamp)

      case (q, None, None, None) =>
      case _ =>
        throw new IllegalArgumentException(
          "Invalid combination of query/timestamp/time range provided")
    }
    if (relation.maxVersions.isDefined) {
      query match {
        case q: Scan => q.setMaxVersions(relation.maxVersions.get)
        case q: Get => q.setMaxVersions(relation.maxVersions.get)
        case _ => throw new IllegalArgumentException("Invalid query provided with maxVersions")
      }
    }
  }

  private def toRowIterator(it: Iterator[Result]): Iterator[Seq[Any]] = {

    val iterator = new Iterator[Seq[Any]] {
      val start = System.currentTimeMillis()
      var rowCount: Int = 0
      val fieldWithIndex = relation.getIndexedProjections(requiredColumns)
      val indexedFields = fieldWithIndex.map(t => (t._1, SHCDataTypeFactory.create(t._1)))

      override def hasNext: Boolean = {
        if (it.hasNext) {
          true
        } else {
          val end = System.currentTimeMillis()
          false
        }
      }

      override def next(): Seq[Any] = {
        rowCount += 1
        val r = it.next()
        buildRow(indexedFields, r)
      }
    }
    iterator
  }

  private def toResultIterator(result: CustomedGetResource): Iterator[Result] = {
    val iterator = new Iterator[Result] {
      var idx = 0
      var cur: Option[Result] = None
      override def hasNext: Boolean = {
        while (idx < result.length && cur.isEmpty) {
          val tmp = result(idx)
          idx += 1
          if (!tmp.isEmpty) {
            cur = Some(tmp)
          }
        }
        if (cur.isEmpty) {
          rddResources.release(result)
        }
        cur.isDefined
      }
      override def next(): Result = {
        hasNext
        val ret = cur.get
        cur = None
        ret
      }
    }
    iterator
  }

  private def toResultIterator(scanner: CustomedScanResource): Iterator[Result] = {
    val iterator = new Iterator[Result] {
      var cur: Option[Result] = None
      override def hasNext: Boolean = {
        if (cur.isEmpty) {
          val r = scanner.next()
          if (r == null) {
            rddResources.release(scanner)
          } else {
            cur = Some(r)
          }
        }
        cur.isDefined
      }
      override def next(): Result = {
        hasNext
        val ret = cur.get
        cur = None
        ret
      }
    }
    iterator
  }

  private def buildGets(
      tbr: CustomedTableResource,
      g: Array[ScanRange[Array[Byte]]],
      columns: Seq[Field],
      filter: Option[HFilter]): Iterator[Result] = {
    val size = SparkHBaseConf.defaultBulkGetSize
    g.grouped(size).flatMap { x =>
      val gets = new ArrayList[Get]()
      x.foreach { y =>
        val g = new Get(y.start.get.point)
        handleTimeSemantics(g)
        columns.foreach { c =>
          g.addColumn(
            relation.catalog.shcTableCoder.toBytes(c.cf),
            relation.catalog.shcTableCoder.toBytes(c.col))
        }
        filter.foreach(g.setFilter(_))
        gets.add(g)
      }
      val tmp = tbr.get(gets)
      rddResources.addResource(tmp)
      toResultIterator(tmp)
    }
  }
  val rddResources: RDDResources

  def close() {
    rddResources.release()
  }

  def compute(split: Partition): Iterator[Row] = {
    computeForCommand(split).map(Row.fromSeq(_))
  }

  def computeForCommand(split: Partition): Iterator[Seq[Any]] = {
    SHCCredentialsManager.processShcToken(serializedToken)
    val ord = hbase.ord // implicitly[Ordering[HBaseType]]
    val partition = split.asInstanceOf[CustomedHBaseScanPartition]
    val (g, s) = partition.scanRanges.partition { x =>
      x.start.isDefined && x.end.isDefined && ScanRange.compare(x.start, x.end, ord) == 0
    }
    val tableResource = CustomedTableResource(relation)
    val typedFilter = CustomedTypedFilter.fromSerializedTypedFilter(partition.tf)
    // INLINE: accelerate key only select
    val selectColumn = columnSet.toSeq
    val filter = if (selectColumn.isEmpty) {
      val t1 =
        CustomedTypedFilter.and(
          typedFilter,
          CustomedTypedFilter(Some(new KeyOnlyFilter()), CustomedFilterType.Atomic))
      val t2 = CustomedTypedFilter.and(
        t1,
        CustomedTypedFilter(Some(new FirstKeyOnlyFilter()), CustomedFilterType.Atomic))
      t2.filter
    } else {
      typedFilter.filter
    }
    val gIt: Iterator[Result] = {
      if (g.isEmpty) {
        Iterator.empty: Iterator[Result]
      } else {
        buildGets(tableResource, g, selectColumn, filter)
      }
    }

    val scans = s.map(x => buildScan(x.start, x.end, selectColumn, filter))

    val sIts = scans.par
      .map { scan =>
        val scanner = tableResource.getScanner(scan)
        rddResources.addResource(scanner)
        scanner
      }
      .map(toResultIterator(_))

    val rIt = sIts.fold(Iterator.empty: Iterator[Result]) {
      case (x, y) =>
        x ++ y
    } ++ gIt

    ShutdownHookManager.addShutdownHook { () =>
      HBaseConnectionCache.close()
    }
    toRowIterator(rIt)
  }
}

class HBaseTableScanUtil(
    val relation: HBaseRelationTrait,
    val requiredColumns: Array[String],
    val filters: Array[Filter],
    limit: Integer)
  extends HBaseTableScanTrait {
  val columnFields: Seq[Field] = relation.splitRowKeyColumns(requiredColumns)._2
  val serializedToken: Array[Byte] = relation.serializedToken
  lazy val rddResources = RDDResources(new mutable.HashSet[Resource]())

  override def buildScan(
      start: Option[Bound[HBaseType]],
      end: Option[Bound[HBaseType]],
      columns: Seq[Field],
      filter: Option[HFilter]): Scan = {
    val scan = {
      (start, end) match {
        case (Some(Bound(lb, lbinc)), Some(Bound(ub, ubinc))) =>
          new Scan().withStartRow(lb, lbinc).withStopRow(ub, ubinc)
        case (Some(Bound(lb, lbinc)), None) => new Scan().withStartRow(lb, lbinc)
        case (None, Some(Bound(ub, ubinc))) => new Scan().withStopRow(ub, ubinc)
        case _ => new Scan
      }
    }
    handleTimeSemantics(scan)
    scan.setLimit(limit)

    columns.foreach { c =>
      scan.addColumn(
        relation.catalog.shcTableCoder.toBytes(c.cf),
        relation.catalog.shcTableCoder.toBytes(c.col))
    }
    val size = SparkHBaseConf.defaultCachingSize
    scan.setCaching(size)
    filter.foreach(scan.setFilter(_))
    scan
  }
}

object HBaseTableScanUtil {

  def apply(
      relation: HBaseRelationTrait,
      requiredColumns: Array[String],
      filters: Array[Filter],
      limit: Integer): HBaseTableScanUtil = {
    new HBaseTableScanUtil(relation, requiredColumns, filters, limit)
  }

  implicit def ScanResToScan(sr: CustomedScanResource): ResultScanner = {
    sr.rs
  }

  implicit def GetResToResult(gr: CustomedGetResource): Array[Result] = {
    gr.rs
  }

  implicit def TableResToTable(tr: CustomedTableResource): Table = {
    tr.table
  }

  implicit def RegionResToRegions(rr: CustomedRegionResource): Seq[CustomedHBaseRegion] = {
    rr.regions
  }
}
