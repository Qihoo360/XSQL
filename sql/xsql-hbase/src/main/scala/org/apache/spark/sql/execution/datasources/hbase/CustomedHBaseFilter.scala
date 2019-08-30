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
 *
 * File modified by Hortonworks, Inc. Modifications are also licensed under
 * the Apache Software License, Version 2.0.
 */

package org.apache.spark.sql.execution.datasources.hbase

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.math.Ordering

import org.apache.hadoop.hbase.CompareOperator
import org.apache.hadoop.hbase.filter.{Filter => HFilter, FilterList => HFilterList, _}
import org.apache.hadoop.hbase.util.Bytes

import org.apache.spark.sql.execution.datasources.hbase
import org.apache.spark.sql.execution.datasources.hbase.CustomedFilterType.CustomedFilterType
import org.apache.spark.sql.execution.datasources.hbase.types.SHCDataTypeFactory
import org.apache.spark.sql.sources._

object CustomedFilterType extends Enumeration {
  type CustomedFilterType = Value
  val And, Or, Atomic, Prefix, Row, Und = Value
  def getOperator(hType: CustomedFilterType): HFilterList.Operator = hType match {
    case And => HFilterList.Operator.MUST_PASS_ALL
    case Or => HFilterList.Operator.MUST_PASS_ONE
  }
  def parseFrom(fType: CustomedFilterType): Array[Byte] => HFilter = fType match {
    case And | Or =>
      x: Array[Byte] =>
        HFilterList.parseFrom(x).asInstanceOf[HFilter]
    case Row =>
      x: Array[Byte] =>
        RowFilter.parseFrom(x).asInstanceOf[HFilter]
    case Atomic =>
      x: Array[Byte] =>
        SingleColumnValueFilter.parseFrom(x).asInstanceOf[HFilter]
    case Prefix =>
      x: Array[Byte] =>
        PrefixFilter.parseFrom(x).asInstanceOf[HFilter]
    case Und => throw new Exception("unknown type")
  }
}

case class CustomedTypedFilter(filter: Option[HFilter], hType: CustomedFilterType)

case class SerializedTypedFilter(b: Option[Array[Byte]], hType: CustomedFilterType)

object CustomedTypedFilter {
  def toSerializedTypedFilter(tf: CustomedTypedFilter): SerializedTypedFilter = {
    val b = tf.filter.map(_.toByteArray)
    SerializedTypedFilter(b, tf.hType)
  }

  def fromSerializedTypedFilter(tf: SerializedTypedFilter): CustomedTypedFilter = {
    val filter = tf.b.map(x => CustomedFilterType.parseFrom(tf.hType)(x))
    CustomedTypedFilter(filter, tf.hType)
  }

  def empty: CustomedTypedFilter = CustomedTypedFilter(None, CustomedFilterType.Und)

  private def getOne(left: CustomedTypedFilter, right: CustomedTypedFilter) = {
    left.filter.fold(right)(x => left)
  }

  private def ops(
      left: CustomedTypedFilter,
      right: CustomedTypedFilter,
      hType: CustomedFilterType) = {
    if (left.hType == hType) {
      val l = left.filter.get.asInstanceOf[HFilterList]
      if (right.hType == hType) {
        val r = right.filter.get.asInstanceOf[HFilterList].getFilters
        r.asScala.foreach(l.addFilter(_))
      } else {
        l.addFilter(right.filter.get)
      }
      left
    } else if (right.hType == hType) {
      val r = right.filter.get.asInstanceOf[HFilterList]
      r.addFilter(left.filter.get)
      right
    } else {
      val nf = new HFilterList(CustomedFilterType.getOperator(hType))
      nf.addFilter(left.filter.get)
      nf.addFilter(right.filter.get)
      CustomedTypedFilter(Some(nf), hType)
    }
  }

  def and(left: CustomedTypedFilter, right: CustomedTypedFilter): CustomedTypedFilter = {
    if (left.filter.isEmpty) {
      right
    } else if (right.filter.isEmpty) {
      left
    } else {
      ops(left, right, CustomedFilterType.And)
    }
  }
  def or(left: CustomedTypedFilter, right: CustomedTypedFilter): CustomedTypedFilter = {
    if (left.filter.isEmpty || right.filter.isEmpty) {
      CustomedTypedFilter.empty
    } else {
      ops(left, right, CustomedFilterType.Or)
    }
  }
}
// Combination of HBase range and filters
case class CustomedHRF[T](
    ranges: Array[ScanRange[T]],
    tf: CustomedTypedFilter,
    handled: Boolean = false)

object CustomedHRF {
  def empty[T]: CustomedHRF[T] =
    CustomedHRF[T](Array(ScanRange.empty[T]), CustomedTypedFilter.empty)
}

object CustomedHBaseFilter extends Logging {
  implicit val order: Ordering[Array[Byte]] = hbase.ord
  var colSet: mutable.HashSet[Field] = _
  def buildFilters(
      filters: Array[Filter],
      relation: HBaseRelationTrait,
      cs: mutable.HashSet[Field]): CustomedHRF[Array[Byte]] = {
    colSet = cs
    if (log.isDebugEnabled) {
      logDebug(s"for all filters: ")
      filters.foreach(x => logDebug(x.toString))
    }
    val filter = filters.reduceOption[Filter](And(_, _))
    val ret = filter.map(buildFilter(_, relation)).getOrElse(CustomedHRF.empty[Array[Byte]])
    if (log.isDebugEnabled) {
      logDebug("ret:")
      ret.ranges.foreach(x => logDebug(x.toString))
    }
    ret
  }

  def process(
      value: Any,
      relation: HBaseRelationTrait,
      attribute: String,
      primary: BoundRanges => CustomedHRF[Array[Byte]],
      column: BoundRanges => CustomedHRF[Array[Byte]],
      composite: BoundRanges => CustomedHRF[Array[Byte]]): CustomedHRF[Array[Byte]] = {
    val b = BoundRange(value, relation.getField(attribute))
    val ret: Option[CustomedHRF[Array[Byte]]] = {
      if (relation.isPrimaryKey(attribute)) {
        b.map(primary(_))
      } else if (relation.isColumn(attribute)) {
        b.map(column(_))
      } else {
        // INLINE: rowkey but not the first
        Some(CustomedHRF.empty[Array[Byte]])
        // composite key does not work, need more work
        /*
        if (!relation.rows.varLength) {
          b.map(composite(_))
        } else {
          None
        } */
      }
    }
    ret.getOrElse(CustomedHRF.empty[Array[Byte]])
  }

  def buildFilter(filter: Filter, relation: HBaseRelationTrait): CustomedHRF[Array[Byte]] = {
    val tCoder = relation.catalog.shcTableCoder
    // We treat greater and greaterOrEqual as the same
    def Greater(attribute: String, value: Any, cor: CompareOperator): CustomedHRF[Array[Byte]] = {
      val include = if (cor.equals(CompareOperator.GREATER_OR_EQUAL)) true else false
      process(
        value,
        relation,
        attribute,
        bound => {
          if (relation.singleKey) {
            CustomedHRF(
              bound.greater
                .map(x => ScanRange(Some(Bound(x.low, include)), Some(Bound(x.upper, true)))),
              CustomedTypedFilter.empty)
          } else {
            val s = bound.greater.map(
              x =>
                ScanRange(
                  relation.rows.length,
                  x.low,
                  include,
                  x.upper,
                  true,
                  relation.getField(attribute).start))
            CustomedHRF(s, CustomedTypedFilter.empty)
          }
        },
        bound => {
          val f = relation.getField(attribute)
          colSet.add(f)
          val filter = bound.greater.map {
            x =>
              val lower = new SingleColumnValueFilter(
                tCoder.toBytes(f.cf),
                tCoder.toBytes(f.col),
                cor,
                x.low)
              val low = CustomedTypedFilter(Some(lower), CustomedFilterType.Atomic)
              val upper = new SingleColumnValueFilter(
                tCoder.toBytes(f.cf),
                tCoder.toBytes(f.col),
                CompareOperator.LESS_OR_EQUAL,
                x.upper)
              val up = CustomedTypedFilter(Some(upper), CustomedFilterType.Atomic)
              CustomedTypedFilter.and(low, up)
          }
          val of = filter.reduce[CustomedTypedFilter] {
            case (x, y) =>
              CustomedTypedFilter.or(x, y)
          }
          CustomedHRF(Array(ScanRange.empty[Array[Byte]]), of)
        },
        bound => {
          val s = bound.greater.map(
            x =>
              ScanRange(
                relation.rows.length,
                x.low,
                include,
                x.upper,
                true,
                relation.getField(attribute).start))
          CustomedHRF(s, CustomedTypedFilter.empty)
        })
    }
    // We treat less and lessOrEqual as the same
    def Less(attribute: String, value: Any, cor: CompareOperator): CustomedHRF[Array[Byte]] = {
      val include = if (cor.equals(CompareOperator.LESS_OR_EQUAL)) true else false
      process(
        value,
        relation,
        attribute,
        bound => {
          if (relation.singleKey) {
            CustomedHRF(
              bound.less
                .map(x => ScanRange(Some(Bound(x.low, true)), Some(Bound(x.upper, include)))),
              CustomedTypedFilter.empty)
          } else {
            val s = bound.less.map(
              x =>
                ScanRange(
                  relation.rows.length,
                  x.low,
                  true,
                  x.upper,
                  include,
                  relation.getField(attribute).start))
            CustomedHRF(s, CustomedTypedFilter.empty)
          }
        },
        bound => {
          val f = relation.getField(attribute)
          colSet.add(f)
          val filter = bound.less.map {
            x =>
              val lower = new SingleColumnValueFilter(
                tCoder.toBytes(f.cf),
                tCoder.toBytes(f.col),
                CompareOperator.GREATER_OR_EQUAL,
                x.low)
              val low = CustomedTypedFilter(Some(lower), CustomedFilterType.Atomic)
              val upper = new SingleColumnValueFilter(
                tCoder.toBytes(f.cf),
                tCoder.toBytes(f.col),
                cor,
                x.upper)
              val up = CustomedTypedFilter(Some(upper), CustomedFilterType.Atomic)
              CustomedTypedFilter.and(low, up)
          }
          val ob = filter.reduce[CustomedTypedFilter] {
            case (x, y) =>
              CustomedTypedFilter.or(x, y)
          }
          CustomedHRF(Array(ScanRange.empty[Array[Byte]]), ob)
        },
        bound => {
          val s = bound.less.map(
            x =>
              ScanRange(
                relation.rows.length,
                x.low,
                true,
                x.upper,
                include,
                relation.getField(attribute).start))
          CustomedHRF(s, CustomedTypedFilter.empty)
        })
    }

    def setDiff(
        inValues: Array[Any],
        notInValues: Array[Any],
        attrib: String): CustomedHRF[Array[Byte]] = {
      val diff = inValues.toSet diff notInValues.toSet
      buildFilter(In(attrib, diff.toArray), relation)
    }

    val f = filter match {
      case And(
          Not(In(notInAttrib: String, notInValues: Array[Any])),
          In(inAttrib: String, inValues: Array[Any])) if inAttrib == notInAttrib =>
        // this is set difference being performed
        setDiff(inValues, notInValues, inAttrib)

      case And(
          In(inAttrib: String, inValues: Array[Any]),
          Not(In(notInAttrib: String, notInValues: Array[Any]))) if inAttrib == notInAttrib =>
        // this is set difference being performed
        setDiff(inValues, notInValues, inAttrib)

      case And(left, right) =>
        and[Array[Byte]](buildFilter(left, relation), buildFilter(right, relation))
      case Or(left, right) =>
        or[Array[Byte]](buildFilter(left, relation), buildFilter(right, relation))
      case LessThan(attribute, value) =>
        Less(attribute, value, CompareOperator.LESS)
      case LessThanOrEqual(attribute, value) =>
        Less(attribute, value, CompareOperator.LESS_OR_EQUAL)
      case GreaterThan(attribute, value) =>
        Greater(attribute, value, CompareOperator.GREATER)
      case GreaterThanOrEqual(attribute, value) =>
        Greater(attribute, value, CompareOperator.GREATER_OR_EQUAL)
      case Not(And(left, right)) =>
        or[Array[Byte]](buildFilter(Not(left), relation), buildFilter(Not(right), relation))
      case Not(Or(left, right)) =>
        and[Array[Byte]](buildFilter(Not(left), relation), buildFilter(Not(right), relation))
      case Not(EqualTo(attribute, value)) =>
        if (relation.isPrimaryKey(attribute)) {
          or[Array[Byte]](
            Less(attribute, value, CompareOperator.LESS),
            Greater(attribute, value, CompareOperator.GREATER))
        } else if (relation.isColumn(attribute)) {
          and[Array[Byte]](
            not[Array[Byte]](buildFilter(EqualTo(attribute, value), relation)),
            buildFilter(IsNotNull(attribute), relation))
        } else {
          CustomedHRF.empty[Array[Byte]]
        }
      case Not(LessThan(attribute, value)) =>
        Greater(attribute, value, CompareOperator.GREATER_OR_EQUAL)
      case Not(LessThanOrEqual(attribute, value)) =>
        Greater(attribute, value, CompareOperator.GREATER)
      case Not(GreaterThan(attribute, value)) =>
        Less(attribute, value, CompareOperator.LESS_OR_EQUAL)
      case Not(GreaterThanOrEqual(attribute, value)) =>
        Less(attribute, value, CompareOperator.LESS)
      // We should also add Not(GreatThan, LessThan, ...)
      // because if we miss some filter, it may result in a large scan range.
      case Not(child @ StringContains(attribute: String, value: String))
          if relation.isColumn(attribute) =>
        and[Array[Byte]](
          not[Array[Byte]](buildFilter(child, relation)),
          buildFilter(IsNotNull(attribute), relation))
      case Not(In(attribute: String, values: Array[Any])) =>
        // converting a "not(key in (x1, x2, x3..)) filter to (key != x1) and (key != x2) and ..
        values
          .map { v =>
            buildFilter(Not(EqualTo(attribute, v)), relation)
          }
          .reduceOption[CustomedHRF[Array[Byte]]] {
            case (lhs, rhs) => and(lhs, rhs)
          }
          .getOrElse(CustomedHRF.empty[Array[Byte]])
      case EqualTo(attribute, value) =>
        process(
          value,
          relation,
          attribute,
          bound => {
            if (relation.singleKey) {
              CustomedHRF(
                Array(ScanRange(Some(Bound(bound.value, true)), Some(Bound(bound.value, true)))),
                CustomedTypedFilter.empty,
                true)
            } else {
              val s = ScanRange(
                relation.rows.length,
                bound.value,
                true,
                bound.value,
                true,
                relation.getField(attribute).start)
              CustomedHRF(Array(s), CustomedTypedFilter.empty)
            }
          },
          bound => {
            val f = relation.getField(attribute)
            colSet.add(f)
            val filter = new SingleColumnValueFilter(
              tCoder.toBytes(f.cf),
              tCoder.toBytes(f.col),
              CompareOperator.EQUAL,
              bound.value)
            CustomedHRF(
              Array(ScanRange.empty[Array[Byte]]),
              CustomedTypedFilter(Some(filter), CustomedFilterType.Atomic),
              true)
          },
          bound => {
            val s = ScanRange(
              relation.rows.length,
              bound.value,
              true,
              bound.value,
              true,
              relation.getField(attribute).start)
            CustomedHRF(Array(s), CustomedTypedFilter.empty)
          })
      case StringStartsWith(attribute, value) =>
        val b = SHCDataTypeFactory.create(relation.getField(attribute).fCoder).toBytes(value)
        if (relation.isPrimaryKey(attribute)) {
          val prefixFilter = new PrefixFilter(b)
          CustomedHRF[Array[Byte]](
            Array(ScanRange.empty[Array[Byte]]),
            CustomedTypedFilter(Some(prefixFilter), CustomedFilterType.Prefix))
        } else if (relation.isColumn(attribute)) {
          val f = relation.getField(attribute)
          colSet.add(f)
          val filter = new SingleColumnValueFilter(
            tCoder.toBytes(f.cf),
            tCoder.toBytes(f.col),
            CompareOperator.EQUAL,
            new BinaryPrefixComparator(b))
          CustomedHRF[Array[Byte]](
            Array(ScanRange.empty[Array[Byte]]),
            CustomedTypedFilter(Some(filter), CustomedFilterType.Atomic),
            handled = true)
        } else {
          CustomedHRF.empty[Array[Byte]]
        }
      case StringEndsWith(attribute, value) =>
        if (relation.isPrimaryKey(attribute)) {
          val rowFilter =
            new RowFilter(CompareOperator.EQUAL, new RegexStringComparator(s".*$value" + "$"))
          CustomedHRF[Array[Byte]](
            Array(ScanRange.empty[Array[Byte]]),
            CustomedTypedFilter(Some(rowFilter), CustomedFilterType.Row))
        } else if (relation.isColumn(attribute)) {
          val f = relation.getField(attribute)
          colSet.add(f)
          val filter = new SingleColumnValueFilter(
            tCoder.toBytes(f.cf),
            tCoder.toBytes(f.col),
            CompareOperator.EQUAL,
            new RegexStringComparator(s".*$value" + "$"))
          CustomedHRF[Array[Byte]](
            Array(ScanRange.empty[Array[Byte]]),
            CustomedTypedFilter(Some(filter), CustomedFilterType.Atomic),
            handled = true)
        } else {
          CustomedHRF.empty[Array[Byte]]
        }
      case StringContains(attribute: String, value: String) =>
        if (relation.isPrimaryKey(attribute)) {
          val rowFilter =
            new RowFilter(CompareOperator.EQUAL, new SubstringComparator(value))
          CustomedHRF[Array[Byte]](
            Array(ScanRange.empty[Array[Byte]]),
            CustomedTypedFilter(Some(rowFilter), CustomedFilterType.Row))
        } else if (relation.isColumn(attribute)) {
          val f = relation.getField(attribute)
          colSet.add(f)
          val filter = new SingleColumnValueFilter(
            tCoder.toBytes(f.cf),
            tCoder.toBytes(f.col),
            CompareOperator.EQUAL,
            new SubstringComparator(value))
          CustomedHRF[Array[Byte]](
            Array(ScanRange.empty[Array[Byte]]),
            CustomedTypedFilter(Some(filter), CustomedFilterType.Atomic),
            handled = true)
        } else {
          CustomedHRF.empty[Array[Byte]]
        }
      case In(attribute: String, values: Array[Any]) =>
        // converting a "key in (x1, x2, x3..) filter to (key == x1) or (key == x2) or ...
        values
          .map { v =>
            buildFilter(EqualTo(attribute, v), relation)
          }
          .reduceOption[CustomedHRF[Array[Byte]]] {
            case (lhs, rhs) => or(lhs, rhs)
          }
          .getOrElse(CustomedHRF.empty[Array[Byte]])
      case IsNull(attribute: String) if relation.isColumn(attribute) =>
        val f = relation.getField(attribute)
        colSet.add(f)
        val filter = new SingleColumnValueFilter(
          tCoder.toBytes(f.cf),
          tCoder.toBytes(f.col),
          CompareOperator.EQUAL,
          new BinaryComparator(Bytes.toBytes("")))
        CustomedHRF[Array[Byte]](
          Array(ScanRange.empty[Array[Byte]]),
          CustomedTypedFilter(Some(filter), CustomedFilterType.Atomic),
          handled = true)
      case IsNotNull(attribute: String) if relation.isColumn(attribute) =>
        val f = relation.getField(attribute)
        colSet.add(f)
        val filter = new SingleColumnValueFilter(
          tCoder.toBytes(f.cf),
          tCoder.toBytes(f.col),
          CompareOperator.NOT_EQUAL,
          new BinaryComparator(Bytes.toBytes("")))
        filter.setFilterIfMissing(true)
        CustomedHRF[Array[Byte]](
          Array(ScanRange.empty[Array[Byte]]),
          CustomedTypedFilter(Some(filter), CustomedFilterType.Atomic),
          handled = true)
      case _ => CustomedHRF.empty[Array[Byte]]
    }
    logDebug(s"""start filter $filter:  ${f.ranges.map(_.toString).mkString(" ")}""")
    f
  }

  def and[T](left: CustomedHRF[T], right: CustomedHRF[T])(
      implicit ordering: Ordering[T]): CustomedHRF[T] = {
    // (0, 5), (10, 15) and with (2, 3) (8, 12) = (2, 3), (10, 12)
    val ranges = ScanRange.and(left.ranges, right.ranges)
    val typeFilter = CustomedTypedFilter.and(left.tf, right.tf)
    CustomedHRF(ranges, typeFilter, left.handled && right.handled)
  }

  def or[T](left: CustomedHRF[T], right: CustomedHRF[T])(
      implicit ordering: Ordering[T]): CustomedHRF[T] = {
    val ranges = ScanRange.or(left.ranges, right.ranges)
    val typeFilter = CustomedTypedFilter.or(left.tf, right.tf)
    CustomedHRF(ranges, typeFilter, left.handled && right.handled)
  }
  def not[T](left: CustomedHRF[T])(implicit ordering: Ordering[T]): CustomedHRF[T] = {
    assert(left.tf.hType.equals(CustomedFilterType.Atomic))
    assert(left.tf.filter.get.isInstanceOf[SingleColumnValueFilter])
    val oldFilter = left.tf.filter.get.asInstanceOf[SingleColumnValueFilter]
    val newFilter = new SingleColumnValueFilter(
      oldFilter.getFamily,
      oldFilter.getQualifier,
      CompareOperator.NOT_EQUAL,
      oldFilter.getComparator)
    left.copy(tf = left.tf.copy(filter = Some(newFilter)))
  }
}
