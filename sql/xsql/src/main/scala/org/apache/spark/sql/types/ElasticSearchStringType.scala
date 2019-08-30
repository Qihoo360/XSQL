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
package org.apache.spark.sql.types

import scala.math.Ordering
import scala.reflect.runtime.universe.typeTag

import org.apache.spark.unsafe.types.UTF8String

/**
 * A Elastic Search string type for compatibility. These datatypes should only used for parsing,
 * and should NOT be used anywhere else. Any instance of these data types should be
 * replaced by a [[StringType]] before analysis.
 */
sealed abstract class ElasticSearchStringType extends AtomicType {
  private[sql] type InternalType = UTF8String

  private[sql] val ordering = implicitly[Ordering[InternalType]]

  @transient private[sql] lazy val tag = typeTag[InternalType]

  /**
   * ElasticSearchStringType is not have The default size.
   */
  override def defaultSize: Int = -1

  private[spark] override def asNullable: ElasticSearchStringType = this

}

object ElasticSearchStringType {
  def replaceCharType(dt: DataType): DataType = dt match {
    case ArrayType(et, nullable) =>
      ArrayType(replaceCharType(et), nullable)
    case MapType(kt, vt, nullable) =>
      MapType(replaceCharType(kt), replaceCharType(vt), nullable)
    case StructType(fields) =>
      StructType(fields.map { field =>
        field.copy(dataType = replaceCharType(field.dataType))
      })
    case _: ElasticSearchStringType => StringType
    case _: DruidComplexType => StringType
    case _ => dt
  }
}

/**
 * Elastic Search text type.
 */
case object TextType extends ElasticSearchStringType {
  override def simpleString: String = s"text"
}

/**
 * Elastic Search keyword type.
 */
case object KeyWordType extends ElasticSearchStringType {
  override def simpleString: String = s"keyword"
}

/**
 * Elastic Search date type with es.mapping.date.rich = false.
 */
case object ElasticDateType extends ElasticSearchStringType {
  override def simpleString: String = s"date"
}

/**
 * Elastic Search half_float type.
 */
case object HalfFloatType extends ElasticSearchStringType {
  override def simpleString: String = s"half_float"
}

/**
 * Elastic Search scaled_float type.
 */
case object ScaledFloatType extends ElasticSearchStringType {
  override def simpleString: String = s"scaled_float"
}
