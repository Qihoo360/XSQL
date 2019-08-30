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

sealed abstract class DruidType extends AtomicType {
  private[sql] type InternalType = UTF8String

  private[sql] val ordering = implicitly[Ordering[InternalType]]

  @transient private[sql] lazy val tag = typeTag[InternalType]

  /**
   * ElasticSearchStringType is not have The default size.
   */
  override def defaultSize: Int = -1

  private[spark] override def asNullable: DruidType = this

}

object DruidType {}

/**
 * Druid count type.
 */
case object CountType extends DruidType {
  override def simpleString: String = s"count"
}

/**
 * Druid longsum type.
 */
case object LongSumType extends DruidType {
  override def simpleString: String = s"longsum"
}

/**
 * Druid longsum type.
 */
case object DoubleSumType extends DruidType {
  override def simpleString: String = s"doublesum"
}

/**
 * Druid floatsum type.
 */
case object FloatSumType extends DruidType {
  override def simpleString: String = s"floatsum"
}

/**
 * Druid doublemin type.
 */
case object DoubleMinType extends DruidType {
  override def simpleString: String = s"doublemin"
}

/**
 * Druid doublemax type.
 */
case object DoubleMaxType extends DruidType {
  override def simpleString: String = s"doublemax"
}

/**
 * Druid floatmin type.
 */
case object FloatMinType extends DruidType {
  override def simpleString: String = s"floatmin"
}

/**
 * Druid floatmax type.
 */
case object FloatMaxType extends DruidType {
  override def simpleString: String = s"floatmax"
}

/**
 * Druid longmin type.
 */
case object LongMinType extends DruidType {
  override def simpleString: String = s"longmin"
}

/**
 * Druid longmax type.
 */
case object LongMaxType extends DruidType {
  override def simpleString: String = s"longmax"
}
