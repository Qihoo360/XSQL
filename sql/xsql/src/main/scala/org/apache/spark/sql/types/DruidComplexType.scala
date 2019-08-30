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

sealed abstract class DruidComplexType extends AtomicType {
  private[sql] type InternalType = UTF8String

  private[sql] val ordering = implicitly[Ordering[InternalType]]

  @transient private[sql] lazy val tag = typeTag[InternalType]

  /**
   * ElasticSearchStringType is not have The default size.
   */
  override def defaultSize: Int = -1

  private[spark] override def asNullable: DruidComplexType = this

}

object DruidComplexType {}

/**
 * Druid HyperUnique type.
 */
case object HyperUniqueType extends DruidComplexType {
  override def simpleString: String = s"hyperunique"
}

/**
 * Druid Cardinality type.
 */
case object CardinalityType extends DruidComplexType {
  override def simpleString: String = s"cardinality"
}

/**
 * Druid thetaSketch type.
 */
case object ThetaSketchType extends DruidComplexType {
  override def simpleString: String = s"thetasketch"
}

/**
 * Druid variance type.
 */
case object VarianceType extends DruidComplexType {
  override def simpleString: String = s"variance"
}
