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
 * A MongoDB string type for compatibility. These datatypes should only used for parsing,
 * and should NOT be used anywhere else. Any instance of these data types should be
 * replaced by a [[StringType]] before analysis.
 */
sealed abstract class MongoStringType extends AtomicType {
  private[sql] type InternalType = UTF8String

  private[sql] val ordering = implicitly[Ordering[InternalType]]

  @transient private[sql] lazy val tag = typeTag[InternalType]

  /**
   * ElasticSearchStringType is not have The default size.
   */
  override def defaultSize: Int = -1

  private[spark] override def asNullable: MongoStringType = this

}

object MongoStringType {
  def replaceCharType(dt: DataType): DataType = dt match {
    case ArrayType(et, nullable) =>
      ArrayType(replaceCharType(et), nullable)
    case MapType(kt, vt, nullable) =>
      MapType(replaceCharType(kt), replaceCharType(vt), nullable)
    case StructType(fields) =>
      StructType(fields.map { field =>
        field.copy(dataType = replaceCharType(field.dataType))
      })
    case _: MongoStringType => StringType
    case _ => dt
  }
}

/**
 * MongoDB Binary data type.
 */
case object BinaryDataType extends MongoStringType {
  override def simpleString: String = s"binData"
}

/**
 * MongoDB OBJECT_ID type.
 */
case object ObjectIDType extends MongoStringType {
  override def simpleString: String = s"OBJECT_ID"
}

/**
 * MongoDB BSON timestamp type.
 */
case object Bson_TimestampType extends MongoStringType {
  override def simpleString: String = s"BSON_TIMESTAMP"
}

/**
 * MongoDB Min key type.
 */
case object MinKeyType extends MongoStringType {
  override def simpleString: String = s"minKey"
}

/**
 * MongoDB Max key type.
 */
case object MaxKeyType extends MongoStringType {
  override def simpleString: String = s"maxKey"
}

/**
 * MongoDB JavaScript type.
 */
case object JavaScriptType extends MongoStringType {
  override def simpleString: String = s"javascript"
}

/**
 * MongoDB JavaScript (with scope) type.
 */
case object JavaScriptWithScopeType extends MongoStringType {
  override def simpleString: String = s"javascriptWithScope"
}

/**
 * MongoDB Regular Expression type.
 */
case object RegexType extends MongoStringType {
  override def simpleString: String = s"regex"
}

/**
 * MongoDB Decimal128 type.
 */
case object Decimal128Type extends MongoStringType {
  override def simpleString: String = s"decimal"
}
