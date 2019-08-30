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
package com.mongodb.spark.sql.xsql

import com.mongodb.spark.sql.BsonValueToJson
import com.mongodb.spark.sql.MapFunctions
import com.mongodb.spark.sql.types.BsonCompatibility
import net.sf.json.JSONObject
import org.bson._

import org.apache.spark.SparkException
import org.apache.spark.sql.types._
import org.apache.spark.sql.xsql.util.Utils

object MongoUtils {

  def convertField(
      fieldName: String,
      propertiesObj: JSONObject,
      parentName: String): StructField = {
    val absoluteName = if (parentName != null) parentName + "." + fieldName else fieldName
    val mongoType = propertiesObj.get("type").asInstanceOf[String]
    val dataType = mongoType.toUpperCase() match {
      case "NULL" => NullType
      case "ARRAY" =>
        throw new SparkException(
          s"MongoDB field $fieldName is array" +
            " type must determine element data type!")
      case "BINARY" => BinaryDataType // May not suitable.
      case "BOOLEAN" | "BOOL" => BooleanType
      case "DATE_TIME" | "DATE" => TimestampType
      case "DOCUMENT" => StringType // May not suitable.
      case "DOUBLE" => DoubleType
      case "INTEGER" | "INT" => IntegerType
      case "LONG" => LongType
      case "STRING" => StringType
      case "OBJECT_ID" => BsonCompatibility.ObjectId.structType
      case "TIMESTAMP" => TimestampType // May not suitable.
      case "MIN_KEY" => MinKeyType // May not suitable.
      case "MAX_KEY" => MaxKeyType // May not suitable.
      case "JAVASCRIPT" => JavaScriptType // May not suitable.
      case "JAVASCRIPT_WITH_SCOPE" => JavaScriptWithScopeType // May not suitable.
      case "REGULAR_EXPRESSION" => RegexType
      case "UNDEFINED" => StringType // May not suitable.
      case "SYMBOL" => StringType // May not suitable.
      case "DB_POINTER" => StringType // May not suitable.
      case "DECIMAL128" => StringType // May not suitable.
      case "BYTE" | "SHORT" | "FLOAT" =>
        throw new SparkException(s"MongoDB not support $mongoType type!")
      case _ => Utils.getSparkSQLDataType(mongoType)
    }
    DataTypes.createStructField(fieldName, dataType, true)
  }

  def documentToRow(
      bsonDocument: BsonDocument,
      schema: StructType,
      requiredColumns: Array[String] = Array.empty[String]): Seq[Any] = {
    val row = MapFunctions.documentToRow(bsonDocument, schema, requiredColumns)
    row.toSeq
  }

  def toInt(bsonValue: BsonValue): Int = {
    bsonValue.getBsonType match {
      case BsonType.DECIMAL128 =>
        bsonValue.asDecimal128().decimal128Value().bigDecimalValue().intValue()
      case BsonType.INT32 => bsonValue.asInt32().intValue()
      case BsonType.INT64 => bsonValue.asInt64().intValue()
      case BsonType.DOUBLE => bsonValue.asDouble().intValue()
      case _ => throw new SparkException(s"Cannot cast ${bsonValue.getBsonType} into a Int")
    }
  }

  def toLong(bsonValue: BsonValue): Long = {
    bsonValue.getBsonType match {
      case BsonType.DECIMAL128 =>
        bsonValue.asDecimal128().decimal128Value().bigDecimalValue().longValue()
      case BsonType.INT32 => bsonValue.asInt32().longValue()
      case BsonType.INT64 => bsonValue.asInt64().longValue()
      case BsonType.DOUBLE => bsonValue.asDouble().longValue()
      case _ => throw new SparkException(s"Cannot cast ${bsonValue.getBsonType} into a Long")
    }
  }

  def toDouble(bsonValue: BsonValue): Double = {
    bsonValue.getBsonType match {
      case BsonType.DECIMAL128 =>
        bsonValue.asDecimal128().decimal128Value().bigDecimalValue().doubleValue()
      case BsonType.INT32 => bsonValue.asInt32().doubleValue()
      case BsonType.INT64 => bsonValue.asInt64().doubleValue()
      case BsonType.DOUBLE => bsonValue.asDouble().doubleValue()
      case _ => throw new SparkException(s"Cannot cast ${bsonValue.getBsonType} into a Double")
    }
  }

  def bsonValueToString(element: BsonValue): String = {
    element.getBsonType match {
      case BsonType.STRING => element.asString().getValue
      case BsonType.OBJECT_ID => element.asObjectId().getValue.toHexString
      case BsonType.INT64 => element.asInt64().getValue.toString
      case BsonType.INT32 => element.asInt32().getValue.toString
      case BsonType.DOUBLE => element.asDouble().getValue.toString
      case BsonType.NULL => null
      case _ => BsonValueToJson(element)
    }
  }

  def toDecimal(bsonValue: BsonValue): BigDecimal = {
    bsonValue.getBsonType match {
      case BsonType.DECIMAL128 => bsonValue.asDecimal128().decimal128Value().bigDecimalValue()
      case BsonType.INT32 => BigDecimal(bsonValue.asInt32().intValue())
      case BsonType.INT64 => BigDecimal(bsonValue.asInt64().longValue())
      case BsonType.DOUBLE => BigDecimal(bsonValue.asDouble().doubleValue())
      case _ =>
        throw new SparkException(s"Cannot cast ${bsonValue.getBsonType} into a BigDecimal")
    }
  }

  def castToStructType(element: BsonValue, elementType: StructType): Any = {
    (element.getBsonType, elementType) match {
      case (BsonType.BINARY, BsonCompatibility.Binary()) =>
        BsonCompatibility.Binary(element.asInstanceOf[BsonBinary], elementType)
      case (BsonType.DOCUMENT, _) =>
        documentToRow(element.asInstanceOf[BsonDocument], elementType)
      case (BsonType.DB_POINTER, BsonCompatibility.DbPointer()) =>
        BsonCompatibility.DbPointer(element.asInstanceOf[BsonDbPointer], elementType)
      case (BsonType.JAVASCRIPT, BsonCompatibility.JavaScript()) =>
        BsonCompatibility.JavaScript(element.asInstanceOf[BsonJavaScript], elementType)
      case (BsonType.JAVASCRIPT_WITH_SCOPE, BsonCompatibility.JavaScriptWithScope()) =>
        BsonCompatibility.JavaScriptWithScope(
          element.asInstanceOf[BsonJavaScriptWithScope],
          elementType)
      case (BsonType.MIN_KEY, BsonCompatibility.MinKey()) =>
        BsonCompatibility.MinKey(element.asInstanceOf[BsonMinKey], elementType)
      case (BsonType.MAX_KEY, BsonCompatibility.MaxKey()) =>
        BsonCompatibility.MaxKey(element.asInstanceOf[BsonMaxKey], elementType)
      case (BsonType.OBJECT_ID, BsonCompatibility.ObjectId()) =>
        BsonCompatibility.ObjectId(element.asInstanceOf[BsonObjectId], elementType)
      case (BsonType.REGULAR_EXPRESSION, BsonCompatibility.RegularExpression()) =>
        BsonCompatibility.RegularExpression(
          element.asInstanceOf[BsonRegularExpression],
          elementType)
      case (BsonType.SYMBOL, BsonCompatibility.Symbol()) =>
        BsonCompatibility.Symbol(element.asInstanceOf[BsonSymbol], elementType)
      case (BsonType.TIMESTAMP, BsonCompatibility.Timestamp()) =>
        BsonCompatibility.Timestamp(element.asInstanceOf[BsonTimestamp], elementType)
      case (BsonType.UNDEFINED, BsonCompatibility.Undefined()) =>
        BsonCompatibility.Undefined(element.asInstanceOf[BsonUndefined], elementType)
      case _ =>
        throw new SparkException(
          s"Cannot cast ${element.getBsonType} into a $elementType (value: $element)")
    }
  }
}
