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

package org.apache.spark.sql.xsql.catalyst.parser

import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.mysql.jdbc.MysqlDefs
import org.antlr.v4.runtime.tree.TerminalNode
import org.elasticsearch.hadoop.serialization.FieldType
import org.elasticsearch.hadoop.serialization.FieldType._

import org.apache.spark.sql.types._
import org.apache.spark.sql.xsql.catalyst.parser.SqlBaseParser.PrimitiveDataTypeContext

object ExternalTypeParser {
  val visitFuns = new ArrayBuffer[(String, List[TerminalNode], TerminalNode) => DataType]
  visitFuns += (visitElasticDataType)
  visitFuns += (visitMongoDataType)
  visitFuns += (visitMysqlDataType)
  visitFuns += (visitDruidType)

  def visit(ctx: PrimitiveDataTypeContext): DataType = {
    val dataType = ctx.identifier.getText.toLowerCase(Locale.ROOT)
    val params = ctx.INTEGER_VALUE().asScala.toList
    val unsigned = ctx.UNSIGNED()

    var result: DataType = null
    val itr = visitFuns.iterator
    while (itr.hasNext && result == null) {
      val fun = itr.next()
      result = fun(dataType, params, unsigned)
    }
    result
  }

  def visitElasticDataType(
      dataType: String,
      params: List[TerminalNode],
      unsigned: TerminalNode): DataType = {
    FieldType.parse(dataType) match {
      case NULL => NullType
      case BINARY => BinaryType
      case BOOLEAN => BooleanType
      case BYTE => ByteType
      case SHORT => ShortType
      case INTEGER => IntegerType
      case LONG => LongType
      case FLOAT => FloatType
      case DOUBLE => DoubleType
      case HALF_FLOAT => HalfFloatType
      case SCALED_FLOAT => ScaledFloatType
      // String type
      case STRING => StringType
      case TEXT => TextType
      case KEYWORD => KeyWordType
      case DATE => StringType
      case OBJECT => StructType(Array.empty[StructField])
      case NESTED => DataTypes.createArrayType(StructType(Array.empty[StructField]))
      case _ => null
    }
  }

  def visitMongoDataType(
      dataType: String,
      params: List[TerminalNode],
      unsigned: TerminalNode): DataType = {
    dataType.toUpperCase() match {
      case "NULL" => DataTypes.NullType
//      case "ARRAY"                 => getSchemaFromArray(bsonValue.asArray().asScala, readConfig)
      case "BINARY_DATA" => BinaryDataType
//      case BsonType.BOOLEAN               => DataTypes.BooleanType
      case "DATE_TIME" => DataTypes.TimestampType
//      case BsonType.DOCUMENT         => getSchemaFromDocument(bsonValue.asDocument(), readConfig)
//      case BsonType.DOUBLE                => DataTypes.DoubleType
//      case BsonType.INT32                 => DataTypes.IntegerType
//      case BsonType.INT64                 => DataTypes.LongType
//      case BsonType.STRING                => DataTypes.StringType
      case "OBJECT_ID" => ObjectIDType
      case "BSON_TIMESTAMP" => Bson_TimestampType
      case "MIN_KEY" => MinKeyType
      case "MAX_KEY" => MaxKeyType
      case "JAVASCRIPT" => JavaScriptType
      case "JAVASCRIPT_WITH_SCOPE" => JavaScriptWithScopeType
      case "REGULAR_EXPRESSION" => RegexType
      case _ => null
//      case "UNDEFINED"             => BsonCompatibility.Undefined.structType
//      case "SYMBOL"                => BsonCompatibility.Symbol.structType
//      case "DB_POINTER"            => BsonCompatibility.DbPointer.structType
//      case "DECIMAL128"            => Decimal128Type
      case _ => null
    }
  }
  def visitMysqlDataType(
      dt: String,
      params: List[TerminalNode],
      unsigned: TerminalNode): DataType = {
    val mysqlDef = new MysqlDefs()
    val method = classOf[MysqlDefs].getDeclaredMethod("mysqlToJavaType", classOf[String])
    method.setAccessible(true)
    getCatalystType(method.invoke(mysqlDef, dt).asInstanceOf[Integer], params, unsigned)
  }

  /**
   * visit druid type
   */
  def visitDruidType(
      dataType: String,
      params: List[TerminalNode],
      unsigned: TerminalNode): DataType = {
    dataType.toLowerCase match {
      case "hyperunique" => HyperUniqueType
      case "cardinality" => CardinalityType
      case "thetasketch" => ThetaSketchType
      case "count" => LongType
      case "longsum" => LongType
      case "doublesum" => DoubleType
      case "floatsum" => FloatType
      case "longmin" => LongType
      case "longmax" => LongType
      case "doublemin" => DoubleType
      case "doublemax" => DoubleType
      case "floatmin" => FloatType
      case "floatmax" => FloatType
      case _ => null
    }
  }

  /**
   * Author: weiwenda Date: 2018-07-13 17:30
   * Description: the replica of JdbcDtils
   */
  private def getCatalystType(
      sqlType: Int,
      params: List[TerminalNode],
      unsigned: TerminalNode): DataType = {
    val (precision, scale) = params.size match {
      case 1 =>
        (params.head.getText.toInt, 0)
      case 2 =>
        (params.head.getText.toInt, params.last.getText.toInt)
      case _ =>
        (0, 0)
    }
    val answer = sqlType match {
      // scalastyle:off
      case java.sql.Types.ARRAY => null
      case java.sql.Types.BIGINT => if (unsigned == null) { LongType } else { DecimalType(20, 0) }
      case java.sql.Types.BINARY => BinaryType
      case java.sql.Types.BIT => BooleanType // @see JdbcDialect for quirks
      case java.sql.Types.BLOB => BinaryType
      case java.sql.Types.BOOLEAN => BooleanType
      case java.sql.Types.CHAR => StringType
      case java.sql.Types.CLOB => StringType
      case java.sql.Types.DATALINK => null
      case java.sql.Types.DATE => DateType
      case java.sql.Types.DECIMAL if precision != 0 || scale != 0 =>
        DecimalType.bounded(precision, scale)
      case java.sql.Types.DECIMAL => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.DISTINCT => null
      case java.sql.Types.DOUBLE => DoubleType
      case java.sql.Types.FLOAT => FloatType
      case java.sql.Types.INTEGER => if (unsigned == null) { IntegerType } else { LongType }
      case java.sql.Types.JAVA_OBJECT => null
      case java.sql.Types.LONGNVARCHAR => StringType
      case java.sql.Types.LONGVARBINARY => BinaryType
      case java.sql.Types.LONGVARCHAR => StringType
      case java.sql.Types.NCHAR => StringType
      case java.sql.Types.NCLOB => StringType
      case java.sql.Types.NULL => null
      case java.sql.Types.NUMERIC if precision != 0 || scale != 0 =>
        DecimalType.bounded(precision, scale)
      case java.sql.Types.NUMERIC => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.NVARCHAR => StringType
      case java.sql.Types.OTHER => null
      case java.sql.Types.REAL => DoubleType
      case java.sql.Types.REF => StringType
      case java.sql.Types.REF_CURSOR => null
      case java.sql.Types.ROWID => LongType
      case java.sql.Types.SMALLINT => IntegerType
      case java.sql.Types.SQLXML => StringType
      case java.sql.Types.STRUCT => StringType
      case java.sql.Types.TIME => TimestampType
      case java.sql.Types.TIME_WITH_TIMEZONE => null
      case java.sql.Types.TIMESTAMP => TimestampType
      case java.sql.Types.TIMESTAMP_WITH_TIMEZONE => null
      case java.sql.Types.TINYINT => ByteType
      case java.sql.Types.VARBINARY => BinaryType
      case java.sql.Types.VARCHAR => StringType
      case _ => null
      // scalastyle:on
    }
    answer
  }
}
