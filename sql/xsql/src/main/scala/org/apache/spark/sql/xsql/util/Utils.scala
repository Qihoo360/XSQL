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

package org.apache.spark.sql.xsql.util

import java.io.{File, IOException}
import java.sql.Date
import java.text.ParseException

import scala.collection.Map
import scala.collection.mutable.{HashMap, HashSet}
import scala.reflect.ClassTag

import net.sf.json.{JSONArray, JSONObject}
import org.apache.commons.io.FileUtils

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.types.DataType

/**
 * Various utility methods used by XSQL.
 */
object Utils {
  val TABLE = "table"
  val FIELDS = "fields"
  val INCLUDE = "include"
  val INCLUDES = "includes"
  val EXCLUDES = "excludes"
  lazy val df = DateTimeUtils.newDateFormat("yyyy-MM-dd HH:mm:ss", DateTimeUtils.TimeZoneGMT)

  /** Extract host and port from URL. */
  @throws(classOf[SparkException])
  def extractHostPortFromUrl(httpUrl: String, protocol: String): (String, Int) = {
    try {
      val uri = new java.net.URI(httpUrl)
      val host = uri.getHost
      val port = uri.getPort
      if (uri.getScheme != protocol ||
          host == null ||
          port < 0 ||
          (uri.getPath != null && !uri.getPath.isEmpty) || // uri.getPath returns "" instead of null
          uri.getFragment != null ||
          uri.getQuery != null ||
          uri.getUserInfo != null) {
        throw new SparkException("Invalid URL: " + httpUrl)
      }
      (host, port)
    } catch {
      case e: java.net.URISyntaxException =>
        throw new SparkException("Invalid URL: " + httpUrl, e)
    }
  }

  /** Load properties present in the given file. */
  def getSettingsFromFile[T: ClassTag](
      filename: String,
      map: HashMap[String, HashMap[String, T]],
      reader: (String, HashMap[String, HashMap[String, T]]) => Unit): Unit = {
    val file = new File(filename)
    require(file.exists(), s"File $file does not exist")
    require(file.isFile(), s"File $file is not a normal file")

    var inputJson: String = null
    try {
      inputJson = FileUtils.readFileToString(file, "UTF-8")
    } catch {
      case e: IOException =>
        throw new SparkException(s"Failed when loading Spark properties from $filename", e)
    }
    reader(inputJson, map)
  }

  /** Load schemas present in the given JSON string. */
  def getSchemasFromStr(
      json: String,
      schemasMap: HashMap[String, HashMap[String, JSONArray]]): Unit = {
    val jsonObj = JSONObject.fromObject(json)
    if (jsonObj != null) {
      val keys = jsonObj.keys()
      while (keys.hasNext()) {
        val db = keys.next().asInstanceOf[String]
        val tables = schemasMap.getOrElseUpdate(db, new HashMap[String, JSONArray])
        val schemas = jsonObj.getJSONArray(db)
        val itr = schemas.iterator()
        while (itr.hasNext()) {
          val tableObj = itr.next().asInstanceOf[JSONObject]
          val name = tableObj.getString(TABLE)
          val fields = tableObj.getJSONArray(FIELDS)
          tables.put(name, fields)
        }
      }
    }
  }

  /** Load partitions information present in the given JSON string */
  def getPartitonsFromStr(
      json: String,
      partitionsMap: HashMap[String, HashMap[String, HashMap[String, String]]]): Unit = {
    val jsonObj = JSONObject.fromObject(json)
    if (jsonObj != null) {
      val keys = jsonObj.keys()
      while (keys.hasNext) {
        val db = keys.next().asInstanceOf[String]
        val tables =
          partitionsMap.getOrElseUpdate(db, new HashMap[String, HashMap[String, String]]())
        val jsonArray = jsonObj.getJSONArray(db)
        val itr = jsonArray.iterator()
        while (itr.hasNext) {
          val parameters = new HashMap[String, String]()
          val tableObj = itr.next().asInstanceOf[JSONObject]
          val name = tableObj.getString(TABLE)
          val partitionColumn = tableObj.getString(JDBCOptions.JDBC_PARTITION_COLUMN)
          val lowerBound = tableObj.getString(JDBCOptions.JDBC_LOWER_BOUND)
          val upperBound = tableObj.getString(JDBCOptions.JDBC_UPPER_BOUND)
          val numPartitions = tableObj.getString(JDBCOptions.JDBC_NUM_PARTITIONS)
          parameters.put(JDBCOptions.JDBC_PARTITION_COLUMN, partitionColumn)
          parameters.put(JDBCOptions.JDBC_LOWER_BOUND, lowerBound)
          parameters.put(JDBCOptions.JDBC_UPPER_BOUND, upperBound)
          parameters.put(JDBCOptions.JDBC_NUM_PARTITIONS, numPartitions)
          tables.put(name, parameters)
        }
      }
    }
  }

  /** Load white list present in the given JSON string. */
  def getWhitelistFromStr(
      json: String,
      whitelist: HashMap[String, HashMap[String, Boolean]]): Unit = {
    val jsonObj = JSONObject.fromObject(json)
    if (jsonObj != null) {
      val keys = jsonObj.keys()
      while (keys.hasNext()) {
        val db = keys.next().asInstanceOf[String]
        val tables = whitelist.getOrElseUpdate(db, new HashMap[String, Boolean])
        val jsonArray = jsonObj.getJSONArray(db)
        val itr = jsonArray.iterator()
        while (itr.hasNext()) {
          val tableObj = itr.next()
          tableObj match {
            case j: JSONObject =>
              val name = j.getString(TABLE)
              val include = j.getBoolean(INCLUDE)
              tables.put(name, include)
            case s: String =>
              tables.put(s, true)
          }
        }
      }
    }
  }

  /** Load whitelist present in the given file. */
  def getWhitelistFromFile[T: ClassTag](
      filename: String,
      whitelist: HashMap[String, HashSet[String]],
      blacklist: HashMap[String, HashSet[String]],
      reader: (String, HashMap[String, HashSet[String]], HashMap[String, HashSet[String]]) => Unit)
    : Unit = {
    val file = new File(filename)
    require(file.exists(), s"Whitelist file $file does not exist")
    require(file.isFile(), s"Whitelist file $file is not a normal file")

    var inputJson: String = null
    try {
      inputJson = FileUtils.readFileToString(file, "UTF-8")
    } catch {
      case e: IOException =>
        throw new SparkException(s"Failed when loading Spark properties from $filename", e)
    }
    reader(inputJson, whitelist, blacklist)
  }

  /** Load white list present in the given JSON string. */
  def getWhitelistFromStr(
      json: String,
      whitelist: HashMap[String, HashSet[String]],
      blacklist: HashMap[String, HashSet[String]]): Unit = {
    val jsonObj = JSONObject.fromObject(json)
    if (jsonObj != null) {
      val keys = jsonObj.keys()
      while (keys.hasNext()) {
        val db = keys.next().asInstanceOf[String]
        val includeTables = new HashSet[String]
        val excludeTables = new HashSet[String]
        val dbObj = jsonObj.getJSONObject(db)
        if (dbObj.containsKey(INCLUDES)) {
          val includesObj = Option(dbObj.getJSONArray(INCLUDES))
          collectlists(includeTables, includesObj)
        }
        // INLINE: add database in whitelist anyway
        whitelist.put(db, includeTables)
        if (dbObj.containsKey(EXCLUDES)) {
          val excludesObj = Option(dbObj.getJSONArray(EXCLUDES))
          collectlists(excludeTables, excludesObj)
          if (!excludeTables.isEmpty) {
            blacklist.put(db, excludeTables)
          }
        }
      }
    }
  }

  /** Load field as array present in the given JSON string. */
  def getfieldAsArrayFromStr(
      json: String,
      list: HashMap[String, HashMap[String, String]]): Unit = {
    val jsonObj = JSONObject.fromObject(json)
    if (jsonObj != null) {
      val keys = jsonObj.keys()
      while (keys.hasNext()) {
        val db = keys.next().asInstanceOf[String]
        val map = list.getOrElseUpdate(db, new HashMap[String, String])
        val dbJsonObj = jsonObj.getJSONObject(db)
        val itr = dbJsonObj.keys()
        while (itr.hasNext()) {
          val table = itr.next().asInstanceOf[String]
          map += ((table, dbJsonObj.getString(table)))
        }
      }
    }
  }

  private def collectlists(tables: HashSet[String], opt: Option[JSONArray]) {
    if (opt != None) {
      val jsonArray = opt.get
      val itr = jsonArray.iterator()
      while (itr.hasNext()) {
        val table = itr.next().asInstanceOf[String]
        tables += table
      }
    }
  }

  /** Get java.util.Date instance from formatted date string. */
  def getFormatDate(dateStr: String): Date = {
    var date: java.util.Date = null
    try {
      date = df.parse(dateStr)
    } catch {
      case p: ParseException =>
    }
    if (date == null) {
      null
    } else {
      new Date(date.getTime)
    }
  }

  /** Get the Spark SQL native DataType from some CatalogDataSource's property. */
  def getSparkSQLDataType(someType: String): DataType = {
    try {
      CatalystSqlParser.parseDataType(someType)
    } catch {
      case e: ParseException =>
        throw new SparkException("Cannot recognize some type : " + someType, e)
    }
  }

  /** Return the path of the specified Spark properties file. */
  def getPropertiesFile(env: Map[String, String] = sys.env, file: String): String = {
    if (SparkEnv.get == null ||
        SparkEnv.get.conf.get("spark.sql.testkey", "false").equals("true")) {
      Thread.currentThread().getContextClassLoader.getResource(file).getPath
    } else if (SparkEnv.get.conf.get("spark.submit.deployMode", "client").equals("cluster")) {
      file
    } else {
      Option(System.getProperty("spark.xsql.conf.dir"))
        .orElse(env.get("SPARK_XSQL_CONF_DIR"))
        .orElse(env.get("SPARK_CONF_DIR"))
        .orElse(env.get("SPARK_HOME").map { t =>
          s"$t${File.separator}conf"
        })
        .map { t =>
          new File(s"$t${File.separator}$file")
        }
        .filter(_.isFile)
        .map(_.getAbsolutePath)
        .orNull
    }
  }

  def main(args: Array[String]): Unit = {
    val json = """
{
  "xsql_mongodb": ["a", "b", "c"]
}
"""
    getWhitelistFromStr(json, new HashMap[String, HashMap[String, Boolean]])
    val json2 = """
{
  "xsql_mongodb": [{
    "table": "a",
    "include": true
  }, {
    "table": "b",
    "include": true
  }, {
    "table": "c",
    "include": true
  }]
}
"""
    getWhitelistFromStr(json2, new HashMap[String, HashMap[String, Boolean]])
    val json3 = """
{
  "logsget": {
    "includes": [
      "pre_newusergamepay",
      "gbc_20171128_hot_1",
      "shoujizhushou_push_logshare_event",
      "pre_qdas_huajiao_unlogin_behavior",
      "pre_usergame_monthly",
      "xitong_debug_a"
    ],
    "excludes": ["xitong_debug_a"]
  }
}
"""
    getWhitelistFromStr(
      json3,
      new HashMap[String, HashSet[String]],
      new HashMap[String, HashSet[String]])

    val json4 = """
{
  "logsget_user_profile": {
    "profile": "per_addr.location",
    "tableA": "a",
    "tableB": "b"
  }
}
"""
    getfieldAsArrayFromStr(json4, new HashMap[String, HashMap[String, String]])
  }

}
