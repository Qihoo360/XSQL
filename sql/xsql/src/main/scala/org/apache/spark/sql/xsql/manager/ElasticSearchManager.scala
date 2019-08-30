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

package org.apache.spark.sql.xsql.manager

import java.util.{Arrays, Collections, LinkedList}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.util.matching.Regex

import net.sf.json.{JSONNull, JSONObject}
import org.apache.commons.collections.CollectionUtils
import org.apache.http.{HttpHost, HttpStatus}
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.methods._
import org.apache.http.entity.ContentType
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.http.nio.entity.NStringEntity
import org.apache.http.util.EntityUtils
import org.elasticsearch.client._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.elasticsearch.hadoop.serialization.FieldType
import org.elasticsearch.hadoop.serialization.FieldType._
import org.elasticsearch.spark.cfg.SparkSettingsManager
import org.elasticsearch.spark.sql.xsql.ElasticsearchSchemaUtils

import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.CatalystTypeConverters.convertToScala
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, SortDirection, SortOrder}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types._
import org.apache.spark.sql.xsql._
import org.apache.spark.sql.xsql.DataSourceType.ELASTICSEARCH
import org.apache.spark.sql.xsql.internal.config.XSQL_DEFAULT_DATABASE
import org.apache.spark.sql.xsql.types.ELASTIC_SEARCH_TYPE_STRING
import org.apache.spark.sql.xsql.util.Utils._

/**
 * Manager for elastic search.
 */
private[xsql] class ElasticSearchManager(conf: SparkConf) extends DataSourceManager with Logging {

  def this() = {
    this(null)
  }

  import DataSourceManager._
  import ElasticSearchManager._

  override def shortName(): String = ELASTICSEARCH.toString

  private var restClient: RestClient = _

  /**
   * When the pushdown is enabled, the default query condition.
   */
  private val defaultCondition = new java.util.HashMap[String, Any]
  defaultCondition.put(MATCH_ALL, new java.util.HashMap[String, String])

  /**
   * Table schema need to discover.
   */
  private val discoverFields = new HashMap[String, HashMap[String, String]]

  /**
   * A reader to read the fields from a JSON string.
   * The schemas of these fields need to discover.
   */
  protected lazy val discoverFieldsReader
    : (String, HashMap[String, HashMap[String, String]]) => Unit = getfieldAsArrayFromStr

  override def parseDiscoverFile(dataSourceName: String, discoverFile: String): Unit = {
    val discoverFilePath = getPropertiesFile(file = discoverFile)
    getSettingsFromFile(discoverFilePath, discoverFields, discoverFieldsReader)
  }

  override protected def cacheDatabase(
      isDefault: Boolean,
      dataSourceName: String,
      infos: Map[String, String],
      dataSourcesByName: HashMap[String, CatalogDataSource],
      dataSourceToCatalogDatabase: HashMap[String, HashMap[String, CatalogDatabase]]): Unit = {
    val url = infos.get(URL)
    if (url == None) {
      throw new SparkException("Data source is Elasticsearch must have uri!")
    }
    val ds: CatalogDataSource = new ElasticSearchDataSource(
      dataSourceName,
      ELASTICSEARCH,
      this,
      url.get,
      infos(USER),
      infos(PASSWORD),
      infos(VERSION))
    dataSourcesByName(ds.getName) = ds

    cacheSpecialProperties(dataSourceName, infos)

    val credentialsProvider = new BasicCredentialsProvider
    credentialsProvider.setCredentials(
      AuthScope.ANY,
      new UsernamePasswordCredentials(ds.getUser, ds.getPwd))
    val (host, port) = extractHostPortFromUrl(ds.getUrl, "http")
    restClient = RestClient
      .builder(new HttpHost(host, port, "http"))
      .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
        def customizeHttpClient(
            httpClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder = {
          return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
        }
      })
      .build
    val response = restClient.performRequest(
      HttpGet.METHOD_NAME,
      ES_CAT_INDICES,
      Collections.singletonMap("pretty", "true"))
    val result = EntityUtils.toString(response.getEntity())
    val databases: Seq[String] =
      result.split("\n").filter { idx =>
        !idx.equals(SEARCHGUARD) && !idx.equals(KIBANA)
      }
    val xdatabases = dataSourceToCatalogDatabase.getOrElseUpdate(
      ds.getName,
      new HashMap[String, CatalogDatabase])
    databases.filter(isSelectedDatabase(isDefault, _, conf.get(XSQL_DEFAULT_DATABASE))).foreach {
      dbName =>
        logDebug(s"Parse elastic search index $dbName")
        val treatedDbName = lineMiddleToUnder(dbName)
        val db = CatalogDatabase(
          id = newDatabaseId,
          dataSourceName = dataSourceName,
          name = treatedDbName,
          description = null,
          locationUri = null,
          properties = Map(ORI_DB_NAME -> dbName))
        xdatabases += ((db.name, db))
    }
  }

  /**
   * When pushdown is closed, pass these properties to `ElasticsearchRelation`.
   */
  private def cacheSpecialProperties(dataSourceName: String, infos: Map[String, String]): Unit = {
    // Parse particular properties of the data source
    val properties =
      specialProperties.getOrElseUpdate(dataSourceName, new HashMap[String, String])
    val mappingDateRich =
      infos.getOrElse(ES_MAPPING_DATE_RICH_OBJECT, ES_MAPPING_DATE_RICH_OBJECT_DEFAULT)
    val scrollKeepAlive = infos.getOrElse(ES_SCROLL_KEEPALIVE, ES_SCROLL_KEEPALIVE_DEFAULT)
    val scrollSize = infos.getOrElse(ES_SCROLL_SIZE, ES_SCROLL_SIZE_DEFAULT)
    val scrollLimit = infos.getOrElse(ES_SCROLL_LIMIT, DEFAULT_SCROLL_LIMIT)
    val readFieldEmptyAsNull =
      infos.getOrElse(ES_READ_FIELD_EMPTY_AS_NULL_LEGACY, ES_READ_FIELD_EMPTY_AS_NULL_DEFAULT)
    properties += ((ES_MAPPING_DATE_RICH_OBJECT, mappingDateRich))
    properties += ((ES_SCROLL_KEEPALIVE, scrollKeepAlive))
    properties += ((ES_SCROLL_SIZE, scrollSize))
    properties += ((ES_SCROLL_LIMIT, scrollLimit))
    properties += ((ES_READ_FIELD_EMPTY_AS_NULL_LEGACY, readFieldEmptyAsNull))
  }

  override protected def cacheTable(
      dataSourceName: String,
      dataSourceToCatalogDatabase: HashMap[String, HashMap[String, CatalogDatabase]],
      dbToCatalogTable: HashMap[Int, HashMap[String, CatalogTable]]): Unit = {
    val xdatabases = dataSourceToCatalogDatabase(dataSourceName)
    xdatabases.foreach { kv =>
      val dbName = kv._1
      val db = kv._2
      val xtables = dbToCatalogTable.getOrElseUpdate(db.id, new HashMap[String, CatalogTable])
      val originDBName = db.properties.getOrElse(ORI_DB_NAME, dbName)
      val response = restClient.performRequest(
        HttpGet.METHOD_NAME,
        s"/$originDBName/_mapping",
        Collections.singletonMap("pretty", "true"))
      val rootObj = JSONObject.fromObject(EntityUtils.toString(response.getEntity()).trim)
      val dbObj = rootObj.get(originDBName).asInstanceOf[JSONObject]
      val mappingsObj = dbObj.get(MAPPINGS).asInstanceOf[JSONObject]
      val (whiteTables, blackTables) = getWhiteAndBlackTables(originDBName)
      val itr = mappingsObj.keys()
      while (itr.hasNext()) {
        val typeName = itr.next().asInstanceOf[String]
        if (isSelectedTable(whiteTables, blackTables, typeName)) {
          val tableOpt = if (isDiscover(originDBName, typeName)) {
            discoverRawTable(dbName, originDBName, typeName)
          } else {
            getTableOption(dataSourceName, dbName, originDBName, typeName, mappingsObj)
          }
          tableOpt.foreach { table =>
            xtables += ((typeName, table))
          }
        }
      }
    }
  }

  override def getDefaultOptions(table: CatalogTable): Map[String, String] = {
    val identifier = table.identifier
    val dsName = identifier.dataSource.get
    val index = identifier.database.get
    val esType = identifier.table
    getDefaultOptions(dsName, index, esType)
  }

  /**
   * Get the default options for `CatalogTable`.
   */
  private def getDefaultOptions(
      dsName: String,
      index: String,
      esType: String): Map[String, String] = {
    val options = new HashMap[String, String]
    val url = cachedProperties(URL)
    val (host, port) = extractHostPortFromUrl(url, "http")
    val user = cachedProperties(USER)
    val password = cachedProperties(PASSWORD)
    options += ((ES_NODES, host))
    options += ((ES_PORT, port.toString))
    options += ((ES_NET_HTTP_AUTH_USER, user))
    options += ((ES_NET_HTTP_AUTH_PASS, password))
    options += ((ES_RESOURCE, s"$index/$esType"))
    if (discoverFields.contains(index)) {
      val readFieldAsArrayInlude = discoverFields(index)(esType)
      options += ((ES_READ_FIELD_AS_ARRAY_INCLUDE, readFieldAsArrayInlude))
    }
    (options ++: specialProperties(dsName)).toMap
  }

  override def getRedactionPattern: Option[Regex] = Some("(?i)pass|user".r)

  override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {
    val response = restClient.performRequest(
      HttpPut.METHOD_NAME,
      s"/${dbDefinition.name}",
      Collections.singletonMap("pretty", "true"))
    if (response.getStatusLine.getStatusCode != HttpStatus.SC_OK) {
      throw new SparkException("Create elastic index failed!")
    }
    CatalogDatabase(
      id = newDatabaseId,
      dataSourceName = dbDefinition.dataSourceName,
      name = dbDefinition.name,
      description = dbDefinition.description,
      locationUri = dbDefinition.locationUri,
      properties = dbDefinition.properties)
  }

  override def createTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit = {
    val dsName = tableDefinition.identifier.dataSource.get
    val dbName = tableDefinition.identifier.database.get
    val typeName = tableDefinition.identifier.table
    verifyColumnDataType(tableDefinition.schema)
    val map = new java.util.HashMap[String, java.util.HashMap[String, Any]]
    tableDefinition.schema.map(toESJson).foreach { kv =>
      map.put(kv._1, kv._2)
    }
    val properties = new JSONObject().put(PROPERTIES, map)
    val jsonEntity = properties.toString
    val entity = new NStringEntity(jsonEntity, ContentType.APPLICATION_JSON)
    val response = restClient.performRequest(
      HttpPut.METHOD_NAME,
      typeMappingUrl(dbName, typeName),
      Collections.singletonMap("pretty", "true"),
      entity)
    if (response.getStatusLine.getStatusCode != HttpStatus.SC_OK) {
      throw new SparkException("Create elastic type failed!")
    }
  }

  // TODO maybe need to load raw table
  override def alterTableDataSchema(
      dbName: String,
      table: String,
      newDataSchema: StructType): Unit = {
    verifyColumnDataType(newDataSchema)
    val map = new java.util.HashMap[String, java.util.HashMap[String, Any]]
    newDataSchema.map(toESJson).foreach { kv =>
      map.put(kv._1, kv._2)
    }
    val properties = new JSONObject().put(PROPERTIES, map)
    val jsonEntity = properties.toString
    val entity = new NStringEntity(jsonEntity, ContentType.APPLICATION_JSON)
    val response = restClient.performRequest(
      HttpPut.METHOD_NAME,
      typeMappingUrl(dbName, table),
      Collections.singletonMap("pretty", "true"),
      entity)
    if (response.getStatusLine.getStatusCode != HttpStatus.SC_OK) {
      throw new SparkException("Alter elastic type add columns failed!")
    }
  }

  override def listTables(dbName: String): Seq[String] = {
    val (whiteTables, blackTables) = getWhiteAndBlackTables(dbName)
    val response = restClient.performRequest(
      HttpGet.METHOD_NAME,
      s"/$dbName/_mapping",
      Collections.singletonMap("pretty", "true"))
    val rootObj = JSONObject.fromObject(EntityUtils.toString(response.getEntity()).trim)
    val dbObj = rootObj.get(dbName).asInstanceOf[JSONObject]
    val mappingsObj = dbObj.get(MAPPINGS).asInstanceOf[JSONObject]
    val itr = mappingsObj.keys()
    val result = ArrayBuffer.empty[String]
    while (itr.hasNext()) {
      val typeName = itr.next().asInstanceOf[String]
      if (isSelectedTable(whiteTables, blackTables, typeName)) {
        result += typeName
      }
    }
    result.seq
  }

  override def tableExists(dbName: String, table: String): Boolean = {
    val response = restClient.performRequest(
      HttpHead.METHOD_NAME,
      s"/$dbName/_mapping/$table",
      Collections.singletonMap("pretty", "true"))
    if (response.getStatusLine.getStatusCode == HttpStatus.SC_OK) {
      true
    } else {
      false
    }
  }

  override protected def doGetRawTable(
      db: String,
      originDB: String,
      table: String): Option[CatalogTable] = {
    if (isDiscover(originDB, table)) {
      discoverRawTable(db, originDB, table)
    } else {
      logDebug(s"Looking up $dsName.$db.$table, $dsName.$originDB.$table in fact.")
      val response = restClient.performRequest(
        HttpGet.METHOD_NAME,
        s"/$originDB/$table/_mapping",
        Collections.singletonMap("pretty", "true"))
      val rootObj = JSONObject.fromObject(EntityUtils.toString(response.getEntity()).trim)
      val dbObj = rootObj.get(originDB).asInstanceOf[JSONObject]
      val mappingsObj = dbObj.get(MAPPINGS).asInstanceOf[JSONObject]
      val itr = mappingsObj.keys()
      if (itr.hasNext()) {
        val typeName = itr.next().asInstanceOf[String]
        getTableOption(dsName, db, originDB, typeName, mappingsObj)
      } else {
        None
      }
    }
  }

  /**
   * Check if the specified table contains fields need to discover.
   */
  private def isDiscover(originDB: String, table: String): Boolean = {
    var flag = false
    if (discoverFields.contains(originDB)) {
      val tableDiscoverFields = discoverFields.get(originDB)
      if (tableDiscoverFields.isDefined && tableDiscoverFields.get.contains(table)) {
        flag = true
      }
    }
    flag
  }

  /**
   * Discover the schema of the specified table.
   */
  private def discoverRawTable(
      db: String,
      originDB: String,
      table: String): Option[CatalogTable] = {
    logDebug(s"Discovering $dsName.$db.$table, $dsName.$originDB.$table in fact.")
    val parameters = getDefaultOptions(dsName, originDB, table)

    val sc = SparkContext.getActive.get
    val cfg = new SparkSettingsManager().load(sc.getConf).merge(parameters.asJava)
    val lazySchema = { ElasticsearchSchemaUtils.discoverMapping(cfg) }
    getTableOption(dsName, db, originDB, table, lazySchema.struct, Some(parameters))
  }

  private def getTableOption(
      dsName: String,
      dbName: String,
      originDB: String,
      typeName: String,
      mappingsObj: JSONObject): Option[CatalogTable] = {
    logDebug(s"Parse elastic search type $typeName")
    val typeObj = mappingsObj.get(typeName).asInstanceOf[JSONObject]
    val schema = convertToStruct(dsName, typeName, typeObj)
    getTableOption(dsName, dbName, originDB, typeName, schema, None)
  }

  /**
   * Construct `CatalogTable` with schema and properties of table.
   */
  private def getTableOption(
      dsName: String,
      dbName: String,
      originDB: String,
      typeName: String,
      schema: StructType,
      existsProperties: Option[Map[String, String]]): Option[CatalogTable] = {
    val parameters: Map[String, String] =
      existsProperties.getOrElse(getDefaultOptions(dsName, originDB, typeName))
    val table = CatalogTable(
      identifier = TableIdentifier(typeName, Option(dbName), Option(dsName)),
      tableType = CatalogTableType.TYPE,
      storage = CatalogStorageFormat(
        locationUri = Option(s"$originDB/$typeName").map(CatalogUtils.stringToURI),
        inputFormat = Some(INPUT_FORMAT),
        outputFormat = Some(OUTPUT_FORMAT),
        serde = None,
        compressed = false,
        properties = parameters),
      schema = schema,
      provider = Some(FULL_PROVIDER),
      properties = Map(ORI_DB_NAME -> originDB))
    Option(table)
  }

  override def buildAndArgument(
      searchArg: java.util.HashMap[String, Any],
      leftSearchArg: java.util.HashMap[String, Any],
      rightSearchArg: java.util.HashMap[String, Any]): Unit = {
    val list = Arrays.asList(leftSearchArg, rightSearchArg)
    val mustArg = new java.util.HashMap[String, Any]
    // The filter clause must appear in matching documents.
    // However unlike must clause the score of the query will be ignored.
    mustArg.put(FILTER, list)
    searchArg.put(BOOL, mustArg)
  }

  override def buildOrArgument(
      searchArg: java.util.HashMap[String, Any],
      leftSearchArg: java.util.HashMap[String, Any],
      rightSearchArg: java.util.HashMap[String, Any]): Unit = {
    val list = Arrays.asList(leftSearchArg, rightSearchArg)
    val mustArg = new java.util.HashMap[String, Any]
    mustArg.put(SHOULD, list)
    searchArg.put(BOOL, mustArg)
  }

  override def buildNotArgument(
      searchArg: java.util.HashMap[String, Any],
      childSearchArg: java.util.HashMap[String, Any]): Unit = {
    val list = Arrays.asList(childSearchArg)
    val mustNotArg = new java.util.HashMap[String, Any]
    mustNotArg.put(MUST_NOT, list)
    searchArg.put(BOOL, mustNotArg)
  }

  override def buildEqualToArgument(
      searchArg: java.util.HashMap[String, Any],
      attribute: String,
      value: Any): Unit = {
    // Note text type not support exact search. refer to :
    // https://www.elastic.co/guide/cn/elasticsearch/guide/current/_finding_exact_values.html
    val termMap = new java.util.HashMap[String, Any]
    termMap.put(attribute, value)
    val filterMap = new java.util.HashMap[String, Any]
    filterMap.put(TERM, termMap)
    val scoreMap = new java.util.HashMap[String, Any]
    scoreMap.put(FILTER, filterMap)
    searchArg.put(CONSTANT_SCORE, scoreMap)
  }

  override def buildLessThanArgument(
      searchArg: java.util.HashMap[String, Any],
      attribute: String,
      value: Any): Unit = {
    val rangeMap = new java.util.HashMap[String, Any]
    val map = new java.util.HashMap[String, Any]
    map.put(LT, value)
    rangeMap.put(attribute, map)
    searchArg.put(RANGE, rangeMap)
  }

  override def buildLessThanOrEqualArgument(
      searchArg: java.util.HashMap[String, Any],
      attribute: String,
      value: Any): Unit = {
    val rangeMap = new java.util.HashMap[String, Any]
    val map = new java.util.HashMap[String, Any]
    map.put(LTE, value)
    rangeMap.put(attribute, map)
    searchArg.put(RANGE, rangeMap)
  }

  override def buildGreaterThanArgument(
      searchArg: java.util.HashMap[String, Any],
      attribute: String,
      value: Any): Unit = {
    val rangeMap = new java.util.HashMap[String, Any]
    val map = new java.util.HashMap[String, Any]
    map.put(GT, value)
    rangeMap.put(attribute, map)
    searchArg.put(RANGE, rangeMap)
  }

  override def buildGreaterThanOrEqualArgument(
      searchArg: java.util.HashMap[String, Any],
      attribute: String,
      value: Any): Unit = {
    val rangeMap = new java.util.HashMap[String, Any]
    val map = new java.util.HashMap[String, Any]
    map.put(GTE, value)
    rangeMap.put(attribute, map)
    searchArg.put(RANGE, rangeMap)
  }

  override def buildIsNullArgument(
      searchArg: java.util.HashMap[String, Any],
      childSearchArg: java.util.HashMap[String, Any]): Unit = {
    val mustNotArg = new java.util.HashMap[String, Any]
    val map = new java.util.HashMap[String, Any]
    map.put(EXISTS, childSearchArg)
    mustNotArg.put(MUST_NOT, map)
    searchArg.put(BOOL, mustNotArg)
  }

  override def buildIsNotNullArgument(
      searchArg: java.util.HashMap[String, Any],
      childSearchArg: java.util.HashMap[String, Any]): Unit = {
    searchArg.put(EXISTS, childSearchArg)
  }

  override def buildLikeArgument(
      searchArg: java.util.HashMap[String, Any],
      attribute: String,
      value: Any): Unit = {
    val map = new java.util.HashMap[String, Any]
    map.put(attribute, wildcardTransform(value))
    searchArg.put(WILDCARD, map)
  }

  override def buildRLikeArgument(
      searchArg: java.util.HashMap[String, Any],
      attribute: String,
      value: Any): Unit = {
    val regexpArg = new java.util.HashMap[String, Any]
    regexpArg.put(attribute, value)
    searchArg.put(REGEXP, regexpArg)
  }

  override def buildInArgument(
      searchArg: java.util.HashMap[String, Any],
      attribute: String,
      values: LinkedList[Any]): Unit = {
    val termsMap = new java.util.HashMap[String, Any]
    termsMap.put(attribute, values)
    val filterMap = new java.util.HashMap[String, Any]
    filterMap.put(TERMS, termsMap)
    val scoreMap = new java.util.HashMap[String, Any]
    scoreMap.put(FILTER, filterMap)
    searchArg.put(CONSTANT_SCORE, scoreMap)
  }

  override def buildAttributeArgument(
      searchArg: java.util.HashMap[String, Any],
      attribute: String): Unit = {
    searchArg.put(FIELD, attribute)
  }

  override def buildGroupArgument(
      fieldName: String,
      groupByKeys: ArrayBuffer[String],
      innerAggsMap: java.util.HashMap[String, Any],
      groupMap: java.util.HashMap[String, Any]): java.util.HashMap[String, Any] = {
    val groupFieldMap = new java.util.HashMap[String, Any]
    groupFieldMap.put(FIELD, fieldName)
    groupFieldMap.put(SIZE, MAX_LIMIT)
    val groupName = INNER_GROUP_ALIAS + fieldName
    groupByKeys.+=:(groupName)
    if (innerAggsMap.containsKey(TERMS)) {
      val childGroupMap = groupMap
      val aggsMap = new java.util.HashMap[String, Any]
      aggsMap.put(TERMS, groupFieldMap)
      aggsMap.put(AGGS, childGroupMap)
      val newGroupMap = new java.util.HashMap[String, Any]
      newGroupMap.put(groupName, aggsMap)
      newGroupMap
    } else {
      innerAggsMap.put(TERMS, groupFieldMap)
      groupMap.put(groupName, innerAggsMap)
      groupMap
    }
  }

  override def buildAggregateArgument(
      structField: StructField,
      aggregatefuncName: String,
      aggsMap: java.util.HashMap[String, Any],
      aggregateAliasOpts: Option[String]): StructField = {
    val fieldName = structField.name
    val usedAggsFieldName = fieldName match {
      case STAR => INDEX
      case _ => fieldName
    }
    val aggregateFieldMap = new java.util.HashMap[String, Any]
    aggregateFieldMap.put(FIELD, usedAggsFieldName)
    val aggregateFunMap = new java.util.HashMap[String, Any]
    val usedAggsFuncName = aggregatefuncName match {
      case COUNT => VALUE_COUNT
      case COUNT_DISTINCT => CARDINALITY
      case _ => aggregatefuncName
    }
    aggregateFunMap.put(usedAggsFuncName, aggregateFieldMap)
    val aggregateOutMap =
      aggsMap
        .getOrDefault(AGGS, new java.util.HashMap[String, Any])
        .asInstanceOf[java.util.HashMap[String, Any]]
    val aggregateAliasName = aggregateAliasOpts.getOrElse(
      INNER_AGGS_ALIAS + usedAggsFuncName + UNDERLINE + usedAggsFieldName)
    aggregateOutMap.put(aggregateAliasName, aggregateFunMap)
    aggsMap.put(AGGS, aggregateOutMap)
    new StructField(aggregateAliasName, DEFAULT_DATA_TYPE)
  }

  override def buildSortArgument(
      order: Seq[SortOrder]): java.util.List[java.util.HashMap[String, Any]] = {
    val sortArg = new LinkedList[java.util.HashMap[String, Any]]
    order.foreach { x =>
      x.child match {
        case attr: UnresolvedAttribute =>
          val map = new java.util.HashMap[String, Any]
          val realName = attr.name.split("\\.").last
          map.put(realName, x.direction.sql)
          sortArg.add(map)
      }
    }
    sortArg
  }

  override def buildSortArgumentInAggregate(
      parentGroupMap: java.util.HashMap[String, Any],
      aggs: java.util.HashMap[String, Any],
      expr: Expression,
      direction: SortDirection,
      isDistinct: Boolean): Unit = {
    aggs.asScala.foreach { x =>
      if (x._1.startsWith(INNER_GROUP_ALIAS)) {
        val groupMap = x._2.asInstanceOf[java.util.HashMap[String, Any]]
        // Parse sort with column.
        if (expr.isInstanceOf[UnresolvedAttribute]) {
          val attr = expr.asInstanceOf[UnresolvedAttribute]
          val realName = attr.name.split("\\.").last
          val termsMap = groupMap.get(TERMS).asInstanceOf[java.util.HashMap[String, Any]]
          val field = termsMap.get(FIELD).asInstanceOf[String]
          if (field.equals(realName)) {
            val orderMap =
              termsMap
                .getOrDefault(ORDER, new LinkedList[java.util.HashMap[String, Any]])
                .asInstanceOf[LinkedList[java.util.HashMap[String, Any]]]
            val directionMap = new java.util.HashMap[String, Any]
            directionMap.put(ORDER_TERM, direction.sql)
            orderMap.add(directionMap)
            termsMap.put(ORDER, orderMap)
          } else if (groupMap.containsKey(AGGS)) {
            val childAggs = groupMap.get(AGGS).asInstanceOf[java.util.HashMap[String, Any]]
            buildSortArgumentInAggregate(groupMap, childAggs, expr, direction, isDistinct)
          }
        } else { // Parse sort with aggregate function.
          val childAggs = groupMap.get(AGGS).asInstanceOf[java.util.HashMap[String, Any]]
          buildSortArgumentInAggregate(groupMap, childAggs, expr, direction, isDistinct)
        }
      } else {
        val aggFunMap = x._2.asInstanceOf[java.util.HashMap[String, Any]]
        expr match {
          case sum @ Sum(child) =>
            buildAggFuncSortArgs(
              parentGroupMap,
              x._1,
              child,
              aggFunMap,
              sum.prettyName,
              direction.sql)
          case avg @ Average(child) =>
            buildAggFuncSortArgs(
              parentGroupMap,
              x._1,
              child,
              aggFunMap,
              avg.prettyName,
              direction.sql)
          case max @ Max(child) =>
            buildAggFuncSortArgs(
              parentGroupMap,
              x._1,
              child,
              aggFunMap,
              max.prettyName,
              direction.sql)
          case min @ Min(child) =>
            buildAggFuncSortArgs(
              parentGroupMap,
              x._1,
              child,
              aggFunMap,
              min.prettyName,
              direction.sql)
          case count @ Count(children) =>
            children.foreach { child =>
              if (isDistinct) {
                buildAggFuncSortArgs(
                  parentGroupMap,
                  x._1,
                  child,
                  aggFunMap,
                  CARDINALITY,
                  direction.sql)
              } else {
                buildAggFuncSortArgs(
                  parentGroupMap,
                  x._1,
                  child,
                  aggFunMap,
                  VALUE_COUNT,
                  direction.sql)
              }
            }
          case attr: UnresolvedAttribute =>
            buildAggFuncSortArgs(
              parentGroupMap,
              x._1,
              attr,
              aggFunMap,
              VALUE_COUNT,
              direction.sql)
        }
      }
    }
  }

  override def buildDistinctArgument(
      columns: java.util.List[String]): (java.util.HashMap[String, Any], Seq[String]) = {
    val arrArg = new LinkedList[java.util.HashMap[String, Any]]
    var distinctAlias = INNER_DISTINCT_ALIAS
    columns.asScala.foreach { column =>
      val sortFieldMap = new java.util.HashMap[String, Any]
      sortFieldMap.put(column, DESC)
      arrArg.add(sortFieldMap)
      distinctAlias = distinctAlias + UNDERLINE + column
    }
    var groupMap = new java.util.HashMap[String, Any]
    val innerAggsMap = new java.util.HashMap[String, Any]
    var childGroupMap: java.util.HashMap[String, Any] = null
    val groupByKeys: ArrayBuffer[String] = ArrayBuffer.empty
    columns.asScala.reverse.foreach { column =>
      val sortFieldMap = new java.util.HashMap[String, Any]
      sortFieldMap.put(column, DESC)
      arrArg.add(sortFieldMap)
      val groupFieldMap = new java.util.HashMap[String, Any]
      groupFieldMap.put(FIELD, column)
      val groupName = INNER_GROUP_ALIAS + column
      groupByKeys.+=:(groupName)
      if (innerAggsMap.containsKey(TERMS)) {
        childGroupMap = groupMap
        groupMap = new java.util.HashMap[String, Any]
        val aggsMap = new java.util.HashMap[String, Any]
        aggsMap.put(TERMS, groupFieldMap)
        aggsMap.put(AGGS, childGroupMap)
        groupMap.put(groupName, aggsMap)
      } else {
        innerAggsMap.put(TERMS, groupFieldMap)
        groupMap.put(groupName, innerAggsMap)
      }
    }
    val sortArg = new java.util.HashMap[String, Any]
    sortArg.put(SORT, arrArg)
    sortArg.put(SOURCE, columns)
    sortArg.put(SIZE, 1)
    val topHitsMap = new java.util.HashMap[String, Any]
    topHitsMap.put(TOP_HITS, sortArg)
    val aggregateOutMap = new java.util.HashMap[String, Any]
    aggregateOutMap.put(distinctAlias, topHitsMap)
    innerAggsMap.put(AGGS, aggregateOutMap)
    (groupMap, groupByKeys.seq)
  }

  override def scanXSQLTable(
      dataSourcesByName: HashMap[String, CatalogDataSource],
      tableDefinition: CatalogTable,
      columns: java.util.List[String],
      condition: java.util.HashMap[String, Any],
      sort: java.util.List[java.util.HashMap[String, Any]],
      aggs: java.util.HashMap[String, Any],
      groupByKeys: Seq[String],
      limit: Int): Seq[Seq[Any]] = {
    val ds = dataSourcesByName(tableDefinition.identifier.dataSource.get)
    val properties = new JSONObject()
    var query: java.util.HashMap[String, Any] = null
    if (condition == null || condition.isEmpty()) {
      query = defaultCondition
    } else {
      query = condition
    }
    if (query != null && query.size() > 0) {
      properties.put(QUERY, query)
      if (columns != null && !columns.isEmpty) {
        properties.put(SOURCE, columns)
      }
      if (CollectionUtils.isNotEmpty(sort)) {
        properties.put(SORT, sort)
      }
    }
    if (aggs == null || aggs.size() == 0) {
      properties.put(SIZE, limit)
    } else {
      // ES only support limit with one group level.
      logWarning("Search ES with `group by` cannot use `limit`.")
      properties.put(AGGS, aggs)
      properties.put(SIZE, 0)
    }
    val jsonEntity = properties.toString
    val entity = new NStringEntity(jsonEntity, ContentType.APPLICATION_JSON)
    val response = restClient.performRequest(
      HttpGet.METHOD_NAME,
      typeSearchUrl(tableDefinition),
      Collections.singletonMap("pretty", "true"),
      entity)
    if (response.getStatusLine.getStatusCode != HttpStatus.SC_OK) {
      throw new SparkException("Scan elastic type failed!")
    }
    val rootObj = JSONObject.fromObject(EntityUtils.toString(response.getEntity()).trim)
    if (aggs == null) {
      parseQuery(rootObj, tableDefinition, columns)
    } else {
      parseAggregate(rootObj, tableDefinition, columns, groupByKeys)
    }
  }

  private def convertToStruct(
      dataSourceName: String,
      fieldName: String,
      parentPropertiesObj: JSONObject,
      parentName: String = null): StructType = {
    val fields: ArrayBuffer[StructField] = ArrayBuffer.empty
    val propertiesObj = parentPropertiesObj.get(PROPERTIES).asInstanceOf[JSONObject]
    if (propertiesObj != null) {
      val itr = propertiesObj.keys()
      while (itr.hasNext()) {
        val fieldName = itr.next().asInstanceOf[String]
        val subPropertiesObj = propertiesObj.get(fieldName).asInstanceOf[JSONObject]
        fields += convertField(dataSourceName, fieldName, subPropertiesObj, parentName)
      }
    }
    DataTypes.createStructType(fields.toArray)
  }

  private def convertField(
      dataSourceName: String,
      fieldName: String,
      propertiesObj: JSONObject,
      parentName: String): StructField = {
    val absoluteName = if (parentName != null) parentName + "." + fieldName else fieldName
    val esType = if (propertiesObj.containsKey(ES_TYPE)) {
      propertiesObj.get(ES_TYPE).asInstanceOf[String]
    } else if (propertiesObj.containsKey(PROPERTIES)) {
      FieldType.OBJECT.toString().toLowerCase()
    } else {
      throw new SparkException("Elasticsearch data type is not correct!")
    }
    val builder = new MetadataBuilder
    builder.putString(ELASTIC_SEARCH_TYPE_STRING, esType)
    val dataType = FieldType.parse(esType) match {
      case NULL => NullType
      case BINARY => BinaryType
      case BOOLEAN => BooleanType
      case BYTE => ByteType
      case SHORT => ShortType
      case INTEGER => IntegerType
      case LONG => LongType
      case FLOAT => FloatType
      case DOUBLE => DoubleType
      case HALF_FLOAT => FloatType
      case SCALED_FLOAT => FloatType
      // String type
      case STRING => StringType
      case TEXT => StringType
      case KEYWORD => StringType
      case DATE =>
        val mappingDateRich =
          specialProperties(dataSourceName)(ES_MAPPING_DATE_RICH_OBJECT).toBoolean
        if (mappingDateRich) TimestampType else StringType
      case OBJECT => convertToStruct(dataSourceName, fieldName, propertiesObj, absoluteName)
      case NESTED =>
        DataTypes.createArrayType(
          convertToStruct(dataSourceName, fieldName, propertiesObj, absoluteName))
      case _ => StringType
    }
    DataTypes.createStructField(fieldName, dataType, true, builder.build())
  }

  override def stop(): Unit = {
    restClient.close()
  }

}

object ElasticSearchManager {
  import DataSourceManager._
  val ES_CAT_INDICES = "/_cat/indices?health=green&h=index"
  val SEARCHGUARD = "searchguard"
  val KIBANA = ".kibana"
  val ES_TYPE = "type"
  val QUESTION_MARK = "?"
  val ASTERISK = "*"
  val WILDCARD = "wildcard"
  val NESTED_TYPE = "nested"
  val BOOST = "boost"
  val IGNORE_ABOVE = "ignore_above"
  val TEMP_KEY = "es_json_temp_key"
  val DEFAULT_IGNORE_ABOVE = 256
  val FORMAT = "format"
  val DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
  val SCALING_FACTOR = "scaling_factor"
  val DEFAULT_SCALING_FACTOR = 100
  val PROPERTIES = "properties"
  val MAPPINGS = "mappings"
  val QUERY = "query"
  val FILTER = "filter"
  val HITS = "hits"
  val TOTAL = "total"
  val DOC_ID = "_id"
  val DOC_COUNT = "doc_count"
  val SOURCE = "_source"
  val INDEX = "_index"
  val SORT = "sort"
  val ORDER = "order"
  val DESC = "desc"
  val SIZE = "size"
  val MATCH_ALL = "match_all"
  val MATCH = "match"
  val REGEXP = "regexp"
  val MUST = "must"
  val MUST_NOT = "must_not"
  val SHOULD = "should"
  val BOOL = "bool"
  val OPERATOR = "operator"
  val AND = "and"
  val RANGE = "range"
  val GTE = "gte"
  val GT = "gt"
  val LTE = "lte"
  val LT = "lt"
  val EXISTS = "exists"
  val FIELD = "field"
  val TERMS = "terms"
  val TERM = "term"
  val CONSTANT_SCORE = "constant_score"
  val ORDER_TERM = "_term"
  val AGGREGATIONS = "aggregations"
  val BUCKETS = "buckets"
  val KEY = "key"
  val VALUE = "value"
  val INLINE = "inline"
  val HAVING = "having"
  val BUCKET_SELECTOR = "bucket_selector"
  val BUCKETS_PATH = "buckets_path"
  val SCRIPT = "script"
  val LANG = "lang"
  val SUM = "sum"
  val VALUE_COUNT = "value_count"
  val CARDINALITY = "cardinality"
  val TOP_HITS = "top_hits"
  val FULL_PROVIDER = "org.elasticsearch.spark.sql"
  val PROVIDER = "ES"
  val INPUT_FORMAT = "org.elasticsearch.hadoop.mr.EsInputFormat"
  val OUTPUT_FORMAT = "org.elasticsearch.hadoop.mr.EsOutputFormat"
  val ES_PREFIX = "es."
  val DEFAULT_SCROLL_LIMIT = "20"

  def verifyColumnDataType(schema: StructType): Unit = {
    schema.foreach(col => getSparkSQLDataType(toESColumn(col)._2))
  }

  /** Builds the native StructField from ES's property. */
  private def fromESColumn(fieldName: String, propertiesObj: JSONObject): StructField = {
    val esType = propertiesObj.get("type").asInstanceOf[String]
    val columnType = getSparkSQLDataType(esType)
    val metadata = if (esType != columnType.catalogString) {
      new MetadataBuilder().putString(ELASTIC_SEARCH_TYPE_STRING, esType).build()
    } else {
      Metadata.empty
    }

    StructField(name = fieldName, dataType = columnType, nullable = true, metadata = metadata)
  }

  /** Converts the native StructField to ES's name and type. */
  def toESColumn(c: StructField): (String, String) = {
    val typeString = if (c.metadata.contains(ELASTIC_SEARCH_TYPE_STRING)) {
      c.metadata.getString(ELASTIC_SEARCH_TYPE_STRING)
    } else {
      c.dataType.catalogString
    }
    (c.name, typeString)
  }

  /** Converts string to ES's name and type. */
  def toESJson(key: String, typeString: String): (String, java.util.HashMap[String, Any]) = {
    if (typeString.startsWith(StructType.simpleString)) {
      val innerMap = new java.util.HashMap[String, Any]
      val objectMap = new java.util.HashMap[String, Any]
      val propertiesString =
        typeString.substring(StructType.simpleString.length + 1, typeString.length - 1)
      val properties = propertiesString.split(COMMA)
      properties.foreach { property =>
        val kv = property.split(COLON)
        val (fieldName, fieldInnerMap) = toESJson(kv(0), kv(1))
        objectMap.put(fieldName, fieldInnerMap)
      }
      innerMap.put(PROPERTIES, objectMap)
      (key, innerMap)
    } else {
      val innerMap = new java.util.HashMap[String, Any]
      val esType = typeString match {
        case "tinyint" => ByteType.typeName
        case "smallint" => ShortType.typeName
        case "int" => IntegerType.typeName
        case "bigint" => LongType.typeName
        case _ => typeString
      }
      innerMap.put(TYPE, esType)
      treatExtraParams(esType, innerMap)
      (key, innerMap)
    }
  }

  /** Converts the native StructField to ES's name and type. */
  def toESJson(c: StructField): (String, java.util.HashMap[String, Any]) = {
    c.dataType match {
      case StructType(fields) =>
        if (c.metadata.contains(ELASTIC_SEARCH_TYPE_STRING)) {
          val typeString = c.metadata.getString(ELASTIC_SEARCH_TYPE_STRING)
          toESJson(c.name, typeString)
        } else {
          val innerMap = new java.util.HashMap[String, Any]
          val objectMap = new java.util.HashMap[String, Any]
          fields.foreach { field =>
            val (fieldName, fieldInnerMap) = toESJson(field)
            objectMap.put(fieldName, fieldInnerMap)
          }
          innerMap.put(PROPERTIES, objectMap)
          (c.name, innerMap)
        }
      case ArrayType(elementType: StructType, containsNull) =>
        val innerMap = new java.util.HashMap[String, Any]
        innerMap.put(TYPE, NESTED_TYPE)
        (c.name, innerMap)
      case _ =>
        val typeString = if (c.metadata.contains(ELASTIC_SEARCH_TYPE_STRING)) {
          c.metadata.getString(ELASTIC_SEARCH_TYPE_STRING)
        } else {
          c.dataType.typeName
        }
        val innerMap = new java.util.HashMap[String, Any]
        innerMap.put(TYPE, typeString)
        treatExtraParams(typeString, innerMap)
        (c.name, innerMap)
    }
  }

  private def treatExtraParams(
      typeString: String,
      innerMap: java.util.HashMap[String, Any]): Unit = {
    if (typeString == KeyWordType.simpleString) {
      innerMap.put(IGNORE_ABOVE, DEFAULT_IGNORE_ABOVE.toString)
    } else if (typeString == DateType.simpleString) {
      innerMap.put(FORMAT, DEFAULT_DATE_FORMAT)
    } else if (typeString == ScaledFloatType.simpleString) {
      innerMap.put(SCALING_FACTOR, DEFAULT_SCALING_FACTOR)
    }
  }

  private def typeMappingUrl(db: String, table: String): String = {
    s"/${db}/_mapping/${table}"
  }

  private def typeSearchUrl(db: String, table: String): String = {
    s"/${db}/${table}/_search"
  }

  private def typeSearchUrl(tableDefinition: CatalogTable): String = {
    if (tableDefinition.properties.isEmpty) {
      typeSearchUrl(tableDefinition.identifier.database.get, tableDefinition.identifier.table)
    } else {
      val dbName = tableDefinition.properties.get(ORI_DB_NAME)
      typeSearchUrl(dbName.get, tableDefinition.identifier.table)
    }
  }

  private def parseQuery(
      rootObj: JSONObject,
      tableDefinition: CatalogTable,
      columns: java.util.List[String]): ArrayBuffer[Seq[Any]] = {
    val hitsObj = rootObj.getJSONObject(HITS)
    val total = hitsObj.getInt(TOTAL)
    val hitsArray = hitsObj.getJSONArray(HITS)
    val itr = hitsArray.iterator()
    val result: ArrayBuffer[Seq[Any]] = ArrayBuffer.empty
    val schema = tableDefinition.schema
    while (itr.hasNext()) {
      val ele = itr.next().asInstanceOf[JSONObject]
      val docId = ele.getString(DOC_ID)
      val sourceObj = ele.getJSONObject(SOURCE)
      // TODO sequence is different between schema and result.
      val fields: ArrayBuffer[Any] = ArrayBuffer.empty
      for (column <- columns.asScala) {
        val structField = schema(column)
        val fieldValue = dataTypeTransform(sourceObj, structField, column)
        fields += fieldValue
      }
      result += fields.toSeq
    }
    result
  }

  private def parseAggregate(
      rootObj: JSONObject,
      tableDefinition: CatalogTable,
      columns: java.util.List[String],
      groupByKeys: Seq[String]): ArrayBuffer[Seq[Any]] = {
    val aggregationsObj = rootObj.getJSONObject(AGGREGATIONS)
    val result: ArrayBuffer[Seq[Any]] = ArrayBuffer.empty
    parseNestedAggregate(
      aggregationsObj,
      columns,
      0,
      groupByKeys,
      None,
      tableDefinition.schema,
      result)
    result
  }

  private def parseNestedAggregate(
      aggregationsObj: JSONObject,
      columns: java.util.List[String],
      currentGroupByIndex: Int,
      groupByKeys: Seq[String],
      outerGroupValues: Option[HashMap[String, Any]],
      schema: StructType,
      result: ArrayBuffer[Seq[Any]]): Unit = {
    if (groupByKeys == null || groupByKeys.isEmpty) {
      // When use count(distinct some column), no group by key.
      val fields: ArrayBuffer[Any] = ArrayBuffer.empty
      for (column <- columns.asScala) {
        val fieldValue = aggregationsObj.getJSONObject(column).getString(VALUE)
        fields += fieldValue
      }
      result += fields.toSeq
      return
    }
    val groupByName = groupByKeys(currentGroupByIndex)
    val groupObj = aggregationsObj.getJSONObject(groupByName)
    val bucketsArray = groupObj.getJSONArray(BUCKETS)
    val itr = bucketsArray.iterator
    var hasChildAggregate = true
    if (groupByKeys.size <= currentGroupByIndex + 1) {
      hasChildAggregate = false
    }
    while (itr.hasNext()) {
      val ele = itr.next().asInstanceOf[JSONObject]
      // val docCount = ele.getInt(DOC_COUNT)
      val groupValues = outerGroupValues.getOrElse(new HashMap[String, Any])
      if (hasChildAggregate) {
        val groupByKey = groupByName.stripPrefix(INNER_GROUP_ALIAS)
        if (columns.contains(groupByKey)) {
          val structField = schema(groupByKey)
          val groupValue = dataTypeTransform(ele, structField, KEY)
          groupValues.put(groupByKey, groupValue)
        }
        parseNestedAggregate(
          ele,
          columns,
          currentGroupByIndex + 1,
          groupByKeys,
          Some(groupValues),
          schema,
          result)
      } else {
        val fields: ArrayBuffer[Any] = ArrayBuffer.empty
        for (column <- columns.asScala) {
          var fieldValue: Any = null
          if (column.equals(INNER_AGGS_ALIAS + VALUE_COUNT)) {
            fieldValue = ele.getLong(DOC_COUNT)
          } else if (ele.containsKey(column)) {
            fieldValue = ele.getJSONObject(column).getString(VALUE)
          } else if (groupValues.contains(column)) {
            fieldValue = groupValues(column)
          } else {
            val structField = schema(column)
            fieldValue = dataTypeTransform(ele, structField, KEY)
          }
          fields += fieldValue
        }
        result += fields.toSeq
      }
    }
  }

  private def aggregateOrder(
      aggs: java.util.HashMap[String, Any],
      currentGroupByIndex: Int,
      groupByKeys: Seq[String],
      sort: java.util.List[java.util.HashMap[String, Any]],
      outerGroupValues: Option[HashMap[String, Any]],
      limit: Int): Unit = {
    if (groupByKeys == null) {
      return
    }
    val groupByName = groupByKeys(currentGroupByIndex)
    val groupMap = aggs.get(groupByName).asInstanceOf[java.util.HashMap[String, Any]]
    val termsMap = groupMap.get(TERMS).asInstanceOf[java.util.HashMap[String, Any]]
    termsMap.put(SIZE, limit)
    if (CollectionUtils.isNotEmpty(sort)) {
      sort.asScala.foreach { sortMap =>
        val field = termsMap.get(FIELD).asInstanceOf[String]
        val arr = new java.util.ArrayList[java.util.HashMap[String, Any]]
        if (sortMap.containsKey(field)) {
          val map = new java.util.HashMap[String, Any]
          map.put(ORDER_TERM, sortMap.get(field))
          arr.add(map)
        } else if (sortMap.containsKey(groupByName)) {
          val map = new java.util.HashMap[String, Any]
          map.put(groupByName, sortMap.get(field))
          arr.add(map)
        }
        termsMap.put(ORDER, arr)
      }
    }
  }

  /**
   * JSONObject will over transform Float to Double, Byte to Integer, Short to Integer and
   * Long to Integer when Long in Integer ranges.
   * See at : net.sf.json.util.JSONUtils#transformNumber
   */
  private def dataTypeTransform(jsonObj: JSONObject, field: StructField, column: String): Any = {
    doDataTypeTransform(jsonObj, field.dataType, column)
  }

  /**
   * Transform the field of `JSON` to data type with `DataType`.
   */
  private def doDataTypeTransform(
      jsonObj: JSONObject,
      dataType: DataType,
      column: String): Any = {
    var fieldValue: Any = null
    if (jsonObj.containsKey(column)) {
      fieldValue = dataType match {
        case FloatType => java.lang.Float.valueOf(jsonObj.getString(column))
        case ByteType => java.lang.Byte.valueOf(jsonObj.getString(column))
        case ShortType => java.lang.Short.valueOf(jsonObj.getString(column))
        case LongType => jsonObj.getLong(column)
        case DateType =>
          val value = jsonObj.getString(column)
          getFormatDate(value)
        case StructType(fields) =>
          val arr = new ArrayBuffer[Any]
          fields.foreach { x =>
            arr += doDataTypeTransform(jsonObj.getJSONObject(column), x.dataType, x.name)
          }
          Row(arr.toSeq: _*)
        case ArrayType(elementType, containsNull) =>
          val jsonArray = jsonObj.getJSONArray(column)
          val arr = new ArrayBuffer[Any]
          val itr = jsonArray.iterator()
          while (itr.hasNext()) {
            val ele = itr.next() match {
              case j: JSONObject => doDataTypeTransform(j, elementType, null)
              case other =>
                val makedJsonObj = new JSONObject
                makedJsonObj.put(TEMP_KEY, other)
                doDataTypeTransform(makedJsonObj, elementType, TEMP_KEY)
            }
            arr += ele
          }
          arr
        case _ => jsonObj.get(column)
      }
    } else {
      fieldValue = null
    }
    if (fieldValue.isInstanceOf[JSONNull]) {
      fieldValue = null
    }
    fieldValue
  }

  /**
   * Transform wildcard from SQL like to ES.
   */
  private def wildcardTransform(value: Any): String = {
    value.asInstanceOf[String].replace(UNDERLINE, QUESTION_MARK).replace(PERCENT_MARK, ASTERISK)
  }

  /**
   * Build sort argument of aggregate function used in aggregate sort.
   */
  private def buildAggFuncSortArgs(
      groupMap: java.util.HashMap[String, Any],
      groupAliasName: String,
      child: Expression,
      aggFunMap: java.util.HashMap[String, Any],
      aggFunName: String,
      direction: String): Unit = {
    val funMapOpt = Option(aggFunMap.get(aggFunName))
    if (funMapOpt != None) {
      val funMap = funMapOpt.get.asInstanceOf[java.util.HashMap[String, Any]]
      val field = funMap.get(FIELD).asInstanceOf[String]
      child match {
        case attr: UnresolvedAttribute =>
          val realName = attr.name.split("\\.").last
          if (field.equals(realName)) {
            val termsMap = groupMap.get(TERMS).asInstanceOf[java.util.HashMap[String, Any]]
            val orderMap = termsMap
              .getOrDefault(ORDER, new LinkedList[java.util.HashMap[String, Any]])
              .asInstanceOf[LinkedList[java.util.HashMap[String, Any]]]
            val directionMap = new java.util.HashMap[String, Any]
            directionMap.put(groupAliasName, direction)
            orderMap.add(directionMap)
            termsMap.put(ORDER, orderMap)
          } else if (groupAliasName.equals(realName)) {
            val termsMap = groupMap.get(TERMS).asInstanceOf[java.util.HashMap[String, Any]]
            val orderMap = termsMap
              .getOrDefault(ORDER, new LinkedList[java.util.HashMap[String, Any]])
              .asInstanceOf[LinkedList[java.util.HashMap[String, Any]]]
            val directionMap = new java.util.HashMap[String, Any]
            directionMap.put(groupAliasName, direction)
            orderMap.add(directionMap)
            termsMap.put(ORDER, orderMap)
          }
        case Literal(v, t) =>
          val value: Any = convertToScala(v, t)
          if (value == 1 && field.equals(INDEX)) {
            val termsMap = groupMap.get(TERMS).asInstanceOf[java.util.HashMap[String, Any]]
            val orderMap = termsMap
              .getOrDefault(ORDER, new LinkedList[java.util.HashMap[String, Any]])
              .asInstanceOf[LinkedList[java.util.HashMap[String, Any]]]
            val directionMap = new java.util.HashMap[String, Any]
            directionMap.put(groupAliasName, direction)
            orderMap.add(directionMap)
            termsMap.put(ORDER, orderMap)
          }
      }
    }
  }
}
