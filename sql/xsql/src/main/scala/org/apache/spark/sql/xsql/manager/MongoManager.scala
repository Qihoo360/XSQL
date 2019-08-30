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

import java.sql.{Date, Timestamp}
import java.util.LinkedList

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap}

import com.mongodb.{ConnectionString, MongoClientSettings, MongoCredential, MongoNamespace}
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.rdd.MongoRDD
import com.mongodb.spark.sql.MongoInferSchema
import com.mongodb.spark.sql.MongoInferSchema._
import com.mongodb.spark.sql.xsql.MongoUtils
import com.mongodb.spark.toSparkContextFunctions
import net.sf.json.JSONObject
import org.apache.commons.collections.CollectionUtils
import org.bson.{BsonDocument, BsonType, BsonValue, Document}

import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{
  Ascending,
  Descending,
  Expression,
  SortDirection,
  SortOrder
}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.xsql._
import org.apache.spark.sql.xsql.DataSourceType.MONGO
import org.apache.spark.sql.xsql.types.MONGO_TYPE_STRING

/**
 * Manager for MongoDB.
 */
private[xsql] class MongoManager(conf: SparkConf) extends DataSourceManager with Logging {

  def this() = {
    this(null)
  }

  import DataSourceManager._
  import MongoManager._

  override def shortName(): String = MONGO.toString

  private var mongoClient: MongoClient = _

  override protected def cacheDatabase(
      isDefault: Boolean,
      dataSourceName: String,
      infos: Map[String, String],
      dataSourcesByName: HashMap[String, CatalogDataSource],
      dataSourceToCatalogDatabase: HashMap[String, HashMap[String, CatalogDatabase]]): Unit = {
    val url = infos.get(URL)
    if (url == None) {
      throw new SparkException("Data source is MongoDB must have url!")
    }
    val ds: CatalogDataSource = new MongoDataSource(
      dataSourceName,
      MONGO,
      this,
      url.get,
      infos(USER),
      infos(PASSWORD),
      infos(VERSION))
    dataSourcesByName(ds.getName) = ds
    val credential = MongoCredential.createCredential(
      ds.getUser,
      infos.getOrElse("authenticationDatabase", ADMIN),
      ds.getPwd.toCharArray())
    mongoClient = MongoClients.create(
      MongoClientSettings
        .builder()
        .applyConnectionString(new ConnectionString(ds.getUrl))
        .credential(credential)
        .build())

    val dbCursor = mongoClient.listDatabaseNames().iterator()
    val xdatabases = dataSourceToCatalogDatabase.getOrElseUpdate(
      ds.getName,
      new HashMap[String, CatalogDatabase])
    while (dbCursor.hasNext()) {
      val dbName = dbCursor.next()
      dbName match {
        case MONITOR | ADMIN | LOCAL | CONFIG =>
          logDebug(s"We do not parse Inner MongoDB $dbName")
        case _ =>
          logDebug(s"Parse MongoDB $dbName")
          val db = CatalogDatabase(
            id = newDatabaseId,
            dataSourceName = dataSourceName,
            name = dbName,
            description = null,
            locationUri = null,
            properties = Map.empty)
          xdatabases += ((db.name, db))
      }
    }
  }

  override protected def cacheTable(
      dataSourceName: String,
      dataSourceToCatalogDatabase: HashMap[String, HashMap[String, CatalogDatabase]],
      dbToCatalogTable: HashMap[Int, HashMap[String, CatalogTable]]): Unit = {
    val xdatabases = dataSourceToCatalogDatabase(dataSourceName)
    val url = cachedProperties(URL)
    xdatabases.foreach { kv =>
      val dbName = kv._1
      val db = kv._2
      val tableSchemasMap = schemasMap.get(dbName)
      val xtables = dbToCatalogTable.getOrElseUpdate(db.id, new HashMap[String, CatalogTable])
      val mongoDB = mongoClient.getDatabase(dbName)
      val collectionCursor = mongoDB.listCollectionNames().iterator()
      while (collectionCursor.hasNext()) {
        val collectionName = collectionCursor.next()
        if (tableSchemasMap != None && tableSchemasMap.get.contains(collectionName)) {
          val fieldsOpt = tableSchemasMap.get.get(collectionName)
          if (fieldsOpt != None) {
            val itr = fieldsOpt.get.iterator()
            val fields: ArrayBuffer[StructField] = ArrayBuffer.empty
            while (itr.hasNext()) {
              val propertyObj = itr.next().asInstanceOf[JSONObject]
              val fieldName = propertyObj.getString(NAME)
              if (propertyObj.get(SCHEMA_TYPE) != null) {
                fields += MongoUtils.convertField(fieldName, propertyObj, null)
              }
            }
            val schema = StructType(fields)
            val table = CatalogTable(
              identifier = TableIdentifier(collectionName, Option(dbName), Option(dataSourceName)),
              tableType = CatalogTableType.COLLECTION,
              storage = CatalogStorageFormat(
                locationUri = None,
                // To avoid ClassNotFound exception, we try our best to not get the format class,
                // but get the class name directly. However, for non-native tables,
                // there is no interface to get the format class name,
                // so we may still throw ClassNotFound in this case.
                inputFormat = None,
                outputFormat = None,
                serde = None,
                compressed = false,
                properties = Map(
                  MONGODB_INPUT_URI -> url,
                  MONGODB_INPUT_DATABASE -> dbName,
                  MONGODB_INPUT_COLLECTION -> collectionName)),
              schema = schema,
              provider = Some(FULL_PROVIDER))
            xtables += ((collectionName, table))
          }
        }
      }
    }
  }

  override def getDefaultOptions(table: CatalogTable): Map[String, String] = {
    assert(table.identifier.dataSource.isDefined && table.identifier.database.isDefined)
    val dataSourceName = table.identifier.dataSource.get
    val url = cachedProperties(URL)
    val database = table.identifier.database.get
    val collection = table.identifier.table
    val options = new HashMap[String, String]
    options += ((MONGODB_INPUT_URI, url))
    options += ((MONGODB_INPUT_DATABASE, database))
    options += ((MONGODB_INPUT_COLLECTION, collection))
    options.toMap
  }

  override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {
    val db = mongoClient.getDatabase(dbDefinition.name)
//    db.createCollection(DEFAULT_COLLECTION)
    val document = new Document("x", 1)
    db.getCollection(DEFAULT_COLLECTION).insertOne(document)
    throw new UnsupportedOperationException(
      "Create MongoDB not supported by driver! Please execute CMD," +
        " reference https://docs.mongodb.com/manual/core/databases-and-collections/")
  }

  override def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = {
    mongoClient.getDatabase(db).drop()
  }

  override def listTables(dsName: String, dbName: String): Seq[String] = {
    val mongoDB = mongoClient.getDatabase(dbName)
    mongoDB.listCollectionNames().asScala.toSeq
  }

  override def doGetRawTable(
      db: String,
      originDB: String,
      table: String): Option[CatalogTable] = {
    if (tableExists(db, table)) {
      val sc = SparkContext.getActive.get
      val url = cachedProperties(URL)
      val options = new HashMap[String, String]
      options += ((MONGODB_INPUT_URI, url))
      options += ((MONGODB_INPUT_DATABASE, originDB))
      options += ((MONGODB_INPUT_COLLECTION, table))
      val mongoRDD: MongoRDD[org.bson.BsonDocument] =
        sc.loadFromMongoDB(ReadConfig(options.toMap))
      val schema = MongoInferSchema(mongoRDD)
      val catalogTable = CatalogTable(
        identifier = TableIdentifier(table, Option(db), Option(dsName)),
        tableType = CatalogTableType.COLLECTION,
        storage = CatalogStorageFormat(
          locationUri = None,
          inputFormat = None,
          outputFormat = None,
          serde = None,
          compressed = false,
          properties = Map(
            MONGODB_INPUT_URI -> url,
            MONGODB_INPUT_DATABASE -> originDB,
            MONGODB_INPUT_COLLECTION -> table)),
        schema = schema,
        provider = Some(FULL_PROVIDER))
      Option(catalogTable)
    } else {
      None
    }
  }

  override def tableExists(dbName: String, table: String): Boolean = {
    val list = mongoClient.getDatabase(dbName).listCollectionNames().asScala.toList
    list.contains(table)
  }

  override def createTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit = {
    val dbName = tableDefinition.database
    val collectionName = tableDefinition.identifier.table
    verifyColumnDataType(tableDefinition.schema)
    mongoClient.getDatabase(dbName).createCollection(collectionName)
  }

  override def renameTable(dbName: String, oldName: String, newName: String): Unit = {
    val newCollectionNamespace = new MongoNamespace(dbName, newName)
    mongoClient
      .getDatabase(dbName)
      .getCollection(oldName)
      .renameCollection(newCollectionNamespace)
  }

  override def dropTable(
      dbName: String,
      tableName: String,
      ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = {
    if (purge) {
      throw new UnsupportedOperationException("DROP TABLE ... PURGE")
    }
    mongoClient.getDatabase(dbName).getCollection(tableName).drop()
  }

  override def buildAndArgument(
      searchArg: java.util.HashMap[String, Any],
      leftSearchArg: java.util.HashMap[String, Any],
      rightSearchArg: java.util.HashMap[String, Any]): Unit = {
    val seq = leftSearchArg.entrySet().asScala ++ rightSearchArg.entrySet().asScala
    val docs = seq.map { entry =>
      new Document(entry.getKey, entry.getValue)
    }
    val doc = new Document(AND, docs.asJava)
    searchArg.put(TEMP_ATTRIBUTE, doc)
  }

  override def buildOrArgument(
      searchArg: java.util.HashMap[String, Any],
      leftSearchArg: java.util.HashMap[String, Any],
      rightSearchArg: java.util.HashMap[String, Any]): Unit = {
    val seq = leftSearchArg.entrySet().asScala ++ rightSearchArg.entrySet().asScala
    val docs = seq.map { entry =>
      new Document(entry.getKey, entry.getValue)
    }
    val doc = new Document(OR, docs.asJava)
    searchArg.put(TEMP_ATTRIBUTE, doc)
  }

  override def buildNotArgument(
      searchArg: java.util.HashMap[String, Any],
      childSearchArg: java.util.HashMap[String, Any]): Unit = {
    childSearchArg.keySet().asScala.foreach { key =>
      val subDoc = childSearchArg.get(key).asInstanceOf[Document]
      val newDoc = new Document
      subDoc.keySet().asScala.foreach { x =>
        x match {
          case EQ =>
            val value: Any = subDoc.get(EQ)
            newDoc.append(NE, value)
          case IN =>
            val value: Any = subDoc.get(IN)
            newDoc.append(NIN, value)
        }
      }
      searchArg.put(key, newDoc)
    }
  }

  override def buildEqualToArgument(
      searchArg: java.util.HashMap[String, Any],
      attribute: String,
      value: Any): Unit = {
    val doc = new Document(EQ, value)
    searchArg.put(attribute, doc)
  }

  override def buildLessThanArgument(
      searchArg: java.util.HashMap[String, Any],
      attribute: String,
      value: Any): Unit = {
    val doc = new Document(LT, value)
    searchArg.put(attribute, doc)
  }

  override def buildLessThanOrEqualArgument(
      searchArg: java.util.HashMap[String, Any],
      attribute: String,
      value: Any): Unit = {
    val doc = new Document(LTE, value)
    searchArg.put(attribute, doc)
  }

  override def buildGreaterThanArgument(
      searchArg: java.util.HashMap[String, Any],
      attribute: String,
      value: Any): Unit = {
    val doc = new Document(GT, value)
    searchArg.put(attribute, doc)
  }

  override def buildGreaterThanOrEqualArgument(
      searchArg: java.util.HashMap[String, Any],
      attribute: String,
      value: Any): Unit = {
    val doc = new Document(GTE, value)
    searchArg.put(attribute, doc)
  }

  override def buildIsNullArgument(
      searchArg: java.util.HashMap[String, Any],
      childSearchArg: java.util.HashMap[String, Any]): Unit = {
    val attribute = childSearchArg.get(ATTRIBUTE).asInstanceOf[String]
    val doc = new Document(EQ, null)
    searchArg.put(attribute, doc)
  }

  override def buildIsNotNullArgument(
      searchArg: java.util.HashMap[String, Any],
      childSearchArg: java.util.HashMap[String, Any]): Unit = {
    val attribute = childSearchArg.get(ATTRIBUTE).asInstanceOf[String]
    val doc = new Document(NE, null)
    searchArg.put(attribute, doc)
  }

  override def buildLikeArgument(
      searchArg: java.util.HashMap[String, Any],
      attribute: String,
      value: Any): Unit = {
    val doc = new Document(REGEX, wildcardTransform(value))
    searchArg.put(attribute, doc)
  }

  override def buildRLikeArgument(
      searchArg: java.util.HashMap[String, Any],
      attribute: String,
      value: Any): Unit = {
    val doc = new Document(REGEX, value)
    searchArg.put(attribute, doc)
  }

  override def buildInArgument(
      searchArg: java.util.HashMap[String, Any],
      attribute: String,
      values: LinkedList[Any]): Unit = {
    val doc = new Document(IN, values)
    searchArg.put(attribute, doc)
  }

  override def buildAttributeArgument(
      searchArg: java.util.HashMap[String, Any],
      attribute: String): Unit = {
    searchArg.put(ATTRIBUTE, attribute)
  }

  override def buildGroupArgument(
      fieldName: String,
      groupByKeys: ArrayBuffer[String],
      innerAggsMap: java.util.HashMap[String, Any],
      groupMap: java.util.HashMap[String, Any]): java.util.HashMap[String, Any] = {
    val groupKeyDoc = new Document(fieldName, DOLLAR + fieldName)
    if (innerAggsMap.containsKey(MONGO_ID)) {
      val appendGroupKey = innerAggsMap.get(MONGO_ID).asInstanceOf[java.util.Map[String, Any]]
      groupKeyDoc.putAll(appendGroupKey)
      innerAggsMap.put(MONGO_ID, groupKeyDoc)
      groupMap
    } else {
      innerAggsMap.put(MONGO_ID, groupKeyDoc)
      groupMap.put(GROUP, innerAggsMap)
      groupMap
    }
  }

  override def buildAggregateArgument(
      structField: StructField,
      aggregatefuncName: String,
      aggsMap: java.util.HashMap[String, Any],
      aggregateAliasOpts: Option[String]): StructField = {
    val fieldName = structField.name
    var usedAggsFieldName = fieldName match {
      case STAR => 1
      case _ => fieldName
    }
    val usedAggsFuncName = aggregatefuncName match {
      case COUNT | COUNT_DISTINCT =>
        usedAggsFieldName = 1
        MONGO_COUNT
      case _ => aggregatefuncName
    }
    var aggregateFunDoc: Document = null
    if (usedAggsFieldName == 1) {
      aggregateFunDoc = new Document(DOLLAR + usedAggsFuncName, usedAggsFieldName)
    } else {
      aggregateFunDoc = new Document(DOLLAR + usedAggsFuncName, DOLLAR + usedAggsFieldName)
    }
    val aggregateOutMap =
      aggsMap
        .getOrDefault(AGGS, new java.util.HashMap[String, Any])
        .asInstanceOf[java.util.HashMap[String, Any]]
    if (COUNT_DISTINCT.equals(aggregatefuncName)) {
      if (aggsMap.containsKey(MONGO_ID)) { // select count(distinct column) with group by
        val groupKeyDoc = aggsMap.get(MONGO_ID).asInstanceOf[Document]
        val newgroupKeyDoc = new Document()
        groupKeyDoc.entrySet().asScala.foreach { entry =>
          val key = entry.getKey
          newgroupKeyDoc.append(key, DOLLAR + MONGO_ID + POINT + key)
        }
        groupKeyDoc.append(fieldName, DOLLAR + fieldName)
        aggsMap.put(MONGO_ID, (groupKeyDoc, newgroupKeyDoc))
      } else { // select count(distinct column) without group by
        aggregateOutMap.put(COUNT_DISTINCT, new Document(fieldName, DOLLAR + fieldName))
      }
    }
    val aggregateAliasName = aggregateAliasOpts.getOrElse(
      INNER_AGGS_ALIAS + usedAggsFuncName + UNDERLINE + usedAggsFieldName)
    aggregateOutMap.put(aggregateAliasName, aggregateFunDoc)
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
          map.put(realName, sortTransform(x.direction))
          sortArg.add(map)
      }
    }
    sortArg
  }

  private def buildAggFuncSortArgs(
      aggsMap: java.util.HashMap[String, Any],
      aggFunName: String): Option[String] = {
    val usedAggsFuncName = aggFunName match {
      case COUNT | COUNT_DISTINCT =>
        MONGO_COUNT
      case _ => aggFunName
    }
    val option = aggsMap.asScala.find { kv =>
      val doc = kv._2.asInstanceOf[Document]
      doc.containsKey(DOLLAR + usedAggsFuncName)
    }
    if (option == None) {
      logWarning(
        s"Order by clause followed by Aggregation Function $aggFunName" +
          "that not appear in projection clause. ")
      None
    } else {
      Some(option.get._1)
    }
  }

  override def buildSortArgumentInAggregate(
      parentGroupMap: java.util.HashMap[String, Any],
      aggs: java.util.HashMap[String, Any],
      expr: Expression,
      direction: SortDirection,
      isDistinct: Boolean): Unit = {
    var sortKey: Option[String] = null
    if (expr.isInstanceOf[UnresolvedAttribute]) {
      val attr = expr.asInstanceOf[UnresolvedAttribute]
      val realName = attr.name.split("\\.").last
      val groupMap = aggs.get(GROUP).asInstanceOf[java.util.HashMap[String, Any]]
      groupMap.get(MONGO_ID) match {
        case singleIdDoc: Document =>
          if (singleIdDoc.containsKey(realName)) {
            sortKey = Some(MONGO_ID + POINT + realName)
          } else {
            sortKey = Some(realName)
          }
        case doubleIdDoc: (Document, Document) =>
          if (doubleIdDoc._2.containsKey(realName)) {
            sortKey = Some(MONGO_ID + POINT + realName)
          } else {
            sortKey = Some(realName)
          }
      }
    } else { // Parse sort with aggregate function.
      val groupMap = aggs.get(GROUP).asInstanceOf[java.util.HashMap[String, Any]]
      val aggsMap = groupMap.get(AGGS).asInstanceOf[java.util.HashMap[String, Any]]
      expr match {
        case sum @ Sum(child) => sortKey = buildAggFuncSortArgs(aggsMap, sum.prettyName)
        case avg @ Average(child) => sortKey = buildAggFuncSortArgs(aggsMap, avg.prettyName)
        case max @ Max(child) => sortKey = buildAggFuncSortArgs(aggsMap, max.prettyName)
        case min @ Min(child) => sortKey = buildAggFuncSortArgs(aggsMap, min.prettyName)
        case count @ Count(children) => sortKey = buildAggFuncSortArgs(aggsMap, count.prettyName)
      }
    }
    if (sortKey == None) return
    val sortKeyDoc = aggs.getOrDefault(SORT, new Document).asInstanceOf[Document]
    sortKeyDoc.append(sortKey.get, sortTransform(direction))
    aggs.put(SORT, sortKeyDoc)
  }

  override def buildDistinctArgument(
      columns: java.util.List[String]): (java.util.HashMap[String, Any], Seq[String]) = {
    var groupMap = new java.util.HashMap[String, Any]
    val innerAggsMap = new java.util.HashMap[String, Any]
    val groupByKeys: ArrayBuffer[String] = ArrayBuffer.empty
    columns.asScala.reverse.foreach { column =>
      buildGroupArgument(column, groupByKeys, innerAggsMap, groupMap)
    }
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
    val schema = tableDefinition.schema
    val projectionDoc = new Document
    val sortDoc = new Document
    val collection: MongoCollection[BsonDocument] =
      mongoClient
        .getDatabase(tableDefinition.identifier.database.get)
        .getCollection(tableDefinition.identifier.table, classOf[BsonDocument])
    if (aggs == null || aggs.size() == 0) {
      val filterDoc = buildFilterDoc(schema, condition)
      if (columns != null && !columns.isEmpty) {
        columns.asScala.foreach { column =>
          projectionDoc.append(column, 1)
        }
      }
      if (CollectionUtils.isNotEmpty(sort)) {
        sort.asScala.foreach { map =>
          map.asScala.iterator.foreach { kv =>
            sortDoc.append(kv._1, kv._2)
          }
        }
      }
      val findItr =
        collection.find(filterDoc).projection(projectionDoc).sort(sortDoc).limit(limit)
      val result: ArrayBuffer[Seq[Any]] = ArrayBuffer.empty
      findItr.asScala.foreach { x =>
        val fields: ArrayBuffer[Any] = ArrayBuffer.empty
        for (column <- columns.asScala) {
          val structField = schema(column)
          val fieldValue = dataTypeTransform(x, structField, column)
          fields += fieldValue
        }
        result += fields.toSeq
      }
      result
    } else {
      val pipeline = new LinkedList[Document]
      // match stage.
      val filterDoc = buildFilterDoc(schema, condition)
      if (!filterDoc.isEmpty()) {
        val matchDoc = new Document(MATCH, filterDoc)
        pipeline.add(matchDoc)
      }
      if (aggs.containsKey(SORT)) {
        val sortKeyDoc = aggs.remove(SORT).asInstanceOf[Document]
        sortDoc.append(SORT, sortKeyDoc)
      }
      // aggregation stage.
      val aggsDocs = ArrayBuffer.empty[Document]
      aggs.entrySet().asScala.foreach { entry =>
        val key = entry.getKey
        if (GROUP.equals(key)) {
          val subMap = entry.getValue.asInstanceOf[java.util.HashMap[String, Any]]
          subMap.entrySet().asScala.foreach { subEntry =>
            val subKey = subEntry.getKey
            if (AGGS.equals(subKey)) {
              val grandMap = subEntry.getValue.asInstanceOf[java.util.HashMap[String, Any]]
              val groupDoc = aggsDocs.last.get(GROUP).asInstanceOf[Document]
              grandMap.entrySet().asScala.foreach { grandEntry =>
                groupDoc.append(grandEntry.getKey, grandEntry.getValue)
              }
            } else {
              subEntry.getValue match {
                case singleIdDoc: Document =>
                  aggsDocs += new Document(GROUP, new Document(subKey, singleIdDoc))
                case doubleIdDoc: (Document, Document) =>
                  // select count(distinct column) with group by
                  aggsDocs += new Document(GROUP, new Document(subKey, doubleIdDoc._1))
                  aggsDocs += new Document(GROUP, new Document(subKey, doubleIdDoc._2))
              }
            }
          }
        } else if (COUNT_DISTINCT.equals(key)) {
          aggsDocs += new Document(GROUP, new Document(MONGO_ID, entry.getValue))
        } else {
          if (aggsDocs.isEmpty) {
            aggsDocs += new Document(GROUP, NULL_ID_DOC)
          } else { // select count(distinct column) without group by
            val groupDoc = aggsDocs.last.get(GROUP).asInstanceOf[Document]
            if (groupDoc.get(MONGO_ID) != null) {
              aggsDocs += new Document(GROUP, NULL_ID_DOC)
            }
          }
          val groupDoc = aggsDocs.last.get(GROUP).asInstanceOf[Document]
          groupDoc.append(key, entry.getValue)
        }
      }
      val groupDoc = aggsDocs.last.get(GROUP).asInstanceOf[Document]
      aggsDocs.foreach { aggsDoc =>
        pipeline.add(aggsDoc)
      }
      // projection stage.
      val projectionInnerDoc = new Document
      var idNotUsed = true
      val idDoc = groupDoc.get(MONGO_ID).asInstanceOf[Document]
      for (column <- columns.asScala) {
        if (idDoc != null && idDoc.containsKey(column)) {
          idNotUsed = false
          // Avoid pull other columns make up '_id'.
          projectionInnerDoc.append(MONGO_ID + POINT + column, PROJECTION_SELECTED)
        } else {
          projectionInnerDoc.append(column, PROJECTION_SELECTED)
        }
      }
      if (idNotUsed) { // Avoid pull '_id' when which not used.
        projectionInnerDoc.append(MONGO_ID, PROJECTION_UNSELECTED)
      }
      projectionDoc.append(PROJECT, projectionInnerDoc)
      if (!projectionDoc.isEmpty()) {
        pipeline.add(projectionDoc)
      }
      // sort stage.
      if (!sortDoc.isEmpty()) {
        pipeline.add(sortDoc)
      }
      // limit stage.
      pipeline.add(new Document(LIMIT, limit))
      val aggIter = collection.aggregate(pipeline)
      val result: ArrayBuffer[Seq[Any]] = ArrayBuffer.empty
      aggIter.asScala.foreach { doc =>
        val fields: ArrayBuffer[Any] = ArrayBuffer.empty
        for (column <- columns.asScala) {
          var fieldValue: Any = null
          if (doc.containsKey(column)) {
            fieldValue = doDataTypeTransform(doc.get(column), null)
          } else if (doc.containsKey(MONGO_ID)) {
            val idDoc = doc.get(MONGO_ID).asInstanceOf[BsonDocument]
            val structField = schema(column)
            fieldValue = dataTypeTransform(idDoc, structField, column)
          } else {
            val structField = schema(column)
            fieldValue = dataTypeTransform(doc, structField, MONGO_ID)
          }
          fields += fieldValue
        }
        result += fields.toSeq
      }
      result
    }
  }

  def convertToStruct(fields: Seq[StructField]): StructType = {
    DataTypes.createStructType(fields.toArray)
  }

  override def stop(): Unit = {
    mongoClient.close()
  }

}

object MongoManager {
  import DataSourceManager._
  val MONITOR = "360monitor"
  val ADMIN = "admin"
  val LOCAL = "local"
  val CONFIG = "config"
  val FULL_PROVIDER = "com.mongodb.spark.sql"
  val PROVIDER = "mongo"
  val DEFAULT_COLLECTION = "test"
  val ATTRIBUTE = "attribute"
  val TEMP_ATTRIBUTE = "temp_attribute"
  val EQ = "$eq"
  val NE = "$ne"
  val GTE = "$gte"
  val GT = "$gt"
  val LTE = "$lte"
  val LT = "$lt"
  val IN = "$in"
  val NIN = "$nin"
  val AND = "$and"
  val OR = "$or"
  val REGEX = "$regex"
  val MATCH = "$match"
  val GROUP = "$group"
  val PROJECT = "$project"
  val SORT = "$sort"
  val LIMIT = "$limit"
  val QUESTION_MARK = "?"
  val ASTERISK = "*"
  val CARET = "^"
  val DOLLAR = "$"
  val POINT = "."
  val DEFAULT_SIZE = 10
  val PROJECTION_SELECTED = 1
  val PROJECTION_UNSELECTED = 0
  val MONGO_ID = "_id"
  val MONGO_COUNT = "sum"
  private val STARTS_WITH = "([^_%]+)%".r
  private val ENDS_WITH = "%([^_%]+)".r
  private val STARTS_AND_ENDS_WITH = "([^_%]+)%([^_%]+)".r
  private val CONTAINS = "%([^_%]+)%".r
  private val EQUAL_TO = "([^_%]*)".r
  private val ENDS_UNDERLINE = "([^_%]+)_".r
  private val STARTS_UNDERLINE = "_([^_%]+)".r
  private val MIDDLE_UNDERLINE = "([^_%]+)_([^_%]+)".r
  private val STARTS_AND_ENDS_UNDERLINE = "_([^_%]+)_".r
  val MONGODB_INPUT_URI = s"${ReadConfig.configPrefix}${ReadConfig.mongoURIProperty}"
  val MONGODB_INPUT_DATABASE = s"${ReadConfig.configPrefix}${ReadConfig.databaseNameProperty}"
  val MONGODB_INPUT_COLLECTION = s"${ReadConfig.configPrefix}${ReadConfig.collectionNameProperty}"
  val NULL_ID_DOC = new Document("_id", null)

  def verifyColumnDataType(schema: StructType): Unit = {
    schema.foreach(col => getSparkSQLDataType(toMongoColumn(col)._2))
  }

  private def checkMongoColumnType(mongoType: String): Unit = {
    if (mongoType.equalsIgnoreCase("float") ||
        mongoType.equalsIgnoreCase("byte") ||
        mongoType.equalsIgnoreCase("short")) {
      throw new SparkException(s"MongoDB not support $mongoType type!")
    }
  }

  /** Builds the native StructField from Mongo's property. */
  private def fromMongoColumn(fieldName: String, propertyObj: JSONObject): StructField = {
    val mongoType = propertyObj.get(SCHEMA_TYPE).asInstanceOf[String]
    checkMongoColumnType(mongoType)
    val columnType = getSparkSQLDataType(mongoType)
    val metadata = if (mongoType != columnType.catalogString) {
      new MetadataBuilder().putString(MONGO_TYPE_STRING, mongoType).build()
    } else {
      Metadata.empty
    }

    StructField(name = fieldName, dataType = columnType, nullable = true, metadata = metadata)
  }

  /** Converts the native StructField to ES's name and type. */
  private def toMongoColumn(c: StructField): (String, String) = {
    val typeString = if (c.metadata.contains(MONGO_TYPE_STRING)) {
      c.metadata.getString(MONGO_TYPE_STRING)
    } else {
      c.dataType.typeName
    }
    (c.name, typeString)
  }

  private def dataTypeTransform(doc: BsonDocument, field: StructField, column: String): Any = {
    doDataTypeTransform(doc.get(column), field.dataType)
  }

  private def doDataTypeTransform(element: BsonValue, dataType: DataType): Any = {
    var fieldValue: Any = null
    if (element != null) {
      fieldValue = (element.getBsonType, dataType) match {
        case (BsonType.DOCUMENT, mapType: MapType) =>
          element
            .asDocument()
            .asScala
            .map(kv => (kv._1, doDataTypeTransform(kv._2, mapType.valueType)))
            .toMap
        case (BsonType.ARRAY, arrayType: ArrayType) =>
          element.asArray().getValues.asScala.map(doDataTypeTransform(_, arrayType.elementType))
        case (BsonType.BINARY, BinaryType) => element.asBinary().getData
        case (BsonType.BOOLEAN, BooleanType) => element.asBoolean().getValue
        case (BsonType.DATE_TIME, DateType) => new Date(element.asDateTime().getValue)
        case (BsonType.DATE_TIME, TimestampType) => new Timestamp(element.asDateTime().getValue)
        case (BsonType.NULL, NullType) => null
        case (_, IntegerType) => MongoUtils.toInt(element)
        case (_, LongType) => MongoUtils.toLong(element)
        case (_, DoubleType) => MongoUtils.toDouble(element)
        case (_, DecimalType()) => MongoUtils.toDecimal(element)
        case (_, elementType @ StructType(fields)) =>
          MongoUtils.castToStructType(element, elementType)
        case (_, StringType) => MongoUtils.bsonValueToString(element)
        case _ =>
          element.getBsonType match {
            case BsonType.INT32 | BsonType.INT64 => MongoUtils.toInt(element).toString()
            case BsonType.DOUBLE => MongoUtils.toDouble(element).toString()
            case BsonType.DECIMAL128 => MongoUtils.toDecimal(element).toString()
            case _ => element
          }
      }
    }

    fieldValue
  }

  private def conditionTransform(dataType: DataType, value: Any): Any = {
    dataType match {
      case DateType =>
        val originDoc = value.asInstanceOf[Document]
        val itr = originDoc.keySet().iterator()
        val newDoc = new Document
        while (itr.hasNext()) {
          val key = itr.next()
          val dateStr = originDoc.get(key).asInstanceOf[String]
          val date = org.apache.spark.sql.xsql.util.Utils.getFormatDate(dateStr)
          newDoc.append(key, date)
        }
        newDoc
      case _ => value
    }
  }

  /**
   * Transform wildcard from SQL like to MongoDB.
   */
  private def wildcardTransform(value: Any): String = {
    value.asInstanceOf[String] match {
      case STARTS_WITH(prefix) if !prefix.endsWith("\\") =>
        CARET + prefix
      case ENDS_WITH(postfix) =>
        postfix + DOLLAR
      case STARTS_AND_ENDS_WITH(prefix, postfix) if !prefix.endsWith("\\") =>
        CARET + prefix + POINT + ASTERISK + postfix + DOLLAR
      case CONTAINS(infix) if !infix.endsWith("\\") =>
        infix
      case EQUAL_TO(str) =>
        str
      case ENDS_UNDERLINE(prefix) if !prefix.endsWith("\\") =>
        prefix + POINT + QUESTION_MARK + DOLLAR
      case STARTS_UNDERLINE(postfix) =>
        CARET + POINT + QUESTION_MARK + postfix
      case STARTS_AND_ENDS_UNDERLINE(infix) if !infix.endsWith("\\") =>
        CARET + POINT + QUESTION_MARK + infix + POINT + QUESTION_MARK + DOLLAR
      case MIDDLE_UNDERLINE(prefix, postfix) if !prefix.endsWith("\\") =>
        CARET + prefix + POINT + QUESTION_MARK + postfix + DOLLAR
    }
  }

  /**
   * Transform sort from SQL to MongoDB.
   */
  private def sortTransform(direction: SortDirection): Int =
    direction match {
      case Ascending => 1
      case Descending => -1
    }

  /**
   * Build filter Document of MongoDB by condition map.
   */
  private def buildFilterDoc(
      schema: StructType,
      condition: java.util.HashMap[String, Any]): Document = {
    val filterDoc = new Document
    if (condition != null && condition.size() > 0) {
      condition.asScala.foreach { kv =>
        kv._1 match {
          case TEMP_ATTRIBUTE =>
            val originDoc = kv._2.asInstanceOf[Document]
            originDoc.entrySet().asScala.foreach { entry =>
              filterDoc.append(entry.getKey, entry.getValue)
            }
          case _ =>
            val subDoc = conditionTransform(schema(kv._1).dataType, kv._2)
            filterDoc.append(kv._1, subDoc)
        }
      }
    }
    filterDoc
  }
}
