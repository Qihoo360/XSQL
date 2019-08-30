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

package org.apache.spark.sql.execution.datasources.druid

import com.ning.http.client.{
  AsyncCompletionHandler,
  AsyncHttpClient,
  AsyncHttpClientConfig,
  Response
}
import org.json4s._
import org.json4s.jackson._
import org.json4s.jackson.JsonMethods._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

import org.apache.spark.internal.Logging

/**
 * Druid query client
 * reference https://github.com/daggerrz/druid-scala-client and changed based on this
 */
case class DruidClient(serverUrl: String)(implicit val executionContext: ExecutionContext)
  extends Logging {
  private val config = new AsyncHttpClientConfig.Builder()
  private val client = new AsyncHttpClient(config.build())
  private val url = s"$serverUrl/druid/v2/?pretty"
  var coordinator: String = _

  def JsonPost(body: String): AsyncHttpClient#BoundRequestBuilder = {

    client
      .preparePost(url)
      .setHeader("Content-Type", "application/json")
      .setBodyEncoding("UTF-8")
      .setBody(body)
  }

  def UrlGet(url: String): AsyncHttpClient#BoundRequestBuilder = {
    client.prepareGet(url)
  }

  private def parseJson(resp: Response): JValue = {
    val body = resp.getResponseBody("UTF-8")
    parse(body)
  }

  /**
   * druid TimeSeries Query
   */
  def apply(ts: TimeSeriesQuery): Future[TimeSeriesResponse] = {
    execute(ts.toJson, TimeSeriesResponse.parse)
  }

  /**
   * druid GroupBy Query
   */
  def apply(ts: GroupByQuery): Future[GroupByResponse] = {
    execute(ts.toJson, GroupByResponse.parse)
  }

  /**
   * druid Select Query
   */
  def apply(ts: SelectQuery): Future[SelectResponse] = {
    execute(ts.toJson, SelectResponse.parse)
  }

  /**
   *  query entrance
   */
  private def execute[R](js: JValue, parser: JValue => R): Future[R] = {
    val p = Promise[Response]
    val body = compactJson(js)
    log.debug(body + "\n")
    JsonPost(body).execute(new AsyncCompletionHandler[Response] {
      override def onCompleted(response: Response): Response = {
        p.success(response)
        response
      }
    })
    p.future.map(parseJson).map(parser)
  }

  private def getExecute[R](url: String, parser: JValue => R): Future[R] = {
    val p = Promise[Response]
    UrlGet(url).execute(new AsyncCompletionHandler[Response] {
      override def onCompleted(response: Response): Response = {
        p.success(response)
        response
      }
    })
    p.future.map(parseJson).map(parser)
  }

  def queryTimeSeries(query: TimeSeriesQuery): Future[TimeSeriesResponse] = {
    execute(query.toJson, TimeSeriesResponse.parse)
  }

  def queryTopN(query: TopNSelectQuery): Future[TopNSelectResponse] = {
    execute(query.toJson, TopNSelectResponse.parse)
  }
  def queryTopN2(query: TopNSelectQuery): Future[TopN2SelectResponse] = {
    execute(query.toJson, TopN2SelectResponse.parse)
  }

  def queryGroupBy(query: GroupByQuery): Future[GroupByResponse] = {
    execute(query.toJson, GroupByResponse.parse)
  }

  def querySelect(query: SelectQuery): Future[SelectResponse] = {
    execute(query.toJson, SelectResponse.parse)
  }

  /**
   * query Druid datasources
   */
  def showTables(url: String): Seq[String] = {
    val future = getExecute(s"$serverUrl".concat(url), DataSourceScannerResponse.parse)
    var data: Seq[String] = null
    future.onComplete {
      case Success(resp) => data = resp.data
      case Failure(ex) =>
        ex.printStackTrace()
    }
    while (!future.isCompleted) {
      Thread.sleep(500)
    }
    data
  }

  /**
   * query Druid datasource fileds include dimensions and metrics
   */
  def descTable(datasouceName: String): Seq[(String, Any)] = {
    val future = execute(DescTableRequest(datasouceName).toJson, DescTableResponse.parse)
    var data: Seq[(String, Any)] = null
    future.onComplete {
      case Success(resp) => data = resp.data
      case Failure(ex) => ex.printStackTrace()
    }
    while (!future.isCompleted) {
      Thread.sleep(500)
    }
    data
  }

  def close(): Unit = {
    client.close()
  }
}
