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

import org.joda.time.Interval
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

case class SelectQuery(
    source: String,
    interval: Interval,
    descending: String = "false",
    granularity: Granularity,
    dimensions: Array[String],
    filter: QueryFilter = QueryFilter.All,
    limit: PagingSpec = PagingSpec(null, 20)) {
  val g: JValue = granularity match {
    case SimpleGranularity(name) => name
    case p: PeriodGranularity => p.toJson
    case d: DurationGranularity => d.toJson
  }
  def toJson: JValue = {
    JObject(
      "queryType" -> "select",
      "dataSource" -> source,
      "granularity" -> g,
      "descending" -> descending,
      "dimensions" -> render(dimensions.toList),
      "intervals" -> Time.intervalToString(interval),
      "filter" -> filter.toJson,
      "pagingSpec" -> limit.toJson)
  }
}

case class SelectResponse(data: Seq[Map[String, Any]])

object SelectResponse {
  implicit val formats = org.json4s.DefaultFormats

  def parse(js: JValue): SelectResponse = {
    val jss = js \ "result" \ "events"
    jss match {
      case JArray(results) =>
        val data = results.map { r =>
          (r \ "event").asInstanceOf[JObject].values
        }
        SelectResponse(data)
      case JNothing =>
        SelectResponse(null)
      case err @ _ =>
        throw new IllegalArgumentException("Invalid select response: " + err)
    }
  }
}
