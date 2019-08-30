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

import org.joda.time.{DateTime, Interval}
import org.json4s.JsonAST._
import org.json4s.JsonDSL._

case class TimeSeriesQuery(
    source: String,
    interval: Interval,
    descending: String = "false",
    granularity: Granularity,
    aggregate: Seq[Aggregation],
    postAggregate: Seq[PostAggregation] = Nil,
    filter: QueryFilter = QueryFilter.All) {
  val g: JValue = granularity match {
    case SimpleGranularity(name) => name
    case p: PeriodGranularity => p.toJson
    case d: DurationGranularity => d.toJson
  }

  def toJson: JValue = {
    JObject(
      "queryType" -> "timeseries",
      "dataSource" -> source,
      "descending" -> descending,
      "granularity" -> g,
      "aggregations" -> aggregate.map(_.toJson),
      "postAggregations" -> postAggregate.map(_.toJson),
      "intervals" -> Time.intervalToString(interval),
      "filter" -> filter.toJson)
  }
}

case class TimeSeriesResponse(data: Seq[(DateTime, Map[String, Any])])

object TimeSeriesResponse {
  implicit val formats = org.json4s.DefaultFormats

  def parse(js: JValue): TimeSeriesResponse = {
    js match {
      case JArray(results) =>
        val data = results.map { r =>
          val time = Time.parse((r \ "timestamp").extract[String])
          val values = (r \ "result").asInstanceOf[JObject].values
          time -> values
        }
        TimeSeriesResponse(data)
      case JNothing =>
        TimeSeriesResponse(null)
      case err @ _ =>
        throw new IllegalArgumentException("Invalid time series response: " + err)
    }
  }
}
