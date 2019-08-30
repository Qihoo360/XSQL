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

import org.json4s.JsonAST._
import org.json4s.JsonDSL._

trait Granularity {
  val dtype: String
}

case class SimpleGranularity(name: String) extends Granularity {
  override val dtype: String = "Simple"
}

object SimpleGranularity {
  val All = SimpleGranularity("all")
  val Second = SimpleGranularity("second")
  val Minute = SimpleGranularity("minute")
  val FifteenMinute = SimpleGranularity("fifteen_minute")
  val ThirtyMinute = SimpleGranularity("thirty_minute")
  val Day = SimpleGranularity("day")
  val Hour = SimpleGranularity("hour")
  val Week = SimpleGranularity("week")
  val Month = SimpleGranularity("month")
  val Quarter = SimpleGranularity("quarter")
  val Year = SimpleGranularity("year")
  val None = SimpleGranularity("none")
}

case class PeriodGranularity(period: String, timeZone: String, origin: String)
  extends Granularity {
  override val dtype: String = "period"

  def toJson: JValue = {
    JObject(
      "type" -> "period",
      "period" -> (if (period != null) period else "P1D"),
      "timeZone" -> (if (timeZone != null) timeZone else "Asia/Shanghai"),
      "origin" -> (if (origin != null) origin else "1970-01-01T00:00:00Z"))
  }
}

case class DurationGranularity(duration: String, timeZone: String, origin: String)
  extends Granularity {
  override val dtype: String = "duration"

  def toJson: JValue = {
    JObject(
      "type" -> "duration",
      "duration" -> (if (duration != null) duration else "3600000"),
      "timeZone" -> (if (timeZone != null) timeZone else "Asia/Shanghai"),
      "origin" -> (if (origin != null) origin else "1970-01-01T00:00:00Z"))
  }
}
