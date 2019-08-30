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

import org.json4s.JsonAST.{JArray, JValue}

case class DataSourceScannerResponse(data: Seq[String])

object DataSourceScannerResponse {
  def parse(js: JValue): DataSourceScannerResponse = {
    js match {
      case JArray(datasources) =>
        val res = datasources.map(ds => ds.values.toString).sortWith(_ < _)
        DataSourceScannerResponse(res)
      case err @ _ =>
        throw new IllegalArgumentException("Invalid datasource response: " + err)
    }
  }

}
