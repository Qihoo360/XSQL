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

package org.apache.spark.monitor

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.ui.{SparkUI, SparkUITab, UIUtils, WebUIPage}

private class ExecutorNumTab(parent: SparkUI) extends SparkUITab(parent, "resources") {

  init()

  private def init(): Unit = {
    attachPage(new ExecutorNumPage(this))
  }

}

private class ExecutorNumPage(parent: SparkUITab) extends WebUIPage("") {

  def render(request: HttpServletRequest): Seq[Node] = {
    val content =
      <div>
        {
        <div id ="echart-container" class="row-fluid" style="height: 600px"></div> ++
        <script type="text/javascript"
                src="http://echarts.baidu.com/gallery/vendors/echarts/echarts.min.js"></script> ++
        <script src={UIUtils.prependBaseUri(
          request, "/static/special/executornumpage.js")}></script>
        }
      </div>

    UIUtils.headerSparkPage(request, "ExecutorNumCurve", content, parent, useDataTables = false)
  }
}
