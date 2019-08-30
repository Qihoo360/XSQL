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
package org.apache.spark.painter

import java.awt.Font
import java.io.{File, FileWriter}

import org.jfree.chart.{ChartFactory, StandardChartTheme}
import org.jfree.data.general.Dataset

abstract class Painter(dataPath: String, picturePath: String) {
  initialize()
  var fw: FileWriter = _

  def initialize(): Unit = {
    val dataFile = new File(dataPath)
    if (dataFile.exists()) {
      dataFile.delete()
    }
    fw = new FileWriter(dataPath, true)
    val standardChartTheme = new StandardChartTheme("CN")
    standardChartTheme.setExtraLargeFont(new Font("Monospaced", Font.BOLD, 20))
    standardChartTheme.setRegularFont(new Font("Monospaced", Font.PLAIN, 15))
    standardChartTheme.setLargeFont(new Font("Monospaced", Font.PLAIN, 15))
    ChartFactory.setChartTheme(standardChartTheme)
  }

  def addPoint(xAxis: Any, yAxis: Any): Unit = {
    fw.write(s"${xAxis},${yAxis}\n")
  }

  def addPoint(xAxis: Any, yAxis: Any, zAxis: Any): Unit = {
    fw.write(s"${xAxis},${yAxis},${zAxis}\n")
  }

  def createDataset(): Dataset

  def paint(
      width: Int,
      height: Int,
      chartTitle: String,
      categoryAxisLabel: String,
      valueAxisLabel: String): Unit
}
