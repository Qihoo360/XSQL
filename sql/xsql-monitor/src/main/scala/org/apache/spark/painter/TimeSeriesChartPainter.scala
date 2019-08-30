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

import java.io.File
import java.util.Scanner

import org.jfree.chart.{ChartFactory, ChartUtils}
import org.jfree.data.time.{FixedMillisecond, TimeSeries, TimeSeriesCollection}
import org.jfree.data.xy.XYDataset

class TimeSeriesChartPainter(dataPath: String, picturePath: String)
  extends Painter(dataPath, picturePath) {

  def createDataset(): XYDataset = {
    fw.flush()
    fw.close()
    val dataset = new TimeSeriesCollection
    val timeSeries = new TimeSeries("default")
    val scaner = new Scanner(new File(dataPath))
    while (scaner.hasNext()) {
      val cols = scaner.next().split(",")
      timeSeries.addOrUpdate(new FixedMillisecond(cols(1).toLong), cols(0).toLong)
    }
    dataset.addSeries(timeSeries)
    dataset
  }

  def paint(
      width: Int,
      height: Int,
      chartTitle: String,
      categoryAxisLabel: String,
      valueAxisLabel: String): Unit = {
    val lineChart = ChartFactory.createTimeSeriesChart(
      chartTitle,
      categoryAxisLabel,
      valueAxisLabel,
      createDataset,
      false,
      false,
      false)
    ChartUtils.saveChartAsJPEG(new File(picturePath), lineChart, width, height)
  }
}
