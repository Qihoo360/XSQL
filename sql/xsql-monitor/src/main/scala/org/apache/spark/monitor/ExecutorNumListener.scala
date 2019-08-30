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

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.util.concurrent.atomic.AtomicBoolean

import com.fasterxml.jackson.annotation.JsonIgnore

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{
  SparkListener,
  SparkListenerExecutorAdded,
  SparkListenerExecutorRemoved
}
import org.apache.spark.util.kvstore.KVIndex

class ExecutorNumListener extends SparkListener with Logging {

  lazy val kvstore = SparkContext.getActive.get.statusStore.store
  var initialized: AtomicBoolean = new AtomicBoolean(false)
  var lastPointTime: Long = new Date().getTime
  var recentEventTime: Long = new Date().getTime
  private val liveExecutors = new util.HashSet[String]()

  def initialize(): Unit = {
    SparkContext.getActive.map(_.ui).flatten.foreach {
      case ui =>
        ui.attachTab(new ExecutorNumTab(ui))
        ui.addStaticHandler("static", "/static/special")
    }
  }

  def maybeAddPoint(time: Long): Unit = {
    if (!initialized.get) {
      initialize()
      initialized.compareAndSet(false, true)
    }
    if (time - lastPointTime > 20 * 1000) {
      addPoint(recentEventTime)
      addPoint(time)
      lastPointTime = time
    }
    recentEventTime = time
  }
  def addPoint(time: Long): Unit = {
    val executorNum = liveExecutors.size
    kvstore.write(new ExecutorNumWrapper(new ExecutorNum(
      s"own ${executorNum} executors at ${new SimpleDateFormat("HH:mm:ss").format(new Date(time))}",
      IndexedSeq(time, executorNum))))
  }

  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit = {
    liveExecutors.add(event.executorId)
    maybeAddPoint(event.time)
  }

  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = {
    liveExecutors.remove(event.executorId)
    maybeAddPoint(event.time)
  }

}

private[spark] class ExecutorNumWrapper(val point: ExecutorNum) {
  @JsonIgnore @KVIndex
  def id: Long = point.value(0)
}

private[spark] class ExecutorNum(val name: String, val value: IndexedSeq[Long])
