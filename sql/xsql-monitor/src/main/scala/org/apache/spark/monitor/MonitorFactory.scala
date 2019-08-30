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

import java.util.ServiceLoader

import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.alarm.Alarm
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.KVStore

object MonitorFactory {

  def create(
      monitorName: String,
      alarms: Seq[Alarm],
      appStore: KVStore,
      conf: SparkConf): Monitor = {
    val loader = Utils.getContextOrSparkClassLoader
    val serviceLoader = ServiceLoader.load(classOf[Monitor], loader)
    val MonitorClass = serviceLoader.asScala
      .filter(_.item.equals(MonitorItem.withName(monitorName)))
      .toList match {
      case head :: Nil =>
        head.getClass
      case _ =>
        throw new SparkException("error when instantiate spark.xsql.monitor.items")
    }
    MonitorClass.newInstance().bind(alarms).bind(appStore).bind(conf)
  }
}
