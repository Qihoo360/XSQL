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
package org.apache.spark.sql.xsql

import java.util.ServiceLoader

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.util.Utils

object DataSourceManagerFactory {

  def create(
      datasourceType: String,
      conf: SparkConf,
      hadoopConf: Configuration): DataSourceManager = {
    val loader = Utils.getContextOrSparkClassLoader
    val serviceLoader = ServiceLoader.load(classOf[DataSourceManager], loader)
    var cls: Class[_] = null
    // As we use ServiceLoader to support creating any user provided DataSourceManager here,
    // META-INF/services/org.apache.spark.sql.sources.DataSourceRegister must be packaged properly
    // in user's jar, and the implementation of DataSourceManager must have a public parameterless
    // constructor. For scala language, def this() = this(null...) just work.
    try {
      cls = serviceLoader.asScala
        .filter(_.shortName().equals(datasourceType))
        .toList match {
        case head :: Nil =>
          head.getClass
        case _ =>
          throw new SparkException(s"error when instantiate datasource ${datasourceType}")
      }
    } catch {
      case _: Exception =>
        throw new SparkException(
          s"""Can't find corresponding DataSourceManager for ${datasourceType} type,
             |please check
             |1. META-INF/services/org.apache.spark.sql.sources.DataSourceRegister is packaged
             |2. your implementation of DataSourceManager's shortname is ${datasourceType}
             |3. your implementation of DataSourceManager must have a public parameterless
             |   constructor. For scala language, def this() = this(null, null, ...) just work.
           """.stripMargin)
    }
    try {
      val constructor = cls.getConstructor(classOf[SparkConf], classOf[Configuration])
      val newHadoopConf = new Configuration(hadoopConf)
      constructor.newInstance(conf, newHadoopConf).asInstanceOf[DataSourceManager]
    } catch {
      case _: NoSuchMethodException =>
        try {
          cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[DataSourceManager]
        } catch {
          case _: NoSuchMethodException =>
            cls.getConstructor().newInstance().asInstanceOf[DataSourceManager]
        }
    }
  }
}
