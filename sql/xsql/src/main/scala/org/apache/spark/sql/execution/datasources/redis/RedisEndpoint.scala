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
package org.apache.spark.sql.execution.datasources.redis

import java.net.URI

import redis.clients.jedis.{Jedis, Protocol}
import redis.clients.util.JedisURIHelper

/**
 * RedisEndpoint represents a redis connection endpoint info: host, port, auth password
 * db number, and timeout
 *
 * @param host the redis host or ip
 * @param port the redis port
 * @param auth the authentication password
 * @param dbNum database number (should be avoided in general)
 */
case class RedisEndpoint(
    val host: String = Protocol.DEFAULT_HOST,
    val port: Int = Protocol.DEFAULT_PORT,
    val auth: String = null,
    val dbNum: Int = Protocol.DEFAULT_DATABASE,
    val timeout: Int = Protocol.DEFAULT_TIMEOUT)
  extends Serializable {

  /**
   * Constructor with Jedis URI
   *
   * @param uri connection URI in the form of redis://:$password@$host:$port/[dbnum]
   */
  def this(uri: URI) {
    this(
      uri.getHost,
      uri.getPort,
      JedisURIHelper.getPassword(uri),
      JedisURIHelper.getDBIndex(uri))
  }

  /**
   * Constructor with Jedis URI from String
   *
   * @param uri connection URI in the form of redis://:$password@$host:$port/[dbnum]
   */
  def this(uri: String) {
    this(URI.create(uri))
  }

  /**
   * Connect tries to open a connection to the redis endpoint,
   * optionally authenticating and selecting a db
   *
   * @return a new Jedis instance
   */
  def connect(): Jedis = {
    ConnectionPool.connect(this)
  }
}
