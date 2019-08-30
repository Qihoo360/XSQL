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
 *
 * File modified by Hortonworks, Inc. Modifications are also licensed under
 * the Apache Software License, Version 2.0.
 */

package org.apache.spark.sql.execution.datasources.hbase

import scala.language.implicitConversions

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._

case class CustomedRegionResource(relation: HBaseRelationTrait) extends ReferencedResource {
  // INLINE: SmartConnection is private[hbase], so we make a fake one here
  var connection: SmartConnection = _
  var rl: RegionLocator = _

  override def init(): Unit = {
    connection = HBaseConnectionCache.getConnection(relation.hbaseConf)
    rl = connection.getRegionLocator(
      TableName.valueOf(relation.catalog.namespace, relation.catalog.name))
  }

  override def destroy(): Unit = {
    if (rl != null) {
      rl.close()
      rl = null
    }
    if (connection != null) {
      connection.close()
      connection = null
    }
  }

  val regions = releaseOnException {
    val keys = rl.getStartEndKeys
    keys.getFirst
      .zip(keys.getSecond)
      .zipWithIndex
      .map(
        x =>
          CustomedHBaseRegion(
            x._2,
            Some(x._1._1),
            Some(x._1._2),
            Some(rl.getRegionLocation(x._1._1).getHostname)))
  }
}

case class CustomedTableResource(relation: HBaseRelationTrait) extends ReferencedResource {
  var connection: SmartConnection = _
  var table: Table = _

  override def init(): Unit = {
    connection = HBaseConnectionCache.getConnection(relation.hbaseConf)
    table =
      connection.getTable(TableName.valueOf(relation.catalog.namespace, relation.catalog.name))
  }

  override def destroy(): Unit = {
    if (table != null) {
      table.close()
      table = null
    }
    if (connection != null) {
      connection.close()
      connection = null
    }
  }

  def get(list: java.util.List[org.apache.hadoop.hbase.client.Get]): CustomedGetResource =
    releaseOnException {
      CustomedGetResource(this, table.get(list))
    }

  def getScanner(scan: Scan): CustomedScanResource = releaseOnException {
    CustomedScanResource(this, table.getScanner(scan))
  }
}
case class CustomedScanResource(tbr: CustomedTableResource, rs: ResultScanner) extends Resource {
  def release() {
    rs.close()
    tbr.release()
  }
}

case class CustomedGetResource(tbr: CustomedTableResource, rs: Array[Result]) extends Resource {
  def release() {
    tbr.release()
  }
}
