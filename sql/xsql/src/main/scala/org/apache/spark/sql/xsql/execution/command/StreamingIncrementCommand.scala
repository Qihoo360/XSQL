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

package org.apache.spark.sql.xsql.execution.command

import java.util.Locale

import org.apache.spark.SparkException
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.streaming.StreamingRelationV2
import org.apache.spark.sql.sources.v2.StreamWriteSupport
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.xsql.DataSourceManager._
import org.apache.spark.sql.xsql.StreamingSinkType

/**
 * Used for structured streaming query.
 */
case class StreamingIncrementCommand(plan: LogicalPlan) extends RunnableCommand {

  private var outputMode: OutputMode = OutputMode.Append
  // dummy
  override def output: Seq[AttributeReference] = Seq.empty
  // dummy
  override def producedAttributes: AttributeSet = plan.producedAttributes

  override def run(sparkSession: SparkSession): Seq[Row] = {
    import StreamingSinkType._
    val qe = new QueryExecution(sparkSession, new ConstructedStreaming(plan))
    val df = new Dataset(sparkSession, qe, RowEncoder(qe.analyzed.schema))
    plan.collectLeaves.head match {
      case StreamingRelationV2(_, _, extraOptions, _, _) =>
        val source = extraOptions.getOrElse(STREAMING_SINK_TYPE, DEFAULT_STREAMING_SINK)
        val sinkOptions = extraOptions.filter(_._1.startsWith(STREAMING_SINK_PREFIX)).map { kv =>
          val key = kv._1.substring(STREAMING_SINK_PREFIX.length)
          (key, kv._2)
        }
        StreamingSinkType.withName(source.toUpperCase(Locale.ROOT)) match {
          case CONSOLE =>
          case TEXT | PARQUET | ORC | JSON | CSV =>
            if (sinkOptions.get(STREAMING_SINK_PATH) == None) {
              throw new SparkException("Sink type is file, must config path")
            }
          case KAFKA =>
            if (sinkOptions.get(STREAMING_SINK_BOOTSTRAP_SERVERS) == None) {
              throw new SparkException("Sink type is kafka, must config bootstrap servers")
            }
            if (sinkOptions.get(STREAMING_SINK_TOPIC) == None) {
              throw new SparkException("Sink type is kafka, must config kafka topic")
            }
          case _ =>
            throw new SparkException(
              "Sink type is invalid, " +
                s"select from ${StreamingSinkType.values}")
        }
        val ds = DataSource.lookupDataSource(source, sparkSession.sessionState.conf)
        val disabledSources = sparkSession.sqlContext.conf.disabledV2StreamingWriters.split(",")
        val sink = ds.newInstance() match {
          case w: StreamWriteSupport if !disabledSources.contains(w.getClass.getCanonicalName) =>
            w
          case _ =>
            val ds = DataSource(
              sparkSession,
              className = source,
              options = sinkOptions.toMap,
              partitionColumns = Nil)
            ds.createSink(InternalOutputModes.Append)
        }
        val outputMode = InternalOutputModes(
          extraOptions.getOrElse(STREAMING_OUTPUT_MODE, DEFAULT_STREAMING_OUTPUT_MODE))
        val duration =
          extraOptions.getOrElse(STREAMING_TRIGGER_DURATION, DEFAULT_STREAMING_TRIGGER_DURATION)
        val trigger =
          extraOptions.getOrElse(STREAMING_TRIGGER_TYPE, DEFAULT_STREAMING_TRIGGER_TYPE) match {
            case STREAMING_MICRO_BATCH_TRIGGER => Trigger.ProcessingTime(duration)
            case STREAMING_ONCE_TRIGGER => Trigger.Once()
            case STREAMING_CONTINUOUS_TRIGGER => Trigger.Continuous(duration)
          }
        val query = sparkSession.sessionState.streamingQueryManager.startQuery(
          extraOptions.get("queryName"),
          extraOptions.get(STREAMING_CHECKPOINT_LOCATION),
          df,
          sinkOptions.toMap,
          sink,
          outputMode,
          useTempCheckpointLocation = source == DEFAULT_STREAMING_SINK,
          recoverFromCheckpointLocation = true,
          trigger = trigger)
        query.awaitTermination()
    }
    // dummy
    Seq.empty
  }
}

case class ConstructedStreaming(child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}
