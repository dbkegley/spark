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

package org.apache.spark.streaming.kinesis

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.kinesis.model._
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

private[kinesis]
class KinesisRDD[T: ClassTag](
    sc: SparkContext,
    val streamName: String,
    val endpointUrl: String,
    val regionName: String,
    val seqNumRanges: DirectSequenceNumberRanges,
    val messageHandler: Record => T,
    val kinesisCreds: SparkAWSCredentials,
    val cloudWatchCreds: Option[SparkAWSCredentials]) extends RDD[T](sc, Nil) with Logging {

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    new KinesisShardIterator(
      kinesisCreds.provider.getCredentials,
      endpointUrl,
      regionName,
      split.asInstanceOf[KinesisRDDPartition].seqNumberRange,
      10000)
      .map(messageHandler)
  }

  override protected def getPartitions: Array[Partition] = {
    seqNumRanges.ranges.zipWithIndex.map { case (range, i) =>
      new KinesisRDDPartition(i, range)
    }.toArray
  }
}

private [kinesis]
object KinesisRDD {

  def apply[T: ClassTag](
    sc: SparkContext,
    streamName: String,
    endpointUrl: String,
    regionName: String,
    fromSeqNumbers: Map[String, String],
    batchTime: Long,
    messageHandler: Record => T,
    kinesisCreds: SparkAWSCredentials,
    cloudWatchCreds: Option[SparkAWSCredentials]): KinesisRDD[T] = {

    val credentials = kinesisCreds.provider.getCredentials
    val ranges = getInitialDirectSequenceNumberRanges(streamName, endpointUrl, fromSeqNumbers, batchTime, credentials)

    new KinesisRDD[T](sc, streamName, endpointUrl, regionName, ranges, messageHandler, kinesisCreds, cloudWatchCreds)
  }

  private def getInitialDirectSequenceNumberRanges(
    streamName: String,
    endpointUrl: String,
    fromSeqNumbers: Map[String, String],
    batchTime: Long,
    credentials: AWSCredentials): DirectSequenceNumberRanges = {

    DirectSequenceNumberRanges(fromSeqNumbers.map { case (shardId, fromSeqNumber) =>
      DirectSequenceNumberRange(streamName, shardId, fromSeqNumber, batchTime, None, None)
    }.toSeq)
  }
}

private[kinesis]
class KinesisRDDPartition(val index: Int, val seqNumberRange: DirectSequenceNumberRange) extends Partition {

   def count(): Long = {
     seqNumberRange.count match {
       case Some(count) => count
       case None => {
         // retrieve the count by creating a new iterator
         // new KinesisShardIterator().count
         0
       }
     }
   }

}
