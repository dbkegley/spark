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

import com.amazonaws.services.kinesis.model.Record
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

private[kinesis]
class KinesisRDD[T](sc: SparkContext,
                    val endpointUrl: String,
                    val regionName: String,
                    val seqNumRanges: SequenceNumberRanges,
                    val messageHandler: Record => T,
                    val kinesisCreds: SparkAWSCredentials) extends RDD[T](sc, Nil) with Logging {

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    new KinesisSequenceRangeIterator(
      kinesisCreds.provider.getCredentials,
      endpointUrl,
      regionName,
      split.asInstanceOf[KinesisRDDPartition].seqNumberRange,
      1)
      .map(messageHandler)
  }

  override protected def getPartitions: Array[KinesisRDDPartition] = {
    seqNumRanges.ranges.zipWithIndex.map { case (range, i) =>
      new KinesisRDDPartition(i, range)
    }.toArray
  }
}

/** Partition storing the information of the ranges of Kinesis sequence numbers to read */
private[kinesis]
class KinesisRDDPartition(val index: Int, val seqNumberRange: SequenceNumberRange) extends Partition {

  /** Number of messages this partition refers to */
  def count(): Long = seqNumberRange.recordCount

}

