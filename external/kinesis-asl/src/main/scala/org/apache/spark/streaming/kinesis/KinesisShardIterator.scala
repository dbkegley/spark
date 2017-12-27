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
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord
import com.amazonaws.services.kinesis.model._
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.util.NextIterator

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

private[kinesis] case class DirectSequenceNumberRange(
  streamName: String,
  shardId: String,
  fromSeqNumber: String,
  toTimestamp: Long,
  toSeqNumber: Option[String],
  count: Option[Int])

/** Class representing an array of Kinesis sequence number ranges */
private[kinesis]
case class DirectSequenceNumberRanges(ranges: Seq[DirectSequenceNumberRange]) {
  def isEmpty(): Boolean = ranges.isEmpty

  def nonEmpty(): Boolean = ranges.nonEmpty

  override def toString(): String = ranges.mkString("DirectSequenceNumberRanges(", ", ", ")")
}

// TODO: Refactor common kinesis consumer code, duplicate for now
// TODO: Expose shardId and subsequence number of Record/UserRecord ?

/**
  * An iterator that return the Kinesis data based on the given range of sequence numbers.
  * Internally, it repeatedly fetches sets of records starting at fromSequenceNumber,
  * until toTimestamp is reached for a single shard
  */
private[kinesis]
class KinesisShardIterator(
  credentials: AWSCredentials,
  endpointUrl: String,
  regionId: String,
  range: DirectSequenceNumberRange,
  retryTimeoutMs: Int) extends NextIterator[Record] with Logging {

  val MAX_RETRIES = 3
  val MIN_RETRY_WAIT_TIME_MS = 100
  val MAX_GET_RECORDS_LIMIT = 10000

  private val client = new AmazonKinesisClient(credentials)
  private val streamName = range.streamName
  private val shardId = range.shardId
  private val toTimestamp = range.toTimestamp // batchTime

  private var toTimestampReached = false
  private var lastSeqNumber: String = null // lastSeqNumber received
  private var internalIterator: Iterator[Record] = null

  client.setEndpoint(endpointUrl)

  override protected def getNext(): Record = {
    var nextRecord: Record = null

    if (toTimestampReached) {
      finished = true
    } else {

      if (internalIterator == null) {

        // If the internal iterator has not been initialized,
        // then fetch records from starting sequence number
        internalIterator = getRecords(ShardIteratorType.AT_SEQUENCE_NUMBER, range.fromSeqNumber, MAX_GET_RECORDS_LIMIT)
      } else if (!internalIterator.hasNext) {

        // If the internal iterator does not have any more records,
        // then fetch more records after the last consumed sequence number
        internalIterator = getRecords(ShardIteratorType.AFTER_SEQUENCE_NUMBER, lastSeqNumber, MAX_GET_RECORDS_LIMIT)
      }

      if (!internalIterator.hasNext) {

        // If the internal iterator still does not have any data, then throw exception
        // and terminate this iterator
        finished = true
        throw new SparkException(
          s"Could not read until the end sequence number of the range: $range")
      } else {

        // Get the record, copy the data into a byte array and remember its sequence number
        nextRecord = internalIterator.next()
        lastSeqNumber = nextRecord.getSequenceNumber()

        // If the this record's timestamp is greater than the batchTime, then make sure
        // the iterator is marked finished next time getNext() is called
        if (nextRecord.getApproximateArrivalTimestamp.getTime > toTimestamp) {
          toTimestampReached = true
        }
      }
    }
    nextRecord
  }

  override protected def close(): Unit = {
    client.shutdown()
  }

  /**
    * Get records starting from or after the given sequence number.
    */
  private def getRecords(iteratorType: ShardIteratorType, seqNum: String, recordCount: Int): Iterator[Record] = {
    val shardIterator = getKinesisIterator(iteratorType, seqNum)
    val result = getRecordsAndNextKinesisIterator(shardIterator)
    result._1
  }

  /**
    * Get the Kinesis shard iterator for getting records starting from or after the given
    * sequence number.
    */
  private def getKinesisIterator(
    iteratorType: ShardIteratorType,
    sequenceNumber: String): String = {

    val getShardIteratorRequest = new GetShardIteratorRequest
    getShardIteratorRequest.setRequestCredentials(credentials)
    getShardIteratorRequest.setStreamName(streamName)
    getShardIteratorRequest.setShardId(shardId)
    getShardIteratorRequest.setShardIteratorType(iteratorType.toString)
    getShardIteratorRequest.setStartingSequenceNumber(sequenceNumber)

    val getShardIteratorResult = retryOrTimeout[GetShardIteratorResult](
      s"getting shard iterator from sequence number $sequenceNumber", retryTimeoutMs) {
      client.getShardIterator(getShardIteratorRequest)
    }

    getShardIteratorResult.getShardIterator
  }

  /**
    * Get the records starting from using a Kinesis shard iterator (which is a progress handle
    * to get records from Kinesis), and get the next shard iterator for next consumption.
    */
  private def getRecordsAndNextKinesisIterator(shardIterator: String): (Iterator[Record], String) = {

    val getRecordsRequest = new GetRecordsRequest
    getRecordsRequest.setRequestCredentials(credentials)
    getRecordsRequest.setShardIterator(shardIterator)

    // todo: how to determine the number of records to request from kinesis [backpressure too]
    getRecordsRequest.setLimit(Math.min(100, MAX_GET_RECORDS_LIMIT))

    val getRecordsResult = retryOrTimeout[GetRecordsResult](
      s"getting records using shard iterator", retryTimeoutMs) {
      client.getRecords(getRecordsRequest)
    }

    val recordIterator = UserRecord.deaggregate(getRecordsResult.getRecords)
    (recordIterator.iterator().asScala, getRecordsResult.getNextShardIterator)
  }

  /** Helper method to retry Kinesis API request with exponential backoff and timeouts */
  private def retryOrTimeout[T](message: String, retryTimeoutMs: Int)(body: => T): T = {

    val startTimeMs = System.currentTimeMillis()
    var retryCount = 0
    var waitTimeMs = MIN_RETRY_WAIT_TIME_MS
    var result: Option[T] = None
    var lastError: Throwable = null

    def isTimedOut = (System.currentTimeMillis() - startTimeMs) >= retryTimeoutMs

    def isMaxRetryDone = retryCount >= MAX_RETRIES

    while (result.isEmpty && !isTimedOut && !isMaxRetryDone) {
      if (retryCount > 0) { // wait only if this is a retry
        Thread.sleep(waitTimeMs)
        waitTimeMs *= 2 // if you have waited, then double wait time for next round
      }
      try {
        result = Some(body)
      } catch {
        case NonFatal(t) =>
          lastError = t
          t match {
            case ptee: ProvisionedThroughputExceededException =>
              logWarning(s"Error while $message [attempt = ${retryCount + 1}]", ptee)
            case e: Throwable =>
              throw new SparkException(s"Error while $message", e)
          }
      }
      retryCount += 1
    }
    result.getOrElse {
      if (isTimedOut) {
        throw new SparkException(
          s"Timed out after $retryTimeoutMs ms while $message, last exception: ", lastError)
      } else {
        throw new SparkException(
          s"Gave up after $retryCount retries while $message, last exception: ", lastError)
      }
    }
  }
}

