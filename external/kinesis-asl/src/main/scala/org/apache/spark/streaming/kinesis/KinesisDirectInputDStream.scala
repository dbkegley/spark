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
import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream.{DStreamCheckpointData, InputDStream}

import scala.collection.mutable
import scala.reflect.ClassTag

private[kinesis] class KinesisDirectInputDStream[T: ClassTag](
    _ssc: StreamingContext,
    val streamName: String,
    val endpointUrl: String,
    val regionName: String,
    val fromSeqNumbers: Map[String, String],
    val messageHandler: Record => T,
    val kinesisCreds: SparkAWSCredentials,
    val cloudWatchCreds: Option[SparkAWSCredentials]
) extends InputDStream[T](_ssc) with Logging {

  private[streaming] override def name: String = s"Kinesis direct stream [$id]"

  protected[streaming] override val checkpointData =
    new KinesisDirectInputDStreamCheckpointData

  override def start(): Unit = {}

  override def stop(): Unit = {}

  override def compute(validTime: Time): Option[KinesisRDD[T]] = {
    Some(KinesisRDD(context.sc, streamName, endpointUrl, regionName, fromSeqNumbers, validTime.milliseconds,
      messageHandler, kinesisCreds, cloudWatchCreds))
  }

  private[streaming]
  class KinesisDirectInputDStreamCheckpointData extends DStreamCheckpointData(this) {
    def batchForTime: mutable.HashMap[Time, DirectSequenceNumberRanges] = {
      data.asInstanceOf[mutable.HashMap[Time, DirectSequenceNumberRanges]]
    }

    override def update(time: Time): Unit = {
      batchForTime.clear()
      generatedRDDs.foreach { kv =>
        batchForTime += kv._1 -> kv._2.asInstanceOf[KinesisRDD[T]].seqNumRanges
      }
    }

    override def cleanup(time: Time): Unit = { }

    override def restore(): Unit = {
      batchForTime.toSeq.sortBy(_._1)(Time.ordering).foreach { case (time, seqNumRanges) =>
        logInfo(s"Restoring KinesiskRDD for time $time $seqNumRanges")

        val rdd : KinesisRDD[T] = new KinesisRDD(context.sc, streamName, endpointUrl, regionName,
          seqNumRanges, messageHandler, kinesisCreds, cloudWatchCreds)
        generatedRDDs += time -> rdd
      }
    }
  }
}

@InterfaceStability.Evolving
object KinesisDirectInputDStream {
  /**
    * Builder for [[KinesisDirectInputDStream]] instances.
    *
    * @since 2.3.0
    */
  @InterfaceStability.Evolving
  class Builder {
    // Required params
    private var streamingContext: Option[StreamingContext] = None
    private var streamName: Option[String] = None
    private var fromSeqNumbers: Option[Map[String, String]] = None

    // Params with defaults
    private var endpointUrl: Option[String] = None
    private var regionName: Option[String] = None
    private var kinesisCredsProvider: Option[SparkAWSCredentials] = None
    private var cloudWatchCredsProvider: Option[SparkAWSCredentials] = None

    /**
      * Sets the StreamingContext that will be used to construct the Kinesis DStream. This is a
      * required parameter.
      *
      * @param ssc [[StreamingContext]] used to construct Kinesis DStreams
      * @return Reference to this [[KinesisDirectInputDStream.Builder]]
      */
    def streamingContext(ssc: StreamingContext): Builder = {
      streamingContext = Option(ssc)
      this
    }

    /**
      * Sets the StreamingContext that will be used to construct the Kinesis DStream. This is a
      * required parameter.
      *
      * @param jssc [[JavaStreamingContext]] used to construct Kinesis DStreams
      * @return Reference to this [[KinesisDirectInputDStream.Builder]]
      */
    def streamingContext(jssc: JavaStreamingContext): Builder = {
      streamingContext = Option(jssc.ssc)
      this
    }

    /**
      * Sets the name of the Kinesis stream that the DStream will read from. This is a required
      * parameter.
      *
      * @param streamName Name of Kinesis stream that the DStream will read from
      * @return Reference to this [[KinesisDirectInputDStream.Builder]]
      */
    def streamName(streamName: String): Builder = {
      this.streamName = Option(streamName)
      this
    }

    /**
      * Sets the point to start reading from the Kinesis stream.  This is a required
      * parameter.
      *
      * @param fromSeqNumbers The shardId and starting seqNumber to begin processing
      * @return Reference to this [[KinesisDirectInputDStream.Builder]]
      */
    def fromSeqNumbers(fromSeqNumbers: Map[String, String]): Builder = {
      this.fromSeqNumbers = Option(fromSeqNumbers)
      this
    }

    /**
      * Sets the AWS Kinesis endpoint URL. Defaults to "https://kinesis.us-east-1.amazonaws.com" if
      * no custom value is specified
      *
      * @param url Kinesis endpoint URL to use
      * @return Reference to this [[KinesisDirectInputDStream.Builder]]
      */
    def endpointUrl(url: String): Builder = {
      endpointUrl = Option(url)
      this
    }

    /**
      * Sets the AWS region to construct clients for. Defaults to "us-east-1" if no custom value
      * is specified.
      *
      * @param regionName Name of AWS region to use (e.g. "us-west-2")
      * @return Reference to this [[KinesisDirectInputDStream.Builder]]
      */
    def regionName(regionName: String): Builder = {
      this.regionName = Option(regionName)
      this
    }

    /**
      * Sets the [[SparkAWSCredentials]] to use for authenticating to the AWS Kinesis
      * endpoint. Defaults to [[DefaultCredentialsProvider]] if no custom value is specified.
      *
      * @param credentials [[SparkAWSCredentials]] to use for Kinesis authentication
      */
    def kinesisCredentials(credentials: SparkAWSCredentials): Builder = {
      kinesisCredsProvider = Option(credentials)
      this
    }

    /**
      * Sets the [[SparkAWSCredentials]] to use for authenticating to the AWS CloudWatch
      * endpoint. Will use the same credentials used for AWS Kinesis if no custom value is set.
      *
      * @param credentials [[SparkAWSCredentials]] to use for CloudWatch authentication
      */
    def cloudWatchCredentials(credentials: SparkAWSCredentials): Builder = {
      cloudWatchCredsProvider = Option(credentials)
      this
    }

    /**
      * Create a new instance of [[KinesisDirectInputDStream]] with configured parameters and the provided
      * message handler.
      *
      * @param handler Function converting [[Record]] instances read by the KCL to DStream type [[T]]
      * @return Instance of [[KinesisDirectInputDStream]] constructed with configured parameters
      */
    def buildWithMessageHandler[T: ClassTag](handler: Record => T): KinesisDirectInputDStream[T] = {
      val ssc = getRequiredParam(streamingContext, "streamingContext")
      new KinesisDirectInputDStream(
        ssc,
        getRequiredParam(streamName, "streamName"),
        endpointUrl.getOrElse(DEFAULT_KINESIS_ENDPOINT_URL),
        regionName.getOrElse(DEFAULT_KINESIS_REGION_NAME),
        getRequiredParam(fromSeqNumbers, "fromSeqNumbers"),
        ssc.sc.clean(handler),
        kinesisCredsProvider.getOrElse(DefaultCredentials),
        cloudWatchCredsProvider)
    }

    /**
      * Create a new instance of [[KinesisDirectInputDStream]] with configured parameters and using the
      * default message handler, which returns [[Array[Byte]]].
      *
      * @return Instance of [[KinesisDirectInputDStream]] constructed with configured parameters
      */
    def build(): KinesisDirectInputDStream[Array[Byte]] = buildWithMessageHandler(defaultMessageHandler)

    private def getRequiredParam[T](param: Option[T], paramName: String): T = param.getOrElse {
      throw new IllegalArgumentException(s"No value provided for required parameter $paramName")
    }
  }

  /**
    * Creates a [[KinesisDirectInputDStream.Builder]] for constructing [[KinesisDirectInputDStream]] instances.
    *
    * @since 2.2.0
    *
    * @return [[KinesisDirectInputDStream.Builder]] instance
    */
  def builder: Builder = new Builder

  private[kinesis] def defaultMessageHandler(record: Record): Array[Byte] = {
    if (record == null) return null
    val byteBuffer = record.getData()
    val byteArray = new Array[Byte](byteBuffer.remaining())
    byteBuffer.get(byteArray)
    byteArray
  }

  private[kinesis] val DEFAULT_KINESIS_REGION_NAME: String = "us-east-1"
  private[kinesis] val DEFAULT_KINESIS_ENDPOINT_URL: String = "https://kinesis.us-east-1.amazonaws.com"
}