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

import org.scalatest.BeforeAndAfterEach
import org.scalatest.mockito.MockitoSugar

import org.apache.spark.streaming.{StreamingContext, TestSuiteBase}

class KinesisDirectInputDStreamBuilderSuite extends TestSuiteBase with BeforeAndAfterEach with MockitoSugar {
  import KinesisDirectInputDStream._

  private val streamName = "a-very-nice-kinesis-stream-name"
  private val fromSeqNumbers = Map("shard1" -> "000")

  private var ssc: StreamingContext = null
  private def baseBuilder = KinesisDirectInputDStream.builder
  private def builder = baseBuilder
    .streamingContext(ssc)
    .streamName(streamName)
    .fromSeqNumbers(fromSeqNumbers)

  override def beforeAll(): Unit = {
    ssc = new StreamingContext(conf, batchDuration)
  }

  override def afterAll(): Unit = {
    ssc.stop()
    super.afterAll()
  }

  test("should raise an exception if the StreamingContext is missing") {
    intercept[IllegalArgumentException] {
      baseBuilder.streamName(streamName).build()
    }
  }

  test("should raise an exception if the streamName is missing") {
    intercept[IllegalArgumentException] {
      baseBuilder.streamingContext(ssc).build()
    }
  }

  test("should raise an exception if fromSeqNumbers is missing") {
    intercept[IllegalArgumentException] {
      baseBuilder.streamName(streamName)
        .streamingContext(ssc)
        .build()
    }
  }

  test("should propagate required values to KinesisDirectInputDStream") {
    val dstream = builder.build()
    assert(dstream.context == ssc)
    assert(dstream.streamName == streamName)
    assert(dstream.fromSeqNumbers == fromSeqNumbers)
  }

  test("should propagate default values to KinesisDirectInputDStream") {
    val dstream = builder.build()
    assert(dstream.endpointUrl == DEFAULT_KINESIS_ENDPOINT_URL)
    assert(dstream.regionName == DEFAULT_KINESIS_REGION_NAME)
    assert(dstream.kinesisCreds == DefaultCredentials)
    assert(dstream.cloudWatchCreds.isEmpty)
  }

  test("should propagate custom non-auth values to KinesisDirectInputDStream") {
    val customEndpointUrl = "https://kinesis.us-west-2.amazonaws.com"
    val customRegion = "us-west-2"
    val customFromSeqNumbers = Map("a" -> "0")
    val customKinesisCreds = mock[SparkAWSCredentials]
    val customCloudWatchCreds = mock[SparkAWSCredentials]

    val dstream = builder
      .endpointUrl(customEndpointUrl)
      .regionName(customRegion)
      .fromSeqNumbers(customFromSeqNumbers)
      .kinesisCredentials(customKinesisCreds)
      .cloudWatchCredentials(customCloudWatchCreds)
      .build()

    assert(dstream.endpointUrl == customEndpointUrl)
    assert(dstream.regionName == customRegion)
    assert(dstream.fromSeqNumbers == customFromSeqNumbers)
    assert(dstream.kinesisCreds == customKinesisCreds)
    assert(dstream.cloudWatchCreds == Option(customCloudWatchCreds))
  }
}

