package org.apache.spark.streaming.kinesis

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kinesis.KinesisTestUtils.defaultEndpointUrl
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.concurrent.Eventually

abstract class KinesisDirectStreamTests(aggregateTestData: Boolean) extends KinesisFunSuite
  with Eventually with BeforeAndAfter with BeforeAndAfterAll {

  private val batchDuration = Seconds(1)

  // Dummy parameters for API testing
  private val dummyEndpointUrl = defaultEndpointUrl
  private val dummyRegionName = KinesisTestUtils.getRegionNameByEndpoint(dummyEndpointUrl)
  private val dummyAWSAccessKey = "dummyAccessKey"
  private val dummyAWSSecretKey = "dummySecretKey"

  private var testUtils: KinesisTestUtils = null
  private var ssc: StreamingContext = null
  private var sc: SparkContext = null

  override def beforeAll(): Unit = {

    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("KinesisDirectStreamSuite") // Setting Spark app name to Kinesis app name

    sc = new SparkContext(conf)

    runIfTestsEnabled("Prepare KinesisTestUtils") {
      testUtils = new KPLBasedKinesisTestUtils()
      testUtils.createStream()
    }
  }

  override def afterAll(): Unit = {
    if (ssc != null) {
      ssc.stop()
    }
    if (sc != null) {
      sc.stop()
    }
    if (testUtils != null) {
      // Delete the AWS Kinesis stream
      testUtils.deleteStream()
    }
  }

  before {
    ssc = new StreamingContext(sc, batchDuration)
  }

  after {
    if (ssc != null) {
      ssc.stop(stopSparkContext = false)
      ssc = null
    }
  }

  test("RDD generation") {

  }
}
