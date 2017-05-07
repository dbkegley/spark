package org.apache.spark.streaming.kinesis

import com.amazonaws.services.kinesis.model.Record
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

/**
  * Created by davidkegley on 5/6/17.
  */
private[kinesis]
class KinesisRDD[T](sc: SparkContext,
                    val endpointUrl: String,
                    val regionName: String,
                    val seqNumRanges: SequenceNumberRanges,
                    val messageHandler: Record => T,
                    val kinesisCreds: SparkAWSCredentials) extends RDD[T](sc, Nil) with Logging {

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {

  }

  override protected def getPartitions: Array[KinesisRDDPartition] = {

  }
}

/** Partition storing the information of the ranges of Kinesis sequence numbers to read */
private[kinesis]
class KinesisRDDPartition(val index: Int, val seqNumberRange: SequenceNumberRange) extends Partition {

  /** Number of messages this partition refers to */
  def count(): Long = seqNumberRange.recordCount

}

