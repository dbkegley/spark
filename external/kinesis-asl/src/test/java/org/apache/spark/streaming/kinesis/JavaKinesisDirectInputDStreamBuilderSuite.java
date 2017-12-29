package org.apache.spark.streaming.kinesis;

import org.apache.spark.streaming.LocalJavaStreamingContext;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class JavaKinesisDirectInputDStreamBuilderSuite extends LocalJavaStreamingContext {
    /**
     * Basic test to ensure that the KinesisDirectInputDStream.Builder interface is accessible from Java.
     */
    @Test
    public void testJavaKinesisDStreamBuilder() {
        String streamName = "a-very-nice-stream-name";
        String endpointUrl = "https://kinesis.us-west-2.amazonaws.com";
        String region = "us-west-2";
        Map<String,String> fromSeqNumbers = new HashMap<>();
        fromSeqNumbers.put("a", "1");

        KinesisDirectInputDStream<byte[]> kinesisDirectInputDStream = KinesisDirectInputDStream.builder()
                .streamingContext(ssc)
                .streamName(streamName)
                .endpointUrl(endpointUrl)
                .regionName(region)
                .fromSeqNumbers(fromSeqNumbers)
                .build();
        
        assert(kinesisDirectInputDStream.streamName().equals(streamName));
        assert(kinesisDirectInputDStream.endpointUrl().equals(endpointUrl));
        assert(kinesisDirectInputDStream.regionName().equals(region));

        // using .equals on the maps directly fails here because Builder converts to immutable map, compare unwrapped k/v instead
        assert(kinesisDirectInputDStream.fromSeqNumbers().get("a").get().equals(fromSeqNumbers.get("a")));
        ssc.stop();
    }
}
