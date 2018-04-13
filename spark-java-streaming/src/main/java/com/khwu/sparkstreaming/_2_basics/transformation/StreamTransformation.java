package com.khwu.sparkstreaming._2_basics.transformation;

import com.khwu.sparkstreaming.util.Utilities;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.Status;

public class StreamTransformation {

    public static void main(String[] args) throws InterruptedException {

        JavaStreamingContext ssc = new JavaStreamingContext("local[*]",
                "StreamingTransformation", Durations.seconds(1));

        Utilities.setUpLogging();

        JavaReceiverInputDStream<Status> statuses = TwitterUtils.createStream(ssc);

        statuses.filter(status -> status.getContributors().length < 3)
                .mapToPair(status -> new Tuple2<>(status.isRetweet(), status.getText().length()))
                .reduceByKey((x, y) -> (x + y) / 2)
                .print();

        ssc.start();
        ssc.awaitTermination();
    }
}
