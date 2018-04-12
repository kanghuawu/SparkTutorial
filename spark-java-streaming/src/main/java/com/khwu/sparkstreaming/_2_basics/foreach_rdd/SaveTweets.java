package com.khwu.sparkstreaming._2_basics.foreach_rdd;

import com.khwu.sparkstreaming.util.Utilities;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;

import java.util.concurrent.atomic.AtomicInteger;

public class SaveTweets {

    public static void main(String[] args) throws InterruptedException {
        Utilities.setUptTwitter();

        JavaStreamingContext ssc = new JavaStreamingContext("local[*]",
                "SaveTweets",
                Durations.seconds(1));

        ssc.checkpoint("checkpoint");
        Utilities.setUpLogging();

        JavaReceiverInputDStream<Status> statues = TwitterUtils.createStream(ssc);

        final AtomicInteger counter = new AtomicInteger(0);

        statues.repartition(1).foreachRDD(rdd -> {
            if (rdd.count() > 0) {
                rdd.map(Status::getUser).saveAsTextFile(String.format("output/savedTweets/user%d", counter.incrementAndGet()));
            }
        });

        ssc.start();
        Thread.sleep(5000);
        ssc.stop(true, true);
    }
}
