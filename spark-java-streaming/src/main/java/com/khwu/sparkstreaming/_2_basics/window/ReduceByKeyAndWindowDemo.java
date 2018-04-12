package com.khwu.sparkstreaming._2_basics.window;

import com.khwu.sparkstreaming.util.Utilities;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.Status;

public class ReduceByKeyAndWindowDemo {

    public static void main(String[] args) throws InterruptedException {
        Utilities.setUptTwitter();

        JavaStreamingContext ssc = new JavaStreamingContext("local[*]",
                "WindowOperation",
                Durations.seconds(1));

        ssc.checkpoint("checkpoint");
        Utilities.setUpLogging();

        JavaReceiverInputDStream<Status> statues = TwitterUtils.createStream(ssc);

        statues.filter(status -> status.getHashtagEntities().length > 0)
                .mapToPair(status -> new Tuple2<>(status.getHashtagEntities()[0].getText(), 1))
                .reduceByKeyAndWindow((x, y) -> x + y, Durations.seconds(5), Durations.seconds(1))
                .print();

        ssc.start();
        Thread.sleep(30000);
        ssc.stop(true, true);
    }
}
