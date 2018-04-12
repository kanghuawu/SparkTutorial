package com.khwu.sparkstreaming._2_basics.window;

import com.khwu.sparkstreaming.util.Utilities;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;

public class CountByValueAndWindowDemo {

    public static void main(String[] args) throws InterruptedException {
        Utilities.setUptTwitter();

        JavaStreamingContext ssc = new JavaStreamingContext("local[*]",
                "CountByValueAndWindowDemo",
                Durations.seconds(1));

        ssc.checkpoint("checkpoint");
        Utilities.setUpLogging();

        JavaReceiverInputDStream<Status> statues = TwitterUtils.createStream(ssc);

        statues.filter(status -> status.getHashtagEntities().length > 0)
                .map(status -> status.getHashtagEntities()[0].getText().length())
                .countByValueAndWindow(Durations.seconds(5), Durations.seconds(3))
                .print();

        ssc.start();
        Thread.sleep(30000);
        ssc.stop(true, true);
    }
}
