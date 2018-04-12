package com.khwu.sparkstreaming._2_basics.transformation;

import com.khwu.sparkstreaming.util.Utilities;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.Status;

import java.util.Arrays;

public class Transformation {

    public static void main(String[] args) throws InterruptedException {

        Utilities.setUptTwitter();

        JavaStreamingContext ssc = new JavaStreamingContext("local[*]",
                "TransformOperation",
                Durations.seconds(10));

        Utilities.setUpLogging();

        JavaReceiverInputDStream<Status> tweets = TwitterUtils.createStream(ssc);

        JavaPairRDD<String, String> langNames = ssc.sparkContext()
                .parallelizePairs(Arrays.asList(new Tuple2<>("de", "German"),
                new Tuple2<>("en", "English"),
                new Tuple2<>("es", "Spanish"),
                new Tuple2<>("fr", "French")));

        tweets.mapToPair(tweet -> new Tuple2<>(tweet.getLang(), tweet.getText()))
                .transformToPair(pair -> pair.join(langNames))
                .print();

        ssc.start();
        ssc.awaitTermination();
    }
}
