package com.khwu.sparkstreaming._3_advanced.accumulators;

import com.khwu.sparkstreaming.util.Utilities;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.apache.spark.util.LongAccumulator;


public class Accumulators {

    public static void main(String[] args) throws InterruptedException {
        Utilities.setUpLogging();

        SparkConf conf = new SparkConf()
                .setAppName("Accumulators")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(2));
        LongAccumulator counter = sc.sc().longAccumulator();

        Utilities.setUptTwitter();

        TwitterUtils.createStream(ssc)
                .filter(status -> status.getUser().getLocation() != null)
                .foreachRDD(rdd -> {
                    counter.add(1);
                    rdd.collect();
                    System.out.println("Total count of user locations: " + counter.sum());
                });

        ssc.start();
        Thread.sleep(10_000);
        ssc.stop(true, true);
    }
}
