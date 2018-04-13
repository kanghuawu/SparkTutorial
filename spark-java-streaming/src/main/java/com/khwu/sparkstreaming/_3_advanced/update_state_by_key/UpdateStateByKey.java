package com.khwu.sparkstreaming._3_advanced.update_state_by_key;

import com.khwu.sparkstreaming.util.Utilities;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;

import java.util.List;

public class UpdateStateByKey {

    public static void main(String[] args) throws InterruptedException {
        Utilities.setUpLogging();

        SparkConf conf = new SparkConf()
                .setAppName("UpdateStateByKey")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(5));

        ssc.checkpoint("checkpoint");

        Utilities.setUptTwitter();

        JavaPairDStream<String, Integer> lang = TwitterUtils.createStream(ssc)
                .filter(status -> status.getLang() != null)
                .mapToPair(status -> new Tuple2<>(status.getLang(), 1));

        lang.updateStateByKey((List<Integer> values, Optional<Integer> state) ->
                Optional.of(state.orElse(0) + values.stream().reduce(0, Integer::sum)))
                .foreachRDD(rdd ->
                                rdd.mapToPair(pair -> new Tuple2<>(pair._2, pair._1))
                                        .sortByKey(false)
                                        .mapToPair(pair -> new Tuple2<>(pair._2, pair._1))
                                        .foreach(pair -> System.out.println(pair._1 + " " + pair._2)));

        ssc.start();
        Thread.sleep(30_000);
        ssc.stop(true, true);
    }
}
